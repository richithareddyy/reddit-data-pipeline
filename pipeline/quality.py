"""Post-load data quality checks.

Every check is a SQL query returning one number; it passes when the observed
value is at or below its threshold. Failed "error" checks fail the run,
failed "warning" checks are recorded but do not.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

from pipeline.config import Settings
from pipeline.constraints import FOREIGN_KEYS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    severity: str
    description: str
    sql: str
    threshold: float = 0.0


@dataclass(frozen=True)
class CheckResult:
    check: Check
    observed: float

    @property
    def passed(self) -> bool:
        return self.observed <= self.check.threshold


def build_checks(settings: Settings) -> list[Check]:
    fk_names = ", ".join(f"'{name}'" for _, name, _ in FOREIGN_KEYS)
    return [
        Check(
            "load_reconciliation", "error",
            "Entities where read != staged + invalid, or staged != duplicates + orphaned + accepted",
            """SELECT count(*) FROM etl.load_stats
               WHERE run_id = %(run_id)s
                 AND (records_read <> records_staged + records_rejected_validation
                      OR records_staged <> duplicates + records_rejected_integrity + records_accepted)""",
        ),
        Check(
            "core_row_coverage", "error",
            "Entities whose core table holds fewer rows than were accepted this run",
            """SELECT count(*) FROM etl.load_stats ls
               WHERE ls.run_id = %(run_id)s AND ls.records_accepted > CASE ls.entity
                   WHEN 'subreddits'  THEN (SELECT count(*) FROM reddit.subreddits)
                   WHEN 'authors'     THEN (SELECT count(*) FROM reddit.authors)
                   WHEN 'submissions' THEN (SELECT count(*) FROM reddit.submissions)
                   WHEN 'comments'    THEN (SELECT count(*) FROM reddit.comments) END""",
        ),
        Check(
            "rejection_rate", "error",
            "Share of input lines rejected by validation or referential checks",
            """SELECT COALESCE(sum(records_rejected_validation + records_rejected_integrity)::numeric
                               / NULLIF(sum(records_read), 0), 0)
               FROM etl.load_stats WHERE run_id = %(run_id)s""",
            settings.max_reject_rate,
        ),
        Check(
            "foreign_keys_enforced", "error",
            "Expected foreign keys that are missing or not validated",
            f"""SELECT {len(FOREIGN_KEYS)} - count(*) FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                WHERE n.nspname = 'reddit' AND c.contype = 'f' AND c.convalidated
                  AND c.conname IN ({fk_names})""",
        ),
        Check(
            "orphan_submissions", "error",
            "Submissions whose subreddit or author is missing",
            """SELECT count(*) FROM reddit.submissions s
               WHERE NOT EXISTS (SELECT 1 FROM reddit.subreddits r WHERE r.subreddit_id = s.subreddit_id)
                  OR (s.author_name IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM reddit.authors a WHERE a.author_name = s.author_name))""",
        ),
        Check(
            "orphan_comments", "error",
            "Comments whose submission or author is missing",
            """SELECT count(*) FROM reddit.comments c
               WHERE NOT EXISTS (SELECT 1 FROM reddit.submissions s WHERE s.submission_id = c.submission_id)
                  OR (c.author_name IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM reddit.authors a WHERE a.author_name = c.author_name))""",
        ),
        Check(
            "comment_subreddit_mismatch", "error",
            "Comments filed under a different subreddit than their submission",
            """SELECT count(*) FROM reddit.comments c
               JOIN reddit.submissions s ON s.submission_id = c.submission_id
               WHERE c.subreddit_id <> s.subreddit_id""",
        ),
        Check(
            "future_timestamps", "error",
            "Rows created more than one day in the future",
            """SELECT (SELECT count(*) FROM reddit.subreddits  WHERE created_at > now() + interval '1 day')
                    + (SELECT count(*) FROM reddit.authors     WHERE created_at > now() + interval '1 day')
                    + (SELECT count(*) FROM reddit.submissions WHERE created_at > now() + interval '1 day')
                    + (SELECT count(*) FROM reddit.comments    WHERE created_at > now() + interval '1 day')""",
        ),
        Check(
            "blank_submission_titles", "error",
            "Submissions with an empty or whitespace-only title",
            "SELECT count(*) FROM reddit.submissions WHERE btrim(title) = ''",
        ),
        Check(
            "orphan_parent_comments", "warning",
            "Share of replies whose parent comment is not in the dataset",
            """SELECT COALESCE(avg(CASE WHEN p.comment_id IS NULL THEN 1 ELSE 0 END), 0)
               FROM reddit.comments c
               LEFT JOIN reddit.comments p ON p.comment_id = substr(c.parent_id, 4)
               WHERE left(c.parent_id, 3) = 't1_'""",
            settings.max_orphan_parent_rate,
        ),
        Check(
            "comments_before_submission", "warning",
            "Comments timestamped before the submission they belong to",
            """SELECT count(*) FROM reddit.comments c
               JOIN reddit.submissions s ON s.submission_id = c.submission_id
               WHERE c.created_at < s.created_at""",
        ),
        Check(
            "duplicate_subreddit_names", "warning",
            "Subreddit names (case-insensitive) that map to more than one subreddit id",
            """SELECT count(*) FROM (
                   SELECT lower(name) FROM reddit.subreddits GROUP BY 1 HAVING count(*) > 1) d""",
        ),
        Check(
            "deleted_comment_author_rate", "warning",
            "Share of comments whose author is deleted or unknown",
            "SELECT COALESCE(avg(CASE WHEN author_name IS NULL THEN 1 ELSE 0 END), 0) FROM reddit.comments",
            0.5,
        ),
    ]


def run_checks(conn: psycopg.Connection, run_id: int, settings: Settings) -> list[CheckResult]:
    results: list[CheckResult] = []
    with conn.cursor() as cur:
        cur.execute("DELETE FROM etl.quality_results WHERE run_id = %s", (run_id,))
        for check in build_checks(settings):
            cur.execute(check.sql, {"run_id": run_id})
            observed = float(cur.fetchone()[0] or 0)
            result = CheckResult(check, observed)
            results.append(result)
            cur.execute(
                "INSERT INTO etl.quality_results"
                " (run_id, check_name, severity, passed, observed, threshold, description)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (run_id, check.name, check.severity, result.passed, observed, check.threshold, check.description),
            )
            level = logging.INFO if result.passed else (
                logging.ERROR if check.severity == "error" else logging.WARNING)
            log.log(level, "Quality check %-28s %s (observed %s, threshold %s)", check.name,
                    "PASS" if result.passed else "FAIL", _fmt(observed), _fmt(check.threshold))
    conn.commit()
    return results


def _fmt(value: float) -> str:
    return f"{value:,.0f}" if value == int(value) else f"{value:.4f}"
