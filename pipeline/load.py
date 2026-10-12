"""Load one entity in two steps.

stage(): validate each line in Python and stream valid rows into an UNLOGGED
staging table with a single COPY. Rejected lines are spooled to a temp file
and copied into etl.rejected_records afterwards (one connection can't run two
COPYs at once).

merge(): in SQL, find superseded duplicates and rows with missing parents,
record the latter as rejects, and upsert everything else into reddit.*.
"""

from __future__ import annotations

import csv
import io
import logging
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import orjson
import psycopg

from pipeline.entities import AUTHORS, COMMENTS, SUBMISSIONS, SUBREDDITS, Entity
from pipeline.extract import iter_lines
from pipeline.validate import RecordValidator, Rejected

log = logging.getLogger(__name__)

RAW_RECORD_MAX_CHARS = 4_000


@dataclass
class StageResult:
    records_read: int = 0
    records_staged: int = 0
    records_rejected_validation: int = 0
    seconds: float = 0.0


@dataclass
class MergeResult:
    duplicates: int = 0
    records_rejected_integrity: int = 0
    records_accepted: int = 0
    records_upserted: int = 0
    seconds: float = 0.0


def _csv_writer(buffer: io.TextIOBase) -> csv.writer:
    # QUOTE_NOTNULL quotes every value except None, so COPY ... (FORMAT csv)
    # reads unquoted empty fields as NULL and "" as an empty string.
    return csv.writer(buffer, quoting=csv.QUOTE_NOTNULL, lineterminator="\n")


def _raw_text(raw: bytes) -> str:
    return raw[: RAW_RECORD_MAX_CHARS * 4].decode("utf-8", "replace").replace("\x00", "")[:RAW_RECORD_MAX_CHARS]


def stage(conn: psycopg.Connection, entity: Entity, path: Path, run_id: int,
          validator: RecordValidator, *, batch_rows: int, progress_every: int) -> StageResult:
    result = StageResult()
    validate = validator.for_entity(entity.name)
    started = time.perf_counter()

    rows = io.StringIO()
    row_writer = _csv_writer(rows)
    pending = 0

    columns = ", ".join(entity.staging_columns)
    with (
        tempfile.SpooledTemporaryFile(max_size=64 << 20, mode="w+", newline="", encoding="utf-8") as rejects,
        conn.cursor() as cur,
    ):
        reject_writer = _csv_writer(rejects)
        cur.execute(f"TRUNCATE staging.{entity.name}")
        with cur.copy(f"COPY staging.{entity.name} ({columns}) FROM STDIN WITH (FORMAT csv)") as copy:
            for line_no, raw in iter_lines(path):
                result.records_read += 1
                if result.records_read % progress_every == 0:
                    log.info("%s: %s lines read, %s rejected", entity.name,
                             f"{result.records_read:,}", f"{result.records_rejected_validation:,}")
                try:
                    record = orjson.loads(raw)
                    if not isinstance(record, dict):
                        raise Rejected("not_an_object")
                    row = validate(record)
                except orjson.JSONDecodeError:
                    reason = "malformed_json"
                except Rejected as exc:
                    reason = exc.reason
                else:
                    row_writer.writerow((*row, line_no))
                    pending += 1
                    if pending >= batch_rows:
                        copy.write(rows.getvalue())
                        rows.seek(0)
                        rows.truncate()
                        result.records_staged += pending
                        pending = 0
                    continue

                result.records_rejected_validation += 1
                reject_writer.writerow((run_id, entity.name, line_no, reason, _raw_text(raw)))

            if pending:
                copy.write(rows.getvalue())
                result.records_staged += pending

        rejects.seek(0)
        with cur.copy("COPY etl.rejected_records (run_id, entity, source_line, reason, raw_record)"
                      " FROM STDIN WITH (FORMAT csv)") as copy:
            while chunk := rejects.read(1 << 20):
                copy.write(chunk)
    conn.commit()

    result.seconds = time.perf_counter() - started
    log.info("%s: staged %s of %s lines (%s invalid) in %.1fs", entity.name, f"{result.records_staged:,}",
             f"{result.records_read:,}", f"{result.records_rejected_validation:,}", result.seconds)
    return result


@dataclass(frozen=True)
class MergeSpec:
    columns: dict[str, str]  # core column -> expression over staging alias "s"
    # Parent checks: the staging columns they read, the joins, and a CASE naming
    # the first missing parent (NULL when the row is fine).
    reference_columns: tuple[str, ...] = ()
    reject_joins: str = ""
    reject_reason: str = "NULL::text"


def _core_columns(columns: tuple[str, ...]) -> dict[str, str]:
    mapped = {c: f"s.{c}" for c in columns if c not in ("created_utc", "retrieved_utc", "source_line")}
    mapped["created_at"] = "to_timestamp(s.created_utc)"
    mapped["retrieved_at"] = "to_timestamp(s.retrieved_utc)"
    return mapped


MERGE_SPECS: dict[str, MergeSpec] = {
    "subreddits": MergeSpec(columns=_core_columns(SUBREDDITS.staging_columns)),
    "authors": MergeSpec(columns=_core_columns(AUTHORS.staging_columns)),
    "submissions": MergeSpec(
        columns=_core_columns(SUBMISSIONS.staging_columns),
        reference_columns=("subreddit_id", "author_name"),
        reject_joins=(
            "LEFT JOIN reddit.subreddits r ON r.subreddit_id = d.subreddit_id "
            "LEFT JOIN reddit.authors a ON a.author_name = d.author_name"
        ),
        reject_reason=(
            "CASE WHEN r.subreddit_id IS NULL THEN 'orphan_subreddit' "
            "WHEN d.author_name IS NOT NULL AND a.author_name IS NULL THEN 'orphan_author' END"
        ),
    ),
    "comments": MergeSpec(
        columns=_core_columns(COMMENTS.staging_columns),
        reference_columns=("submission_id", "subreddit_id", "author_name"),
        reject_joins=(
            "LEFT JOIN reddit.submissions s ON s.submission_id = d.submission_id "
            "LEFT JOIN reddit.authors a ON a.author_name = d.author_name"
        ),
        reject_reason=(
            "CASE WHEN s.submission_id IS NULL THEN 'orphan_submission' "
            "WHEN s.subreddit_id <> d.subreddit_id THEN 'subreddit_mismatch' "
            "WHEN d.author_name IS NOT NULL AND a.author_name IS NULL THEN 'orphan_author' END"
        ),
    ),
}


def merge(conn: psycopg.Connection, entity: Entity, run_id: int, staged: int, *,
          bulk: bool = False) -> MergeResult:
    spec = MERGE_SPECS[entity.name]
    result = MergeResult()
    started = time.perf_counter()
    key = entity.key
    table = entity.name

    insert_columns = ", ".join(spec.columns)
    select_exprs = ", ".join(spec.columns.values())
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in spec.columns if c != key)
    ref_columns = "".join(f", {c}" for c in spec.reference_columns)

    with conn.cursor() as cur:
        # Collect only the rows that must NOT be loaded: superseded duplicates (latest
        # retrieval wins, ties go to the later line) and rows with a missing parent.
        # Working on this narrow set avoids sorting or copying full-width records.
        cur.execute(f"""
            CREATE TEMP TABLE merge_excluded ON COMMIT DROP AS
            SELECT source_line, reason FROM (
                SELECT d.source_line,
                       CASE WHEN d.rn > 1 THEN 'duplicate' ELSE {spec.reject_reason} END AS reason
                FROM (
                    SELECT {key}, source_line{ref_columns},
                           row_number() OVER (PARTITION BY {key}
                                              ORDER BY retrieved_utc DESC NULLS LAST, source_line DESC) AS rn
                    FROM staging.{table}
                ) d
                {spec.reject_joins}
            ) x
            WHERE reason IS NOT NULL
        """)
        cur.execute("ANALYZE merge_excluded")
        cur.execute("SELECT count(*) FILTER (WHERE reason = 'duplicate'),"
                    " count(*) FILTER (WHERE reason <> 'duplicate') FROM merge_excluded")
        result.duplicates, result.records_rejected_integrity = cur.fetchone()
        result.records_accepted = staged - result.duplicates - result.records_rejected_integrity

        cur.execute(
            f"INSERT INTO etl.rejected_records (run_id, entity, source_line, reason, raw_record) "
            f"SELECT %s, %s, s.source_line, e.reason, row_to_json(s)::text "
            f"FROM merge_excluded e JOIN staging.{table} s USING (source_line) "
            f"WHERE e.reason <> 'duplicate'",
            (run_id, table),
        )

        if bulk:
            # Truncating an empty target in this transaction gives it a new relfilenode,
            # which with wal_level=minimal lets PostgreSQL skip WAL for the insert below.
            cur.execute(f"SELECT NOT EXISTS (SELECT 1 FROM reddit.{table})")
            if cur.fetchone()[0]:
                cur.execute(f"TRUNCATE reddit.{table}")

        # Only overwrite an existing row with a strictly newer retrieval, so re-running
        # the same input is a no-op and older dumps never clobber newer data.
        cur.execute(f"""
            INSERT INTO reddit.{table} AS t ({insert_columns})
            SELECT {select_exprs}
            FROM staging.{table} s
            WHERE NOT EXISTS (SELECT 1 FROM merge_excluded e WHERE e.source_line = s.source_line)
            ON CONFLICT ({key}) DO UPDATE SET {updates}, loaded_at = now()
            WHERE EXCLUDED.retrieved_at > t.retrieved_at
               OR (t.retrieved_at IS NULL AND EXCLUDED.retrieved_at IS NOT NULL)
        """)
        result.records_upserted = cur.rowcount
        cur.execute(f"TRUNCATE staging.{table}")
    conn.commit()

    result.seconds = time.perf_counter() - started
    log.info("%s: merged in %.1fs - %s duplicates, %s integrity rejects, %s accepted, %s inserted/updated",
             entity.name, result.seconds, f"{result.duplicates:,}", f"{result.records_rejected_integrity:,}",
             f"{result.records_accepted:,}", f"{result.records_upserted:,}")
    return result
