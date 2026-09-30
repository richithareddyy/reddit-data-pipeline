"""Foreign keys and secondary indexes on the core tables.

They are defined here rather than in schema.sql so a bulk load can drop them,
load with only primary keys in place, and rebuild them afterwards. Re-adding a
foreign key validates every existing row in a single pass, which is much
cheaper than per-row checks during a multi-million row insert and still
guarantees referential integrity once the load finishes.
"""

from __future__ import annotations

import logging
import time

import psycopg

log = logging.getLogger(__name__)

# (table, constraint name, definition)
FOREIGN_KEYS: tuple[tuple[str, str, str], ...] = (
    ("submissions", "fk_submissions_subreddit",
     "FOREIGN KEY (subreddit_id) REFERENCES reddit.subreddits (subreddit_id)"),
    ("submissions", "fk_submissions_author",
     "FOREIGN KEY (author_name) REFERENCES reddit.authors (author_name)"),
    ("comments", "fk_comments_submission",
     "FOREIGN KEY (submission_id) REFERENCES reddit.submissions (submission_id)"),
    ("comments", "fk_comments_subreddit",
     "FOREIGN KEY (subreddit_id) REFERENCES reddit.subreddits (subreddit_id)"),
    ("comments", "fk_comments_author",
     "FOREIGN KEY (author_name) REFERENCES reddit.authors (author_name)"),
)

# (index name, definition)
INDEXES: tuple[tuple[str, str], ...] = (
    ("ix_subreddits_name", "reddit.subreddits (lower(name))"),
    ("ix_authors_author_id", "reddit.authors (author_id)"),
    ("ix_submissions_subreddit_created", "reddit.submissions (subreddit_id, created_at)"),
    ("ix_submissions_author", "reddit.submissions (author_name)"),
    ("ix_comments_submission", "reddit.comments (submission_id)"),
    ("ix_comments_author", "reddit.comments (author_name)"),
    ("ix_comments_subreddit_created", "reddit.comments (subreddit_id, created_at)"),
)


def drop(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for table, name, _ in FOREIGN_KEYS:
            cur.execute(f"ALTER TABLE reddit.{table} DROP CONSTRAINT IF EXISTS {name}")
        for name, _ in INDEXES:
            cur.execute(f"DROP INDEX IF EXISTS reddit.{name}")
    conn.commit()
    log.info("Dropped %d foreign keys and %d secondary indexes for bulk load",
             len(FOREIGN_KEYS), len(INDEXES))


def ensure(conn: psycopg.Connection) -> None:
    """Create any missing indexes and foreign keys. Adding a foreign key validates all rows."""
    with conn.cursor() as cur:
        for name, definition in INDEXES:
            if not _index_exists(cur, name):
                started = time.perf_counter()
                cur.execute(f"CREATE INDEX {name} ON {definition}")
                log.info("Built index %s in %.1fs", name, time.perf_counter() - started)
        for table, name, definition in FOREIGN_KEYS:
            if not _constraint_exists(cur, name):
                started = time.perf_counter()
                cur.execute(f"ALTER TABLE reddit.{table} ADD CONSTRAINT {name} {definition}")
                log.info("Added and validated %s in %.1fs", name, time.perf_counter() - started)
    conn.commit()


def _index_exists(cur: psycopg.Cursor, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"reddit.{name}",))
    return bool(cur.fetchone()[0])


def _constraint_exists(cur: psycopg.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_constraint c"
        " JOIN pg_namespace n ON n.oid = c.connamespace"
        " WHERE n.nspname = 'reddit' AND c.conname = %s)",
        (name,),
    )
    return bool(cur.fetchone()[0])
