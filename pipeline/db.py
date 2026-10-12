from __future__ import annotations

from pathlib import Path

import psycopg

from pipeline import constraints
from pipeline.config import Settings

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

# Any constant works; it just has to be the same for every pipeline process.
RUN_LOCK_KEY = 72_756_110_001


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.database_url, application_name="reddit-etl")


def configure_bulk_session(conn: psycopg.Connection, settings: Settings) -> None:
    # synchronous_commit=off risks losing the last few commits on a crash, which is
    # fine here: a rerun reloads the same input idempotently.
    with conn.cursor() as cur:
        for name, value in (
            ("synchronous_commit", "off"),
            ("work_mem", settings.work_mem),
            ("maintenance_work_mem", settings.maintenance_work_mem),
        ):
            cur.execute("SELECT set_config(%s, %s, false)", (name, value))
    conn.commit()


def try_acquire_run_lock(conn: psycopg.Connection) -> bool:
    # Session-level, so it's released automatically if the process dies.
    acquired = conn.execute("SELECT pg_try_advisory_lock(%s)", (RUN_LOCK_KEY,)).fetchone()[0]
    conn.commit()
    return bool(acquired)


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_FILE.read_text())
    conn.commit()
    constraints.ensure(conn)


def core_tables_empty(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        "SELECT NOT EXISTS (SELECT 1 FROM reddit.subreddits)"
        " AND NOT EXISTS (SELECT 1 FROM reddit.authors)"
        " AND NOT EXISTS (SELECT 1 FROM reddit.submissions)"
        " AND NOT EXISTS (SELECT 1 FROM reddit.comments)"
    ).fetchone()
    return bool(row[0])
