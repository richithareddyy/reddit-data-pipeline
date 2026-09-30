"""Database connection and schema helpers."""

from __future__ import annotations

from pathlib import Path

import psycopg

from pipeline import constraints
from pipeline.config import Settings

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def connect(settings: Settings, *, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(settings.database_url, autocommit=autocommit, application_name="reddit-etl")


def configure_bulk_session(conn: psycopg.Connection, settings: Settings) -> None:
    """Session settings that favour load throughput over per-commit durability."""
    with conn.cursor() as cur:
        for name, value in (
            ("synchronous_commit", "off"),
            ("work_mem", settings.work_mem),
            ("maintenance_work_mem", settings.maintenance_work_mem),
        ):
            cur.execute("SELECT set_config(%s, %s, false)", (name, value))
    conn.commit()


# Arbitrary application-wide key for the advisory lock that serializes pipeline runs.
RUN_LOCK_KEY = 72_756_110_001


def try_acquire_run_lock(conn: psycopg.Connection) -> bool:
    """Session-level lock, released automatically when the connection closes."""
    acquired = conn.execute("SELECT pg_try_advisory_lock(%s)", (RUN_LOCK_KEY,)).fetchone()[0]
    conn.commit()
    return bool(acquired)


def apply_schema(conn: psycopg.Connection) -> None:
    """Create schemas and tables (idempotent), then ensure indexes and foreign keys exist."""
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
