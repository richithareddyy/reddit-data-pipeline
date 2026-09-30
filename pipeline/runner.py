"""End-to-end pipeline run: stage, merge, enforce constraints, check quality."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg

from pipeline import constraints, db, load, quality
from pipeline.config import Settings
from pipeline.entities import LOAD_ORDER
from pipeline.extract import find_source
from pipeline.validate import RecordValidator

log = logging.getLogger(__name__)


class RunInProgressError(RuntimeError):
    pass


@dataclass
class RunOutcome:
    run_id: int
    status: str
    seconds: float
    records_read: int
    records_accepted: int
    quality: list[quality.CheckResult]


def run(settings: Settings, data_dir: Path, mode: str = "auto") -> RunOutcome:
    sources = {e.name: find_source(data_dir, e.name) for e in LOAD_ORDER}
    if not any(sources.values()):
        raise FileNotFoundError(
            f"No input files found in {data_dir}. Expected files such as "
            "subreddits.ndjson, authors.ndjson, submissions.ndjson, comments.ndjson (optionally .gz or .zst)."
        )

    with db.connect(settings) as conn:
        # Staging tables and dropped constraints are shared state, so only one run may proceed.
        if not db.try_acquire_run_lock(conn):
            raise RunInProgressError("Another pipeline run holds the run lock; try again when it finishes.")
        db.apply_schema(conn)
        _fail_interrupted_runs(conn)
        db.configure_bulk_session(conn, settings)
        if mode == "auto":
            mode = "bulk" if db.core_tables_empty(conn) else "incremental"
        run_id = _start_run(conn, data_dir, mode)
        log.info("Run %d started (%s mode) from %s", run_id, mode, data_dir)

        started = time.perf_counter()
        totals = {"read": 0, "accepted": 0}
        try:
            if mode == "bulk":
                constraints.drop(conn)
            validator = RecordValidator()
            for entity in LOAD_ORDER:
                path = sources[entity.name]
                if path is None:
                    log.warning("%s: no input file found, skipping", entity.name)
                    continue
                staged = load.stage(conn, entity, path, run_id, validator,
                                    batch_rows=settings.copy_batch_rows,
                                    progress_every=settings.progress_every)
                merged = load.merge(conn, entity, run_id, staged.records_staged, bulk=mode == "bulk")
                _record_stats(conn, run_id, entity.name, path, staged, merged)
                totals["read"] += staged.records_read
                totals["accepted"] += merged.records_accepted
            if mode == "bulk":
                log.info("Rebuilding secondary indexes and validating foreign keys")
            constraints.ensure(conn)
            _analyze(conn)
            results = quality.run_checks(conn, run_id, settings)
        except Exception as exc:
            conn.rollback()
            _finish_run(conn, run_id, "failed", error=f"{type(exc).__name__}: {exc}")
            log.exception("Run %d failed", run_id)
            _restore_constraints(conn)
            raise

        failed = [r for r in results if not r.passed and r.check.severity == "error"]
        status = "quality_failed" if failed else "succeeded"
        _finish_run(conn, run_id, status)
        seconds = time.perf_counter() - started
        log.info("Run %d %s in %.1fs: %s lines read, %s records accepted (%s lines/s)",
                 run_id, status, seconds, f"{totals['read']:,}", f"{totals['accepted']:,}",
                 f"{totals['read'] / max(seconds, 1e-9):,.0f}")
        return RunOutcome(run_id, status, seconds, totals["read"], totals["accepted"], results)


def check_latest(settings: Settings) -> list[quality.CheckResult]:
    """Re-run quality checks against the most recent run."""
    with db.connect(settings) as conn:
        row = conn.execute("SELECT max(run_id) FROM etl.pipeline_runs").fetchone()
        if not row or row[0] is None:
            raise RuntimeError("No pipeline runs found; run the pipeline first.")
        return quality.run_checks(conn, row[0], settings)


def _fail_interrupted_runs(conn: psycopg.Connection) -> None:
    """Runs still marked 'running' while we hold the lock were killed before finishing."""
    cur = conn.execute(
        "UPDATE etl.pipeline_runs SET status = 'failed', finished_at = now(),"
        " error = 'Interrupted before completion' WHERE status = 'running'"
    )
    conn.commit()
    if cur.rowcount:
        log.warning("Marked %d interrupted run(s) as failed", cur.rowcount)


def _restore_constraints(conn: psycopg.Connection) -> None:
    """Best effort after a failure: leave the core tables with their indexes and foreign keys."""
    try:
        constraints.ensure(conn)
    except Exception:
        conn.rollback()
        log.exception("Could not restore constraints; they will be rebuilt on the next run")


def _start_run(conn: psycopg.Connection, data_dir: Path, mode: str) -> int:
    run_id = conn.execute(
        "INSERT INTO etl.pipeline_runs (data_dir, load_mode) VALUES (%s, %s) RETURNING run_id",
        (str(data_dir), mode),
    ).fetchone()[0]
    conn.commit()
    return run_id


def _finish_run(conn: psycopg.Connection, run_id: int, status: str, error: str | None = None) -> None:
    conn.execute(
        "UPDATE etl.pipeline_runs SET status = %s, finished_at = now(), error = %s WHERE run_id = %s",
        (status, error, run_id),
    )
    conn.commit()


def _record_stats(conn: psycopg.Connection, run_id: int, entity: str, path: Path,
                  staged: load.StageResult, merged: load.MergeResult) -> None:
    conn.execute(
        """INSERT INTO etl.load_stats (
               run_id, entity, source_file, records_read, records_rejected_validation, records_staged,
               duplicates, records_rejected_integrity, records_accepted, records_upserted,
               extract_seconds, merge_seconds)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (run_id, entity, path.name, staged.records_read, staged.records_rejected_validation,
         staged.records_staged, merged.duplicates, merged.records_rejected_integrity,
         merged.records_accepted, merged.records_upserted, round(staged.seconds, 3), round(merged.seconds, 3)),
    )
    conn.commit()


def _analyze(conn: psycopg.Connection) -> None:
    started = time.perf_counter()
    for entity in LOAD_ORDER:
        conn.execute(f"ANALYZE reddit.{entity.name}")
    conn.commit()
    log.info("Analyzed core tables in %.1fs", time.perf_counter() - started)
