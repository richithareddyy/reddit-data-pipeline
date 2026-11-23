"""Interrupt a load at each phase boundary, rerun it, and compare with an uninterrupted load.

Runs only when TEST_DATABASE_URL is set; it drops the pipeline schemas in that database.
"""

import os
from dataclasses import replace

import psycopg
import pytest

from pipeline import constraints, load, quality, runner
from pipeline.config import load_settings
from pipeline.generate import generate

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
TABLES = ("subreddits", "authors", "submissions", "comments")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is not set"),
]


class Kill(BaseException):
    """Not an Exception, so the runner's cleanup never sees it: the closest in-process
    stand-in for the process being killed (the connection closes and rolls back)."""


def _reset():
    with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS staging, reddit, etl CASCADE")


def _snapshot():
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        core = {
            t: conn.execute(
                f"SELECT count(*), md5(coalesce(string_agg(row, '|' ORDER BY row), '')) "
                f"FROM (SELECT (to_jsonb(x) - 'loaded_at')::text AS row FROM reddit.{t} x) r"
            ).fetchone()
            for t in TABLES
        }
        fks = conn.execute(
            "SELECT count(*) FROM pg_constraint"
            " WHERE contype = 'f' AND convalidated AND conname = ANY(%s)",
            ([name for _, name, _ in constraints.FOREIGN_KEYS],)).fetchone()[0]
        indexes = sum(
            conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"reddit.{name}",)).fetchone()[0]
            for name, _ in constraints.INDEXES)
        run_id, status = conn.execute(
            "SELECT run_id, status FROM etl.pipeline_runs ORDER BY run_id DESC LIMIT 1").fetchone()
        stats = conn.execute(
            "SELECT entity, records_read, records_rejected_validation, records_staged, duplicates,"
            " records_rejected_integrity, records_accepted FROM etl.load_stats"
            " WHERE run_id = %s ORDER BY entity",
            (run_id,)).fetchall()
        rejects = conn.execute(
            "SELECT entity, source_line, reason FROM etl.rejected_records"
            " WHERE run_id = %s ORDER BY 1, 2, 3",
            (run_id,)).fetchall()
        # Rows written across every run, failed ones included, must add up to one clean load.
        upserted = conn.execute(
            "SELECT entity, sum(records_upserted) FROM etl.load_stats"
            " GROUP BY entity ORDER BY entity").fetchall()
        checks_failed = conn.execute(
            "SELECT count(*) FROM etl.quality_results WHERE run_id = %s AND NOT passed",
            (run_id,)).fetchone()[0]
        leftover_running = conn.execute(
            "SELECT count(*) FROM etl.pipeline_runs WHERE status = 'running'").fetchone()[0]
    return {"core": core, "fks": fks, "indexes": indexes, "status": status, "stats": stats,
            "rejects": rejects, "upserted": upserted, "checks_failed": checks_failed,
            "leftover_running": leftover_running}


def _interrupt_once(monkeypatch, module, name, exc, when=lambda *a, **k: True, skip_calls=0):
    original = getattr(module, name)
    state = {"calls": 0, "fired": False}

    def wrapper(*args, **kwargs):
        state["calls"] += 1
        if not state["fired"] and state["calls"] > skip_calls and when(*args, **kwargs):
            state["fired"] = True
            raise exc
        return original(*args, **kwargs)

    monkeypatch.setattr(module, name, wrapper)
    return state


def _entity(name):
    return lambda conn, entity, *a, **k: entity.name == name


# (boundary, module, function, condition, calls to let through first)
BOUNDARIES = [
    ("after dropping constraints, before staging", load, "stage", _entity("subreddits"), 0),
    ("after staging submissions, before merging them", load, "merge", _entity("submissions"), 0),
    ("after merging submissions, before staging comments", load, "stage", _entity("comments"), 0),
    ("after staging comments, before merging them", load, "merge", _entity("comments"), 0),
    # the first ensure() call is apply_schema at start-up; the second rebuilds after the merges
    ("after all merges, before rebuilding constraints",
     constraints, "ensure", lambda *a, **k: True, 1),
    ("after rebuilding constraints, before quality checks",
     quality, "run_checks", lambda *a, **k: True, 0),
]


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    path = tmp_path_factory.mktemp("dump")
    generate(path, scale="tiny", defect_rate=0.03, seed=5)
    return path


@pytest.fixture(scope="module")
def baseline(dataset):
    _reset()
    settings = replace(load_settings(), database_url=TEST_DATABASE_URL)
    assert runner.run(settings, dataset).status == "succeeded"
    return _snapshot()


@pytest.mark.parametrize("kind", ["error", "kill"])
@pytest.mark.parametrize("boundary, module, function, when, skip", BOUNDARIES,
                         ids=[b[0] for b in BOUNDARIES])
def test_interrupted_load_converges_to_the_uninterrupted_result(
        dataset, baseline, monkeypatch, kind, boundary, module, function, when, skip):
    _reset()
    settings = replace(load_settings(), database_url=TEST_DATABASE_URL)
    exc = RuntimeError("injected failure") if kind == "error" else Kill()
    injected = _interrupt_once(monkeypatch, module, function, exc, when, skip)

    with pytest.raises(type(exc)):
        runner.run(settings, dataset)
    assert injected["fired"]

    outcome = runner.run(settings, dataset)
    assert outcome.status == "succeeded"
    after = _snapshot()

    assert after["core"] == baseline["core"]
    assert after["fks"] == len(constraints.FOREIGN_KEYS)
    assert after["indexes"] == len(constraints.INDEXES)
    assert after["stats"] == baseline["stats"]
    assert after["rejects"] == baseline["rejects"]
    assert after["upserted"] == baseline["upserted"]
    assert after["checks_failed"] == 0
    assert after["leftover_running"] == 0  # a killed run is marked failed by the next one
