"""End-to-end test against a real PostgreSQL database.

Runs only when TEST_DATABASE_URL is set. The test drops and recreates the
pipeline schemas in that database, so point it at a disposable database.
"""

import gzip
import os
from dataclasses import replace

import orjson
import psycopg
import pytest

from pipeline import db, load, runner
from pipeline.config import load_settings
from pipeline.constraints import FOREIGN_KEYS
from pipeline.generate import generate

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is not set"),
]


@pytest.fixture
def settings():
    with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS staging, reddit, etl CASCADE")
    return replace(load_settings(), database_url=TEST_DATABASE_URL, progress_every=10_000)


def _stats(run_id):
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        rows = conn.execute(
            "SELECT entity, records_read, records_rejected_validation, duplicates,"
            " records_rejected_integrity, records_accepted, records_upserted"
            " FROM etl.load_stats WHERE run_id = %s", (run_id,)).fetchall()
        counts = {t: conn.execute(f"SELECT count(*) FROM reddit.{t}").fetchone()[0]
                  for t in ("subreddits", "authors", "submissions", "comments")}
    return {r[0]: r[1:] for r in rows}, counts


def test_pipeline_loads_exactly_the_clean_records(tmp_path, settings):
    manifest = generate(tmp_path, scale="tiny", defect_rate=0.03, seed=5, compress="zst")

    outcome = runner.run(settings, tmp_path)
    assert outcome.status == "succeeded"
    assert all(r.passed for r in outcome.quality if r.check.severity == "error")

    stats, counts = _stats(outcome.run_id)
    for entity, expected in manifest["entities"].items():
        read, invalid, duplicates, orphaned, accepted, upserted = stats[entity]
        assert read == expected["lines"], entity
        assert invalid == expected["validation_rejects"], entity
        assert duplicates == expected["duplicates"], entity
        assert orphaned == expected["integrity_rejects"], entity
        assert accepted == upserted == counts[entity] == expected["clean"], entity


def test_rerun_is_idempotent(tmp_path, settings):
    manifest = generate(tmp_path, scale="tiny", defect_rate=0.02, seed=9)
    first = runner.run(settings, tmp_path)
    second = runner.run(settings, tmp_path)
    assert first.status == second.status == "succeeded"

    stats, counts = _stats(second.run_id)
    for entity, expected in manifest["entities"].items():
        assert counts[entity] == expected["clean"]
        assert stats[entity][-1] == 0, f"{entity} rows rewritten on identical re-run"

    with psycopg.connect(TEST_DATABASE_URL) as conn:
        modes = conn.execute("SELECT load_mode FROM etl.pipeline_runs ORDER BY run_id").fetchall()
    assert [m[0] for m in modes] == ["bulk", "incremental"]


def test_newer_retrieval_updates_existing_rows(tmp_path, settings):
    generate(tmp_path, scale="tiny", defect_rate=0.0, seed=1)
    runner.run(settings, tmp_path)

    path = tmp_path / "subreddits.ndjson"
    first = orjson.loads(path.read_bytes().splitlines()[0])
    first["subscribers"] = 999_999
    first["retrieved_on"] += 10
    path.write_bytes(orjson.dumps(first) + b"\n")
    for other in ("authors", "submissions", "comments"):
        (tmp_path / f"{other}.ndjson").unlink()

    outcome = runner.run(settings, tmp_path)
    assert outcome.status == "succeeded"
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        subscribers = conn.execute("SELECT subscribers FROM reddit.subreddits WHERE subreddit_id = %s",
                                   (first["id"],)).fetchone()[0]
    assert subscribers == 999_999


def _fetch(sql, params=()):
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        return conn.execute(sql, params).fetchall()


def test_pushshift_field_quirks(tmp_path, settings):
    """Prefixed ids, string timestamps, NUL bytes, deleted authors, bad UTF-8 and gzip input."""
    def write(entity, lines):
        with gzip.open(tmp_path / f"{entity}.ndjson.gz", "wb") as fh:
            fh.write(b"\n".join(lines) + b"\n")

    write("subreddits", [orjson.dumps({"id": "t5_2qh1i", "display_name": "AskReddit",
                                       "created_utc": "1201233135"})])
    write("authors", [orjson.dumps({"name": "some_user", "id": "t2_abc", "created_utc": 1_300_000_000})])
    write("submissions", [orjson.dumps({"id": "t3_p1", "subreddit_id": "t5_2qh1i", "author": "some_user",
                                        "title": "Hello", "created_utc": 1_600_000_000.0})])
    write("comments", [
        orjson.dumps({"id": "c1", "link_id": "t3_p1", "parent_id": "t3_p1", "subreddit_id": "t5_2qh1i",
                      "author": "[deleted]", "body": "nul\u0000byte", "created_utc": "1600000100"}),
        orjson.dumps({"id": "c2", "link_id": "t3_p1", "parent_id": "t1_c1", "subreddit_id": "t5_2qh1i",
                      "author": "some_user", "body": "emoji \U0001F600", "created_utc": 1_600_000_200}),
        b'{"id": "c3", "link_id": "t3_p1", "body": "lone \\ud800 surrogate"}',
        b'{"id": "c4", "link_id": "t3_p1", "body": "bad \xff bytes"}',
    ])

    # 2 of 7 lines are deliberately unparseable, above the default 5% rejection threshold.
    outcome = runner.run(replace(settings, max_reject_rate=0.5), tmp_path)
    assert outcome.status == "succeeded"
    assert _fetch("SELECT comment_id, author_name, body FROM reddit.comments ORDER BY 1") == [
        ("c1", None, "nulbyte"), ("c2", "some_user", "emoji \U0001F600")]
    assert _fetch("SELECT source_line, reason FROM etl.rejected_records ORDER BY 1") == [
        (3, "malformed_json"), (4, "malformed_json")]


def test_concurrent_run_is_refused(tmp_path, settings):
    generate(tmp_path, scale="tiny", seed=2)
    with psycopg.connect(TEST_DATABASE_URL) as holder:
        assert db.try_acquire_run_lock(holder)
        with pytest.raises(runner.RunInProgressError):
            runner.run(settings, tmp_path)
    assert runner.run(settings, tmp_path).status == "succeeded"


def test_interrupted_run_is_marked_failed(tmp_path, settings):
    generate(tmp_path, scale="tiny", seed=4)
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        db.apply_schema(conn)
        conn.execute("INSERT INTO etl.pipeline_runs (data_dir, load_mode) VALUES ('/old', 'bulk')")
    runner.run(settings, tmp_path)
    assert _fetch("SELECT status, error FROM etl.pipeline_runs ORDER BY run_id") == [
        ("failed", "Interrupted before completion"), ("succeeded", None)]


def test_failed_bulk_run_restores_constraints(tmp_path, settings, monkeypatch):
    generate(tmp_path, scale="tiny", seed=6)
    original_merge = load.merge

    def failing_merge(conn, entity, *args, **kwargs):
        if entity.name == "comments":
            raise RuntimeError("simulated failure")
        return original_merge(conn, entity, *args, **kwargs)

    monkeypatch.setattr(load, "merge", failing_merge)
    with pytest.raises(RuntimeError, match="simulated failure"):
        runner.run(settings, tmp_path)

    assert _fetch("SELECT status FROM etl.pipeline_runs") == [("failed",)]
    validated = _fetch("SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND convalidated"
                       " AND conname = ANY(%s)", ([name for _, name, _ in FOREIGN_KEYS],))
    assert validated == [(len(FOREIGN_KEYS),)]
