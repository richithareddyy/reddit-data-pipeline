import io
import json
import random
import urllib.error

import orjson
import pytest

from pipeline import __main__ as cli
from pipeline import fetch


class FakeApi:
    """Mimics the search endpoints as probed: sorted by created_utc, whole-second exclusive
    after/before, records within a second in a fixed order, and at most `limit` per page."""

    def __init__(self, rows, *, repeat=False, fail_on=()):
        self.rows = sorted(rows, key=lambda r: r["created_utc"])  # stable: keeps tie order
        self.repeat = repeat
        self.fail_on = set(fail_on)
        self.calls = []

    def __call__(self, endpoint, params):
        self.calls.append(params)
        if len(self.calls) in self.fail_on:
            raise RuntimeError("Giving up after retries")
        if endpoint == "subreddits/search":
            return [{"display_name": "Example", "id": "abc", "created_utc": 50,
                     "_meta": {"num_posts": len(self.rows), "num_comments": len(self.rows)}}]
        hits = [r for r in self.rows if params["after"] < r["created_utc"] < params["before"]]
        if self.repeat:
            hits = [copy for r in hits for copy in (r, r)]
        return hits[:params["limit"]]


def make_rows(stamps):
    return [{"id": f"r{i}", "created_utc": ts} for i, ts in enumerate(stamps)]


def ids(path):
    return [orjson.loads(line)["id"] for line in path.read_bytes().splitlines()]


def stream(path, api, *, start=50, end=200):
    return fetch.fetch_stream(path, "comments/search", "Example", start=start, end=end, delay=0,
                              request=api)


@pytest.fixture
def small_pages(monkeypatch):
    monkeypatch.setattr(fetch, "PAGE_LIMIT", 3)


def test_reported_case_is_flagged_not_skipped_silently(tmp_path, monkeypatch):
    # Three records share second 100, pages hold two, bounds are exclusive.
    monkeypatch.setattr(fetch, "PAGE_LIMIT", 2)
    result = stream(tmp_path / "c.ndjson", FakeApi(make_rows([100, 100, 100])), start=99, end=101)
    assert result.rows == 2
    assert result.saturated == [100]
    assert ids(tmp_path / "c.ndjson") == ["r0", "r1"]


def test_same_case_completes_when_the_page_can_hold_the_second(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch, "PAGE_LIMIT", 4)
    result = stream(tmp_path / "c.ndjson", FakeApi(make_rows([100, 100, 100])), start=99, end=101)
    assert (result.rows, result.saturated) == (3, [])


def test_repeated_timestamps_straddling_pages(tmp_path, small_pages):
    rows = make_rows([100, 100, 101, 101, 101, 105, 105, 106, 106, 107])
    # 101 has three records, a full page: it is flagged even though nothing was lost there,
    # because a fourth record would have been unreachable.
    result = stream(tmp_path / "c.ndjson", FakeApi(rows))
    assert ids(tmp_path / "c.ndjson") == [r["id"] for r in rows]
    assert result.saturated == [101]


def test_runs_below_the_page_size_complete_exactly(tmp_path, small_pages):
    rows = make_rows([100, 100, 101, 101, 105, 105, 106, 107, 107, 150])
    result = stream(tmp_path / "c.ndjson", FakeApi(rows))
    assert ids(tmp_path / "c.ndjson") == [r["id"] for r in rows]
    assert result.saturated == []


def test_duplicate_records_from_the_api_are_written_once(tmp_path):
    rows = make_rows([60, 61, 61, 90, 140])
    result = stream(tmp_path / "c.ndjson", FakeApi(rows, repeat=True))
    assert ids(tmp_path / "c.ndjson") == [r["id"] for r in rows]
    assert result.rows == 5


def test_empty_windows_and_window_boundaries(tmp_path, monkeypatch, small_pages):
    monkeypatch.setattr(fetch, "WINDOW_SECONDS", 3)
    rows = make_rows([53, 56, 60, 61, 90, 91, 92, 140])  # several fall exactly on window edges
    result = stream(tmp_path / "c.ndjson", FakeApi(rows))
    assert ids(tmp_path / "c.ndjson") == [r["id"] for r in rows]
    assert result.saturated == []


def test_snapshot_upper_bound_is_enforced(tmp_path):
    rows = make_rows([60, 120, 150, 151, 300])
    api = FakeApi(rows)
    stream(tmp_path / "c.ndjson", api, end=150)
    assert ids(tmp_path / "c.ndjson") == ["r0", "r1", "r2"]  # 150 is included, 151 is not
    assert all(call["before"] <= 151 for call in api.calls)


def test_resume_after_a_failed_request_and_a_half_written_line(tmp_path, small_pages):
    rows = make_rows([100, 100, 101, 102, 102, 103, 150, 151, 152])
    path = tmp_path / "c.ndjson"
    with pytest.raises(RuntimeError):
        stream(path, FakeApi(rows, fail_on={3}))
    written = path.read_bytes()
    path.write_bytes(written + b'{"id": "r9", "crea')  # crash mid-write
    result = stream(path, FakeApi(rows))
    assert ids(path) == [r["id"] for r in rows]
    assert result.rows == len(rows)


def test_randomized_paging_never_loses_records_silently(tmp_path, monkeypatch):
    rng = random.Random(11)
    for trial in range(400):
        limit = rng.randint(2, 6)
        monkeypatch.setattr(fetch, "PAGE_LIMIT", limit)
        monkeypatch.setattr(fetch, "WINDOW_SECONDS", rng.randint(1, 40))
        hot = rng.randint(51, 199)
        stamps = [rng.choice([rng.randint(51, 200), hot]) for _ in range(rng.randint(0, 40))]
        rows = make_rows(sorted(stamps))
        path = tmp_path / f"t{trial}.ndjson"
        result = stream(path, FakeApi(rows, repeat=rng.random() < 0.3))

        got = ids(path)
        assert len(got) == len(set(got)), trial
        per_second = {}
        for r in rows:
            per_second.setdefault(r["created_utc"], []).append(r["id"])
        missing = {r["id"] for r in rows} - set(got)
        if not result.saturated:
            assert got == [r["id"] for r in rows], trial
        for r in rows:
            if r["id"] in missing:
                assert r["created_utc"] in result.saturated, trial


def test_fetch_subreddit_marks_snapshot_complete(tmp_path):
    source = fetch.fetch_subreddit("example", tmp_path, delay=0, until=200,
                                   request=FakeApi(make_rows([60, 61, 120])))
    assert (source["posts"], source["comments"]) == (3, 3)
    assert (source["complete"], source["status"]) == (True, "complete")
    assert source["snapshot_end"] == 200
    assert {p.name for p in tmp_path.iterdir()} == {
        "subreddits.ndjson", "submissions.ndjson", "comments.ndjson", "source.json"}


def test_fetch_subreddit_marks_saturated_snapshot_incomplete(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch, "PAGE_LIMIT", 2)
    source = fetch.fetch_subreddit("example", tmp_path, delay=0, until=200,
                                   request=FakeApi(make_rows([100, 100, 100, 120])))
    assert (source["complete"], source["status"]) == (False, "incomplete")
    assert source["saturated_seconds"] == {"posts": [100], "comments": [100]}
    assert json.loads((tmp_path / "source.json").read_text())["complete"] is False


def test_resume_keeps_the_original_snapshot_end(tmp_path):
    rows = make_rows([60, 61, 120])
    with pytest.raises(RuntimeError):
        fetch.fetch_subreddit("example", tmp_path, delay=0, until=150,
                              request=FakeApi(rows, fail_on={3}))
    assert json.loads((tmp_path / "source.json").read_text())["status"] == "in_progress"

    later = [*rows, {"id": "new", "created_utc": 170}]  # arrived after the snapshot
    source = fetch.fetch_subreddit("example", tmp_path, delay=0, request=FakeApi(later))
    assert source["snapshot_end"] == 150 and source["complete"]
    assert ids(tmp_path / "comments.ndjson") == ["r0", "r1", "r2"]


def test_saturation_survives_a_resume(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch, "PAGE_LIMIT", 2)
    rows = make_rows([100, 100, 100, 120])
    # Call 1 looks up the subreddit, calls 2-4 page the posts (saturating at 100), and the
    # first comments request, call 5, fails.
    first = FakeApi(rows, fail_on={5})
    with pytest.raises(RuntimeError):
        fetch.fetch_subreddit("example", tmp_path, delay=0, until=200, request=first)
    assert first.calls[-1]["after"] == 49  # it did fail on the first comments page
    source = fetch.fetch_subreddit("example", tmp_path, delay=0, request=FakeApi(rows))
    assert source["saturated_seconds"]["posts"] == [100]
    assert source["complete"] is False


def test_resume_refuses_a_different_snapshot_or_untracked_directory(tmp_path):
    fetch.fetch_subreddit("example", tmp_path, delay=0, until=150, request=FakeApi(make_rows([60])))
    with pytest.raises(ValueError, match="snapshot up to 150"):
        fetch.fetch_subreddit("example", tmp_path, delay=0, until=160,
                              request=FakeApi(make_rows([60])))
    (tmp_path / "source.json").write_text(json.dumps({"subreddit": "Example", "posts": 1}))
    with pytest.raises(ValueError, match="predates snapshot tracking"):
        fetch.fetch_subreddit("example", tmp_path, delay=0, request=FakeApi(make_rows([60])))


def test_cli_exits_nonzero_for_an_incomplete_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch, "fetch_subreddit", lambda *a, **k: {"complete": False})
    assert cli.main(["fetch", "--subreddit", "example", "--out", str(tmp_path)]) == 1
    monkeypatch.setattr(fetch, "fetch_subreddit", lambda *a, **k: {"complete": True})
    assert cli.main(["fetch", "--subreddit", "example", "--out", str(tmp_path)]) == 0


def test_unknown_subreddit_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="nope"):
        fetch.fetch_subreddit("nope", tmp_path, delay=0, request=lambda *_: [])


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code):
    return urllib.error.HTTPError("https://x", code, "error", {}, io.BytesIO(b"busy"))


@pytest.mark.parametrize("error", [http_error(422), http_error(429), http_error(503),
                                   ConnectionResetError(54, "reset"), TimeoutError()])
def test_request_retries_transient_errors(monkeypatch, error):
    outcomes = [error, FakeResponse(b'{"data": [{"id": "a"}]}')]

    def urlopen(request, timeout):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(fetch.time, "sleep", lambda _: None)
    assert fetch._request("posts/search", {}) == [{"id": "a"}]


def test_request_gives_up_after_repeated_failures(monkeypatch):
    def urlopen(request, timeout):
        raise http_error(422)

    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(fetch.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="Giving up"):
        fetch._request("posts/search", {})


def test_request_does_not_retry_a_bad_request(monkeypatch):
    def urlopen(request, timeout):
        raise http_error(400)

    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError):
        fetch._request("posts/search", {})
