import io
import urllib.error

import orjson
import pytest

from pipeline import fetch


class FakeApi:
    """Serves rows in pages, treating `after` and `before` as exclusive timestamp bounds."""

    def __init__(self, rows, page_size=6):
        self.rows = rows
        self.page_size = page_size
        self.calls = []

    def __call__(self, endpoint, params):
        self.calls.append(params)
        if endpoint == "subreddits/search":
            return [{"display_name": "Example", "id": "abc", "created_utc": 50,
                     "_meta": {"num_posts": len(self.rows),
                               "num_comments": len(self.rows)}}]
        after, before = params["after"], params["before"]
        return [r for r in self.rows if after < r["created_utc"] < before][:self.page_size]


def rows_with_shared_seconds():
    # Page boundaries fall inside the runs of equal timestamps.
    stamps = [100, 100, 100, 100, 100, 101, 101, 105, 105, 105, 105, 106]
    return [{"id": f"r{i}", "created_utc": ts} for i, ts in enumerate(stamps)]


def ids(path):
    return [orjson.loads(line)["id"] for line in path.read_bytes().splitlines()]


def test_paging_keeps_every_row_once_when_seconds_span_pages(tmp_path):
    api = FakeApi(rows_with_shared_seconds())
    path = tmp_path / "comments.ndjson"
    assert fetch.fetch_stream(path, "comments/search", "Example", start=50, delay=0, request=api, until=200) == 12
    assert ids(path) == [f"r{i}" for i in range(12)]


def test_paging_crosses_empty_windows(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch, "WINDOW_SECONDS", 3)
    rows = [{"id": f"r{i}", "created_utc": ts} for i, ts in enumerate([60, 61, 90, 91, 92, 140])]
    path = tmp_path / "comments.ndjson"
    assert fetch.fetch_stream(path, "comments/search", "Example", start=50, delay=0,
                              request=FakeApi(rows, page_size=2), until=200) == 6
    assert ids(path) == [f"r{i}" for i in range(6)]


def test_resume_continues_and_trims_a_half_written_line(tmp_path):
    rows = rows_with_shared_seconds()
    path = tmp_path / "comments.ndjson"
    path.write_bytes(b"".join(orjson.dumps(r) + b"\n" for r in rows[:6]) + b'{"id": "r6", "crea')
    assert fetch.fetch_stream(path, "comments/search", "Example", start=50, delay=0,
                              request=FakeApi(rows), until=200) == 12
    assert ids(path) == [f"r{i}" for i in range(12)]


def test_fetch_subreddit_writes_pipeline_inputs(tmp_path):
    api = FakeApi(rows_with_shared_seconds())
    source = fetch.fetch_subreddit("example", tmp_path, delay=0, request=api)
    assert (source["posts"], source["comments"]) == (12, 12)
    assert {p.name for p in tmp_path.iterdir()} == {
        "subreddits.ndjson", "submissions.ndjson", "comments.ndjson", "source.json"}


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


def test_request_does_not_retry_a_bad_request(monkeypatch):
    def urlopen(request, timeout):
        raise http_error(400)

    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError):
        fetch._request("posts/search", {})
