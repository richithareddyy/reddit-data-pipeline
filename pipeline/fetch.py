"""Download one subreddit from the Arctic Shift API into the pipeline's input format.

The API is a free service meant for small pulls, so this keeps a delay between
requests and honours rate limits. For anything large, use the monthly dumps.
"""

from __future__ import annotations

import http.client
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import orjson

log = logging.getLogger(__name__)

API_URL = "https://arctic-shift.photon-reddit.com/api"
USER_AGENT = "reddit-data-pipeline"
MAX_ATTEMPTS = 8
MIN_COMPLETENESS = 0.99
OVERLOAD_WAIT = 10
WINDOW_SECONDS = 30 * 86_400


def _request(endpoint: str, params: dict) -> list[dict]:
    url = f"{API_URL}/{endpoint}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, MAX_ATTEMPTS + 1):
        wait = min(2**attempt, 60)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return orjson.loads(response.read())["data"]
        except urllib.error.HTTPError as exc:
            # Under load the API answers 422 "Timeout. Maybe slow down a bit", so back off further.
            if exc.code not in (422, 429) and exc.code < 500:
                raise
            if exc.code == 429:
                wait = int(exc.headers.get("X-RateLimit-Reset", wait)) + 1
            elif exc.code == 422:
                wait = max(wait, OVERLOAD_WAIT)
            log.warning("HTTP %d from %s, retrying in %ds: %s", exc.code, endpoint, wait,
                        exc.read(200).decode("utf-8", "replace"))
        # OSError covers URLError, timeouts and connections reset mid-read (e.g. after a laptop sleeps)
        except (OSError, http.client.HTTPException, orjson.JSONDecodeError) as exc:
            log.warning("%s from %s, retrying in %ds", type(exc).__name__, endpoint, wait)
        if attempt < MAX_ATTEMPTS:
            time.sleep(wait)
    raise RuntimeError(f"Giving up on {endpoint} after {MAX_ATTEMPTS} attempts")


def _recent(seen: dict[str, int], last: int) -> dict[str, int]:
    return {i: ts for i, ts in seen.items() if ts >= last - 1}


def _resume_point(path: Path) -> tuple[int, int | None, dict[str, int]]:
    """Return (rows kept, newest timestamp, recent id -> timestamp), trimming a half-written line."""
    if not path.exists():
        return 0, None, {}
    rows = valid_bytes = 0
    last = None
    seen: dict[str, int] = {}
    with path.open("rb") as fh:
        for line in fh:
            try:
                record = orjson.loads(line)
            except orjson.JSONDecodeError:
                break
            rows += 1
            valid_bytes += len(line)
            seen[record["id"]] = int(record["created_utc"])
            last = max(last or 0, seen[record["id"]])
    with path.open("r+b") as fh:
        fh.truncate(valid_bytes)
    return rows, last, _recent(seen, last) if last else {}


def fetch_stream(path: Path, endpoint: str, subreddit: str, *, start: int, delay: float,
                 request: Callable[[str, dict], list[dict]] = _request, until: int | None = None) -> int:
    rows, last, seen = _resume_point(path)
    # Open-ended `after` queries time out on the server once a subreddit has a lot of history,
    # so every request is bounded to a window. Several records can share a second and a page
    # can end mid-second, so the cursor steps back a second and already-seen ids are dropped.
    cursor = last - 1 if last is not None else start
    until = until or int(time.time())
    pages = 0
    with path.open("ab") as fh:
        while cursor <= until:
            params = {"subreddit": subreddit, "sort": "asc", "limit": "auto",
                      "after": cursor, "before": cursor + WINDOW_SECONDS}
            page = request(endpoint, params)
            new = [r for r in page if r["id"] not in seen]
            if not new:
                # Window exhausted: move past what this page showed, or to the next window. Both
                # bounds are exclusive, so windows overlap by a second to keep boundary records.
                cursor = max((int(r["created_utc"]) for r in page), default=cursor + WINDOW_SECONDS - 1)
                time.sleep(delay)
                continue
            fh.write(b"".join(orjson.dumps(r, option=orjson.OPT_APPEND_NEWLINE) for r in new))
            fh.flush()
            rows += len(new)
            seen.update({r["id"]: int(r["created_utc"]) for r in new})
            last = max(seen.values())
            seen = _recent(seen, last)
            cursor = last - 1
            pages += 1
            if pages % 50 == 0:
                log.info("%s: %s rows so far, up to %s", path.name, f"{rows:,}",
                         datetime.fromtimestamp(last, UTC).date())
            time.sleep(delay)
    return rows


def fetch_subreddit(subreddit: str, out_dir: Path, *, delay: float = 1.0,
                    request: Callable[[str, dict], list[dict]] = _request) -> dict:
    matches = request("subreddits/search", {"subreddit_prefix": subreddit, "limit": 100})
    info = next((s for s in matches if s["display_name"].lower() == subreddit.lower()), None)
    if info is None:
        raise ValueError(f"Subreddit {subreddit!r} not found")
    name = info["display_name"]
    meta = info.get("_meta", {})

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "subreddits.ndjson").write_bytes(orjson.dumps(info, option=orjson.OPT_APPEND_NEWLINE))
    start = int(info["created_utc"]) - 1  # nothing in a subreddit predates the subreddit
    posts = fetch_stream(out_dir / "submissions.ndjson", "posts/search", name, start=start,
                         delay=delay, request=request)
    comments = fetch_stream(out_dir / "comments.ndjson", "comments/search", name, start=start,
                            delay=delay, request=request)

    source = {
        "subreddit": name, "source": API_URL, "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "posts": posts, "comments": comments,
        "expected_posts": meta.get("num_posts"), "expected_comments": meta.get("num_comments"),
    }
    (out_dir / "source.json").write_text(json.dumps(source, indent=2) + "\n")
    for label, got, expected in (("posts", posts, meta.get("num_posts")),
                                 ("comments", comments, meta.get("num_comments"))):
        if expected and got < expected * MIN_COMPLETENESS:
            log.warning("%s: got %s but the API reports %s", label, f"{got:,}", f"{expected:,}")
    log.info("Fetched r/%s: %s posts, %s comments", name, f"{posts:,}", f"{comments:,}")
    return source
