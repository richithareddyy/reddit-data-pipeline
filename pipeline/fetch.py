"""Download one subreddit from the Arctic Shift API into the pipeline's input format.

The API is a free service meant for small pulls, so this keeps a delay between
requests and honours rate limits. For anything large, use the monthly dumps.

The search endpoints only page by `created_utc` (whole seconds, exclusive
`after`/`before`); there is no secondary cursor, and records that share a
second come back in a fixed but undocumented order, the same for asc and desc.
So if one second holds a full page of records, the rest of that second can't
be reached. That case is detected and the snapshot is marked incomplete
instead of being skipped silently.
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
from dataclasses import dataclass, field
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
# Explicit rather than "auto" (anywhere from 100 to 1000), so a full page is recognisable.
PAGE_LIMIT = 100


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
        # OSError covers URLError, timeouts and connections reset mid-read (e.g. after a laptop
        # sleeps)
        except (OSError, http.client.HTTPException, orjson.JSONDecodeError) as exc:
            log.warning("%s from %s, retrying in %ds", type(exc).__name__, endpoint, wait)
        if attempt < MAX_ATTEMPTS:
            time.sleep(wait)
    raise RuntimeError(f"Giving up on {endpoint} after {MAX_ATTEMPTS} attempts")


def _recent(seen: dict[str, int], last: int) -> dict[str, int]:
    return {i: ts for i, ts in seen.items() if ts >= last - 1}


def _resume_point(path: Path) -> tuple[int, int | None, dict[str, int]]:
    """Return (rows kept, newest timestamp, recent id -> timestamp).

    Trims a half-written last line.
    """
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


@dataclass
class StreamResult:
    rows: int
    # Seconds that returned a full page of records already seen: they hold at least PAGE_LIMIT
    # records, and any beyond the first PAGE_LIMIT could not be fetched.
    saturated: list[int] = field(default_factory=list)


def fetch_stream(path: Path, endpoint: str, subreddit: str, *, start: int, end: int, delay: float,
                 request: Callable[[str, dict], list[dict]] = _request) -> StreamResult:
    """Fetch records with start < created_utc <= end, appending to (and resuming from) path."""
    rows, last, seen = _resume_point(path)
    result = StreamResult(rows)
    # Open-ended queries time out on the server once a subreddit has a lot of history, so each
    # request covers at most one window. Both bounds are exclusive.
    cursor = start if last is None else max(start, last - 1)
    pages = 0
    with path.open("ab") as fh:
        while cursor < end:
            # At least two seconds wide: (cursor, cursor + 1) is empty and would never advance.
            before = min(cursor + max(WINDOW_SECONDS, 2), end + 1)
            page = request(endpoint, {"subreddit": subreddit, "sort": "asc", "limit": PAGE_LIMIT,
                                      "after": cursor, "before": before})
            new: dict[str, dict] = {}
            for r in page:  # the API can repeat a record; keep the first copy
                if r["id"] not in seen and r["id"] not in new:
                    new[r["id"]] = r
            if new:
                fh.write(b"".join(orjson.dumps(r, option=orjson.OPT_APPEND_NEWLINE)
                                  for r in new.values()))
                fh.flush()
                result.rows += len(new)
                seen.update({i: int(r["created_utc"]) for i, r in new.items()})
                last = max(seen.values())
                seen = _recent(seen, last)

            if len(page) < PAGE_LIMIT:
                # A short page means nothing else lies between the bounds.
                cursor = before - 1
            elif new:
                # More may follow, possibly in the same second as the last record: step back a
                # second and let the seen ids filter the overlap.
                cursor = last - 1
            else:
                # The same full page again, so a single second holds at least PAGE_LIMIT records.
                stuck = max(int(r["created_utc"]) for r in page)
                result.saturated.append(stuck)
                log.error("%s: second %d has at least %d records; any beyond those can't be "
                          "fetched through this API, so the snapshot is incomplete",
                          path.name, stuck, PAGE_LIMIT)
                cursor = stuck

            pages += 1
            if pages % 50 == 0 and last is not None:
                log.info("%s: %s rows so far, up to %s", path.name, f"{result.rows:,}",
                         datetime.fromtimestamp(last, UTC).date())
            time.sleep(delay)
    return result


def _write_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(path)


def fetch_subreddit(subreddit: str, out_dir: Path, *, delay: float = 1.0, until: int | None = None,
                    request: Callable[[str, dict], list[dict]] = _request) -> dict:
    """Download posts and comments created up to a fixed snapshot time.

    source.json records the snapshot end, so a resumed download keeps the same upper bound, and
    says whether the snapshot is complete.
    """
    state_path = out_dir / "source.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    if state:
        if state.get("subreddit", "").lower() != subreddit.lower():
            raise ValueError(f"{out_dir} holds r/{state.get('subreddit')}, not r/{subreddit}")
        if "snapshot_end" not in state:
            raise ValueError(f"{out_dir} predates snapshot tracking; download into a new directory")
        if until is not None and until != state["snapshot_end"]:
            raise ValueError(f"{out_dir} is a snapshot up to {state['snapshot_end']}; "
                             "download a different end time into a new directory")
    snapshot_end = state.get("snapshot_end") or until or int(time.time())

    matches = request("subreddits/search", {"subreddit_prefix": subreddit, "limit": 100})
    info = next((s for s in matches if s["display_name"].lower() == subreddit.lower()), None)
    if info is None:
        raise ValueError(f"Subreddit {subreddit!r} not found")
    name = info["display_name"]
    meta = info.get("_meta", {})

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "subreddits.ndjson").write_bytes(
        orjson.dumps(info, option=orjson.OPT_APPEND_NEWLINE))
    saturated = state.get("saturated_seconds", {"posts": [], "comments": []})
    state = {
        "subreddit": name, "source": API_URL, "page_limit": PAGE_LIMIT,
        "snapshot_end": snapshot_end,
        "snapshot_end_utc": datetime.fromtimestamp(snapshot_end, UTC).isoformat(timespec="seconds"),
        "status": "in_progress", "complete": False, "saturated_seconds": saturated,
    }
    _write_state(state_path, state)

    start = int(info["created_utc"]) - 1  # nothing in a subreddit predates the subreddit
    counts = {}
    for label, filename, endpoint in (("posts", "submissions.ndjson", "posts/search"),
                                      ("comments", "comments.ndjson", "comments/search")):
        stream = fetch_stream(out_dir / filename, endpoint, name, start=start, end=snapshot_end,
                              delay=delay, request=request)
        counts[label] = stream.rows
        saturated[label] = sorted(set(saturated[label]) | set(stream.saturated))
        _write_state(state_path, state)

    complete = not any(saturated.values())
    state.update({
        "status": "complete" if complete else "incomplete", "complete": complete,
        "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "posts": counts["posts"], "comments": counts["comments"],
        # Live counts from the API's subreddit metadata, not bounded by snapshot_end.
        "expected_posts": meta.get("num_posts"), "expected_comments": meta.get("num_comments"),
    })
    _write_state(state_path, state)
    for label, expected in (("posts", meta.get("num_posts")),
                            ("comments", meta.get("num_comments"))):
        if expected and counts[label] < expected * MIN_COMPLETENESS:
            log.warning("%s: got %s but the API reports %s", label, f"{counts[label]:,}",
                        f"{expected:,}")
    log.info("Fetched r/%s up to %s: %s posts, %s comments (%s)", name, state["snapshot_end_utc"],
             f"{counts['posts']:,}", f"{counts['comments']:,}", state["status"])
    return state
