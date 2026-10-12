"""Per-record validation for Pushshift-style Reddit records.

Each entity method returns a tuple in that entity's staging column order, or
raises Rejected with a short reason code. Checks that need other records
(duplicates, missing parents) happen later in SQL.
"""

from __future__ import annotations

import math
import re
import time
from typing import Any

_BASE36 = re.compile(r"[0-9a-z]{1,13}")
_SUBREDDIT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]{1,20}")
_AUTHOR_NAME = re.compile(r"[A-Za-z0-9_-]{3,20}")
_PARENT_ID = re.compile(r"t[13]_[0-9a-z]{1,13}")
_DELETED_AUTHORS = frozenset({"", "[deleted]", "[removed]"})

REDDIT_EPOCH = 1_119_484_800  # 2005-06-23, nothing on Reddit predates this
FUTURE_TOLERANCE_SECONDS = 86_400
INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1


class Rejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _require(record: dict, field: str) -> Any:
    value = record.get(field)
    if value is None:
        raise Rejected(f"missing_{field}")
    return value


def _base36(value: Any, field: str, prefix: str | None = None) -> str:
    if not isinstance(value, str):
        raise Rejected(f"invalid_{field}")
    if prefix and value.startswith(prefix):
        value = value[len(prefix):]
    if not _BASE36.fullmatch(value):
        raise Rejected(f"invalid_{field}")
    return value


def _int(value: Any, field: str, *, minimum: int = -INT32_MAX, maximum: int = INT32_MAX,
         default: int | None = None) -> int | None:
    if value is None:
        return default
    # bool is a subclass of int, so it has to be ruled out before the int branch
    if isinstance(value, bool):
        raise Rejected(f"invalid_{field}")
    if isinstance(value, float):
        if not value.is_integer():
            raise Rejected(f"invalid_{field}")
        value = int(value)
    elif isinstance(value, str):
        try:
            value = int(value)
        except ValueError:
            raise Rejected(f"invalid_{field}") from None
    elif not isinstance(value, int):
        raise Rejected(f"invalid_{field}")
    if not minimum <= value <= maximum:
        raise Rejected(f"out_of_range_{field}")
    return value


def _bool(value: Any, field: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise Rejected(f"invalid_{field}")


def _text(value: Any, field: str, *, max_length: int | None = None,
          required: bool = False) -> str | None:
    if value is None:
        if required:
            raise Rejected(f"missing_{field}")
        return None
    if not isinstance(value, str):
        raise Rejected(f"invalid_{field}")
    # PostgreSQL text can't store NUL, so strip it rather than reject the whole record
    value = value.replace("\x00", "")
    if required and not value.strip():
        raise Rejected(f"blank_{field}")
    if max_length is not None and len(value) > max_length:
        raise Rejected(f"too_long_{field}")
    return value


def _name(value: Any, field: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise Rejected(f"invalid_{field}")
    return value


def _author(value: Any) -> str | None:
    if value is None or (isinstance(value, str) and value in _DELETED_AUTHORS):
        return None
    return _name(value, "author", _AUTHOR_NAME)


class RecordValidator:
    # `now` is injectable so tests can pin the future-timestamp cutoff.
    def __init__(self, now: float | None = None) -> None:
        self._max_ts = int(now if now is not None else time.time()) + FUTURE_TOLERANCE_SECONDS

    def for_entity(self, name: str):
        return getattr(self, name)

    def _timestamp(self, value: Any, field: str) -> int:
        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                raise Rejected(f"invalid_{field}") from None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise Rejected(f"invalid_{field}")
        if isinstance(value, float) and not math.isfinite(value):
            raise Rejected(f"invalid_{field}")
        ts = int(value)
        if not REDDIT_EPOCH <= ts <= self._max_ts:
            raise Rejected(f"out_of_range_{field}")
        return ts

    def _retrieved(self, r: dict) -> int | None:
        value = r.get("retrieved_on", r.get("retrieved_utc"))
        return None if value is None else self._timestamp(value, "retrieved_on")

    def subreddits(self, r: dict) -> tuple:
        return (
            _base36(_require(r, "id"), "id", prefix="t5_"),
            _name(_require(r, "display_name"), "display_name", _SUBREDDIT_NAME),
            _text(r.get("title"), "title", max_length=500),
            _text(r.get("public_description"), "public_description", max_length=10_000),
            _int(r.get("subscribers"), "subscribers", minimum=0),
            _bool(r.get("over18"), "over18"),
            _text(r.get("subreddit_type"), "subreddit_type", max_length=32),
            self._timestamp(_require(r, "created_utc"), "created_utc"),
            self._retrieved(r),
        )

    def authors(self, r: dict) -> tuple:
        author_id = r.get("id")
        return (
            _name(_require(r, "name"), "name", _AUTHOR_NAME),
            None if author_id is None else _base36(author_id, "id", prefix="t2_"),
            _int(r.get("link_karma"), "link_karma", minimum=-INT64_MAX, maximum=INT64_MAX),
            _int(r.get("comment_karma"), "comment_karma", minimum=-INT64_MAX, maximum=INT64_MAX),
            self._timestamp(_require(r, "created_utc"), "created_utc"),
            self._retrieved(r),
        )

    def submissions(self, r: dict) -> tuple:
        return (
            _base36(_require(r, "id"), "id", prefix="t3_"),
            _base36(_require(r, "subreddit_id"), "subreddit_id", prefix="t5_"),
            _author(r.get("author")),
            _text(r.get("title"), "title", max_length=300, required=True),
            _text(r.get("selftext"), "selftext", max_length=40_000),
            _text(r.get("url"), "url", max_length=2_048),
            _text(r.get("domain"), "domain", max_length=253),
            _int(r.get("score"), "score", default=0),
            _int(r.get("num_comments"), "num_comments", minimum=0, default=0),
            _bool(r.get("over_18"), "over_18"),
            _bool(r.get("is_self"), "is_self"),
            self._timestamp(_require(r, "created_utc"), "created_utc"),
            self._retrieved(r),
        )

    def comments(self, r: dict) -> tuple:
        # parent_id keeps its t1_/t3_ prefix since that's what says whether it's a reply
        parent_id = _require(r, "parent_id")
        if not isinstance(parent_id, str) or not _PARENT_ID.fullmatch(parent_id):
            raise Rejected("invalid_parent_id")
        link_id = _require(r, "link_id")
        if not isinstance(link_id, str) or not link_id.startswith("t3_"):
            raise Rejected("invalid_link_id")
        return (
            _base36(_require(r, "id"), "id", prefix="t1_"),
            _base36(link_id, "link_id", prefix="t3_"),
            parent_id,
            _base36(_require(r, "subreddit_id"), "subreddit_id", prefix="t5_"),
            _author(r.get("author")),
            _text(r.get("body"), "body", max_length=40_000),
            _int(r.get("score"), "score", default=0),
            _int(r.get("controversiality"), "controversiality", minimum=0, maximum=1, default=0),
            self._timestamp(_require(r, "created_utc"), "created_utc"),
            self._retrieved(r),
        )
