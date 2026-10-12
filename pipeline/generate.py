"""Generate Pushshift-style Reddit dumps with a controlled share of defective records.

The clean dataset is internally consistent (every comment points to a real
submission, subreddit and author). Defects are emitted as additional lines,
so the expected outcome of a pipeline run is known exactly and written to
manifest.json:

  loaded rows      == clean
  validation rejects, duplicates and integrity rejects match the counts per entity
"""

from __future__ import annotations

import gzip
import logging
import random
import time
from array import array
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO

import orjson
import zstandard

log = logging.getLogger(__name__)

# Clean record counts per scale. With the default 1% defect rate, "full" emits ~18.9M lines.
SCALES: dict[str, dict[str, int]] = {
    "tiny":   {"subreddits": 200,    "authors": 2_000,     "submissions": 5_000,     "comments": 20_000},
    "small":  {"subreddits": 1_000,  "authors": 18_000,    "submissions": 25_000,    "comments": 145_000},
    "medium": {"subreddits": 6_000,  "authors": 182_000,   "submissions": 248_000,   "comments": 1_435_000},
    "full":   {"subreddits": 60_000, "authors": 1_820_000, "submissions": 2_480_000, "comments": 14_350_000},
}

# Defect kind -> outcome category expected from the pipeline.
VALIDATION, INTEGRITY, DUPLICATE = "validation_rejects", "integrity_rejects", "duplicates"
DEFECTS: dict[str, dict[str, str]] = {
    "subreddits": {
        "malformed_json": VALIDATION, "missing_id": VALIDATION, "bad_timestamp": VALIDATION,
        "bad_number": VALIDATION, "bad_name": VALIDATION, "duplicate": DUPLICATE,
    },
    "authors": {
        "malformed_json": VALIDATION, "missing_name": VALIDATION, "bad_timestamp": VALIDATION,
        "bad_number": VALIDATION, "bad_name": VALIDATION, "duplicate": DUPLICATE,
    },
    "submissions": {
        "malformed_json": VALIDATION, "missing_id": VALIDATION, "bad_timestamp": VALIDATION,
        "bad_number": VALIDATION, "blank_title": VALIDATION, "negative_count": VALIDATION,
        "orphan_subreddit": INTEGRITY, "orphan_author": INTEGRITY, "duplicate": DUPLICATE,
    },
    "comments": {
        "malformed_json": VALIDATION, "missing_id": VALIDATION, "bad_timestamp": VALIDATION,
        "bad_number": VALIDATION, "bad_parent": VALIDATION, "orphan_submission": INTEGRITY,
        "orphan_author": INTEGRITY, "subreddit_mismatch": INTEGRITY, "duplicate": DUPLICATE,
    },
}

_WORDS = (
    "data python rust linux game music movie book science space history travel food coffee "
    "garden photo design art code server cloud network security privacy market stock bike "
    "running soccer chess puzzle robot camera phone laptop keyboard city river mountain ocean "
    "winter summer weekend project question advice update guide review idea story problem "
    "solution release version feature bug patch team player season episode trailer recipe "
    "budget career school college teacher student research paper theory model engine"
).split()
_ADJECTIVES = "quick lazy happy brave calm eager fancy jolly kind lucky proud silly witty bold keen".split()
_NOUNS = "otter falcon badger panda tiger koala raven lynx heron bison moose gecko walrus".split()
_OPENERS = ("What is", "How do I", "Why does", "Anyone else", "TIL", "Thoughts on", "Help with",
            "Best way to", "Showcase:", "Question about", "My first", "Update on")
_DOMAINS = ("i.redd.it", "youtube.com", "github.com", "imgur.com", "nytimes.com", "medium.com", "bbc.co.uk")
_TYPES = ("public", "public", "public", "restricted", "private")

_EPOCH_2008, _EPOCH_2016, _EPOCH_2024 = 1_199_145_600, 1_451_606_400, 1_704_067_200
_JUNK_ID_START = 36**8  # clean ids stay far below this, so junk ids never collide


def base36(n: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = []
    while True:
        n, r = divmod(n, 36)
        out.append(digits[r])
        if n == 0:
            return "".join(reversed(out))


class _Writer:
    def __init__(self, fh: BinaryIO, entity: str) -> None:
        self.fh = fh
        self.entity = entity
        self.lines = 0
        self.clean = 0
        self.defects: dict[str, int] = {}

    def record(self, rec: dict[str, Any]) -> None:
        self.fh.write(orjson.dumps(rec, option=orjson.OPT_APPEND_NEWLINE))
        self.lines += 1
        self.clean += 1

    def defect(self, kind: str, payload: bytes) -> None:
        self.fh.write(payload)
        self.fh.write(b"\n")
        self.lines += 1
        self.defects[kind] = self.defects.get(kind, 0) + 1

    def manifest(self, filename: str) -> dict[str, Any]:
        outcome = {VALIDATION: 0, INTEGRITY: 0, DUPLICATE: 0}
        for kind, count in self.defects.items():
            outcome[DEFECTS[self.entity][kind]] += count
        return {"file": filename, "lines": self.lines, "clean": self.clean, **outcome,
                "defects": dict(sorted(self.defects.items()))}


@contextmanager
def _open_output(path: Path, compress: str) -> Iterator[BinaryIO]:
    if compress == "gz":
        with gzip.open(path, "wb", compresslevel=3) as fh:
            yield fh
    elif compress == "zst":
        with path.open("wb") as raw, zstandard.ZstdCompressor(level=3).stream_writer(raw) as fh:
            yield fh
    else:
        with path.open("wb", buffering=1 << 20) as fh:
            yield fh


class Generator:
    def __init__(self, counts: dict[str, int], defect_rate: float, seed: int) -> None:
        if counts["subreddits"] < 2:
            raise ValueError("At least two subreddits are required")
        self.counts = counts
        self.defect_rate = defect_rate
        self.rng = random.Random(seed)
        self._junk = _JUNK_ID_START
        rng = self.rng
        self.titles = [
            f"{rng.choice(_OPENERS)} {' '.join(rng.choices(_WORDS, k=rng.randint(2, 8)))}"
            for _ in range(20_000)
        ]
        self.sentences = [
            " ".join(rng.choices(_WORDS, k=rng.randint(4, 16))).capitalize() + "."
            for _ in range(5_000)
        ]
        self.subreddit_ids = [base36(36**4 + i) for i in range(counts["subreddits"])]
        self.author_names = [
            f"{rng.choice(_ADJECTIVES).capitalize()}{rng.choice(_NOUNS).capitalize()}{base36(i)}"
            for i in range(counts["authors"])
        ]
        # Per-submission subreddit index and creation time, used to build consistent comments.
        self.sub_subreddit = array("l")
        self.sub_created = array("q")

    def _junk_id(self) -> str:
        self._junk += 1
        return base36(self._junk)

    def _skewed(self, n: int, power: float) -> int:
        # Heavy-tailed pick: a few popular subreddits/authors/submissions get most activity.
        return int(n * self.rng.random() ** power)

    def _author(self, deleted_share: float) -> str:
        if self.rng.random() < deleted_share:
            return "[deleted]"
        return self.author_names[self._skewed(len(self.author_names), 2.5)]

    def _text(self, low: int, high: int) -> str:
        return " ".join(self.rng.choices(self.sentences, k=self.rng.randint(low, high)))

    def _maybe_defect(self, out: _Writer, rec: dict[str, Any]) -> None:
        if self.rng.random() >= self.defect_rate:
            return
        kind = self.rng.choice(tuple(DEFECTS[out.entity]))
        out.defect(kind, self._make_defect(out.entity, kind, dict(rec)))

    def _make_defect(self, entity: str, kind: str, rec: dict[str, Any]) -> bytes:
        id_field = "name" if entity == "authors" else "id"
        if kind == "duplicate":
            # A later re-crawl of the same object: newer retrieval with a changed score.
            rec["retrieved_on"] += 3_600
            for field in ("score", "subscribers", "link_karma"):
                if field in rec:
                    rec[field] += 1
            return orjson.dumps(rec)

        rec[id_field] = self._junk_id() if entity != "authors" else f"junk_{self._junk_id()}"
        if kind == "malformed_json":
            encoded = orjson.dumps(rec)
            return encoded[: max(1, len(encoded) // 2)]
        if kind in ("missing_id", "missing_name"):
            del rec[id_field]
        elif kind == "bad_timestamp":
            rec["created_utc"] = self.rng.choice(["last tuesday", -86_400, 4_102_444_800])
        elif kind == "bad_number":
            field = {"subreddits": "subscribers", "authors": "link_karma"}.get(entity, "score")
            rec[field] = self.rng.choice(["n/a", "12k", 1.5])
        elif kind == "bad_name":
            rec["display_name" if entity == "subreddits" else "name"] = "not a valid name!"
        elif kind == "blank_title":
            rec["title"] = "   "
        elif kind == "negative_count":
            rec["num_comments"] = -self.rng.randint(1, 50)
        elif kind == "bad_parent":
            rec["parent_id"] = f"t9_{self._junk_id()}"
        elif kind == "orphan_subreddit":
            rec["subreddit_id"] = f"t5_{self._junk_id()}"
        elif kind == "orphan_author":
            rec["author"] = f"orphan_{self._junk_id()}"
        elif kind == "orphan_submission":
            rec["link_id"] = f"t3_{self._junk_id()}"
            rec["parent_id"] = rec["link_id"]
        elif kind == "subreddit_mismatch":
            rec["subreddit_id"] = self._other_subreddit(rec["subreddit_id"])
        else:
            raise ValueError(f"Unknown defect kind {kind!r}")
        return orjson.dumps(rec)

    def _other_subreddit(self, fullname: str) -> str:
        while True:
            candidate = f"t5_{self.rng.choice(self.subreddit_ids)}"
            if candidate != fullname:
                return candidate

    def subreddits(self, out: _Writer) -> None:
        rng = self.rng
        for sid in self.subreddit_ids:
            name = f"{rng.choice(_WORDS)}_{sid}"
            created = rng.randint(_EPOCH_2008, _EPOCH_2016)
            rec = {
                "id": sid, "name": f"t5_{sid}", "display_name": name,
                "title": f"{name.replace('_', ' ').title()} community",
                "public_description": self._text(1, 2),
                "subscribers": int(rng.paretovariate(1.2) * 50),
                "over18": rng.random() < 0.04,
                "subreddit_type": rng.choice(_TYPES),
                "created_utc": created, "retrieved_on": _EPOCH_2024 + rng.randint(0, 86_400 * 30),
            }
            out.record(rec)
            self._maybe_defect(out, rec)

    def authors(self, out: _Writer) -> None:
        rng = self.rng
        for i, name in enumerate(self.author_names):
            created = rng.randint(_EPOCH_2008, _EPOCH_2016)
            rec = {
                "id": base36(36**5 + i), "name": name,
                "link_karma": int(rng.paretovariate(1.1) * 10),
                "comment_karma": int(rng.paretovariate(1.1) * 25),
                "created_utc": created, "retrieved_on": _EPOCH_2024 + rng.randint(0, 86_400 * 30),
            }
            out.record(rec)
            self._maybe_defect(out, rec)

    def submissions(self, out: _Writer) -> None:
        rng = self.rng
        n_subreddits = len(self.subreddit_ids)
        for i in range(self.counts["submissions"]):
            sub_idx = self._skewed(n_subreddits, 3.0)
            created = rng.randint(_EPOCH_2016, _EPOCH_2024 - 86_400 * 30)
            is_self = rng.random() < 0.45
            domain = f"self.{self.subreddit_ids[sub_idx]}" if is_self else rng.choice(_DOMAINS)
            sid = base36(36**6 + i)
            rec = {
                "id": sid, "subreddit_id": f"t5_{self.subreddit_ids[sub_idx]}",
                "author": self._author(0.08), "title": rng.choice(self.titles),
                "selftext": self._text(1, 5) if is_self else "",
                "url": f"https://www.reddit.com/comments/{sid}/" if is_self else f"https://{domain}/{sid}",
                "domain": domain, "score": int(rng.paretovariate(1.3)) - 1,
                "num_comments": 0, "over_18": rng.random() < 0.03, "is_self": is_self,
                "created_utc": created, "retrieved_on": created + rng.randint(3_600, 86_400 * 3),
            }
            self.sub_subreddit.append(sub_idx)
            self.sub_created.append(created)
            out.record(rec)
            self._maybe_defect(out, rec)

    def comments(self, out: _Writer) -> None:
        rng = self.rng
        n_submissions = len(self.sub_created)
        last_comment = array("q", bytes(8 * n_submissions))  # latest comment index + 1, per submission
        for i in range(self.counts["comments"]):
            s = self._skewed(n_submissions, 1.8)
            link_id = f"t3_{base36(36**6 + s)}"
            prev = last_comment[s]
            parent = f"t1_{base36(36**7 + prev - 1)}" if prev and rng.random() < 0.55 else link_id
            created = self.sub_created[s] + int(rng.expovariate(1 / 7_200))
            rec = {
                "id": base36(36**7 + i), "link_id": link_id, "parent_id": parent,
                "subreddit_id": f"t5_{self.subreddit_ids[self.sub_subreddit[s]]}",
                "author": self._author(0.12), "body": self._text(1, 3),
                "score": int(rng.paretovariate(1.5)) - 1, "controversiality": int(rng.random() < 0.02),
                "created_utc": created, "retrieved_on": created + rng.randint(3_600, 86_400 * 3),
            }
            last_comment[s] = i + 1
            out.record(rec)
            self._maybe_defect(out, rec)


def generate(out_dir: Path, scale: str = "small", defect_rate: float = 0.01, seed: int = 42,
             compress: str = "none", counts: dict[str, int] | None = None) -> dict[str, Any]:
    counts = counts or SCALES[scale]
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = {"none": ".ndjson", "gz": ".ndjson.gz", "zst": ".ndjson.zst"}[compress]
    gen = Generator(counts, defect_rate, seed)
    manifest: dict[str, Any] = {"scale": scale, "seed": seed, "defect_rate": defect_rate, "entities": {}}

    started = time.perf_counter()
    for entity in ("subreddits", "authors", "submissions", "comments"):
        path = out_dir / f"{entity}{suffix}"
        for stale in out_dir.glob(f"{entity}.ndjson*"):
            stale.unlink()
        t0 = time.perf_counter()
        with _open_output(path, compress) as fh:
            writer = _Writer(fh, entity)
            getattr(gen, entity)(writer)
        manifest["entities"][entity] = writer.manifest(path.name)
        log.info("Wrote %s lines to %s in %.1fs", f"{writer.lines:,}", path, time.perf_counter() - t0)

    total = sum(e["lines"] for e in manifest["entities"].values())
    manifest["total_lines"] = total
    (out_dir / "manifest.json").write_bytes(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))
    log.info("Generated %s lines in %.1fs", f"{total:,}", time.perf_counter() - started)
    return manifest
