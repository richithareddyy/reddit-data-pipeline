"""Locate and stream newline-delimited JSON dump files (plain, gzip or zstd)."""

from __future__ import annotations

import gzip
import io
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator

import zstandard

SUFFIXES = (".ndjson", ".jsonl", ".json", ".ndjson.gz", ".jsonl.gz", ".ndjson.zst", ".jsonl.zst", ".zst")

# Pushshift archives are compressed with long-distance matching and need a 2 GiB window.
_ZSTD_MAX_WINDOW = 2**31


def find_source(data_dir: Path, entity: str) -> Path | None:
    """Return the dump file for an entity, e.g. data/comments.ndjson.zst."""
    for suffix in SUFFIXES:
        candidate = data_dir / f"{entity}{suffix}"
        if candidate.is_file():
            return candidate
    return None


@contextmanager
def open_binary(path: Path) -> Iterator[BinaryIO]:
    name = path.name
    if name.endswith(".gz"):
        with gzip.open(path, "rb") as fh:
            yield fh
    elif name.endswith(".zst"):
        with path.open("rb") as raw:
            reader = zstandard.ZstdDecompressor(max_window_size=_ZSTD_MAX_WINDOW).stream_reader(raw)
            with io.BufferedReader(reader, buffer_size=1 << 20) as fh:
                yield fh
    else:
        with path.open("rb", buffering=1 << 20) as fh:
            yield fh


def iter_lines(path: Path) -> Iterator[tuple[int, bytes]]:
    """Yield (1-based line number, raw line) for every non-blank line."""
    with open_binary(path) as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if line:
                yield line_no, line
