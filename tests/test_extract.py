import gzip

import pytest
import zstandard

from pipeline.extract import find_source, iter_lines

LINES = [b'{"id": "a"}', b"", b'{"id": "b"}', b"  ", b'{"id": "c"}']
PAYLOAD = b"\n".join(LINES) + b"\n"


def _write(path, compression):
    if compression == "gz":
        with gzip.open(path, "wb") as fh:
            fh.write(PAYLOAD)
    elif compression == "zst":
        path.write_bytes(zstandard.ZstdCompressor().compress(PAYLOAD))
    else:
        path.write_bytes(PAYLOAD)


@pytest.mark.parametrize("suffix,compression", [
    (".ndjson", None), (".ndjson.gz", "gz"), (".ndjson.zst", "zst"),
])
def test_iter_lines_skips_blank_lines_and_keeps_line_numbers(tmp_path, suffix, compression):
    path = tmp_path / f"comments{suffix}"
    _write(path, compression)
    assert list(iter_lines(path)) == [(1, b'{"id": "a"}'), (3, b'{"id": "b"}'), (5, b'{"id": "c"}')]


def test_find_source(tmp_path):
    assert find_source(tmp_path, "authors") is None
    (tmp_path / "authors.jsonl.gz").write_bytes(b"")
    assert find_source(tmp_path, "authors") == tmp_path / "authors.jsonl.gz"
