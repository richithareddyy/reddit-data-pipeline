import orjson

from pipeline.generate import DEFECTS, SCALES, generate
from pipeline.validate import RecordValidator, Rejected


def test_manifest_matches_files(tmp_path):
    manifest = generate(tmp_path, scale="tiny", defect_rate=0.05, seed=7)
    for entity, info in manifest["entities"].items():
        lines = (tmp_path / info["file"]).read_bytes().splitlines()
        assert len(lines) == info["lines"]
        assert info["clean"] == SCALES["tiny"][entity]
        assert info["lines"] == info["clean"] + sum(info["defects"].values())
        assert set(info["defects"]) <= set(DEFECTS[entity])


def test_validation_rejects_match_manifest(tmp_path):
    manifest = generate(tmp_path, scale="tiny", defect_rate=0.05, seed=11)
    validator = RecordValidator()
    for entity, info in manifest["entities"].items():
        validate = validator.for_entity(entity)
        rejected = 0
        for line in (tmp_path / info["file"]).read_bytes().splitlines():
            try:
                validate(orjson.loads(line))
            except (orjson.JSONDecodeError, Rejected):
                rejected += 1
        assert rejected == info["validation_rejects"], entity


def test_generation_is_deterministic(tmp_path):
    a = generate(tmp_path / "a", scale="tiny", seed=3)
    b = generate(tmp_path / "b", scale="tiny", seed=3)
    assert a == b
    assert (tmp_path / "a" / "comments.ndjson").read_bytes() == (tmp_path / "b" / "comments.ndjson").read_bytes()
