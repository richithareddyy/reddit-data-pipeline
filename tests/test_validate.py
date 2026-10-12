import pytest

from pipeline.entities import BY_NAME
from pipeline.validate import RecordValidator, Rejected

NOW = 1_727_568_000  # 2024-09-29
V = RecordValidator(now=NOW)


def subreddit(**overrides):
    rec = {"id": "2qh1i", "display_name": "AskReddit", "title": "Ask Reddit", "subscribers": 100,
           "over18": False, "subreddit_type": "public", "created_utc": 1_201_233_135,
           "retrieved_on": 1_700_000_000}
    rec.update(overrides)
    return rec


def author(**overrides):
    rec = {"id": "t2_abc12", "name": "some_user", "link_karma": 10, "comment_karma": 20,
           "created_utc": 1_300_000_000}
    rec.update(overrides)
    return rec


def submission(**overrides):
    rec = {"id": "abc123", "subreddit_id": "t5_2qh1i", "author": "some_user", "title": "Hello",
           "selftext": "", "url": "https://example.com", "domain": "example.com", "score": 5,
           "num_comments": 2, "over_18": False, "is_self": False, "created_utc": 1_600_000_000,
           "retrieved_on": 1_600_100_000}
    rec.update(overrides)
    return rec


def comment(**overrides):
    rec = {"id": "c0ffee", "link_id": "t3_abc123", "parent_id": "t3_abc123", "subreddit_id": "t5_2qh1i",
           "author": "some_user", "body": "Nice post", "score": 1, "controversiality": 0,
           "created_utc": 1_600_000_100, "retrieved_on": 1_600_100_000}
    rec.update(overrides)
    return rec


def reason(fn, rec):
    with pytest.raises(Rejected) as exc:
        fn(rec)
    return exc.value.reason


@pytest.mark.parametrize("entity,factory", [
    ("subreddits", subreddit), ("authors", author), ("submissions", submission), ("comments", comment),
])
def test_valid_records_match_staging_columns(entity, factory):
    row = V.for_entity(entity)(factory())
    assert len(row) == len(BY_NAME[entity].record_columns)


def test_subreddit_normalization():
    row = V.subreddits(subreddit(id="t5_2qh1i", created_utc="1201233135.0"))
    assert row[0] == "2qh1i"
    assert row[7] == 1_201_233_135


def test_author_prefix_is_stripped_and_optional():
    assert V.authors(author())[1] == "abc12"
    assert V.authors(author(id=None))[1] is None


def test_submission_defaults_and_deleted_author():
    row = V.submissions(submission(author="[deleted]", score=None, num_comments=None, over_18=None))
    cols = dict(zip(BY_NAME["submissions"].record_columns, row, strict=True))
    assert cols["author_name"] is None
    assert cols["score"] == 0 and cols["num_comments"] == 0 and cols["over_18"] is False


def test_comment_ids_are_normalized():
    cols = dict(zip(BY_NAME["comments"].record_columns, V.comments(comment(id="t1_c0ffee")), strict=True))
    assert cols["comment_id"] == "c0ffee"
    assert cols["submission_id"] == "abc123"
    assert cols["subreddit_id"] == "2qh1i"
    assert cols["parent_id"] == "t3_abc123"


def test_nul_bytes_are_removed_not_rejected():
    cols = dict(zip(BY_NAME["comments"].record_columns, V.comments(comment(body="a\x00b")), strict=True))
    assert cols["body"] == "ab"


def test_float_scores_that_are_integers_are_accepted():
    assert V.comments(comment(score=3.0))[6] == 3


@pytest.mark.parametrize("fn,rec,expected", [
    (V.subreddits, subreddit(id=None), "missing_id"),
    (V.subreddits, subreddit(display_name="bad name!"), "invalid_display_name"),
    (V.subreddits, subreddit(subscribers=-1), "out_of_range_subscribers"),
    (V.subreddits, subreddit(subscribers="many"), "invalid_subscribers"),
    (V.authors, author(name=None), "missing_name"),
    (V.authors, author(name="ab"), "invalid_name"),
    (V.authors, author(link_karma="n/a"), "invalid_link_karma"),
    (V.submissions, submission(id="ABC"), "invalid_id"),
    (V.submissions, submission(title="   "), "blank_title"),
    (V.submissions, submission(title="x" * 301), "too_long_title"),
    (V.submissions, submission(title=None), "missing_title"),
    (V.submissions, submission(num_comments=-5), "out_of_range_num_comments"),
    (V.submissions, submission(score=1.5), "invalid_score"),
    (V.submissions, submission(score=True), "invalid_score"),
    (V.submissions, submission(over_18="yes"), "invalid_over_18"),
    (V.submissions, submission(author="has space"), "invalid_author"),
    (V.submissions, submission(author={"name": "x"}), "invalid_author"),
    (V.submissions, submission(created_utc="last tuesday"), "invalid_created_utc"),
    (V.submissions, submission(created_utc=-86_400), "out_of_range_created_utc"),
    (V.submissions, submission(created_utc=NOW + 2 * 86_400), "out_of_range_created_utc"),
    (V.submissions, submission(created_utc=float("nan")), "invalid_created_utc"),
    (V.submissions, submission(subreddit_id=None), "missing_subreddit_id"),
    (V.comments, comment(parent_id="t9_abc"), "invalid_parent_id"),
    (V.comments, comment(link_id="abc123"), "invalid_link_id"),
    (V.comments, comment(controversiality=2), "out_of_range_controversiality"),
    (V.comments, comment(retrieved_on="soon"), "invalid_retrieved_on"),
])
def test_rejections(fn, rec, expected):
    assert reason(fn, rec) == expected
