"""Entity definitions shared by the extractor, loader and generator."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Entity:
    name: str
    key: str
    staging_columns: tuple[str, ...]

    @property
    def record_columns(self) -> tuple[str, ...]:
        """Columns produced by the validator (everything except source_line)."""
        return self.staging_columns[:-1]


SUBREDDITS = Entity(
    name="subreddits",
    key="subreddit_id",
    staging_columns=(
        "subreddit_id", "name", "title", "description", "subscribers", "over_18",
        "subreddit_type", "created_utc", "retrieved_utc", "source_line",
    ),
)

AUTHORS = Entity(
    name="authors",
    key="author_name",
    staging_columns=(
        "author_name", "author_id", "link_karma", "comment_karma",
        "created_utc", "retrieved_utc", "source_line",
    ),
)

SUBMISSIONS = Entity(
    name="submissions",
    key="submission_id",
    staging_columns=(
        "submission_id", "subreddit_id", "author_name", "title", "selftext", "url", "domain",
        "score", "num_comments", "over_18", "is_self", "created_utc", "retrieved_utc", "source_line",
    ),
)

COMMENTS = Entity(
    name="comments",
    key="comment_id",
    staging_columns=(
        "comment_id", "submission_id", "parent_id", "subreddit_id", "author_name", "body",
        "score", "controversiality", "created_utc", "retrieved_utc", "source_line",
    ),
)

# Load order: parents before children so referential checks can run set-based.
LOAD_ORDER: tuple[Entity, ...] = (SUBREDDITS, AUTHORS, SUBMISSIONS, COMMENTS)
BY_NAME: dict[str, Entity] = {e.name: e for e in LOAD_ORDER}
