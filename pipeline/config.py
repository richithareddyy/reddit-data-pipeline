"""Runtime settings, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote


def _default_database_url() -> str:
    user = quote(os.environ.get("POSTGRES_USER", "reddit"), safe="")
    password = quote(os.environ.get("POSTGRES_PASSWORD", "reddit"), safe="")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "reddit")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@dataclass(frozen=True)
class Settings:
    database_url: str
    # Valid rows buffered in memory before each write to the COPY stream.
    copy_batch_rows: int = 50_000
    # Log extraction progress every N input lines.
    progress_every: int = 1_000_000
    # Maximum share of input lines that may be rejected before the run fails quality checks.
    max_reject_rate: float = 0.05
    # Maximum share of comments whose parent comment is not present in the dataset.
    max_orphan_parent_rate: float = 0.05
    # Session-level memory settings used for sorts, hash joins and index builds.
    work_mem: str = "256MB"
    maintenance_work_mem: str = "1GB"


def load_settings() -> Settings:
    return Settings(
        database_url=os.environ.get("DATABASE_URL") or _default_database_url(),
        copy_batch_rows=int(os.environ.get("ETL_COPY_BATCH_ROWS", 50_000)),
        progress_every=int(os.environ.get("ETL_PROGRESS_EVERY", 1_000_000)),
        max_reject_rate=float(os.environ.get("ETL_MAX_REJECT_RATE", 0.05)),
        max_orphan_parent_rate=float(os.environ.get("ETL_MAX_ORPHAN_PARENT_RATE", 0.05)),
        work_mem=os.environ.get("ETL_WORK_MEM", "256MB"),
        maintenance_work_mem=os.environ.get("ETL_MAINTENANCE_WORK_MEM", "1GB"),
    )
