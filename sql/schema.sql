-- Schema for the Reddit ingestion pipeline.
--
--   staging : unlogged landing tables written by COPY, no constraints
--   reddit  : validated core tables with primary keys and CHECK constraints
--             (foreign keys and secondary indexes are managed by pipeline/constraints.py
--              so they can be dropped and rebuilt around bulk loads)
--   etl     : run metadata, load statistics, rejected records and quality results

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS reddit;
CREATE SCHEMA IF NOT EXISTS etl;

-- ---------------------------------------------------------------------------
-- Staging
-- ---------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS staging.subreddits (
    subreddit_id    text,
    name            text,
    title           text,
    description     text,
    subscribers     integer,
    over_18         boolean,
    subreddit_type  text,
    created_utc     bigint,
    retrieved_utc   bigint,
    source_line     bigint
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.authors (
    author_name     text,
    author_id       text,
    link_karma      bigint,
    comment_karma   bigint,
    created_utc     bigint,
    retrieved_utc   bigint,
    source_line     bigint
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.submissions (
    submission_id   text,
    subreddit_id    text,
    author_name     text,
    title           text,
    selftext        text,
    url             text,
    domain          text,
    score           integer,
    num_comments    integer,
    over_18         boolean,
    is_self         boolean,
    created_utc     bigint,
    retrieved_utc   bigint,
    source_line     bigint
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.comments (
    comment_id       text,
    submission_id    text,
    parent_id        text,
    subreddit_id     text,
    author_name      text,
    body             text,
    score            integer,
    controversiality smallint,
    created_utc      bigint,
    retrieved_utc    bigint,
    source_line      bigint
);

-- ---------------------------------------------------------------------------
-- Core
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS reddit.subreddits (
    subreddit_id    text PRIMARY KEY CHECK (subreddit_id ~ '^[0-9a-z]{1,13}$'),
    name            text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9_]{1,20}$'),
    title           text,
    description     text,
    subscribers     integer CHECK (subscribers >= 0),
    over_18         boolean NOT NULL DEFAULT false,
    subreddit_type  text,
    created_at      timestamptz NOT NULL,
    retrieved_at    timestamptz,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reddit.authors (
    author_name     text PRIMARY KEY CHECK (author_name ~ '^[A-Za-z0-9_-]{3,20}$'),
    author_id       text CHECK (author_id ~ '^[0-9a-z]{1,13}$'),
    link_karma      bigint,
    comment_karma   bigint,
    created_at      timestamptz NOT NULL,
    retrieved_at    timestamptz,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reddit.submissions (
    submission_id   text PRIMARY KEY CHECK (submission_id ~ '^[0-9a-z]{1,13}$'),
    subreddit_id    text NOT NULL,
    author_name     text,
    title           text NOT NULL CHECK (length(btrim(title)) BETWEEN 1 AND 300),
    selftext        text,
    url             text,
    domain          text,
    score           integer NOT NULL DEFAULT 0,
    num_comments    integer NOT NULL DEFAULT 0 CHECK (num_comments >= 0),
    over_18         boolean NOT NULL DEFAULT false,
    is_self         boolean NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL,
    retrieved_at    timestamptz,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reddit.comments (
    comment_id       text PRIMARY KEY CHECK (comment_id ~ '^[0-9a-z]{1,13}$'),
    submission_id    text NOT NULL,
    parent_id        text NOT NULL CHECK (parent_id ~ '^t[13]_[0-9a-z]{1,13}$'),
    subreddit_id     text NOT NULL,
    author_name      text,
    body             text,
    score            integer NOT NULL DEFAULT 0,
    controversiality smallint NOT NULL DEFAULT 0 CHECK (controversiality IN (0, 1)),
    created_at       timestamptz NOT NULL,
    retrieved_at     timestamptz,
    loaded_at        timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- ETL metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id       bigserial PRIMARY KEY,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    status       text NOT NULL DEFAULT 'running'
                 CHECK (status IN ('running', 'succeeded', 'quality_failed', 'failed')),
    data_dir     text NOT NULL,
    load_mode    text NOT NULL CHECK (load_mode IN ('bulk', 'incremental')),
    error        text
);

CREATE TABLE IF NOT EXISTS etl.load_stats (
    run_id                      bigint NOT NULL REFERENCES etl.pipeline_runs (run_id) ON DELETE CASCADE,
    entity                      text NOT NULL,
    source_file                 text NOT NULL,
    records_read                bigint NOT NULL,
    records_rejected_validation bigint NOT NULL,
    records_staged              bigint NOT NULL,
    duplicates                  bigint NOT NULL,
    records_rejected_integrity  bigint NOT NULL,
    records_accepted            bigint NOT NULL,
    records_upserted            bigint NOT NULL,
    extract_seconds             numeric(12, 3) NOT NULL,
    merge_seconds               numeric(12, 3) NOT NULL,
    PRIMARY KEY (run_id, entity)
);

CREATE TABLE IF NOT EXISTS etl.rejected_records (
    rejected_id  bigserial PRIMARY KEY,
    run_id       bigint NOT NULL REFERENCES etl.pipeline_runs (run_id) ON DELETE CASCADE,
    entity       text NOT NULL,
    source_line  bigint,
    reason       text NOT NULL,
    raw_record   text,
    rejected_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_rejected_records_run_entity_reason
    ON etl.rejected_records (run_id, entity, reason);

CREATE TABLE IF NOT EXISTS etl.quality_results (
    run_id      bigint NOT NULL REFERENCES etl.pipeline_runs (run_id) ON DELETE CASCADE,
    check_name  text NOT NULL,
    severity    text NOT NULL CHECK (severity IN ('error', 'warning')),
    passed      boolean NOT NULL,
    observed    numeric NOT NULL,
    threshold   numeric NOT NULL,
    description text NOT NULL,
    checked_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);
