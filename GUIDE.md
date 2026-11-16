# Reddit Data Pipeline

Load Reddit-format archive files into PostgreSQL with validation, deduplication, referential checks, and per-run audit records. The pipeline uses streaming `COPY` and set-based SQL to process submissions, comments, authors, and communities.

**Python · PostgreSQL · SQL · Docker**

The problem is making repeated archive loads both inspectable and safe to rerun: malformed records need an explanation, older snapshots must not overwrite newer rows, and a failed run needs a recoverable state. PostgreSQL handles the merge and integrity work; Python handles streaming input and validation.

## At a glance

| Input | Processing | Output |
| --- | --- | --- |
| Newline-delimited JSON, plain or compressed with gzip/Zstandard | Validation, staging, deduplication, and upserts | Relational tables, rejected-record reports, and 13 data-quality checks |

**Benchmark scope:** The reported 18.9-million-line benchmark uses generated data with deliberately injected defects. It demonstrates the pipeline on a reproducible test dataset; runtime depends on hardware and input characteristics. A separate [real-data run](#real-data-run) loads the full r/dataengineering archive (708,040 lines).

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Data model](#data-model)
- [Validation and integrity rules](#validation-and-integrity-rules)
- [Data quality checks](#data-quality-checks)
- [Bulk load optimizations](#bulk-load-optimizations)
- [Benchmark](#benchmark)
- [Real-data run](#real-data-run)
- [Using real Reddit dumps](#using-real-reddit-dumps)
- [Configuration](#configuration)
- [Testing](#testing)
- [Reliability](#reliability)
- [Project structure](#project-structure)

## Architecture

```
  dump files                   pipeline (Python)                          PostgreSQL
 ─────────────       ───────────────────────────────────       ─────────────────────────────────
 subreddits.*   ──►  extract    stream lines (plain/gz/zst)
 authors.*           validate   types, formats, ranges,    ──► staging.*   UNLOGGED, via COPY
 submissions.*                  required fields, NUL bytes  ──► etl.rejected_records (invalid)
 comments.*
                     merge      find superseded duplicates
                                and rows missing parents    ──► etl.rejected_records (orphans)
                                upsert valid rows           ──► reddit.*    PKs, CHECKs, FKs
                     enforce    rebuild indexes, validate FKs
                     check      13 quality checks           ──► etl.quality_results
                     record     per-entity counts & timing  ──► etl.load_stats, etl.pipeline_runs
```

Entities load in dependency order (subreddits → authors → submissions → comments), so each child entity's referential checks can run as a single set-based join against parents that are already loaded.

## Quick start

Requirements: Docker with Compose v2, and `make`.

```bash
make generate SCALE=small
```

```bash
make load
```

`make load` starts PostgreSQL, runs the pipeline against `./data`, and prints a report of load statistics, rejections by reason, and quality check results. The command exits non-zero if any error-level quality check fails.

Other commands:

| Command | Description |
| --- | --- |
| `make generate SCALE=tiny\|small\|medium\|full` | Write synthetic dumps to `./data` (≈25K / 190K / 1.9M / 18.9M lines) |
| `make fetch SUBREDDIT=name` | Download one subreddit from the Arctic Shift API to `./data/<name>` (resumable) |
| `make load-real` / `make report-real` | Load that download into the separate `reddit_real` database / print its report |
| `make load` | Run the full pipeline |
| `make report` | Print the latest run summary |
| `make check` | Re-run the quality checks |
| `make test` | Run unit and integration tests inside Docker |
| `make psql` | Open a `psql` shell (`\i /sql/example_queries.sql` for sample queries) |
| `make down` / `make clean` | Stop services / also remove the database volume and `./data` |

The CLI can also be called directly: `docker compose run --rm pipeline --help`.

## Data model

| Schema | Table | Purpose |
| --- | --- | --- |
| `reddit` | `subreddits` | Communities, keyed by base-36 id |
| `reddit` | `authors` | User accounts, keyed by username (`created_at` is unknown for derived authors) |
| `reddit` | `submissions` | Posts → `subreddits`, `authors` |
| `reddit` | `comments` | Comments → `submissions`, `subreddits`, `authors` |
| `staging` | one per entity | Unlogged, constraint-free landing tables written by `COPY` |
| `etl` | `pipeline_runs` | One row per run: status, mode, timing, error |
| `etl` | `load_stats` | Per-entity counts: read, invalid, duplicates, orphaned, accepted, upserted, timing |
| `etl` | `rejected_records` | Validation/integrity rejects with source line and reason; duplicates are counted separately |
| `etl` | `quality_results` | Observed value, threshold and pass/fail for each check |

Core tables enforce primary keys, `NOT NULL`, and `CHECK` constraints on ID formats, name formats, title length, non-negative counts and enumerated values. Deleted accounts (`[deleted]`) are stored as a `NULL` author, and the author foreign keys permit this.

Foreign keys and secondary indexes are defined in [`pipeline/constraints.py`](pipeline/constraints.py) so bulk loads can drop and rebuild them (see [Bulk load optimizations](#bulk-load-optimizations)).

## Validation and integrity rules

**Record validation** ([`pipeline/validate.py`](pipeline/validate.py)) runs on every line before it reaches the database:

- The line must be well-formed JSON and decode to an object.
- Required fields must be present: IDs, names, `created_utc`, `subreddit_id`, `link_id` and `parent_id`.
- IDs must be base-36. Reddit's type prefixes (`t1_`, `t2_`, `t3_`, `t5_`) are accepted and normalized away.
- Subreddit and user names must match Reddit's naming rules. Usernames are 3 to 20 characters, plus Reddit's longer `<subreddit>-ModTeam` moderator accounts.
- Timestamps must be numeric and fall between Reddit's launch (2005-06-23) and one day in the future.
- Integers must be real integers (booleans, fractional floats and non-numeric strings are rejected) and within column range. `num_comments` and `subscribers` must be non-negative, and `controversiality` must be 0 or 1.
- Titles cannot be blank or longer than 300 characters. Text fields have length limits.
- `parent_id` must be a `t1_` or `t3_` fullname.
- NUL characters, which PostgreSQL text cannot store, are stripped rather than causing the record to be rejected.

**Set-based integrity checks** ([`pipeline/load.py`](pipeline/load.py)) run during the merge:

- **Deduplication**: one row per natural key. The version with the latest `retrieved_on` wins, and ties go to the version later in the file.
- **Referential integrity**: submissions need an existing subreddit and author. Comments need an existing submission and author, and must belong to the same subreddit as their submission. Rows that fail are written to `etl.rejected_records` with reasons such as `orphan_submission` or `subreddit_mismatch`.
- **Idempotent upserts**: an existing row is replaced only by a strictly newer retrieval. Re-running the same input changes nothing, and an older dump never overwrites newer data.

Validation rejects retain a sanitized excerpt of at most 4,000 characters from the input line. Integrity rejects retain the normalized staging row as JSON. Superseded duplicates contribute to counts but are not individually inserted into `etl.rejected_records`. Keep the original dumps for complete source records.

## Data quality checks

After loading, [`pipeline/quality.py`](pipeline/quality.py) runs these checks and stores the results in `etl.quality_results`. A failed **error** check marks the run `quality_failed` and exits non-zero. A failed **warning** check is recorded but does not fail the run.

| Check | Severity | Passes when |
| --- | --- | --- |
| `load_reconciliation` | error | For every entity, read = staged + invalid, and staged = duplicates + orphaned + accepted |
| `core_row_coverage` | error | Every core table holds at least the rows accepted this run |
| `rejection_rate` | error | Rejected lines ≤ 5% of lines read (configurable) |
| `foreign_keys_enforced` | error | All 5 foreign keys exist and are validated |
| `orphan_submissions` | error | No submission lacks its subreddit or author |
| `orphan_comments` | error | No comment lacks its submission or author |
| `comment_subreddit_mismatch` | error | Every comment's subreddit matches its submission's |
| `future_timestamps` | error | No row is created more than a day in the future |
| `blank_submission_titles` | error | No empty titles |
| `orphan_parent_comments` | warning | ≤ 5% of replies reference a parent comment that is not in the dataset |
| `comments_before_submission` | warning | No comment predates its submission |
| `duplicate_subreddit_names` | warning | No two subreddit ids share a case-insensitive name |
| `deleted_comment_author_rate` | warning | ≤ 50% of comments have a deleted author |

## Bulk load optimizations

- **Streaming with bounded row batches.** Files are decompressed and parsed line by line (with `orjson` for fast JSON parsing). Valid rows go to PostgreSQL through a single `COPY ... FROM STDIN` per entity, in 50K-row chunks. Rejected lines spool to a temporary file and are copied in one batch.
- **Unlogged staging tables.** Raw rows land in `UNLOGGED` tables with no constraints or indexes, so writing them generates no WAL and does no per-row checking.
- **Narrow set-based merge.** One window-function pass over narrow columns (key, source line, parent references) collects only the rows that must *not* load: superseded duplicates and rows with missing parents. The upsert then anti-joins staging against that small set, so full-width records are never sorted or copied to a temp table. There are no per-row round trips.
- **WAL-free initial insert.** In bulk mode, an empty core table is truncated inside the merge transaction. With `wal_level=minimal`, PostgreSQL can then write the new rows without WAL and sync the file once at commit.
- **Drop and rebuild constraints (bulk mode).** When the core tables are empty, the pipeline drops the foreign keys and secondary indexes and loads with only primary keys in place. Afterwards it rebuilds the indexes, using parallel maintenance workers, and re-adds the foreign keys. PostgreSQL validates each re-added key against every row in a single pass, so integrity is still guaranteed. Later runs switch to incremental mode automatically and keep the constraints in place.
- **Session and server tuning.** The load session uses `synchronous_commit=off` and a larger `work_mem` and `maintenance_work_mem`. The server runs with `wal_level=minimal`, a larger `shared_buffers` and `max_wal_size`, and a longer checkpoint interval. `fsync` stays on.
- **Fresh statistics.** The core tables are `ANALYZE`d after each load, so the quality checks and downstream queries get good query plans.

## Benchmark

The following is a previously reported development benchmark; its raw timing log is not committed. It describes a full-scale run (`make generate SCALE=full && make load`) into an empty database. Docker Desktop was limited to 10 CPUs and 8 GB of memory, on an Apple Silicon Mac with an SSD. The input was 18,897,027 lines (5.9 GB of uncompressed NDJSON) with a 1% defect rate.

| Entity | Lines read | Invalid | Duplicates | Orphaned | Loaded |
| --- | ---: | ---: | ---: | ---: | ---: |
| subreddits | 60,594 | 506 | 88 | 0 | 60,000 |
| authors | 1,838,137 | 15,109 | 3,028 | 0 | 1,820,000 |
| submissions | 2,504,640 | 16,369 | 2,737 | 5,534 | 2,480,000 |
| comments | 14,493,656 | 79,424 | 15,973 | 48,259 | 14,350,000 |
| **total** | **18,897,027** | **111,408** | **21,826** | **53,793** | **18,710,000** |

Each count matches the generator's manifest exactly, and all 13 quality checks passed.

| Phase | Time |
| --- | ---: |
| Parse, validate and `COPY` to staging | 97 s |
| Deduplicate, integrity-check and upsert | 137 s |
| Rebuild 7 secondary indexes | 34 s |
| Re-add and validate 5 foreign keys | 15 s |
| Quality checks | 61 s |
| **End to end** | **345 s (≈55K lines/s)** |

The narrow-exclusion merge described above, with WAL skipped on the initial insert, cut end-to-end time from 584 s to 345 s. The comments merge alone went from 267 s to 102 s. The loaded database is about 5.9 GB.

## Real-data run

Every r/dataengineering post and comment that the [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) API returned, with timestamps from February 6, 2015 to November 7, 2026, was downloaded with `make fetch SUBREDDIT=dataengineering` and loaded with `make load-real`, on the same machine as the benchmark above. The [run report](results/dataengineering.md) is committed; the 1.3 GB download is not.

| Entity | Lines read | Invalid | Duplicates | Orphaned | Loaded |
| --- | ---: | ---: | ---: | ---: | ---: |
| subreddits | 1 | 0 | 0 | 0 | 1 |
| submissions | 72,558 | 0 | 0 | 0 | 72,558 |
| comments | 635,481 | 0 | 0 | 210 | 635,271 |
| **total** | **708,040** | **0** | **0** | **210** | **707,830** |

The load took 15.6 s end to end and produced a 353 MB database. All 13 quality checks passed: 0.03% of lines were rejected, 0.04% of replies point to a parent comment that is not in the archive, and 3.6% of comments have a deleted author. 82,383 authors were derived from the posts and comments.

What the real data showed, compared with generated data:

- **Orphans come from gaps in the archive.** The 210 rejected comments belong to 22 posts that the archive does not have: its ID lookup endpoint returns none of them.
- **A validator rule was too strict.** The first load rejected 9,203 comments, every one from the `dataengineering-ModTeam` account. Reddit creates a `<subreddit>-ModTeam` account for each community, and its name is longer than the 20-character username limit. The rule now accepts that form, and the reload rejected none.
- **No authors file exists.** Reddit dumps contain only posts and comments, which is why `--derive-authors` exists.

The download spanned about 18 hours, much of it paused while the laptop slept. The API served 100 records per request, and it answered open-ended queries with HTTP 422 ("Timeout. Maybe slow down a bit") once the history got large. `fetch` therefore pages through 30-day windows, waits at least 10 s after a 422, and resumes from what is already on disk.

### How complete is it?

This snapshot is everything the API returned, not a certified complete copy of the subreddit:

- **The archive has gaps.** Arctic Shift is a third-party archive. Anything it lacks, such as the 22 posts above, cannot be downloaded.
- **The download predates the current `fetch`.** It used `limit=auto`, had no fixed end time (posts and comments were fetched hours apart, so their cut-offs differ slightly), and had no saturation check. The paging logic it used could skip records only when a single second held at least a full page (100) of them. In the data received, the busiest second holds 7 comments (3 posts) and no ID appears twice, so that skip did not happen here, provided the API returns a full page whenever more records exist. The API documents neither that nor how it orders records that share a second.
- **Counts were not reconciled against a fixed total.** The 72,558 posts and 635,481 comments exceed the API's subreddit metadata (72,065 and 633,618), but that metadata is a live, cached figure rather than a count for the same time range.

Downloading again with the current `fetch` produces a snapshot with a recorded end time and a `complete` flag.

## Using real Reddit dumps

Put one file per entity in the data directory, named after the entity:

```
data/
  subreddits.ndjson.zst
  authors.ndjson.zst
  submissions.ndjson.zst
  comments.ndjson.zst
```

Accepted extensions are `.ndjson`, `.jsonl` and `.json`, optionally compressed as `.gz` or `.zst`. A bare `.zst` also works (for example `comments.zst`). Zstandard archives compressed with a long window, like the Pushshift archives, are supported. Records use the Pushshift field names (`id`, `subreddit_id`, `author`, `link_id`, `parent_id`, `created_utc`, `retrieved_on`, and so on). Extra fields are ignored.

If an entity's file is missing, that entity is skipped with a warning. Child records that reference its rows are then rejected as orphans, so load related dumps together.

Reddit dumps have no authors file. `run --derive-authors` inserts each distinct author name found in the staged posts and comments before the integrity checks, leaving `created_at` and karma `NULL`. Without the flag, named authors are rejected as orphans as before.

To get one subreddit's posts and comments, `pipeline fetch --subreddit NAME --out DIR` (or `make fetch SUBREDDIT=NAME`) downloads them from the [Arctic Shift API](https://github.com/ArthurHeitmann/arctic_shift/tree/master/api) into `subreddits.ndjson`, `submissions.ndjson` and `comments.ndjson`. It waits 1 s between requests, backs off on rate limits and timeouts, and resumes an interrupted download.

`source.json` records the snapshot: `snapshot_end` (now, or `--until EPOCH`) is fixed when the download starts and kept on resume, so only records created up to that time are fetched, and a resume with a different end time is refused. When the download finishes, `complete` says whether every record in range could be fetched, and `fetch` exits with status 1 if not.

The API pages only by `created_utc`, in whole seconds, with exclusive `after`/`before` bounds. It has no secondary cursor, and records that share a second come back in a fixed order (the same for ascending and descending sorts), so there is no way to page within a second. `fetch` asks for pages of 100. If one second holds a full page of records, any beyond the first 100 cannot be reached; that second is listed under `saturated_seconds`, and the snapshot is marked incomplete rather than silently missing records. The completeness flag relies on behaviour that was observed but is not documented: results sorted by `created_utc`, and a full page returned whenever more records exist. The API is a free service intended for modest pulls; for large volumes use its monthly dumps. Archive content is user-generated and has no stated license, so check the terms that apply to your use, and keep downloaded data out of version control.

## Configuration

Database settings come from `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST` and `POSTGRES_PORT`, or from a single `DATABASE_URL`. Copy `.env.example` to `.env` to override the defaults, for example to change the host port if 5432 is taken.

| Variable | Default | Description |
| --- | --- | --- |
| `ETL_MAX_REJECT_RATE` | `0.05` | Error threshold for the share of rejected lines |
| `ETL_MAX_ORPHAN_PARENT_RATE` | `0.05` | Warning threshold for replies whose parent comment is missing |
| `ETL_COPY_BATCH_ROWS` | `50000` | Rows buffered per write to the COPY stream |
| `ETL_PROGRESS_EVERY` | `1000000` | Lines between progress log messages |
| `ETL_WORK_MEM` / `ETL_MAINTENANCE_WORK_MEM` | `256MB` / `1GB` | Session memory for merges and index builds |

Run options: `run --mode auto|bulk|incremental` (default `auto`) and `--derive-authors`; `generate --scale`, `--defect-rate`, `--seed` and `--compress none|gz|zst`; `fetch --subreddit`, `--out`, `--delay` and `--until`.

## Testing

```bash
make test
```

- **Unit tests** cover every validation rule and rejection reason, reading plain, gzip and zstd files, and the generator's determinism and manifest accuracy. Fetch tests use a fake API with the probed paging behaviour to check repeated timestamps across pages, saturated seconds (flagged, never silently skipped), records the API repeats, empty windows and window edges, the snapshot end bound, resuming after a failed request or a half-written line, the snapshot end and saturation surviving a resume, and retries on 422, 429, 5xx, timeouts and dropped connections. A randomized test checks that any record not fetched lies in a second reported as saturated.
- **Integration tests** run against a separate `reddit_test` database:
  - They generate a dataset with planted defects and check that the pipeline loads exactly the clean records, and that its counts of invalid lines, duplicates and orphans match the generator's manifest exactly.
  - They check that re-running the same input changes no rows and that a newer retrieval updates an existing row.
  - They cover real-dump quirks: prefixed IDs, string timestamps, NUL bytes, deleted authors, invalid UTF-8 and gzip input.
  - They cover failure handling: a second run is refused while one holds the lock, an interrupted run is marked failed, and foreign keys are restored after a failed bulk load.
  - They check that `--derive-authors` loads dumps with no authors file, and that without it named authors are still rejected as orphans.
  - They interrupt a load at six phase boundaries, with both a raised error and a simulated kill that skips all cleanup, rerun it, and check that the final rows, foreign keys and indexes, load statistics and rejected records match an uninterrupted load.

Unit tests also run without Docker: `pip install -r requirements-dev.txt && pytest`. The integration tests are skipped unless `TEST_DATABASE_URL` is set.

A GitHub Actions workflow ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the full suite against a PostgreSQL 16 service container. It also builds the Docker image and does a small generate-and-load smoke test.

## Reliability

- **One run at a time.** Each run takes a PostgreSQL advisory lock before touching the shared staging tables and constraints. A second concurrent run exits with a clear message instead of corrupting the first.
- **Crash recovery.** If a process was killed mid-run, its run is still marked `running`. The next run marks it `failed` and rebuilds any foreign keys or indexes that bulk mode had dropped.
- **Failure boundaries.** Staging and merges commit separately for each entity. An error rolls back the active transaction, records the failure, and attempts to restore constraints; already committed entities remain. A quality failure also leaves loaded rows in place and marks the run `quality_failed`. This is a rerunnable pipeline, not one all-or-nothing transaction across the entire archive.
- **Safe to re-run.** Upserts only apply strictly newer data, so re-running after a failure or re-loading an overlapping dump never duplicates or regresses rows.

The supplied Compose settings favor a local bulk-load experiment (`wal_level=minimal`, no replication, and asynchronous commits). They are not a production durability or replication configuration. `fetch` is a one-off archive download; there is no scheduler or live Reddit API collector in this repository.

## Project structure

```
reddit-data-pipeline/
├── docker-compose.yml        PostgreSQL 16 (load-tuned) + pipeline service
├── Dockerfile
├── Makefile
├── .github/workflows/ci.yml  tests + Docker smoke test on every push
├── docker/initdb/            creates the reddit_test database
├── sql/
│   ├── schema.sql            staging, core and etl schemas
│   └── example_queries.sql
├── pipeline/
│   ├── __main__.py           CLI: generate, fetch, init-db, run, check, report
│   ├── config.py             environment-driven settings
│   ├── entities.py           entity definitions and load order
│   ├── extract.py            file discovery and streaming decompression
│   ├── validate.py           record validation and normalization
│   ├── load.py               COPY staging and set-based merge
│   ├── constraints.py        foreign keys and secondary indexes
│   ├── quality.py            post-load quality checks
│   ├── runner.py             run orchestration and bookkeeping
│   ├── report.py             run summary output
│   ├── fetch.py              one-subreddit downloader for the Arctic Shift API
│   └── generate.py           synthetic dump generator with planted defects
├── results/                  committed report from the real-data run
└── tests/
```
