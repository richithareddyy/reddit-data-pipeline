# Reddit Data Pipeline

A Reddit archive can contain broken JSON, duplicate records, and comments whose parent posts are missing. This pipeline loads Reddit-format archives into PostgreSQL 16, records why it rejects a row, and produces a report of what happened during the run.

Python streams and validates the input; PostgreSQL handles staging, merges, and integrity checks.

## Run locally

Requires Docker with Compose v2 or newer and `make`. From the repository root:

```bash
make generate SCALE=small
make load
make report
make test
```

The first command creates sample data with deliberately injected defects. `make load` starts PostgreSQL, loads `./data`, and reports accepted records, rejections, and quality checks. Error-level quality failures produce a nonzero exit status. `make report` reprints the latest run, and `make test` runs the test suite in Docker.

To change the database credentials or host port, copy `.env.example` to `.env` before the first start. The `ETL_*` settings in the [guide](GUIDE.md#configuration) are read by the CLI, but Compose does not pass them to the container; set them with `docker compose run -e`.

For your own archives, see the [input format](GUIDE.md#using-real-reddit-dumps). Plain, gzip, and Zstandard-compressed newline-delimited JSON are supported.

## Real data

The pipeline has also been run on real data: the r/dataengineering posts and comments that the [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) Reddit archive API returned, with timestamps from February 2015 to November 7, 2026. These commands download and load a subreddit:

```bash
make fetch SUBREDDIT=dataengineering
make load-real
```

The run read 708,040 lines (72,558 posts, 635,481 comments and one community record; 1.39 GB) and accepted 707,830 of them, with all 13 quality checks passing. The recorded load took 15.6 seconds; repeat loads of the same files on the same machine took 22 to 40 seconds. The 210 rejected comments belong to 22 posts that are missing from the archive. The [run report](results/dataengineering.md) has the full numbers.

That download was made over several sessions with earlier versions of `fetch`, before it had a fixed snapshot end and a completeness check, so it is not certified complete; the [guide](GUIDE.md#how-complete-is-it) sets out what is known. A new download will contain different records. `fetch` now fixes a snapshot end time and marks the download incomplete if any single second holds a full page of records (100) or more, because the API cannot page within a second. It cannot detect records the archive itself lacks.

The download has no authors file, so `make load-real` passes `--derive-authors`, which builds the authors table from names in the posts and comments (82,383 here); their account IDs, karma and creation dates are left unknown. The first real load also exposed a validator bug: Reddit's `<subreddit>-ModTeam` account names can exceed the 20-character username limit, so all 9,203 comments from `dataengineering-ModTeam` were rejected as invalid until the rule was fixed. One of them is still rejected because its post is missing. The downloaded data is not committed.

## How it works

Python reads the archive line by line, validates each record, and sends valid ones to staging tables with PostgreSQL’s bulk `COPY` command. SQL then excludes duplicates and records whose parents are missing, and inserts or updates the rest. Communities and authors load before posts and comments, unless authors are derived from them. An existing row is replaced only by a record with a later retrieval time, or by one that has a retrieval time when the row has none, and a database lock keeps two runs from using staging at once.

The merge uses a small set of columns to identify duplicates and rejected rows, rather than sorting complete records. Loads into an empty database also drop the foreign keys and secondary indexes first, then rebuild them after loading, which validates every foreign key. The [guide](GUIDE.md) explains the bulk-load settings and reported benchmark.

The 90 tests cover validation, the data generator, repeat loads, newer-record updates, compressed input, derived authors, concurrent runs, loads interrupted at six phase boundaries, and download paging, retries and resume against a fake API. Twenty-one of them need PostgreSQL and are skipped without `TEST_DATABASE_URL`. GitHub Actions runs ruff, all 90 tests, a Docker build, and a small load. Thirteen quality checks inspect the loaded database.

## Boundaries

The published 18.9-million-line benchmark uses generated data. `make generate SCALE=full` reproduces the input; the raw timing log is not committed. Treat it as a reported development measurement, not a production throughput guarantee. The real-data run above is about 27 times smaller.

Each record type commits separately, so a failed comment load can leave communities, authors and posts already loaded. The Compose configuration favors local bulk loading, not production durability or replication, and it publishes PostgreSQL on all host interfaces with default credentials. Rejected records are kept in the database as excerpts or normalized copies, which can include usernames and text. `make fetch` downloads one subreddit's archive from the Arctic Shift API; there is no scheduler or live Reddit collector.

Released under the [MIT License](LICENSE).

[Full guide and benchmark](GUIDE.md) · [Example queries](sql/example_queries.sql) · [Pipeline code](pipeline/)
