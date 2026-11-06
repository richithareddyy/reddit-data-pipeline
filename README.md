# Reddit Data Pipeline

A Reddit archive can contain broken JSON, duplicate records, and comments whose parent posts are missing. This pipeline loads Reddit-format archives into PostgreSQL 16, records why it rejects a row, and produces a report of what happened during the run.

Python streams and validates the input; PostgreSQL handles staging, merges, and integrity checks.

## Run locally

Requires Docker with Compose v2 and `make`. From the repository root:

```bash
make generate SCALE=small
make load
make report
make test
```

The first command creates sample data with deliberately injected defects. `make load` starts PostgreSQL, loads `./data`, and reports accepted records, rejections, and quality checks. Error-level quality failures produce a nonzero exit status.

For your own archives, see the [input format](GUIDE.md#using-real-reddit-dumps). Plain, gzip, and Zstandard-compressed newline-delimited JSON are supported.

## How it works

Python reads the archive a piece at a time and sends records to staging tables with PostgreSQL’s bulk `COPY` command. SQL checks the staged rows, removes duplicates, and inserts or updates valid records. Communities and authors load before posts and comments. Older records cannot overwrite newer versions, and a database lock keeps two runs from using staging at once.

The merge checks a small set of columns to identify rejected rows, rather than sorting or copying complete records. Empty-database loads also postpone index and foreign-key creation until after loading, then rebuild and validate them. The [guide](GUIDE.md) explains the bulk-load settings and reported benchmark.

Tests cover validation, repeat loads, newer-record updates, compressed input, and interrupted bulk loads. Thirteen quality checks inspect the loaded database.

## Boundaries

The published 18.9-million-line benchmark uses generated data; its raw timing log is not committed. Treat it as a reported development measurement, not a production throughput guarantee.

Each record type commits separately, so a failed comment load can leave authors and posts already loaded. The Compose configuration favors local bulk loading, not production durability or replication. This repository does not include a scheduler or live Reddit collector.

[Full guide and benchmark](GUIDE.md) · [Example queries](sql/example_queries.sql) · [Pipeline code](pipeline/)
