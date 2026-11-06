# Reddit Data Pipeline

Archive dumps are full of malformed lines, duplicates, and records whose parents never show up. A naive load either crashes or quietly stores bad data. This pipeline loads Reddit-format archives into PostgreSQL 16 and keeps a reason for every record it rejects, along with an audit trail for each run.

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

Records enter unlogged staging tables through streaming `COPY`, then set-based SQL selects valid rows and applies upserts. Loads follow dependency order: communities, authors, submissions, then comments. Older snapshots cannot overwrite newer rows, so re-running after a failure is safe. An advisory lock prevents concurrent runs from sharing staging state.

The main performance decision is the merge. One window-function pass over narrow columns finds only the rows that must *not* load, and the upsert anti-joins against that small set, so full-width records are never sorted or copied to a temp table. Along with skipping WAL on the initial insert, that took the generated 18.9-million-line benchmark from 584 s to 345 s. On an empty database, the pipeline also drops foreign keys and secondary indexes, then rebuilds and revalidates them after the load.

Tests cover validation, repeat loads, newer-record updates, compressed input, and interrupted bulk loads. Thirteen quality checks inspect the loaded database.

## Boundaries

The published 18.9-million-line benchmark uses generated data; its raw timing log is not committed. Treat it as a reported development measurement, not a production throughput guarantee.

Transactions commit by entity, so a failed run can leave earlier entities loaded. The Compose configuration favors local bulk loading, not production durability or replication. This repository does not include a scheduler or live Reddit collector.

[Full guide and benchmark](GUIDE.md) · [Example queries](sql/example_queries.sql) · [Pipeline code](pipeline/)
