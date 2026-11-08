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

## Real data

The pipeline has also been run on the complete history of r/dataengineering (February 2015 to November 2026), downloaded from the [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) Reddit archive:

```bash
make fetch SUBREDDIT=dataengineering
make load-real
```

It read 708,040 lines (72,558 posts and 635,481 comments, 1.3 GB) and loaded 707,830 of them in 15.6 seconds, with all 13 quality checks passing. The 210 rejected comments belong to 22 posts that are missing from the archive. The [run report](results/dataengineering.md) has the full numbers.

Reddit dumps have no authors file, so `--derive-authors` builds the authors table from the posts and comments; their account creation dates are left unknown. The first real load also exposed a validator bug: Reddit's `<subreddit>-ModTeam` accounts are longer than the 20-character username limit, so 9,203 moderator comments were wrongly rejected until the rule was fixed. The downloaded data is not committed.

## How it works

Python reads the archive a piece at a time and sends records to staging tables with PostgreSQL’s bulk `COPY` command. SQL checks the staged rows, removes duplicates, and inserts or updates valid records. Communities and authors load before posts and comments. Older records cannot overwrite newer versions, and a database lock keeps two runs from using staging at once.

The merge checks a small set of columns to identify rejected rows, rather than sorting or copying complete records. Empty-database loads also postpone index and foreign-key creation until after loading, then rebuild and validate them. The [guide](GUIDE.md) explains the bulk-load settings and reported benchmark.

Tests cover validation, repeat loads, newer-record updates, compressed input, and interrupted bulk loads. Thirteen quality checks inspect the loaded database.

## Boundaries

The published 18.9-million-line benchmark uses generated data; its raw timing log is not committed. Treat it as a reported development measurement, not a production throughput guarantee. The real-data run above is about 27 times smaller.

Each record type commits separately, so a failed comment load can leave authors and posts already loaded. The Compose configuration favors local bulk loading, not production durability or replication. `make fetch` downloads one subreddit's archive from the Arctic Shift API; there is no scheduler or live Reddit collector.

[Full guide and benchmark](GUIDE.md) · [Example queries](sql/example_queries.sql) · [Pipeline code](pipeline/)
