# r/dataengineering load report

Source: Arctic Shift API (https://arctic-shift.photon-reddit.com): every r/dataengineering post and comment it returned, fetched 2026-11-07. This download predates the snapshot end and completeness check in `fetch`; see GUIDE.md, "How complete is it?".
Command: make fetch SUBREDDIT=dataengineering && make load-real
Machine: Apple Silicon Mac, Docker Desktop limited to 10 CPUs and 8 GB of memory, PostgreSQL 16.

```
Run 1: succeeded (bulk load from /data/dataengineering)
Started 2026-11-08 01:36:11 UTC, took 15.6s

Load statistics
entity          read  invalid  duplicates  orphaned  accepted  upserted  seconds
-----------  -------  -------  ----------  --------  --------  --------  -------
subreddits         1        0           0         0         1         1   0.0060
authors            0        0           0         0         0    82,383        0
submissions   72,558        0           0         0    72,558    72,558   1.9920
comments     635,481        0           0       210   635,271   635,271  11.4190
total        708,040        0           0       210   707,830   790,213  13.4170
Throughput: 52,772 lines/s

Rejections by reason
entity    reason             count
--------  -----------------  -----
comments  orphan_submission    210

Quality checks
check                        severity  result  observed  threshold
---------------------------  --------  ------  --------  ---------
blank_submission_titles      error       PASS         0          0
comment_subreddit_mismatch   error       PASS         0          0
core_row_coverage            error       PASS         0          0
foreign_keys_enforced        error       PASS         0          0
future_timestamps            error       PASS         0          0
load_reconciliation          error       PASS         0          0
orphan_comments              error       PASS         0          0
orphan_submissions           error       PASS         0          0
rejection_rate               error       PASS    0.0003     0.0500
comments_before_submission   warning     PASS         0          0
deleted_comment_author_rate  warning     PASS    0.0361     0.5000
duplicate_subreddit_names    warning     PASS         0          0
orphan_parent_comments       warning     PASS    0.0004     0.0500
```

Download summary (data/dataengineering/source.json):

```json
{
  "subreddit": "dataengineering",
  "source": "https://arctic-shift.photon-reddit.com/api",
  "fetched_at": "2026-11-08T01:33:48+00:00",
  "posts": 72558,
  "comments": 635481,
  "expected_posts": 72065,
  "expected_comments": 633618
}
```
