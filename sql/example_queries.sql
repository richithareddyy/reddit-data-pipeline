-- Example queries against the loaded data. Run with: make psql, then \i /sql/example_queries.sql

-- Latest run summary
SELECT r.run_id, r.status, r.load_mode, r.finished_at - r.started_at AS duration,
       sum(s.records_read) AS lines_read, sum(s.records_accepted) AS accepted
FROM etl.pipeline_runs r
JOIN etl.load_stats s USING (run_id)
GROUP BY r.run_id
ORDER BY r.run_id DESC
LIMIT 5;

-- Most active communities by comment volume
SELECT sr.name, count(*) AS comments, count(DISTINCT c.author_name) AS commenters
FROM reddit.comments c
JOIN reddit.subreddits sr USING (subreddit_id)
GROUP BY sr.name
ORDER BY comments DESC
LIMIT 10;

-- Monthly submission and comment activity
SELECT date_trunc('month', created_at) AS month, count(*) AS submissions
FROM reddit.submissions
GROUP BY 1
ORDER BY 1 DESC
LIMIT 12;

-- Authors with the highest total comment score
SELECT author_name, count(*) AS comments, sum(score) AS total_score
FROM reddit.comments
WHERE author_name IS NOT NULL
GROUP BY author_name
ORDER BY total_score DESC
LIMIT 10;

-- Reported comment counts versus comments actually present in the dataset
SELECT s.submission_id, s.title, s.num_comments AS reported, count(c.comment_id) AS loaded
FROM reddit.submissions s
LEFT JOIN reddit.comments c USING (submission_id)
GROUP BY s.submission_id
ORDER BY loaded DESC
LIMIT 10;

-- Sample rejected records for inspection
SELECT entity, reason, source_line, left(raw_record, 120) AS raw_record
FROM etl.rejected_records
WHERE run_id = (SELECT max(run_id) FROM etl.pipeline_runs)
ORDER BY entity, reason
LIMIT 20;
