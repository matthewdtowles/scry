-- Price-ingest DB metrics (scry#23 / #22).
--
-- Run against the ingest database right after a daily price run to size
-- table/index bloat and confirm the granular tables dominate write cost. Pair
-- this with the per-table write timings the binary now logs at the end of each
-- ingest pass ("... write totals (ms/calls): ..."). Prod is PostgreSQL 18.
--
--   psql "$DATABASE_URL" -f scripts/price_ingest_metrics.sql

-- 1. Size + live/dead tuples per price table. A large n_dead_tup relative to
--    n_live_tup (especially on granular_price_history) means weekly-only
--    retention + daily churn is outrunning autovacuum -> bloated indexes ->
--    slower upserts. Compare total_size vs index_size: the cost is in the index.
SELECT
    relname                                       AS table,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_indexes_size(relid))        AS index_size,
    n_live_tup,
    n_dead_tup,
    last_autovacuum,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE relname IN ('price', 'price_history', 'granular_price', 'granular_price_history')
ORDER BY pg_total_relation_size(relid) DESC;

-- 2. How many daily snapshots are currently retained in the granular history
--    table (the volume that accumulates between weekly retention runs, and the
--    table the per-batch upsert grows into during a run).
SELECT date, COUNT(*) AS rows
FROM granular_price_history
GROUP BY date
ORDER BY date DESC
LIMIT 40;

-- 3. Checkpoint activity. Frequent *requested* (vs timed) checkpoints during a
--    run signal WAL pressure from the bulk upserts - a likely driver of the
--    within-run throughput cliff. Also set `log_checkpoints = on` for one run.
--    (PG17+ split these out of pg_stat_bgwriter into pg_stat_checkpointer.)
SELECT num_timed, num_requested, write_time, sync_time, buffers_written, stats_reset
FROM pg_stat_checkpointer;
