-- R2: Rebalance plan preview.
-- Dry-runs get_rebalance_table_shards_plan() to estimate bytes moved,
-- per-worker inbound/outbound, wall-time, strategy sanity, and disk
-- amplification from deferred-drop during the move.
--
-- Inputs (override with psql -v):
--   r2_disk_warn_pct        = 85       -- warn if projected disk > this pct
--   r2_disk_crit_pct        = 95       -- critical threshold
--   r2_move_mb_per_sec      = 20       -- assumed logical-rep move throughput
--   r2_include_zero_moves   = 0        -- emit row even if moves = 0

\pset pager off
\pset border 2
\pset format aligned

\set r2_disk_warn_pct     85
\set r2_disk_crit_pct     95
\set r2_move_mb_per_sec   20

\echo '==================== R2 : rebalance plan preview ===================='

-- Guard: plan function must exist.
DO $$
BEGIN
  IF to_regprocedure('pg_catalog.get_rebalance_table_shards_plan(regclass,real,integer,bigint[],boolean,name,real)') IS NULL THEN
    RAISE NOTICE 'R2 requires get_rebalance_table_shards_plan() (Citus 10+). Skipping.';
  END IF;
END $$;

-- Active default strategy (used by any citus_rebalance_start() invocation).
DROP TABLE IF EXISTS _r2_strategy;
CREATE TEMP TABLE _r2_strategy AS
SELECT
  name                             AS strategy_name,
  shard_cost_function::text        AS shard_cost_function,
  node_capacity_function::text     AS node_capacity_function,
  default_threshold,
  minimum_threshold,
  improvement_threshold
FROM pg_catalog.pg_dist_rebalance_strategy
WHERE default_strategy = true;

-- Dry-run plan. Empty result = perfect balance (no moves needed).
DROP TABLE IF EXISTS _r2_plan;
CREATE TEMP TABLE _r2_plan AS
SELECT
  p.table_name::regclass::text  AS table_name,
  p.shardid,
  p.shard_size,
  p.sourcename,
  p.sourceport,
  p.targetname,
  p.targetport
FROM pg_catalog.get_rebalance_table_shards_plan(
       NULL::regclass,       -- all colocation groups
       NULL::real,            -- default threshold
       1000000,               -- max moves: large cap
       '{}'::bigint[],        -- no exclusions
       false,                 -- drain_only = false
       NULL::name,            -- default strategy
       NULL::real             -- default improvement threshold
     ) p;

-- Per-worker total current size (from citus_shards) for disk projection.
DROP TABLE IF EXISTS _r2_worker_sizes;
CREATE TEMP TABLE _r2_worker_sizes AS
SELECT
  n.nodename,
  n.nodeport,
  COALESCE(SUM(s.shard_size), 0)::bigint AS current_bytes
FROM pg_dist_node n
LEFT JOIN citus_shards s
  ON s.nodename = n.nodename AND s.nodeport = n.nodeport
WHERE n.isactive = true AND n.noderole = 'primary' AND n.shouldhaveshards = true
GROUP BY n.nodename, n.nodeport;

-- Free / total disk from a fan-out: pg_tablespace_size(pg_default) as proxy
-- for data directory usage; disk capacity must be supplied by user
-- via -v r2_disk_mb_<nodename>_<nodeport>= (optional). If not set we still
-- report per-worker deltas.
-- For simplicity we compute inbound/outbound from the plan alone.

DROP TABLE IF EXISTS _r2_moves_per_node;
CREATE TEMP TABLE _r2_moves_per_node AS
WITH outb AS (
  SELECT sourcename AS nodename, sourceport AS nodeport,
         COUNT(*) AS out_moves, SUM(shard_size)::bigint AS out_bytes
  FROM _r2_plan GROUP BY sourcename, sourceport
),
inb AS (
  SELECT targetname AS nodename, targetport AS nodeport,
         COUNT(*) AS in_moves, SUM(shard_size)::bigint AS in_bytes
  FROM _r2_plan GROUP BY targetname, targetport
)
SELECT
  w.nodename, w.nodeport,
  w.current_bytes,
  COALESCE(inb.in_moves, 0)   AS in_moves,
  COALESCE(inb.in_bytes, 0)   AS in_bytes,
  COALESCE(outb.out_moves, 0) AS out_moves,
  COALESCE(outb.out_bytes, 0) AS out_bytes,
  (w.current_bytes + COALESCE(inb.in_bytes,0))::bigint AS peak_bytes_during_move
FROM _r2_worker_sizes w
LEFT JOIN inb  ON inb.nodename  = w.nodename AND inb.nodeport  = w.nodeport
LEFT JOIN outb ON outb.nodename = w.nodename AND outb.nodeport = w.nodeport;

-- Wall-time estimate: parallelism = citus.max_background_task_executors_per_node
-- per source node. Bytes-per-second from r2_move_mb_per_sec.
DROP TABLE IF EXISTS _r2_timing;
CREATE TEMP TABLE _r2_timing AS
WITH cfg AS (
  SELECT
    GREATEST(
      COALESCE(NULLIF(current_setting('citus.max_background_task_executors_per_node', true),'')::int, 1),
      1
    ) AS exec_per_node,
    (:r2_move_mb_per_sec)::bigint * 1048576 AS bytes_per_sec
),
bottleneck AS (
  -- The worker with the most outbound bytes is the bottleneck
  -- since moves from that node are the slowest critical path.
  SELECT MAX(out_bytes) AS max_out_bytes, SUM(out_bytes) AS total_bytes
  FROM _r2_moves_per_node
)
SELECT
  (SELECT COUNT(*) FROM _r2_plan) AS total_moves,
  COALESCE(b.total_bytes, 0)      AS total_bytes,
  COALESCE(b.max_out_bytes, 0)    AS max_source_out_bytes,
  cfg.exec_per_node,
  cfg.bytes_per_sec,
  CASE
    WHEN COALESCE(b.max_out_bytes,0) = 0 THEN 0
    ELSE CEIL(
           (b.max_out_bytes::numeric)
           / (cfg.bytes_per_sec * cfg.exec_per_node)
         )::bigint
  END AS estimated_seconds
FROM cfg, bottleneck b;


-- R2a: Default strategy sanity
\echo
\echo '-- R2a. Default rebalance strategy in effect --'
SELECT strategy_name,
       shard_cost_function,
       node_capacity_function,
       default_threshold,
       improvement_threshold
FROM _r2_strategy;


-- R2b: Plan summary
\echo
\echo '-- R2b. Plan summary (dry-run) --'
SELECT
  total_moves,
  pg_size_pretty(total_bytes)            AS total_bytes_to_move,
  pg_size_pretty(max_source_out_bytes)   AS bottleneck_source_bytes,
  exec_per_node                          AS executors_per_node,
  estimated_seconds                      AS est_wall_seconds,
  (estimated_seconds / 60)::text || ' min' AS est_wall_min,
  CASE
    WHEN total_moves = 0
      THEN 'balanced: default strategy sees no improvement over current placement'
    ELSE
      'predicted wall time assumes '||(:r2_move_mb_per_sec)||' MB/s per executor'
  END AS note
FROM _r2_timing;


-- R2c: Per-worker impact
\echo
\echo '-- R2c. Per-worker impact --'
SELECT
  nodename||':'||nodeport          AS node,
  pg_size_pretty(current_bytes)    AS current_size,
  in_moves, pg_size_pretty(in_bytes)   AS inbound,
  out_moves, pg_size_pretty(out_bytes) AS outbound,
  pg_size_pretty(peak_bytes_during_move) AS peak_during_move,
  CASE
    WHEN in_moves = 0 AND out_moves = 0 THEN 'idle'
    WHEN in_bytes  > out_bytes THEN 'net inbound (receives data)'
    WHEN out_bytes > in_bytes  THEN 'net outbound (drains)'
    ELSE 'balanced'
  END AS role_during_move
FROM _r2_moves_per_node
ORDER BY peak_bytes_during_move DESC;


-- R2d: Top-moving colocation groups / tables
\echo
\echo '-- R2d. Top 10 movers by bytes --'
SELECT
  table_name,
  COUNT(*)                                  AS n_moves,
  pg_size_pretty(SUM(shard_size)::bigint)   AS total_bytes,
  pg_size_pretty(MAX(shard_size)::bigint)   AS largest_shard,
  string_agg(DISTINCT sourcename||':'||sourceport, ', ' ORDER BY sourcename||':'||sourceport) AS sources,
  string_agg(DISTINCT targetname||':'||targetport, ', ' ORDER BY targetname||':'||targetport) AS targets
FROM _r2_plan
GROUP BY table_name
ORDER BY SUM(shard_size) DESC
LIMIT 10;


-- R2e: Deferred-drop disk amplification warning
--
-- With citus.defer_drop_after_shard_move = on (default), each successful
-- move leaves the source copy on disk until
-- citus.defer_shard_delete_interval fires. During a large rebalance that
-- means every source transiently holds its current set + not-yet-dropped
-- copies of moved-out shards. For a conservative projection we assume all
-- deferred drops pile up mid-rebalance -> worst-case source footprint
-- equals current_bytes (old copies still there) + 0 (nothing landed).
-- The target, however, holds current_bytes + in_bytes until the source
-- cleanup runs -- that's the real amplification.
\echo
\echo '-- R2e. Deferred-drop / peak-disk warning (targets) --'
SELECT
  nodename||':'||nodeport               AS node,
  pg_size_pretty(current_bytes)         AS current,
  pg_size_pretty(in_bytes)              AS incoming,
  pg_size_pretty(peak_bytes_during_move) AS peak_bytes_during_move,
  CASE
    WHEN current_bytes = 0 THEN 'n/a'
    ELSE ROUND( (peak_bytes_during_move::numeric / current_bytes::numeric) * 100 , 1)::text || '%'
  END AS peak_vs_current,
  CASE
    WHEN in_bytes = 0 THEN 'no inbound'
    WHEN peak_bytes_during_move > 2 * current_bytes
      THEN 'WARN: target will more than double during move; verify disk headroom'
    WHEN peak_bytes_during_move > 1.5 * current_bytes
      THEN 'NOTICE: target grows >1.5x during move; ensure free disk >= in_bytes x 1.5'
    ELSE 'ok'
  END AS disk_amp_verdict
FROM _r2_moves_per_node
WHERE in_bytes > 0
ORDER BY peak_bytes_during_move DESC;


-- R2f: Strategy sanity (cross-check against observed skew intent)
\echo
\echo '-- R2f. Strategy sanity --'
SELECT
  s.strategy_name,
  CASE
    WHEN s.shard_cost_function = 'citus_shard_cost_1'
      THEN 'by_shard_count: counts every shard equally; bad when shards differ in size'
    WHEN s.shard_cost_function = 'citus_shard_cost_by_disk_size'
      THEN 'by_disk_size: uses shard bytes; recommended for mixed-size workloads'
    ELSE 'custom cost function'
  END AS cost_model,
  CASE
    WHEN s.shard_cost_function = 'citus_shard_cost_1'
         AND EXISTS (
           SELECT 1 FROM citus_shards
           GROUP BY colocation_id
           HAVING MAX(shard_size)::numeric > 5 * NULLIF(AVG(shard_size),0)
         )
      THEN 'WARN: shard sizes vary >5x within a colocation group. by_shard_count cannot fix data skew. Switch to by_disk_size.'
    ELSE 'ok'
  END AS recommendation
FROM _r2_strategy s;


-- Headline
\echo
DO $R2H$
DECLARE
  moves           bigint;
  bytes_total     bigint;
  est_seconds     bigint;
  peak_doublers   int;
  skew_strategy_warn boolean;
  line            text;
BEGIN
  SELECT total_moves, total_bytes, estimated_seconds
    INTO moves, bytes_total, est_seconds
  FROM _r2_timing;

  SELECT COUNT(*) INTO peak_doublers
  FROM _r2_moves_per_node
  WHERE in_bytes > 0 AND peak_bytes_during_move > 2 * GREATEST(current_bytes, 1);

  SELECT EXISTS (
    SELECT 1 FROM _r2_strategy s
    WHERE s.shard_cost_function = 'citus_shard_cost_1'
      AND EXISTS (
        SELECT 1 FROM citus_shards
        GROUP BY colocation_id
        HAVING MAX(shard_size)::numeric > 5 * NULLIF(AVG(shard_size),0)
      )
  ) INTO skew_strategy_warn;

  IF moves = 0 THEN
    line := 'OK : cluster is balanced under the default strategy; no moves planned.';
  ELSIF peak_doublers > 0 THEN
    line := format(
      'WARN : %s planned moves, %s to transfer; %s target node(s) would >2x in size during move. Verify free disk before starting citus_rebalance_start().',
      moves, pg_size_pretty(bytes_total), peak_doublers);
  ELSIF skew_strategy_warn THEN
    line := format(
      'WARN : %s moves, %s; shards vary >5x in size but default strategy is by_shard_count. Switch to by_disk_size for effective rebalance.',
      moves, pg_size_pretty(bytes_total));
  ELSE
    line := format(
      'NOTICE : %s moves, %s total; est wall time %s s at configured executors.',
      moves, pg_size_pretty(bytes_total), est_seconds);
  END IF;

  RAISE NOTICE '%', line;
  EXECUTE format('SELECT %L AS headline', line);
END $R2H$;

-- Re-emit headline as a pretty single-column table so the wrapper can tail it.
SELECT (
  SELECT CASE
    WHEN moves = 0 THEN 'OK : cluster is balanced under the default strategy; no moves planned.'
    WHEN EXISTS (SELECT 1 FROM _r2_moves_per_node
                 WHERE in_bytes > 0 AND peak_bytes_during_move > 2 * GREATEST(current_bytes, 1))
      THEN format('WARN : %s planned moves, %s to transfer; target(s) would >2x in size during move. Verify free disk.',
                  moves, pg_size_pretty(bytes_total))
    WHEN EXISTS (
           SELECT 1 FROM _r2_strategy s
           WHERE s.shard_cost_function = 'citus_shard_cost_1'
             AND EXISTS (SELECT 1 FROM citus_shards GROUP BY colocation_id
                         HAVING MAX(shard_size)::numeric > 5 * NULLIF(AVG(shard_size),0))
         )
      THEN format('WARN : %s moves, %s; default strategy is by_shard_count but shards vary >5x. Switch to by_disk_size.',
                  moves, pg_size_pretty(bytes_total))
    ELSE format('NOTICE : %s moves, %s total; est wall time %s s at configured executors.',
                moves, pg_size_pretty(bytes_total), estimated_seconds)
  END
  FROM (SELECT total_moves AS moves, total_bytes AS bytes_total, estimated_seconds FROM _r2_timing) t
) AS "Advisor R2 headline";

DROP TABLE _r2_strategy;
DROP TABLE _r2_plan;
DROP TABLE _r2_worker_sizes;
DROP TABLE _r2_moves_per_node;
DROP TABLE _r2_timing;
