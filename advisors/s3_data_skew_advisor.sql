-- =====================================================================
-- citus_analyze / S3 : data skew advisor
-- ---------------------------------------------------------------------
-- Detects shard-level and node-level size imbalance on distributed
-- tables. Operates on the `citus_shards` view which exposes
-- `shard_size` per placement, eliminating the need to fan out for
-- pg_total_relation_size per shard.
--
-- Three checks:
--   S3a  Per-colocation-group shard skew (max/avg ratio, p95/mean,
--        Gini). The hot-shard anti-pattern.
--   S3b  Per-table top-N largest shards with isolate_tenant_to_new_shard
--        hint when the table's distribution key is an integer/text
--        (likely tenant-id).
--   S3c  Per-worker placement-bytes skew (max/avg ratio). Drives the
--        decision to run citus_rebalance_start(rebalance_strategy
--        => 'by_disk_size').
--
-- Inputs (override with -v):
--   warn_ratio      max/avg ratio that flags WARN; default 2.0
--   crit_ratio      max/avg ratio that flags CRITICAL; default 5.0
--   min_group_bytes smallest colocation group to evaluate (ignore tiny
--                   or empty groups); default 1048576 (1 MB)
--   top_n           number of hot shards per table to list; default 5
--   worker_warn     per-worker max/avg ratio WARN threshold; default 1.3
--   worker_crit     per-worker max/avg ratio CRIT threshold; default 2.0
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?warn_ratio}      \else \set warn_ratio       2.0   \endif
\if :{?crit_ratio}      \else \set crit_ratio       5.0   \endif
\if :{?min_group_bytes} \else \set min_group_bytes  1048576 \endif
\if :{?top_n}           \else \set top_n            5     \endif
\if :{?worker_warn}     \else \set worker_warn      1.3   \endif
\if :{?worker_crit}     \else \set worker_crit      2.0   \endif

\echo
\echo '==================== S3 : data skew advisor ===================='

-- ---------------------------------------------------------------------
-- S3a : per-colocation-group shard skew
-- ---------------------------------------------------------------------
\echo
\echo '-- S3a. Per-colocation-group shard skew --'

WITH shard_bytes AS (
    -- For each physical shard across all colocated tables, sum the
    -- placement sizes. Under R=1 each shardid has one placement; under
    -- R=N we sum to get the bytes-per-shard "bucket".
    SELECT colocation_id, shardid, sum(shard_size)::bigint AS bytes
    FROM citus_shards
    WHERE citus_table_type = 'distributed'
    GROUP BY colocation_id, shardid
),
bucket AS (
    SELECT colocation_id, shardid,
           sum(bytes) AS bucket_bytes
    FROM shard_bytes
    GROUP BY colocation_id, shardid
),
stats AS (
    SELECT colocation_id,
           count(*)                                                       AS n_shards,
           sum(bucket_bytes)                                              AS total_bytes,
           max(bucket_bytes)                                              AS max_bytes,
           min(bucket_bytes)                                              AS min_bytes,
           (avg(bucket_bytes))::bigint                                    AS avg_bytes,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY bucket_bytes)     AS p95_bytes,
           stddev_pop(bucket_bytes)                                       AS stddev_bytes
    FROM bucket
    GROUP BY colocation_id
)
SELECT colocation_id                                                      AS colocation,
       n_shards,
       pg_size_pretty(total_bytes)                                        AS total,
       pg_size_pretty(max_bytes)                                          AS max_shard,
       pg_size_pretty(avg_bytes)                                          AS avg_shard,
       pg_size_pretty(p95_bytes::bigint)                                  AS p95_shard,
       round( (max_bytes::numeric / NULLIF(avg_bytes,0))::numeric, 2)     AS max_over_avg,
       round( (p95_bytes::numeric / NULLIF(avg_bytes,0))::numeric, 2)     AS p95_over_avg,
       CASE
         WHEN total_bytes < :min_group_bytes THEN 'SKIP (tiny group)'
         WHEN (max_bytes::numeric / NULLIF(avg_bytes,0)) >= :crit_ratio THEN 'CRITICAL'
         WHEN (max_bytes::numeric / NULLIF(avg_bytes,0)) >= :warn_ratio THEN 'WARN'
         ELSE 'OK'
       END                                                                AS verdict
FROM stats
ORDER BY max_over_avg DESC NULLS LAST;

-- ---------------------------------------------------------------------
-- S3b : per-table hot-shard list with tenant-isolation hint
-- ---------------------------------------------------------------------
\echo
\echo '-- S3b. Hot shards (top-:top_n per table) + isolate_tenant_to_new_shard hint --'

WITH ranked AS (
    SELECT s.table_name,
           s.shardid,
           s.colocation_id,
           sum(s.shard_size)::bigint AS bytes,
           row_number() OVER (PARTITION BY s.table_name ORDER BY sum(s.shard_size) DESC) AS rnk,
           avg(sum(s.shard_size)) OVER (PARTITION BY s.table_name)::bigint AS tbl_avg
    FROM citus_shards s
    WHERE s.citus_table_type = 'distributed'
    GROUP BY s.table_name, s.shardid, s.colocation_id
),
dist_col AS (
    SELECT p.logicalrelid::regclass AS table_name,
           a.attname AS dist_col,
           t.typname AS dist_type
    FROM pg_dist_partition p
    JOIN pg_attribute a ON a.attrelid = p.logicalrelid
                       AND a.attnum  = (string_to_array(p.partkey, ' ')::int[])[array_position(string_to_array(p.partkey,' ')::text[],'1')]
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE p.partmethod = 'h'
    -- best-effort: partkey parsing is fragile across versions; fall back to NULL
    UNION ALL
    SELECT p.logicalrelid::regclass, NULL, NULL
    FROM pg_dist_partition p WHERE p.partmethod='h'
    AND NOT EXISTS (SELECT 1 FROM pg_attribute a2
                      WHERE a2.attrelid=p.logicalrelid AND a2.attnum > 0 LIMIT 1)
)
SELECT r.table_name,
       r.rnk                                       AS rank,
       r.shardid,
       pg_size_pretty(r.bytes)                     AS size,
       pg_size_pretty(r.tbl_avg)                   AS avg_shard,
       round((r.bytes::numeric / NULLIF(r.tbl_avg,0)), 2) AS ratio,
       CASE WHEN r.bytes::numeric / NULLIF(r.tbl_avg,0) >= :crit_ratio
            THEN format('SELECT isolate_tenant_to_new_shard(%L, <tenant_value>);', r.table_name::text)
            WHEN r.bytes::numeric / NULLIF(r.tbl_avg,0) >= :warn_ratio
            THEN 'investigate: check top tenant_id values in this shard'
            ELSE '' END                            AS hint
FROM ranked r
WHERE r.rnk <= :top_n
ORDER BY r.table_name, r.rnk;

-- ---------------------------------------------------------------------
-- S3c : per-worker placement-bytes skew
-- ---------------------------------------------------------------------
\echo
\echo '-- S3c. Per-worker placement-bytes distribution --'

WITH per_node AS (
    SELECT s.nodename, s.nodeport,
           sum(s.shard_size)::bigint AS bytes,
           count(DISTINCT s.shardid) AS shards
    FROM citus_shards s
    WHERE s.citus_table_type = 'distributed'
    GROUP BY s.nodename, s.nodeport
),
tot AS (SELECT sum(bytes) AS total, avg(bytes)::bigint AS avg FROM per_node)
SELECT pn.nodename                             AS node,
       pn.nodeport                             AS port,
       pn.shards,
       pg_size_pretty(pn.bytes)                AS bytes,
       pg_size_pretty(tot.avg)                 AS avg_node,
       round((pn.bytes::numeric / NULLIF(tot.avg,0)), 2) AS ratio,
       CASE
         WHEN tot.total < :min_group_bytes THEN 'SKIP (tiny)'
         WHEN pn.bytes::numeric / NULLIF(tot.avg,0) >= :worker_crit THEN 'CRITICAL : rebalance(strategy=by_disk_size)'
         WHEN pn.bytes::numeric / NULLIF(tot.avg,0) >= :worker_warn THEN 'WARN : consider rebalance'
         ELSE 'OK'
       END                                    AS verdict
FROM per_node pn, tot
ORDER BY pn.bytes DESC;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\echo '-- S3 headline --'
\pset tuples_only on

WITH shard_bytes AS (
    SELECT colocation_id, shardid, sum(shard_size)::bigint AS bytes
    FROM citus_shards WHERE citus_table_type='distributed'
    GROUP BY colocation_id, shardid
),
stats AS (
    SELECT colocation_id,
           sum(bytes) AS total_bytes,
           max(bytes)::numeric / NULLIF(avg(bytes),0) AS mx_avg
    FROM shard_bytes GROUP BY colocation_id
),
worst_group AS (
    SELECT * FROM stats
    WHERE total_bytes >= :min_group_bytes
    ORDER BY mx_avg DESC NULLS LAST LIMIT 1
),
per_node AS (
    SELECT sum(shard_size)::bigint AS bytes
    FROM citus_shards WHERE citus_table_type='distributed'
    GROUP BY nodename, nodeport
),
node_stats AS (SELECT max(bytes)::numeric / NULLIF(avg(bytes),0) AS mx_avg FROM per_node)
SELECT
  CASE
    WHEN (SELECT mx_avg FROM worst_group) IS NULL
      THEN 'OK : no distributed data large enough to evaluate skew.'
    WHEN (SELECT mx_avg FROM worst_group) >= :crit_ratio
      THEN format('CRITICAL : colocation %s has max/avg = %s (>= %s). Isolate hot tenant or re-pick distribution key.',
                  (SELECT colocation_id FROM worst_group),
                  round((SELECT mx_avg FROM worst_group), 2),
                  :crit_ratio::text)
    WHEN (SELECT mx_avg FROM worst_group) >= :warn_ratio
      THEN format('WARN : colocation %s has max/avg = %s (>= %s). Investigate top tenants.',
                  (SELECT colocation_id FROM worst_group),
                  round((SELECT mx_avg FROM worst_group), 2),
                  :warn_ratio::text)
    ELSE 'OK : shard skew within thresholds.'
  END
UNION ALL
SELECT
  CASE
    WHEN (SELECT mx_avg FROM node_stats) IS NULL THEN ''
    WHEN (SELECT mx_avg FROM node_stats) >= :worker_crit
      THEN format('CRITICAL : node-level bytes max/avg = %s. Run citus_rebalance_start(rebalance_strategy => ''by_disk_size'').',
                  round((SELECT mx_avg FROM node_stats), 2))
    WHEN (SELECT mx_avg FROM node_stats) >= :worker_warn
      THEN format('WARN : node-level bytes max/avg = %s. Consider rebalance.',
                  round((SELECT mx_avg FROM node_stats), 2))
    ELSE 'OK : node-level bytes balanced.'
  END;
\pset tuples_only off
