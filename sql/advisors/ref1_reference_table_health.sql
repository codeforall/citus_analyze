-- REF1: Reference-table health.
-- Reference tables are replicated in full to every worker that hosts
-- placements. They enable local joins from any shard but cost RAM +
-- disk * N_workers. Failure modes:
--   1. Oversized: each copy occupies shared_buffers / disk on every
--      worker; at >100 MB the cost multiplies.
--   2. Placement count mismatch: repmodel='t' + partmethod='n' should
--      mean exactly one placement per active worker (plus coord if it
--      hosts shards). Mismatch = failed replicate_reference_tables.
--   3. Size drift across placements: a reference table copy diverging
--      across workers signals replication broke after a write.
--   4. High write-rate: writes hit every replica simultaneously and
--      serialize with 2PC; large refs + frequent writes = bottleneck.
--
-- Inputs (override via psql -v):
--   ref1_size_warn_mb  = 100    -- per-copy size that triggers WARN
--   ref1_size_crit_mb  = 1024   -- per-copy size that triggers CRITICAL
--   ref1_drift_pct     = 10     -- size drift across placements
--                                  that triggers WARN

\pset pager off
\pset border 2
\pset format aligned

\set ref1_size_warn_mb 100
\set ref1_size_crit_mb 1024
\set ref1_drift_pct    10

\echo '==================== REF1 : reference-table health ===================='

-- Reference-table inventory (partmethod='n', repmodel='t').
-- 'n' = non-distributed (Citus terminology: replicated to every worker),
-- 't' = two-phase-commit-replicated (the reference-table flavor).
DROP TABLE IF EXISTS _ref1_tables;
CREATE TEMP TABLE _ref1_tables AS
SELECT
  p.logicalrelid::regclass::text  AS table_name,
  p.colocationid,
  n.nspname                        AS schema_name,
  c.relname                        AS rel_name
FROM pg_dist_partition p
JOIN pg_class c      ON c.oid = p.logicalrelid
JOIN pg_namespace n  ON n.oid = c.relnamespace
WHERE p.partmethod = 'n' AND p.repmodel = 't';

-- Expected placement count: all active primary nodes.
-- Citus replicates reference tables to every active primary regardless of
-- shouldhaveshards (the coord is included unless explicitly excluded via
-- citus_set_node_property('shouldhaveshards',false) BEFORE the table was
-- created; existing refs are NOT auto-removed from the coord on that flip).
DROP TABLE IF EXISTS _ref1_expected;
CREATE TEMP TABLE _ref1_expected AS
SELECT COUNT(*)::int AS expected_placements
FROM pg_dist_node
WHERE isactive = true
  AND noderole = 'primary';

-- Per-placement size from citus_shards.
DROP TABLE IF EXISTS _ref1_placements;
CREATE TEMP TABLE _ref1_placements AS
SELECT
  cs.table_name::text        AS table_name,
  cs.shardid,
  cs.nodename,
  cs.nodeport,
  cs.shard_size::bigint      AS shard_bytes
FROM citus_shards cs
WHERE cs.citus_table_type = 'reference';

-- Summary per reference table.
DROP TABLE IF EXISTS _ref1_summary;
CREATE TEMP TABLE _ref1_summary AS
SELECT
  t.table_name,
  t.colocationid,
  COUNT(p.shardid)                                       AS actual_placements,
  COALESCE(MAX(p.shard_bytes), 0)::bigint                AS max_bytes,
  COALESCE(MIN(p.shard_bytes), 0)::bigint                AS min_bytes,
  COALESCE(AVG(p.shard_bytes), 0)::bigint                AS avg_bytes,
  CASE
    WHEN COALESCE(AVG(p.shard_bytes),0) = 0 THEN 0
    ELSE ROUND(
      ((COALESCE(MAX(p.shard_bytes),0) - COALESCE(MIN(p.shard_bytes),0))::numeric
        / NULLIF(AVG(p.shard_bytes),0)) * 100, 2)
  END                                                    AS drift_pct
FROM _ref1_tables t
LEFT JOIN _ref1_placements p
  ON p.table_name = t.table_name
GROUP BY t.table_name, t.colocationid;

-- REF1a: inventory
\echo
\echo '-- REF1a. Reference-table inventory --'
SELECT
  s.table_name,
  s.colocationid,
  s.actual_placements,
  (SELECT expected_placements FROM _ref1_expected) AS expected_placements,
  pg_size_pretty(s.avg_bytes)                       AS avg_copy_size,
  pg_size_pretty(s.max_bytes)                       AS max_copy_size,
  s.drift_pct::text || '%'                          AS size_drift,
  CASE
    WHEN s.actual_placements = 0 THEN 'no placements'
    WHEN s.actual_placements < (SELECT expected_placements FROM _ref1_expected)
      THEN 'missing placements'
    WHEN s.actual_placements > (SELECT expected_placements FROM _ref1_expected)
      THEN 'extra placements'
    ELSE 'ok'
  END AS placement_status
FROM _ref1_summary s
ORDER BY s.max_bytes DESC;

-- REF1b: oversized reference tables (per-copy size vs thresholds)
\echo
\echo '-- REF1b. Oversized reference tables --'
SELECT
  s.table_name,
  pg_size_pretty(s.max_bytes)                                                 AS max_copy,
  (SELECT expected_placements FROM _ref1_expected)                            AS copies,
  pg_size_pretty(s.max_bytes * (SELECT expected_placements FROM _ref1_expected)) AS cluster_footprint,
  CASE
    WHEN s.max_bytes >= (:ref1_size_crit_mb)::bigint * 1048576 THEN 'CRITICAL'
    WHEN s.max_bytes >= (:ref1_size_warn_mb)::bigint * 1048576 THEN 'WARN'
    ELSE 'ok'
  END AS verdict
FROM _ref1_summary s
WHERE s.max_bytes >= (:ref1_size_warn_mb)::bigint * 1048576
ORDER BY s.max_bytes DESC;

-- REF1c: Placement count mismatches
\echo
\echo '-- REF1c. Placement count mismatches --'
SELECT
  s.table_name,
  s.actual_placements,
  (SELECT expected_placements FROM _ref1_expected) AS expected_placements,
  CASE
    WHEN s.actual_placements < (SELECT expected_placements FROM _ref1_expected)
      THEN 'CRITICAL: run SELECT replicate_reference_tables();'
    WHEN s.actual_placements > (SELECT expected_placements FROM _ref1_expected)
      THEN 'WARN: extra placements -- check pg_dist_cleanup and citus_cleanup_orphaned_resources()'
    ELSE 'ok'
  END AS recommendation
FROM _ref1_summary s
WHERE s.actual_placements <> (SELECT expected_placements FROM _ref1_expected);

-- REF1d: size drift across workers
\echo
\echo '-- REF1d. Size drift across placements --'
SELECT
  s.table_name,
  pg_size_pretty(s.min_bytes) AS min_copy,
  pg_size_pretty(s.max_bytes) AS max_copy,
  s.drift_pct::text || '%'     AS drift_pct,
  CASE
    WHEN s.drift_pct >= (:ref1_drift_pct)::numeric
      THEN 'WARN: copies diverge -- recent write failed to replicate on a worker, or autovacuum ran unevenly. Inspect with citus_shards.'
    ELSE 'ok'
  END AS recommendation
FROM _ref1_summary s
WHERE s.max_bytes > 0 AND s.drift_pct >= (:ref1_drift_pct)::numeric;

-- REF1e: per-placement breakdown (only for non-trivial refs, to avoid noise)
\echo
\echo '-- REF1e. Per-placement sizes (non-trivial refs only) --'
SELECT
  p.table_name,
  p.nodename||':'||p.nodeport AS node,
  pg_size_pretty(p.shard_bytes) AS copy_size
FROM _ref1_placements p
JOIN _ref1_summary s ON s.table_name = p.table_name
WHERE s.max_bytes >= (:ref1_size_warn_mb)::bigint * 1048576
   OR s.drift_pct >= (:ref1_drift_pct)::numeric
   OR s.actual_placements <> (SELECT expected_placements FROM _ref1_expected)
ORDER BY p.table_name, p.nodename, p.nodeport;


-- Headline
\echo
SELECT (
  SELECT CASE
    WHEN (SELECT COUNT(*) FROM _ref1_tables) = 0
      THEN 'OK : no reference tables defined in this cluster.'
    WHEN EXISTS (
           SELECT 1 FROM _ref1_summary
           WHERE actual_placements < (SELECT expected_placements FROM _ref1_expected)
         )
      THEN format(
        'CRITICAL : %s reference table(s) have missing placements -- run SELECT replicate_reference_tables();',
        (SELECT COUNT(*) FROM _ref1_summary
          WHERE actual_placements < (SELECT expected_placements FROM _ref1_expected))
      )
    WHEN EXISTS (
           SELECT 1 FROM _ref1_summary
           WHERE max_bytes >= (:ref1_size_crit_mb)::bigint * 1048576
         )
      THEN format(
        'CRITICAL : %s reference table(s) exceed %s MB per copy; cluster footprint is that x worker count. Consider distributing instead.',
        (SELECT COUNT(*) FROM _ref1_summary
          WHERE max_bytes >= (:ref1_size_crit_mb)::bigint * 1048576),
        (:ref1_size_crit_mb)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _ref1_summary
           WHERE max_bytes >= (:ref1_size_warn_mb)::bigint * 1048576
         )
      THEN format(
        'WARN : %s reference table(s) are oversized (>= %s MB per copy). See REF1b.',
        (SELECT COUNT(*) FROM _ref1_summary
          WHERE max_bytes >= (:ref1_size_warn_mb)::bigint * 1048576),
        (:ref1_size_warn_mb)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _ref1_summary
           WHERE max_bytes > 0 AND drift_pct >= (:ref1_drift_pct)::numeric
         )
      THEN format(
        'WARN : %s reference table(s) have >= %s%% size drift across workers. Investigate replication. See REF1d.',
        (SELECT COUNT(*) FROM _ref1_summary
          WHERE max_bytes > 0 AND drift_pct >= (:ref1_drift_pct)::numeric),
        (:ref1_drift_pct)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _ref1_summary
           WHERE actual_placements > (SELECT expected_placements FROM _ref1_expected)
         )
      THEN 'WARN : reference table(s) have extra placements. See REF1c.'
    ELSE format(
      'OK : %s reference table(s), all healthy (placements complete, no drift, sizes under %s MB/copy).',
      (SELECT COUNT(*) FROM _ref1_tables), (:ref1_size_warn_mb)::text
    )
  END
) AS "Advisor REF1 headline";

DROP TABLE _ref1_tables;
DROP TABLE _ref1_expected;
DROP TABLE _ref1_placements;
DROP TABLE _ref1_summary;
