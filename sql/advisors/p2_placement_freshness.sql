-- =====================================================================
-- P2 : placement stats freshness (pg_dist_placement.shardlength)
-- ---------------------------------------------------------------------
-- Asks: does pg_dist_placement.shardlength reflect the actual on-disk
--       size of each shard, or has it drifted (stale)?
--
-- Why this matters:
--   The Citus shard rebalancer picks moves using a cost model that
--   reads shardlength from pg_dist_placement. If shardlength is stale
--   (usually 0 because it was never updated after COPY/INSERT), the
--   rebalancer's cost model is lying to it -- it will pick the wrong
--   moves, produce an unbalanced plan, or declare the cluster
--   "balanced" when it is not.
--
--   The fix is a cheap call that the rebalancer does for you on
--   citus_rebalance_start(), but many clusters hold stale values
--   for months because no one triggered a rebalance. Refresh with
--     SELECT citus_update_shard_statistics(shardid);
--   or (batch) SELECT citus_update_table_statistics('<table>');
--
-- Model:
--   For each placement:
--     disk_bytes    = live size on the placement's node (via
--                     run_command_on_placements + pg_total_relation_size)
--     recorded      = pg_dist_placement.shardlength
--     drift_ratio   = |disk_bytes - recorded| / max(disk_bytes, 1)
--     stale_zero    = recorded = 0 AND disk_bytes > :p2_zero_floor
--
--   Flag:
--     CRITICAL  stale_zero
--     WARN      drift_ratio > :p2_drift_pct AND disk_bytes > :p2_min_bytes
--     OK        otherwise
--
-- Inputs
--   :p2_drift_pct    default 25     (ratio threshold as a percent)
--   :p2_zero_floor   default 8192   (bytes; ignore placements with ~ empty heap)
--   :p2_min_bytes    default 1048576 (don't bother with tiny placements for % checks)
--   :p2_top_n        default 40
-- =====================================================================

\if :{?p2_drift_pct}  \else \set p2_drift_pct  25       \endif
\if :{?p2_zero_floor} \else \set p2_zero_floor 8192     \endif
\if :{?p2_min_bytes}  \else \set p2_min_bytes  1048576  \endif
\if :{?p2_top_n}      \else \set p2_top_n      40       \endif

\pset pager off
\pset border 2
\pset format aligned

\echo
\echo '==================== P2 : placement stats freshness ===================='

-- Fetch real per-placement on-disk sizes from every node in one shot.
-- Using run_command_on_placements gives us (shardid, nodename, nodeport,
-- success, result) for every single placement; we then join back to
-- pg_dist_placement.shardlength for the comparison.
DROP TABLE IF EXISTS _p2_disk;
CREATE TEMP TABLE _p2_disk (
    shardid bigint,
    nodename text,
    nodeport int,
    success boolean,
    result text
);

-- citus.override_table_visibility is turned OFF by citus_analyze.sh so the
-- per-shard tables are visible to pg_class; regclass below requires the
-- shard relation to be visible on the node executing the query.
INSERT INTO _p2_disk
SELECT p.shardid,
       n.nodename, n.nodeport,
       r.success, r.result
FROM pg_dist_shard s
JOIN pg_dist_placement p ON p.shardid = s.shardid
JOIN pg_dist_node n ON n.groupid = p.groupid AND n.isactive AND n.noderole='primary'
JOIN LATERAL run_command_on_placements(
    s.logicalrelid,
    'SELECT pg_total_relation_size(''%s'')::text'
) r ON r.shardid = p.shardid
   AND r.nodename = n.nodename
   AND r.nodeport = n.nodeport;

DROP TABLE IF EXISTS _p2_join;
CREATE TEMP TABLE _p2_join AS
SELECT
    s.logicalrelid::regclass::text                       AS table_name,
    p.shardid,
    p.placementid,
    p.groupid,
    n.nodename,
    n.nodeport,
    p.shardlength::bigint                                AS recorded_bytes,
    CASE WHEN d.success AND d.result ~ '^[0-9]+$'
         THEN d.result::bigint ELSE NULL END             AS disk_bytes,
    d.success                                            AS probe_success
FROM pg_dist_shard s
JOIN pg_dist_placement p ON p.shardid = s.shardid
JOIN pg_dist_node n ON n.groupid = p.groupid AND n.isactive
LEFT JOIN _p2_disk d
       ON d.shardid = p.shardid
      AND d.nodename = n.nodename
      AND d.nodeport = n.nodeport;

-- Classify drift.
DROP TABLE IF EXISTS _p2_flagged;
CREATE TEMP TABLE _p2_flagged AS
SELECT
    j.*,
    CASE
      WHEN NOT j.probe_success OR j.disk_bytes IS NULL
        THEN 'INFO'
      -- Stale-zero: shardlength is 0 but there's measurable data on disk.
      -- Rebalancer treats this placement as weightless and mis-plans.
      WHEN j.recorded_bytes = 0
           AND j.disk_bytes > (:p2_zero_floor)::bigint
        THEN 'CRITICAL'
      -- Large percentage drift on a non-trivial placement.
      WHEN j.disk_bytes > (:p2_min_bytes)::bigint
           AND abs(j.disk_bytes - j.recorded_bytes)::numeric
               / GREATEST(j.disk_bytes, 1)::numeric
               * 100.0 > (:p2_drift_pct)::numeric
        THEN 'WARN'
      ELSE 'OK'
    END AS severity,
    CASE
      WHEN j.disk_bytes IS NULL THEN NULL
      ELSE round(abs(j.disk_bytes - j.recorded_bytes)::numeric
                 / GREATEST(j.disk_bytes, 1)::numeric * 100.0, 1)
    END AS drift_pct
FROM _p2_join j;

\echo
\echo '-- P2a. Top drifted placements --'
SELECT severity, table_name, shardid,
       nodename || ':' || nodeport      AS placement,
       pg_size_pretty(recorded_bytes)   AS recorded,
       CASE WHEN disk_bytes IS NULL THEN 'unreachable'
            ELSE pg_size_pretty(disk_bytes) END AS on_disk,
       COALESCE(drift_pct::text || ' %%', 'n/a') AS drift
FROM _p2_flagged
WHERE severity IN ('CRITICAL','WARN','INFO')
ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
         COALESCE(drift_pct, 0) DESC,
         shardid
LIMIT :p2_top_n;

-- Rebalance-pending check: if there is any running or pending rebalance
-- job, stale stats are actively dangerous (the running rebalance will
-- use them). pg_dist_background_job exists on Citus >= 11.1; on older
-- versions we degrade to 'unknown' (= false).
DROP TABLE IF EXISTS _p2_rebal;
CREATE TEMP TABLE _p2_rebal (has_active_rebalance boolean);

DO $$
DECLARE
  has_tbl boolean;
  has_active boolean := false;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'pg_dist_background_job'
      AND n.nspname = 'pg_catalog'
  ) INTO has_tbl;

  IF has_tbl THEN
    EXECUTE $q$
      SELECT EXISTS (
        SELECT 1 FROM pg_catalog.pg_dist_background_job
        WHERE state IN ('running','scheduled')
          AND job_type LIKE 'rebalance%'
      )
    $q$ INTO has_active;
  END IF;

  INSERT INTO _p2_rebal VALUES (has_active);
END $$;

\echo
\echo '-- P2b. Summary --'
SELECT
  count(*) FILTER (WHERE severity='CRITICAL')     AS stale_zero_count,
  count(*) FILTER (WHERE severity='WARN')         AS warn_drift_count,
  count(*) FILTER (WHERE severity='INFO')         AS unprobed_count,
  count(*) FILTER (WHERE severity='OK')           AS fresh_count,
  (SELECT has_active_rebalance FROM _p2_rebal)    AS active_rebalance
FROM _p2_flagged;

-- Headline. CRITICAL when there is BOTH stale stats AND an active
-- rebalance; else WARN for significant drift; else INFO for unprobed.
\echo
\pset tuples_only on
SELECT CASE
  WHEN (SELECT count(*) FROM _p2_flagged WHERE severity='CRITICAL') > 0
       AND (SELECT has_active_rebalance FROM _p2_rebal) THEN
    format('CRITICAL : %s placement(s) report shardlength=0 while data is on disk AND a rebalance is running. Cost model is lying; stop the rebalance and refresh: SELECT citus_update_table_statistics(''<table>'').',
           (SELECT count(*) FROM _p2_flagged WHERE severity='CRITICAL'))
  WHEN (SELECT count(*) FROM _p2_flagged WHERE severity='CRITICAL') > 0 THEN
    format('WARN : %s placement(s) report shardlength=0 while data is on disk. Rebalancer cost model is wrong; refresh with citus_update_table_statistics() before the next rebalance.',
           (SELECT count(*) FROM _p2_flagged WHERE severity='CRITICAL'))
  WHEN (SELECT count(*) FROM _p2_flagged WHERE severity='WARN') > 0 THEN
    format('WARN : %s placement(s) have shardlength drift > %s%% vs on-disk size.',
           (SELECT count(*) FROM _p2_flagged WHERE severity='WARN'),
           :p2_drift_pct)
  WHEN (SELECT count(*) FROM _p2_flagged WHERE severity='INFO') > 0 THEN
    format('INFO : %s placement(s) could not be probed; fresh state unknown.',
           (SELECT count(*) FROM _p2_flagged WHERE severity='INFO'))
  WHEN (SELECT count(*) FROM _p2_flagged) = 0 THEN
    'INFO : no distributed-table placements found.'
  ELSE
    'OK : pg_dist_placement.shardlength matches on-disk sizes within tolerance.'
END AS "P2 headline";
\pset tuples_only off

DROP TABLE _p2_rebal;
DROP TABLE _p2_flagged;
DROP TABLE _p2_join;
DROP TABLE _p2_disk;
