-- =====================================================================
-- citus_analyze / D1 : disk capacity & shard-growth runway
-- ---------------------------------------------------------------------
-- Answers the "when does a node fill up?" question, which in Citus is
-- strictly a node-local property — the coordinator can exhaust free
-- space independently of every worker, and vice versa. Predicts
-- days-until-full per node from current database+WAL footprint, the
-- user-supplied disk size, and a growth rate derived from pg_stat_*.
--
-- Model (per node):
--   used_bytes      = pg_database_size(current DB) + WAL dir size
--   free_bytes      = total_disk_bytes - used_bytes
--   tuples_net/s    = (sum(tup_ins + tup_upd - tup_del) across user tables)
--                       / epoch_seconds_since(COALESCE(stats_reset,
--                                                     postmaster_start))
--   bytes_per_tuple = pg_database_size / GREATEST(sum(n_live_tup), 1)
--   growth_bytes/day = tuples_net/s * bytes_per_tuple * 86400
--   runway_days      = free_bytes / NULLIF(growth_bytes_per_day, 0)
--
-- Inputs (override with -v):
--   coord_disk_mb        total disk MB on coordinator (0 = unknown; skip)
--   worker_disk_mb       total disk MB per worker    (0 = unknown; skip)
--   warn_runway_days     WARN if runway below this; default 90
--   crit_runway_days     CRITICAL if runway below this; default 30
--   warn_free_pct        WARN if free% below this; default 20
--   crit_free_pct        CRITICAL if free% below this; default 10
--   top_n                number of top growers/hogs to list; default 5
--   min_history_sec      require >= this many seconds since stats baseline
--                        before reporting a runway; default 3600 (1 hour).
--
-- Caveats
--   * used_bytes is a node-local LOWER BOUND: it covers the current
--     database (incl. its out-of-PGDATA tablespaces, via
--     pg_database_size) plus the WAL directory (which already includes
--     WAL pinned by replication slots). It does NOT count: other
--     databases on the same instance, shared/global cluster files,
--     temp-file spills, PG logs, backups, core dumps, or filesystem
--     reserved blocks. Reserve at least 10-20 % extra headroom.
--   * Growth rate is the historical average since stats were last reset
--     (or since postmaster start). Recent bursts or idle windows distort
--     the estimate; if the stats baseline is younger than
--     :min_history_sec the runway is suppressed as "insufficient history".
--   * bytes_per_tuple is a database-wide average; tables with large
--     TOAST columns grow faster than the average implies. UPDATE-heavy
--     workloads inflate tuple churn without growing disk under HOT,
--     so the runway can be pessimistic for such workloads.
--   * WAL retention spikes (stuck replication slots, failing archiver)
--     are not projected into growth_bpd — see REP1 for that.
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?coord_disk_mb}     \else \set coord_disk_mb     0   \endif
\if :{?worker_disk_mb}    \else \set worker_disk_mb    0   \endif
\if :{?warn_runway_days}  \else \set warn_runway_days  90  \endif
\if :{?crit_runway_days}  \else \set crit_runway_days  30  \endif
\if :{?warn_free_pct}     \else \set warn_free_pct     20  \endif
\if :{?crit_free_pct}     \else \set crit_free_pct     10  \endif
\if :{?top_n}             \else \set top_n             5   \endif
\if :{?min_history_sec}   \else \set min_history_sec   3600 \endif

\echo
\echo '==================== D1 : disk capacity & growth runway ===================='

-- ---------------------------------------------------------------------
-- Per-node collection via run_command_on_all_nodes.
-- Each node returns a single pipe-separated string:
--   db_bytes|wal_bytes|tuples_net|elapsed_sec|live_tuples
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _d1_raw;
CREATE TEMP TABLE _d1_raw (
    nodeid      int,
    success     boolean,
    result      text
);

INSERT INTO _d1_raw
SELECT nodeid, success, result
FROM run_command_on_all_nodes(
$CMD$
WITH s AS (
  SELECT
    pg_database_size(current_database())                     AS db_bytes,
    COALESCE((SELECT sum(size) FROM pg_ls_waldir()), 0)      AS wal_bytes,
    COALESCE((SELECT sum(tup_inserted + tup_updated - tup_deleted)
                FROM pg_stat_database
               WHERE datname = current_database()), 0)       AS tuples_net,
    EXTRACT(EPOCH FROM now() - COALESCE(
        (SELECT stats_reset FROM pg_stat_database
          WHERE datname=current_database()),
        pg_postmaster_start_time()))::bigint                 AS elapsed_sec,
    COALESCE((SELECT sum(n_live_tup)::bigint
                FROM pg_stat_user_tables), 0)                AS live_tuples
)
SELECT db_bytes::text || '|' || wal_bytes || '|' ||
       tuples_net || '|' || elapsed_sec || '|' || live_tuples
  FROM s;
$CMD$
, parallel := true);

-- Parse the pipe-separated string into typed columns and join node role.
DROP TABLE IF EXISTS _d1;
CREATE TEMP TABLE _d1 AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata THEN 'worker (MX entry)'
         ELSE 'worker' END                                    AS role,
    n.nodename, n.nodeport, r.success,
    n.hasmetadata, n.groupid,
    CASE WHEN r.success THEN split_part(r.result,'|',1)::bigint END AS db_bytes,
    CASE WHEN r.success THEN split_part(r.result,'|',2)::bigint END AS wal_bytes,
    CASE WHEN r.success THEN split_part(r.result,'|',3)::bigint END AS tuples_net,
    CASE WHEN r.success THEN split_part(r.result,'|',4)::bigint END AS elapsed_sec,
    CASE WHEN r.success THEN split_part(r.result,'|',5)::bigint END AS live_tuples,
    -- disk_mb comes from the user; map coordinator vs worker.
    CASE WHEN n.groupid = 0 THEN :coord_disk_mb::bigint
         ELSE :worker_disk_mb::bigint END                     AS disk_mb_input
FROM _d1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE n.isactive
ORDER BY n.groupid, n.nodeport;

-- Per-node derived fields
DROP TABLE IF EXISTS _d1_calc;
CREATE TEMP TABLE _d1_calc AS
SELECT *,
    (db_bytes + wal_bytes)                                    AS used_bytes,
    (disk_mb_input * 1048576)::bigint                         AS total_bytes,
    CASE WHEN disk_mb_input > 0
         THEN (disk_mb_input * 1048576)::bigint - (db_bytes + wal_bytes)
         END                                                  AS free_bytes,
    CASE WHEN disk_mb_input > 0 AND disk_mb_input * 1048576 > 0
         THEN ROUND(100.0 * ((disk_mb_input * 1048576)::numeric
                           - (db_bytes + wal_bytes))
                        / NULLIF(disk_mb_input * 1048576, 0), 1)
         END                                                  AS free_pct,
    -- tuples/sec (net)
    CASE WHEN elapsed_sec > 0
         THEN tuples_net::numeric / elapsed_sec
         ELSE 0 END                                           AS tuples_per_sec,
    -- bytes/tuple (DB-wide average)
    CASE WHEN live_tuples > 0
         THEN db_bytes::numeric / live_tuples
         ELSE NULL END                                        AS bytes_per_tuple,
    -- bytes/day (NULL = cannot estimate; 0 = idle; negative = net deletes)
    CASE
      WHEN elapsed_sec < :min_history_sec::bigint THEN NULL   -- insufficient history
      WHEN live_tuples = 0                         THEN NULL   -- cannot derive bytes/tuple
      WHEN tuples_net = 0                          THEN 0       -- idle
      ELSE (tuples_net::numeric / elapsed_sec)
           * (db_bytes::numeric / live_tuples)
           * 86400
    END                                                       AS growth_bpd
FROM _d1;

-- Per-node report
\pset tuples_only on
SELECT format(
'----  %s:%s  (role: %s)
  total disk                   : %s
  used  (db + WAL)             : %s MB  (db %s MB, WAL %s MB)
  free                         : %s  (%s%% of disk)
  stats baseline               : %s sec ago
  net tuples since baseline    : %s   (avg %s tuples/sec)
  avg bytes/tuple (DB-wide)    : %s
  projected growth / day       : %s
  runway at current rate       : %s
  %s',
  c.nodename, c.nodeport, c.role,
  CASE WHEN c.disk_mb_input > 0 THEN format('%s MB', c.disk_mb_input)
       ELSE 'unknown (pass -v coord_disk_mb=... or -v worker_disk_mb=...)'
  END,
  round((c.used_bytes / 1048576.0)::numeric, 1),
  round((c.db_bytes  / 1048576.0)::numeric, 1),
  round((c.wal_bytes / 1048576.0)::numeric, 1),
  CASE WHEN c.free_bytes IS NULL THEN 'unknown'
       ELSE format('%s MB', round((c.free_bytes / 1048576.0)::numeric, 1))
  END,
  COALESCE(c.free_pct::text, 'n/a'),
  c.elapsed_sec,
  c.tuples_net,
  round(c.tuples_per_sec, 2),
  CASE WHEN c.bytes_per_tuple IS NULL THEN 'n/a (empty DB)'
       ELSE round(c.bytes_per_tuple, 1)::text || ' B'
  END,
  CASE
    WHEN c.growth_bpd IS NULL AND c.elapsed_sec < :min_history_sec::bigint
      THEN format('insufficient history (need >= %s sec since stats baseline)', :min_history_sec)
    WHEN c.growth_bpd IS NULL THEN 'n/a (empty DB)'
    WHEN c.growth_bpd = 0     THEN 'idle (no net tuple churn)'
    WHEN c.growth_bpd < 0     THEN round((c.growth_bpd / 1048576.0)::numeric, 2)::text
                                   || ' MB/day (net deletes; disk may not shrink until vacuum)'
    ELSE round((c.growth_bpd / 1048576.0)::numeric, 2)::text || ' MB/day'
  END,
  CASE
    WHEN c.free_bytes IS NULL THEN 'n/a (disk size unknown)'
    WHEN c.growth_bpd IS NULL AND c.elapsed_sec < :min_history_sec::bigint
      THEN 'n/a (insufficient history)'
    WHEN c.growth_bpd IS NULL THEN 'n/a (empty DB)'
    WHEN c.growth_bpd <= 0    THEN 'n/a (no positive growth measured)'
    ELSE round((c.free_bytes / c.growth_bpd)::numeric, 0)::text || ' days'
  END,
  -- per-node verdict
  CASE
    WHEN NOT c.success
      THEN format('ERROR : node unreachable during collection.')
    WHEN c.disk_mb_input = 0
      THEN format('INFO : pass -v %s_disk_mb=<total disk MB> to get a runway verdict.',
                  CASE WHEN c.groupid = 0 THEN 'coord' ELSE 'worker' END)
    WHEN c.free_pct < :crit_free_pct::numeric
      THEN format('CRITICAL : only %s%% free — node will fill up imminently.', c.free_pct)
    WHEN c.growth_bpd > 0
         AND (c.free_bytes / c.growth_bpd) < :crit_runway_days::numeric
      THEN format('CRITICAL : runway %s days (< %s). Scale disk or rebalance now.',
                  round((c.free_bytes / c.growth_bpd)::numeric, 0),
                  :crit_runway_days)
    WHEN c.free_pct < :warn_free_pct::numeric
      THEN format('WARN : only %s%% free (< %s%%) — schedule disk expansion.',
                  c.free_pct, :warn_free_pct)
    WHEN c.growth_bpd > 0
         AND (c.free_bytes / c.growth_bpd) < :warn_runway_days::numeric
      THEN format('WARN : runway %s days (< %s). Plan disk expansion.',
                  round((c.free_bytes / c.growth_bpd)::numeric, 0),
                  :warn_runway_days)
    WHEN c.growth_bpd IS NULL AND c.elapsed_sec < :min_history_sec::bigint
      THEN format('INFO : free%% OK; runway not estimated (stats baseline only %s sec old).',
                  c.elapsed_sec)
    ELSE 'OK : plenty of runway under current growth rate.'
  END
) AS "Per-node disk model"
FROM _d1_calc c ORDER BY c.groupid, c.nodeport;
\pset tuples_only off

-- ---------------------------------------------------------------------
-- D1b : top N largest distributed tables (across all shards)
-- ---------------------------------------------------------------------
\echo
\echo '-- D1b. Top distributed tables by total size --'
SELECT
    s.table_name::text                                         AS table_name,
    count(*)::int                                              AS shards,
    pg_size_pretty(sum(s.shard_size))                          AS total_size,
    pg_size_pretty(max(s.shard_size))                          AS max_shard,
    pg_size_pretty(avg(s.shard_size)::bigint)                  AS avg_shard,
    round(max(s.shard_size)::numeric
          / NULLIF(avg(s.shard_size), 0), 2)                   AS max_avg_ratio
FROM citus_shards s
GROUP BY s.table_name
ORDER BY sum(s.shard_size) DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- D1c : top N largest individual shards (hot-shard candidates)
-- ---------------------------------------------------------------------
\echo
\echo '-- D1c. Top individual shards by size --'
SELECT
    s.shardid,
    s.table_name::text                                         AS table_name,
    s.nodename || ':' || s.nodeport                            AS on_node,
    pg_size_pretty(s.shard_size)                               AS size
FROM citus_shards s
ORDER BY s.shard_size DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
SELECT CASE
  WHEN bool_or(NOT success) THEN
    format('WARN : %s node(s) unreachable during collection.',
           count(*) FILTER (WHERE NOT success))
  WHEN bool_or(sev = 'CRITICAL') THEN
    format('CRITICAL : %s node(s) will fill up within %s days or have <%s%% free.',
           count(*) FILTER (WHERE sev='CRITICAL'),
           :crit_runway_days, :crit_free_pct)
  WHEN bool_or(sev = 'WARN') THEN
    format('WARN : %s node(s) have disk-capacity warnings.',
           count(*) FILTER (WHERE sev='WARN'))
  WHEN bool_or(sev = 'INFO') THEN
    'INFO : pass -v coord_disk_mb=... -v worker_disk_mb=... to get runway verdicts.'
  ELSE 'OK : all nodes have adequate disk runway.'
END
FROM (
  SELECT success,
    CASE
      WHEN NOT success THEN 'ERR'
      WHEN disk_mb_input = 0 THEN 'INFO'
      WHEN free_pct < :crit_free_pct::numeric THEN 'CRITICAL'
      WHEN growth_bpd > 0 AND (free_bytes / growth_bpd) < :crit_runway_days::numeric THEN 'CRITICAL'
      WHEN free_pct < :warn_free_pct::numeric THEN 'WARN'
      WHEN growth_bpd > 0 AND (free_bytes / growth_bpd) < :warn_runway_days::numeric THEN 'WARN'
      ELSE 'OK'
    END AS sev
  FROM _d1_calc
) s;
\pset tuples_only off

DROP TABLE _d1_calc;
DROP TABLE _d1;
DROP TABLE _d1_raw;
