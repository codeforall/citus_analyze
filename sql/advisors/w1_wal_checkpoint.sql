-- W1: WAL & checkpoint pressure.
-- Monitors the write path that backs every commit on every worker:
-- WAL volume, checkpoint trigger mix, archiver health, replication
-- safety GUCs. Misconfiguration here either stalls writes (full WAL,
-- archive backlog), amplifies I/O (max_wal_size too small -> requested
-- checkpoints dominate), or silently loses durability (wal_level,
-- synchronous_commit set wrong when replication is expected).
--
-- PG17+: pg_stat_checkpointer carries checkpoint counters.
-- PG<17 : pg_stat_bgwriter carries checkpoints_timed/checkpoints_req.
-- PG16+ : pg_stat_wal has per-node WAL bytes/records.
--
-- Inputs (override via psql -v):
--   w1_req_ratio_pct      = 10     -- warn if checkpoints_req / total > this
--   w1_archive_stale_min  = 15     -- warn if last archive > this many minutes old
--                                     AND archive_mode is on
--   w1_min_wal_buffers_mb = 16     -- warn if wal_buffers < this on any node

\pset pager off
\pset border 2
\pset format aligned

\set w1_req_ratio_pct      10
\set w1_archive_stale_min  15
\set w1_min_wal_buffers_mb 16

\echo '==================== W1 : WAL & checkpoint pressure ===================='

-- Fan-out: collect per-node WAL / checkpoint / archiver stats + GUCs as JSON.
DROP TABLE IF EXISTS _w1_raw;
CREATE TEMP TABLE _w1_raw (nodeid int, success boolean, result text);

INSERT INTO _w1_raw (nodeid, success, result)
SELECT r.nodeid, r.success, r.result
FROM (
  -- Build the remote payload on the coord so we can splice in the
  -- right checkpoint source at PARSE time: pg_stat_checkpointer on
  -- PG17+, pg_stat_bgwriter on <PG17. (The columns don't coexist, so
  -- a CASE at runtime still parse-fails.) We assume worker PG version
  -- matches coord -- V1 enforces that invariant.
  SELECT format($CMD$
WITH
  v AS (
    SELECT current_setting('server_version_num')::int AS pgv
  ),
  gucs AS (
    SELECT jsonb_object_agg(name, setting) AS g
    FROM pg_settings
    WHERE name IN (
      'wal_level', 'wal_compression', 'wal_buffers', 'max_wal_size',
      'min_wal_size', 'checkpoint_timeout', 'checkpoint_completion_target',
      'checkpoint_warning', 'checkpoint_flush_after', 'wal_writer_delay',
      'wal_writer_flush_after', 'synchronous_commit',
      'synchronous_standby_names', 'archive_mode', 'archive_command',
      'archive_library', 'archive_timeout', 'fsync', 'full_page_writes',
      'max_replication_slots', 'max_wal_senders', 'hot_standby'
    )
  ),
  cp AS (
    SELECT %s AS c
  ),
  wal AS (
    SELECT %s AS w
  ),
  arch AS (
    SELECT jsonb_build_object(
      'archived_count',     archived_count,
      'failed_count',       failed_count,
      'last_archived_time', last_archived_time::text,
      'last_failed_time',   last_failed_time::text,
      'stats_reset',        stats_reset::text
    ) AS a
    FROM pg_stat_archiver
  ),
  repl AS (
    SELECT COALESCE(
      jsonb_agg(jsonb_build_object(
        'application_name', application_name,
        'client_addr',      client_addr::text,
        'state',            state,
        'sync_state',       sync_state,
        'write_lag_ms',     EXTRACT(EPOCH FROM write_lag)*1000,
        'flush_lag_ms',     EXTRACT(EPOCH FROM flush_lag)*1000,
        'replay_lag_ms',    EXTRACT(EPOCH FROM replay_lag)*1000
      )), '[]'::jsonb
    ) AS r
    FROM pg_stat_replication
  ),
  slots AS (
    SELECT COALESCE(
      jsonb_agg(jsonb_build_object(
        'slot_name',   slot_name,
        'slot_type',   slot_type,
        'active',      active,
        'restart_lag_bytes',
          CASE WHEN restart_lsn IS NOT NULL
               THEN (pg_current_wal_lsn() - restart_lsn)::bigint
               ELSE NULL END
      )), '[]'::jsonb
    ) AS s
    FROM pg_replication_slots
  )
SELECT jsonb_build_object(
  'server_version_num', (SELECT pgv FROM v),
  'gucs',               (SELECT g  FROM gucs),
  'cp',                 (SELECT c  FROM cp),
  'wal',                (SELECT w  FROM wal),
  'arch',               (SELECT a  FROM arch),
  'repl',               (SELECT r  FROM repl),
  'slots',              (SELECT s  FROM slots),
  'wal_lsn',            pg_current_wal_lsn()::text,
  'now',                now()::text
)::text
$CMD$,
    -- checkpoint source
    CASE
      WHEN current_setting('server_version_num')::int >= 170000
        THEN $$(SELECT jsonb_build_object(
                  'timed',num_timed,'requested',num_requested,
                  'write_time',write_time,'sync_time',sync_time,
                  'buffers_written',buffers_written,
                  'stats_reset',stats_reset::text)
                FROM pg_stat_checkpointer)$$
      ELSE $$(SELECT jsonb_build_object(
                  'timed',checkpoints_timed,'requested',checkpoints_req,
                  'write_time',checkpoint_write_time,'sync_time',checkpoint_sync_time,
                  'buffers_written',buffers_checkpoint,
                  'stats_reset',stats_reset::text)
                FROM pg_stat_bgwriter)$$
    END,
    -- WAL source (pg_stat_wal exists in PG14+)
    CASE
      WHEN current_setting('server_version_num')::int >= 140000
        THEN $$(SELECT jsonb_build_object(
                  'wal_records',wal_records,'wal_fpi',wal_fpi,
                  'wal_bytes',wal_bytes::text,
                  'wal_buffers_full',wal_buffers_full,
                  'wal_write',wal_write,'wal_sync',wal_sync,
                  'stats_reset',stats_reset::text)
                FROM pg_stat_wal)$$
      ELSE $$NULL::jsonb$$
    END) AS c
) cmd,
run_command_on_all_nodes(cmd.c, parallel := true) r;

-- Parse per-node JSON.
DROP TABLE IF EXISTS _w1;
CREATE TEMP TABLE _w1 AS
SELECT
  n.nodeid                     AS nodeid,
  r.success,
  n.nodename, n.nodeport,
  CASE WHEN n.groupid = 0 THEN 'coord' ELSE 'worker' END AS role,
  (r.result)::jsonb            AS p
FROM _w1_raw r
LEFT JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE r.success;

-- Flat view of GUCs + counters per node for easy consumption.
DROP TABLE IF EXISTS _w1_flat;
CREATE TEMP TABLE _w1_flat AS
SELECT
  role, nodename||':'||nodeport AS node,
  (p->'gucs'->>'wal_level')                        AS wal_level,
  (p->'gucs'->>'wal_compression')                  AS wal_compression,
  (p->'gucs'->>'wal_buffers')::bigint * 8192       AS wal_buffers_bytes,
  (p->'gucs'->>'max_wal_size')                     AS max_wal_size,
  (p->'gucs'->>'min_wal_size')                     AS min_wal_size,
  (p->'gucs'->>'checkpoint_timeout')::int          AS checkpoint_timeout_s,
  (p->'gucs'->>'checkpoint_completion_target')::numeric AS cct,
  (p->'gucs'->>'synchronous_commit')               AS synchronous_commit,
  (p->'gucs'->>'synchronous_standby_names')        AS sync_standby,
  (p->'gucs'->>'archive_mode')                     AS archive_mode,
  (p->'gucs'->>'archive_command')                  AS archive_command,
  (p->'gucs'->>'archive_library')                  AS archive_library,
  (p->'gucs'->>'archive_timeout')::int             AS archive_timeout_s,
  (p->'gucs'->>'fsync')                            AS fsync,
  (p->'gucs'->>'full_page_writes')                 AS fpw,
  (p->'gucs'->>'max_replication_slots')::int       AS max_repl_slots,
  (p->'gucs'->>'max_wal_senders')::int             AS max_wal_senders,
  COALESCE((p->'cp'->>'timed')::bigint, 0)         AS cp_timed,
  COALESCE((p->'cp'->>'requested')::bigint, 0)     AS cp_req,
  COALESCE((p->'cp'->>'write_time')::numeric, 0)   AS cp_write_ms,
  COALESCE((p->'cp'->>'sync_time')::numeric, 0)    AS cp_sync_ms,
  COALESCE((p->'cp'->>'buffers_written')::bigint, 0) AS cp_buffers,
  NULLIF(p->'cp'->>'stats_reset','')::timestamptz  AS cp_stats_reset,
  COALESCE((p->'arch'->>'archived_count')::bigint, 0) AS arch_ok,
  COALESCE((p->'arch'->>'failed_count')::bigint, 0)   AS arch_fail,
  NULLIF(p->'arch'->>'last_archived_time','')::timestamptz AS last_arch_time,
  NULLIF(p->'arch'->>'last_failed_time','')::timestamptz   AS last_fail_time,
  COALESCE((p->'wal'->>'wal_bytes')::numeric, 0)::bigint  AS wal_bytes,
  COALESCE((p->'wal'->>'wal_buffers_full')::bigint, 0)    AS wal_buffers_full,
  NULLIF(p->'wal'->>'stats_reset','')::timestamptz AS wal_stats_reset,
  p->'repl'                                        AS repl,
  p->'slots'                                       AS slots,
  (p->>'wal_lsn')::pg_lsn                          AS wal_lsn
FROM _w1;

-- W1a: key WAL GUCs per node (cross-node drift surfaces as duplicate rows)
\echo
\echo '-- W1a. Key WAL / checkpoint GUCs per node --'
SELECT role, node,
       pg_size_pretty(wal_buffers_bytes) AS wal_buffers,
       max_wal_size, min_wal_size,
       (checkpoint_timeout_s || 's')     AS checkpoint_timeout,
       cct                               AS checkpoint_completion_target,
       wal_level, wal_compression, synchronous_commit, archive_mode
FROM _w1_flat
ORDER BY role DESC, node;

-- W1b: checkpoint trigger mix. Requested checkpoints firing too often
-- means max_wal_size is undersized for the workload.
\echo
\echo '-- W1b. Checkpoint trigger mix (timed vs requested) --'
SELECT role, node,
       cp_timed, cp_req,
       CASE WHEN (cp_timed + cp_req) = 0 THEN 0
            ELSE ROUND((cp_req::numeric / (cp_timed + cp_req)) * 100, 1)
       END AS req_pct,
       cp_stats_reset,
       CASE
         WHEN (cp_timed + cp_req) = 0
           THEN 'no checkpoints since stats_reset'
         WHEN (cp_req::numeric / (cp_timed + cp_req)) * 100 >= (:w1_req_ratio_pct)::numeric
           THEN format(
             'WARN: %s%% requested (>%s%%). max_wal_size is too small for the workload -- raise it or lengthen checkpoint_timeout.',
             ROUND((cp_req::numeric / (cp_timed + cp_req)) * 100, 1),
             (:w1_req_ratio_pct)::text)
         ELSE 'ok'
       END AS verdict
FROM _w1_flat
ORDER BY role DESC, node;

-- W1c: WAL generation rate (PG14+) and wal_buffers_full churn.
\echo
\echo '-- W1c. WAL generation & wal_buffers pressure --'
SELECT role, node,
       pg_size_pretty(wal_bytes)                  AS wal_since_reset,
       wal_stats_reset,
       CASE
         WHEN wal_stats_reset IS NULL THEN NULL
         ELSE pg_size_pretty((wal_bytes / GREATEST(EXTRACT(EPOCH FROM (now()-wal_stats_reset))::bigint, 1))::bigint)
       END AS wal_per_sec_avg,
       wal_buffers_full,
       CASE
         -- Only flag wal_buffers size if we also see evidence of pressure
         -- (wal_buffers_full > 0). PG's default of -1 auto-sizes wal_buffers
         -- to min(shared_buffers/32, 16MB); on small shared_buffers this is
         -- documented behaviour and raising wal_buffers alone changes
         -- nothing. The real signal is wal_buffers_full.
         WHEN wal_buffers_bytes < (:w1_min_wal_buffers_mb)::bigint * 1048576
              AND COALESCE(wal_buffers_full, 0) > 0
           THEN format(
             'WARN: wal_buffers=%s < %s MB AND wal_buffers_full=%s > 0 -- raise wal_buffers (or shared_buffers so auto-size goes up).',
             pg_size_pretty(wal_buffers_bytes), (:w1_min_wal_buffers_mb)::text, wal_buffers_full)
         WHEN wal_buffers_bytes < (:w1_min_wal_buffers_mb)::bigint * 1048576
           THEN format(
             'INFO: wal_buffers=%s < %s MB but no wal_buffers_full pressure observed (default auto-size on small shared_buffers).',
             pg_size_pretty(wal_buffers_bytes), (:w1_min_wal_buffers_mb)::text)
         WHEN wal_buffers_full > 1000 AND wal_stats_reset IS NOT NULL
              AND wal_buffers_full::numeric
                  / GREATEST(EXTRACT(EPOCH FROM (now()-wal_stats_reset))::bigint, 1)
                  > 1
           THEN 'WARN: wal_buffers_full > 1/sec -- raise wal_buffers or investigate commit rate'
         ELSE 'ok'
       END AS verdict
FROM _w1_flat
ORDER BY role DESC, node;

-- W1d: archiver health
\echo
\echo '-- W1d. Archiver health --'
SELECT role, node, archive_mode,
       CASE
         WHEN length(archive_command) > 40 THEN substring(archive_command,1,37)||'...'
         ELSE archive_command
       END AS archive_command,
       archive_library,
       arch_ok, arch_fail, last_arch_time, last_fail_time,
       CASE
         WHEN archive_mode NOT IN ('on','always') THEN 'archive_mode=off -- skipping'
         WHEN archive_command IS NULL AND archive_library IS NULL
              OR archive_command = '' AND archive_library = ''
           THEN 'CRITICAL: archive_mode is on but no archive_command / archive_library -- WAL will pile up'
         WHEN arch_fail > arch_ok AND arch_fail > 10
           THEN 'CRITICAL: more failed archives than succeeded -- destination is broken'
         WHEN last_fail_time IS NOT NULL
              AND (last_arch_time IS NULL OR last_fail_time > last_arch_time)
           THEN 'WARN: most recent archive attempt failed'
         WHEN last_arch_time IS NOT NULL
              AND now() - last_arch_time > ((:w1_archive_stale_min)::int * interval '1 minute')
           THEN format(
             'WARN: last successful archive was %s ago (> %s min). Idle cluster or stuck archiver?',
             date_trunc('second', now() - last_arch_time)::text,
             (:w1_archive_stale_min)::text)
         ELSE 'ok'
       END AS verdict
FROM _w1_flat
ORDER BY role DESC, node;

-- W1e: Replication slots & replicas
\echo
\echo '-- W1e. Replication slots & replicas --'
SELECT role, node,
       jsonb_array_length(slots)        AS slot_count,
       jsonb_array_length(repl)         AS active_repl_count,
       synchronous_commit, sync_standby
FROM _w1_flat
ORDER BY role DESC, node;

-- W1f: Durability / safety red flags
\echo
\echo '-- W1f. Durability red flags --'
SELECT role, node,
       fsync, fpw, wal_level,
       CASE
         WHEN fsync = 'off'
           THEN 'CRITICAL: fsync=off -- any crash can corrupt the cluster'
         WHEN fpw = 'off'
           THEN 'CRITICAL: full_page_writes=off -- crash corruption risk'
         WHEN wal_level = 'minimal' AND (max_wal_senders > 0 OR max_repl_slots > 0)
           THEN 'WARN: wal_level=minimal but replication slots / senders configured'
         WHEN sync_standby <> '' AND synchronous_commit IN ('off','local')
           THEN 'WARN: synchronous_standby_names set but synchronous_commit does not wait for them'
         ELSE 'ok'
       END AS verdict
FROM _w1_flat
WHERE fsync = 'off' OR fpw = 'off'
   OR (wal_level = 'minimal' AND (max_wal_senders > 0 OR max_repl_slots > 0))
   OR (sync_standby <> '' AND synchronous_commit IN ('off','local'));

-- Headline
\echo
SELECT (
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM _w1_flat WHERE fsync='off' OR fpw='off')
      THEN 'CRITICAL : fsync or full_page_writes is OFF on a node -- durability is broken. See W1f.'
    WHEN EXISTS (
           SELECT 1 FROM _w1_flat
           WHERE archive_mode IN ('on','always')
             AND ( (COALESCE(archive_command,'')='' AND COALESCE(archive_library,'')='')
                   OR (arch_fail > arch_ok AND arch_fail > 10) )
         )
      THEN 'CRITICAL : archive_mode is on but archiving is broken on a node -- WAL will pile up. See W1d.'
    WHEN EXISTS (
           SELECT 1 FROM _w1_flat
           WHERE (cp_timed + cp_req) > 0
             AND (cp_req::numeric / (cp_timed + cp_req)) * 100 >= (:w1_req_ratio_pct)::numeric
         )
      THEN format(
        'WARN : %s node(s) have >= %s%% requested checkpoints -- raise max_wal_size. See W1b.',
        (SELECT COUNT(*) FROM _w1_flat
          WHERE (cp_timed + cp_req) > 0
            AND (cp_req::numeric / (cp_timed + cp_req)) * 100 >= (:w1_req_ratio_pct)::numeric),
        (:w1_req_ratio_pct)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _w1_flat
           WHERE wal_buffers_bytes < (:w1_min_wal_buffers_mb)::bigint * 1048576
             AND COALESCE(wal_buffers_full, 0) > 0
         )
      THEN format(
        'WARN : %s node(s) have wal_buffers < %s MB AND observed wal_buffers_full pressure. See W1c.',
        (SELECT COUNT(*) FROM _w1_flat
          WHERE wal_buffers_bytes < (:w1_min_wal_buffers_mb)::bigint * 1048576
            AND COALESCE(wal_buffers_full, 0) > 0),
        (:w1_min_wal_buffers_mb)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _w1_flat
           WHERE archive_mode IN ('on','always')
             AND last_fail_time IS NOT NULL
             AND (last_arch_time IS NULL OR last_fail_time > last_arch_time)
         )
      THEN 'WARN : archiver has a recent failure on a node. See W1d.'
    ELSE 'OK : WAL path healthy across all nodes; checkpoint mix dominated by timed, durability GUCs safe.'
  END
) AS "Advisor W1 headline";

DROP TABLE _w1_raw;
DROP TABLE _w1;
DROP TABLE _w1_flat;
