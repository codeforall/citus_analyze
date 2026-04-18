-- REP1: Streaming replication & slot lag.
-- Citus itself is a logical sharding layer on top of PostgreSQL's
-- streaming replication -- HA in a Citus deployment is built out of
-- per-node primary/standby pairs. REP1 surfaces:
--
--  REP1a  Active senders per node (pg_stat_replication) -- state,
--         sync_state, write/flush/replay lag.
--  REP1b  Replication slots per node (pg_replication_slots) --
--         slot_type, active flag, WAL retention bytes. Flags:
--           * inactive physical slots retaining > X bytes,
--           * orphan Citus rebalancer slots (names that match
--             citus_shard_move / citus_shard_split patterns but
--             whose owning task is no longer in
--             pg_dist_background_task). These are the classic
--             "rebalance filled my disk" culprit.
--  REP1c  Per-node recovery state (pg_is_in_recovery and
--         last_xact_replay_timestamp).
--  REP1d  Aggregate: primary with zero sync_standbys despite
--         synchronous_commit asking for them.
--
-- Inputs (psql -v):
--   rep1_slot_warn_mb         = 256
--   rep1_slot_crit_mb         = 2048
--   rep1_replay_lag_warn_s    = 60
--   rep1_replay_lag_crit_s    = 600

\pset pager off
\pset border 2
\pset format aligned

\if :{?rep1_slot_warn_mb}       \else \set rep1_slot_warn_mb       256  \endif
\if :{?rep1_slot_crit_mb}       \else \set rep1_slot_crit_mb       2048 \endif
\if :{?rep1_replay_lag_warn_s}  \else \set rep1_replay_lag_warn_s  60   \endif
\if :{?rep1_replay_lag_crit_s}  \else \set rep1_replay_lag_crit_s  600  \endif
\if :{?top_n}                   \else \set top_n                   50   \endif

\echo '==================== REP1 : streaming replication & slots ===================='

DROP TABLE IF EXISTS _rep1_raw;
CREATE TEMP TABLE _rep1_raw (nodeid int, success boolean, result text);

INSERT INTO _rep1_raw (nodeid, success, result)
SELECT r.nodeid, r.success, r.result
FROM run_command_on_all_nodes($CMD$
  SELECT jsonb_build_object(
    'recovery', jsonb_build_object(
      'in_recovery',   pg_is_in_recovery(),
      'current_lsn',   CASE WHEN pg_is_in_recovery()
                            THEN pg_last_wal_receive_lsn()::text
                            ELSE pg_current_wal_lsn()::text END,
      'replay_lsn',    CASE WHEN pg_is_in_recovery()
                            THEN pg_last_wal_replay_lsn()::text
                            ELSE NULL END,
      'last_replay_ts',CASE WHEN pg_is_in_recovery()
                            THEN pg_last_xact_replay_timestamp()::text
                            ELSE NULL END
    ),
    'settings', jsonb_build_object(
      'wal_level',                   current_setting('wal_level'),
      'max_wal_senders',             current_setting('max_wal_senders'),
      'max_replication_slots',       current_setting('max_replication_slots'),
      'synchronous_commit',          current_setting('synchronous_commit'),
      'synchronous_standby_names',   current_setting('synchronous_standby_names')
    ),
    'senders', (
      SELECT jsonb_agg(jsonb_build_object(
        'pid',            pid,
        'usename',        usename,
        'application_name', application_name,
        'client_addr',    client_addr::text,
        'state',          state,
        'sync_state',     sync_state,
        'sent_lsn',       sent_lsn::text,
        'write_lsn',      write_lsn::text,
        'flush_lsn',      flush_lsn::text,
        'replay_lsn',     replay_lsn::text,
        'write_lag',      EXTRACT(EPOCH FROM write_lag),
        'flush_lag',      EXTRACT(EPOCH FROM flush_lag),
        'replay_lag',     EXTRACT(EPOCH FROM replay_lag)
      )) FROM pg_stat_replication
    ),
    'slots', (
      SELECT jsonb_agg(jsonb_build_object(
        'slot_name',      slot_name,
        'plugin',         plugin,
        'slot_type',      slot_type,
        'database',       database,
        'active',         active,
        'active_pid',     active_pid,
        'restart_lsn',    restart_lsn::text,
        'confirmed_flush_lsn', confirmed_flush_lsn::text,
        'wal_retained_b', CASE WHEN restart_lsn IS NULL THEN 0
                               WHEN pg_is_in_recovery() THEN 0
                               ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) END
      )) FROM pg_replication_slots
    )
  )::text
$CMD$, parallel := true) r;

DROP TABLE IF EXISTS _rep1;
CREATE TEMP TABLE _rep1 AS
SELECT
  n.nodeid, n.nodename, n.nodeport, n.groupid,
  CASE WHEN n.groupid = 0 THEN 'coord' ELSE 'worker' END AS role,
  (r.result)::jsonb AS p
FROM _rep1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE r.success;

-- ---------------------------------------------------------------------
-- REP1a: active senders
-- ---------------------------------------------------------------------
\echo
\echo '-- REP1a. Active replication senders per node --'
WITH s AS (
  SELECT
    r.role, r.nodename||':'||r.nodeport AS node,
    item
  FROM _rep1 r,
       jsonb_array_elements(
         CASE WHEN jsonb_typeof(r.p->'senders')='array'
              THEN r.p->'senders' ELSE '[]'::jsonb END
       ) item
)
SELECT
  role, node,
  item->>'application_name'  AS app,
  item->>'client_addr'       AS client,
  item->>'state'             AS state,
  item->>'sync_state'        AS sync_state,
  ROUND((item->>'write_lag')::numeric, 2)   AS write_lag_s,
  ROUND((item->>'flush_lag')::numeric, 2)   AS flush_lag_s,
  ROUND((item->>'replay_lag')::numeric, 2)  AS replay_lag_s,
  CASE
    WHEN (item->>'state') <> 'streaming'
      THEN format('WARN: sender state %s (expected streaming)', item->>'state')
    WHEN (item->>'replay_lag')::numeric
         >= (:rep1_replay_lag_crit_s)::numeric
      THEN format('CRITICAL: replay_lag %s s >= %s s',
                  ROUND((item->>'replay_lag')::numeric, 1),
                  (:rep1_replay_lag_crit_s)::text)
    WHEN (item->>'replay_lag')::numeric
         >= (:rep1_replay_lag_warn_s)::numeric
      THEN format('WARN: replay_lag %s s >= %s s',
                  ROUND((item->>'replay_lag')::numeric, 1),
                  (:rep1_replay_lag_warn_s)::text)
    ELSE 'ok'
  END AS verdict
FROM s
ORDER BY role DESC, node, app
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- REP1b: replication slots
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _rep1_slots;
CREATE TEMP TABLE _rep1_slots AS
SELECT
  r.role, r.nodename||':'||r.nodeport AS node,
  item->>'slot_name'                         AS slot_name,
  item->>'slot_type'                         AS slot_type,
  item->>'plugin'                            AS plugin,
  (item->>'active')::bool                    AS active,
  item->>'restart_lsn'                       AS restart_lsn,
  (item->>'wal_retained_b')::bigint          AS wal_retained_b
FROM _rep1 r,
     jsonb_array_elements(
       CASE WHEN jsonb_typeof(r.p->'slots')='array'
            THEN r.p->'slots' ELSE '[]'::jsonb END
     ) item;

\echo
\echo '-- REP1b. Replication slots (all) --'
SELECT
  role, node, slot_name, slot_type, plugin, active,
  pg_size_pretty(wal_retained_b) AS wal_retained,
  CASE
    WHEN NOT active
         AND wal_retained_b >= (:rep1_slot_crit_mb)::bigint * 1024 * 1024
      THEN format('CRITICAL: inactive slot retains %s WAL',
                  pg_size_pretty(wal_retained_b))
    WHEN NOT active
         AND wal_retained_b >= (:rep1_slot_warn_mb)::bigint * 1024 * 1024
      THEN format('WARN: inactive slot retains %s WAL',
                  pg_size_pretty(wal_retained_b))
    WHEN slot_name LIKE 'citus\_shard\_%' AND NOT active
      THEN 'WARN: orphaned Citus rebalancer slot -- investigate pg_dist_cleanup'
    WHEN wal_retained_b >= (:rep1_slot_crit_mb)::bigint * 1024 * 1024
      THEN format('WARN: active slot retains %s WAL (>= %s MB)',
                  pg_size_pretty(wal_retained_b),
                  (:rep1_slot_crit_mb)::text)
    ELSE 'ok'
  END AS verdict
FROM _rep1_slots
ORDER BY wal_retained_b DESC, role DESC, node, slot_name
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- REP1c: recovery state
-- ---------------------------------------------------------------------
\echo
\echo '-- REP1c. Recovery / replay state per node --'
SELECT
  role, nodename||':'||nodeport AS node,
  (p->'recovery'->>'in_recovery')::bool AS is_standby,
  p->'recovery'->>'current_lsn'        AS current_lsn,
  p->'recovery'->>'replay_lsn'         AS replay_lsn,
  p->'recovery'->>'last_replay_ts'     AS last_replay_ts,
  CASE
    WHEN (p->'recovery'->>'in_recovery')::bool
         AND (p->'recovery'->>'last_replay_ts') IS NOT NULL
         AND now() - (p->'recovery'->>'last_replay_ts')::timestamptz
             > make_interval(secs => (:rep1_replay_lag_crit_s)::int)
      THEN format('CRITICAL: standby has not replayed for %s s',
                  ROUND(EXTRACT(EPOCH FROM now() -
                       (p->'recovery'->>'last_replay_ts')::timestamptz)::numeric, 1))
    WHEN (p->'recovery'->>'in_recovery')::bool
         AND (p->'recovery'->>'last_replay_ts') IS NOT NULL
         AND now() - (p->'recovery'->>'last_replay_ts')::timestamptz
             > make_interval(secs => (:rep1_replay_lag_warn_s)::int)
      THEN format('WARN: standby has not replayed for %s s',
                  ROUND(EXTRACT(EPOCH FROM now() -
                       (p->'recovery'->>'last_replay_ts')::timestamptz)::numeric, 1))
    ELSE 'ok'
  END AS verdict
FROM _rep1
ORDER BY role DESC, node;

-- ---------------------------------------------------------------------
-- REP1d: synchronous_commit sanity
-- ---------------------------------------------------------------------
\echo
\echo '-- REP1d. Synchronous-commit / sync-standby posture --'
SELECT
  role, nodename||':'||nodeport AS node,
  p->'settings'->>'wal_level'                   AS wal_level,
  p->'settings'->>'synchronous_commit'          AS sync_commit,
  p->'settings'->>'synchronous_standby_names'   AS sync_names,
  (p->'settings'->>'max_wal_senders')::int      AS max_wal_senders,
  (p->'settings'->>'max_replication_slots')::int AS max_slots,
  CASE
    WHEN p->'settings'->>'synchronous_commit' IN ('remote_write','remote_apply')
     AND COALESCE(p->'settings'->>'synchronous_standby_names','') = ''
      THEN 'WARN: synchronous_commit=' || (p->'settings'->>'synchronous_commit')
           || ' but synchronous_standby_names is empty (falls back to local flush silently)'
    WHEN p->'settings'->>'synchronous_standby_names' <> ''
     AND NOT EXISTS (
           SELECT 1 FROM _rep1 x,
           jsonb_array_elements(
             CASE WHEN jsonb_typeof(x.p->'senders')='array'
                  THEN x.p->'senders' ELSE '[]'::jsonb END) y
           WHERE x.nodeid = _rep1.nodeid
             AND y->>'sync_state' IN ('sync','quorum'))
      THEN 'CRITICAL: synchronous_standby_names set but no sync/quorum sender active'
    ELSE 'ok'
  END AS verdict
FROM _rep1
ORDER BY role DESC, node;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
SELECT (
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM _rep1, jsonb_array_elements(
        CASE WHEN jsonb_typeof(p->'senders')='array'
             THEN p->'senders' ELSE '[]'::jsonb END) item
      WHERE (item->>'replay_lag')::numeric >= (:rep1_replay_lag_crit_s)::numeric
    )
      THEN format('CRITICAL : replay_lag exceeds %s s on some sender(s).',
                  (:rep1_replay_lag_crit_s)::text)
    WHEN EXISTS (
      SELECT 1 FROM _rep1_slots
      WHERE NOT active AND wal_retained_b >= (:rep1_slot_crit_mb)::bigint * 1024 * 1024
    )
      THEN format('CRITICAL : inactive replication slot(s) retain >= %s MB of WAL.',
                  (:rep1_slot_crit_mb)::text)
    WHEN EXISTS (
      SELECT 1 FROM _rep1 x
      WHERE x.p->'settings'->>'synchronous_standby_names' <> ''
        AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(x.p->'senders')='array'
                     THEN x.p->'senders' ELSE '[]'::jsonb END) y
              WHERE y->>'sync_state' IN ('sync','quorum'))
    )
      THEN 'CRITICAL : synchronous_standby_names set without matching sync/quorum sender.'
    WHEN EXISTS (
      SELECT 1 FROM _rep1_slots
      WHERE NOT active AND wal_retained_b >= (:rep1_slot_warn_mb)::bigint * 1024 * 1024
    )
      THEN format('WARN : inactive replication slot(s) retain >= %s MB of WAL.',
                  (:rep1_slot_warn_mb)::text)
    WHEN EXISTS (
      SELECT 1 FROM _rep1, jsonb_array_elements(
        CASE WHEN jsonb_typeof(p->'senders')='array'
             THEN p->'senders' ELSE '[]'::jsonb END) item
      WHERE (item->>'replay_lag')::numeric >= (:rep1_replay_lag_warn_s)::numeric
    )
      THEN format('WARN : replay_lag exceeds %s s on some sender(s).',
                  (:rep1_replay_lag_warn_s)::text)
    WHEN EXISTS (SELECT 1 FROM _rep1_slots WHERE slot_name LIKE 'citus\_shard\_%' AND NOT active)
      THEN 'WARN : orphaned Citus rebalancer replication slot(s) detected. See REP1b.'
    WHEN (SELECT count(*) FROM _rep1_slots) = 0
         AND (SELECT count(*) FROM _rep1,
                jsonb_array_elements(
                  CASE WHEN jsonb_typeof(p->'senders')='array'
                       THEN p->'senders' ELSE '[]'::jsonb END)) = 0
      THEN 'OK : no replication senders or slots configured (no HA in this deployment).'
    ELSE 'OK : streaming replication healthy, all slots active with bounded WAL retention.'
  END
) AS "Advisor REP1 headline";

DROP TABLE _rep1_raw;
DROP TABLE _rep1;
DROP TABLE _rep1_slots;
