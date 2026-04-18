-- =====================================================================
-- citus_analyze / Q1 : long-running queries & lock-wait snapshot
-- ---------------------------------------------------------------------
-- Surfaces sessions that are the most common availability / throughput
-- risks in a Citus cluster:
--   * Long-running queries (coord or any worker in MX mode) that hold
--     resources and block rebalancer / DDL.
--   * "idle in transaction" sessions — the #1 cause of 2PC backlog,
--     vacuum starvation, and stuck shard moves.
--   * Cross-node lock chains visible only via citus_lock_waits.
--   * Top relations by lock contention on the coordinator.
--
-- Inputs (override with -v):
--   warn_query_sec         WARN on any active query older than this;   default 300
--   crit_query_sec         CRITICAL threshold for active queries;      default 3600
--   warn_idle_in_tx_sec    WARN on any idle-in-tx session older than;  default 300
--   crit_idle_in_tx_sec    CRITICAL threshold for idle-in-tx;          default 1800
--   top_n                  number of rows to show in each section;     default 10
--   include_internal       1 = include citus_internal sessions;        default 0
--                          (internal are worker-side sessions opened
--                           by the coordinator; often long-lived even
--                           when healthy — excluded by default.)
--
-- Caveats
--   * Snapshot is taken at advisor run-time; this is a point-in-time
--     probe, not a time-series. Rerun during the problem window to
--     catch transients (e.g. `watch -n 10` or in a cron loop).
--   * Q1c fans out citus_lock_waits to every metadata-bearing node,
--     so MX-initiated waits are surfaced. The same logical pair may
--     be reported more than once (distinguished by seen_on).
--   * Q1d is COORDINATOR-LOCAL pg_locks only. Worker-local lock
--     storms (e.g. a long DDL queued on a single shard without any
--     cross-node wait) are not covered by this release.
--   * The `query` text is trimmed to 200 chars to keep the report
--     readable; use pg_stat_activity directly for full statements.
--   * Internal sessions (application_name 'citus_internal%') are
--     excluded by default (include_internal=0). The headline discloses
--     how many were excluded and the oldest age; rerun with
--     -v include_internal=1 to see worker fragments of distributed
--     queries.
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?warn_query_sec}       \else \set warn_query_sec        300  \endif
\if :{?crit_query_sec}       \else \set crit_query_sec       3600  \endif
\if :{?warn_idle_in_tx_sec}  \else \set warn_idle_in_tx_sec   300  \endif
\if :{?crit_idle_in_tx_sec}  \else \set crit_idle_in_tx_sec  1800  \endif
\if :{?top_n}                \else \set top_n                 10   \endif
\if :{?include_internal}     \else \set include_internal       0   \endif

\echo
\echo '==================== Q1 : long-running queries & locks ===================='

-- ---------------------------------------------------------------------
-- Per-node pg_stat_activity snapshot via run_command_on_all_nodes.
-- Each node returns a JSON array (one object per suspect session) so
-- the coordinator can un-nest without pipe-splitting.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _q1_raw;
CREATE TEMP TABLE _q1_raw (
    nodeid  int,
    success boolean,
    result  text
);

INSERT INTO _q1_raw
SELECT nodeid, success, result
FROM run_command_on_all_nodes(
format($CMD$
WITH suspect AS (
  SELECT *
  FROM pg_stat_activity
  WHERE backend_type = 'client backend'
    AND pid <> pg_backend_pid()
    AND (
          (state = 'active'
           AND query_start < now() - interval '%s seconds')
      OR  (state = 'idle in transaction'
           AND xact_start < now() - interval '%s seconds')
      OR  (state = 'idle in transaction (aborted)')
    )
),
external_suspect AS (
  SELECT * FROM suspect
  WHERE %s          -- include/exclude citus_internal
  ORDER BY GREATEST(
      EXTRACT(EPOCH FROM now() - COALESCE(xact_start, query_start, state_change))::bigint,
      0) DESC
  LIMIT 50          -- bound per-node payload even on sick clusters
),
internal_excluded AS (
  SELECT count(*)::int                                       AS n,
         COALESCE(max(EXTRACT(EPOCH FROM
             now() - COALESCE(xact_start, query_start))::bigint), 0) AS max_age_sec
  FROM suspect
  WHERE application_name LIKE 'citus_internal%%'
    AND %s = false        -- only meaningful when internal is excluded
)
SELECT jsonb_build_object(
  'sessions', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'pid',              pid,
      'datname',          datname,
      'usename',          usename,
      'application_name', application_name,
      'state',            state,
      'wait_event_type',  wait_event_type,
      'wait_event',       wait_event,
      'backend_xmin',     backend_xmin::text,
      'query_age_sec',    EXTRACT(EPOCH FROM now() - query_start)::bigint,
      'xact_age_sec',     EXTRACT(EPOCH FROM now() - xact_start)::bigint,
      'state_age_sec',    EXTRACT(EPOCH FROM now() - state_change)::bigint,
      'query',            left(regexp_replace(query, E'[\n\r\t ]+', ' ', 'g'), 200)
  )) FROM external_suspect), '[]'::jsonb),
  'internal_excluded_count',   (SELECT n           FROM internal_excluded),
  'internal_excluded_max_age', (SELECT max_age_sec FROM internal_excluded),
  'lock_waits', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'waiting_gpid',    waiting_gpid,
      'blocking_gpid',   blocking_gpid,
      'waiting_nodeid',  waiting_nodeid,
      'blocking_nodeid', blocking_nodeid,
      'waiting_stmt',    left(regexp_replace(blocked_statement, E'[\n\r\t ]+',' ','g'),120),
      'blocking_stmt',   left(regexp_replace(current_statement_in_blocking_process,
                                             E'[\n\r\t ]+',' ','g'),120)
  )) FROM pg_catalog.citus_lock_waits), '[]'::jsonb)
)::text;
$CMD$,
 :warn_query_sec::text,
 :warn_idle_in_tx_sec::text,
 CASE WHEN :include_internal::int = 1 THEN 'true'
      ELSE 'application_name NOT LIKE ''citus_internal%%''' END,
 CASE WHEN :include_internal::int = 1 THEN 'true'
      ELSE 'false' END
)
, parallel := true);

-- Parse the per-node JSON payload (keyed object with sessions + lock_waits
-- + internal-excluded summary). Sessions un-nest into _q1; lock waits fan
-- out across all metadata nodes into _q1_lw; internal-exclusion summary
-- rolls up in _q1_intsum.
DROP TABLE IF EXISTS _q1;
CREATE TEMP TABLE _q1 AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata  THEN 'worker (MX)'
         ELSE 'worker' END                          AS role,
    n.nodename, n.nodeport, n.nodeid,
    (elem->>'pid')::int                             AS pid,
    elem->>'datname'                                AS datname,
    elem->>'usename'                                AS usename,
    elem->>'application_name'                       AS application_name,
    elem->>'state'                                  AS state,
    elem->>'wait_event_type'                        AS wait_event_type,
    elem->>'wait_event'                             AS wait_event,
    NULLIF(elem->>'backend_xmin','')                AS backend_xmin,
    NULLIF(elem->>'query_age_sec','')::bigint       AS query_age_sec,
    NULLIF(elem->>'xact_age_sec','')::bigint        AS xact_age_sec,
    NULLIF(elem->>'state_age_sec','')::bigint       AS state_age_sec,
    -- effective idle age tolerates aborted xacts with NULL xact_start
    GREATEST(COALESCE(NULLIF(elem->>'xact_age_sec','')::bigint, 0),
             COALESCE(NULLIF(elem->>'state_age_sec','')::bigint, 0))
                                                    AS eff_idle_age_sec,
    elem->>'query'                                  AS query
FROM _q1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
LEFT JOIN LATERAL jsonb_array_elements(
    CASE WHEN r.success AND r.result <> ''
         THEN COALESCE(r.result::jsonb -> 'sessions', '[]'::jsonb)
         ELSE '[]'::jsonb END) elem ON true
WHERE n.isactive;

-- Cross-node lock-wait rows, aggregated from every metadata node.
DROP TABLE IF EXISTS _q1_lw;
CREATE TEMP TABLE _q1_lw AS
SELECT
    n.nodename || ':' || n.nodeport                      AS seen_on,
    (lw->>'waiting_gpid')::bigint                        AS waiting_gpid,
    (lw->>'blocking_gpid')::bigint                       AS blocking_gpid,
    (lw->>'waiting_nodeid')::int                         AS waiting_nodeid,
    (lw->>'blocking_nodeid')::int                        AS blocking_nodeid,
    lw->>'waiting_stmt'                                  AS waiting_stmt,
    lw->>'blocking_stmt'                                 AS blocking_stmt
FROM _q1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
LEFT JOIN LATERAL jsonb_array_elements(
    CASE WHEN r.success AND r.result <> ''
         THEN COALESCE(r.result::jsonb -> 'lock_waits', '[]'::jsonb)
         ELSE '[]'::jsonb END) lw ON true
WHERE n.isactive AND n.hasmetadata
  AND lw IS NOT NULL;

-- Per-node internal-session exclusion summary (for disclosure in headline).
DROP TABLE IF EXISTS _q1_intsum;
CREATE TEMP TABLE _q1_intsum AS
SELECT n.nodename || ':' || n.nodeport                   AS node,
       COALESCE((r.result::jsonb ->> 'internal_excluded_count')::int, 0)
                                                         AS excluded_n,
       COALESCE((r.result::jsonb ->> 'internal_excluded_max_age')::bigint, 0)
                                                         AS excluded_max_age
FROM _q1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE r.success AND n.isactive;

-- ---------------------------------------------------------------------
-- Q1a : long-running ACTIVE queries
-- ---------------------------------------------------------------------
\echo
\echo '-- Q1a. Long-running active queries (per node) --'
SELECT
    role,
    nodename || ':' || nodeport                                  AS node,
    pid,
    query_age_sec                                                AS age_s,
    COALESCE(wait_event_type || ':' || wait_event, '-')          AS waiting_on,
    application_name,
    query
FROM _q1
WHERE state = 'active'
  AND query_age_sec IS NOT NULL
ORDER BY query_age_sec DESC NULLS LAST
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Q1b : idle-in-transaction sessions (true poison for 2PC / VACUUM)
-- Effective age = GREATEST(xact_age, state_age) so that
-- 'idle in transaction (aborted)' sessions with NULL xact_start still rank.
-- ---------------------------------------------------------------------
\echo
\echo '-- Q1b. Idle-in-transaction sessions (per node) --'
SELECT
    role,
    nodename || ':' || nodeport                                  AS node,
    pid,
    state,
    xact_age_sec                                                 AS xact_age_s,
    state_age_sec                                                AS idle_age_s,
    eff_idle_age_sec                                             AS effective_s,
    backend_xmin,
    application_name,
    query                                                        AS last_query
FROM _q1
WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')
ORDER BY eff_idle_age_sec DESC NULLS LAST
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Q1c : cross-node lock chains (citus_lock_waits)
-- Fanned out to every metadata-bearing node so MX-initiated waits are
-- surfaced. Rows may appear on multiple nodes; we distinguish by seen_on.
-- ---------------------------------------------------------------------
\echo
\echo '-- Q1c. citus_lock_waits (cross-node blockers, fanned out to all MX nodes) --'
SELECT
    seen_on,
    waiting_gpid,
    blocking_gpid,
    waiting_nodeid,
    blocking_nodeid,
    waiting_stmt,
    blocking_stmt
FROM _q1_lw
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Q1d : ungranted lock requests on the COORDINATOR (local-only scope)
--   Surfaces coord-side DDL / rebalance contention. Worker-local lock
--   storms need to be inspected directly on the worker (not covered
--   here in this release).
-- ---------------------------------------------------------------------
\echo
\echo '-- Q1d. Ungranted locks on coordinator (coord-local scope) --'
DROP TABLE IF EXISTS _q1d;
CREATE TEMP TABLE _q1d AS
SELECT
    l.locktype,
    l.mode,
    COALESCE(c.relname, l.locktype)                              AS object,
    count(*) FILTER (WHERE NOT l.granted)::int                   AS waiters,
    count(*) FILTER (WHERE l.granted)::int                       AS holders,
    EXTRACT(EPOCH FROM now() -
        min(a.xact_start) FILTER (WHERE NOT l.granted))::bigint  AS longest_wait_s
FROM pg_locks l
LEFT JOIN pg_class  c ON c.oid = l.relation
LEFT JOIN pg_stat_activity a ON a.pid = l.pid
GROUP BY 1,2,3
HAVING count(*) FILTER (WHERE NOT l.granted) > 0;

SELECT * FROM _q1d
ORDER BY waiters DESC, longest_wait_s DESC NULLS LAST
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sev AS (
  SELECT
    max(CASE WHEN state = 'active'
                 AND query_age_sec >= :crit_query_sec::bigint              THEN 3
             WHEN state IN ('idle in transaction','idle in transaction (aborted)')
                 AND eff_idle_age_sec >= :crit_idle_in_tx_sec::bigint      THEN 3
             WHEN state = 'active'
                 AND query_age_sec >= :warn_query_sec::bigint              THEN 2
             WHEN state IN ('idle in transaction','idle in transaction (aborted)')
                 AND eff_idle_age_sec >= :warn_idle_in_tx_sec::bigint      THEN 2
             ELSE 0 END) AS s,
    count(*) FILTER (WHERE state='active'
                       AND query_age_sec >= :warn_query_sec::bigint)       AS long_active,
    count(*) FILTER (WHERE state IN ('idle in transaction',
                                     'idle in transaction (aborted)')
                       AND eff_idle_age_sec >= :warn_idle_in_tx_sec::bigint) AS idle_in_tx,
    (SELECT count(DISTINCT waiting_gpid || ':' || blocking_gpid)
       FROM _q1_lw)                                                        AS lock_wait_rows,
    (SELECT COALESCE(sum(waiters),0)::int   FROM _q1d)                     AS coord_lock_waiters,
    (SELECT COALESCE(max(longest_wait_s),0) FROM _q1d)                     AS coord_lock_max_s,
    (SELECT COALESCE(sum(excluded_n),0)     FROM _q1_intsum)               AS internal_excluded,
    (SELECT COALESCE(max(excluded_max_age),0) FROM _q1_intsum)             AS internal_excluded_max_age,
    (SELECT count(*) FROM _q1_raw WHERE NOT success)                       AS unreachable
  FROM _q1
)
SELECT CASE
  WHEN unreachable > 0 THEN
    format('WARN : %s node(s) unreachable during Q1 snapshot.', unreachable)
  WHEN s = 3 THEN
    format('CRITICAL : %s long active + %s idle-in-tx session(s); %s cross-node lock wait(s); %s coord lock waiter(s). Kill or commit now.',
           long_active, idle_in_tx, lock_wait_rows, coord_lock_waiters)
  WHEN coord_lock_max_s >= :crit_query_sec::bigint THEN
    format('CRITICAL : coord lock wait %s sec (> %s); %s waiter(s) blocked.',
           coord_lock_max_s, :crit_query_sec, coord_lock_waiters)
  WHEN s = 2 THEN
    format('WARN : %s long active + %s idle-in-tx session(s); %s cross-node lock wait(s); %s coord lock waiter(s). Investigate before rebalance/DDL.',
           long_active, idle_in_tx, lock_wait_rows, coord_lock_waiters)
  WHEN coord_lock_max_s >= :warn_query_sec::bigint THEN
    format('WARN : coord lock wait %s sec (> %s); %s waiter(s) blocked.',
           coord_lock_max_s, :warn_query_sec, coord_lock_waiters)
  WHEN lock_wait_rows > 0 THEN
    format('WARN : %s cross-node lock wait(s) but no single session over WARN threshold.', lock_wait_rows)
  WHEN internal_excluded > 0 AND internal_excluded_max_age >= :warn_query_sec::bigint THEN
    format('INFO : %s citus_internal session(s) excluded; oldest %s sec. Rerun with -v include_internal=1 to see worker fragments of distributed queries.',
           internal_excluded, internal_excluded_max_age)
  ELSE 'OK : no long-running queries or idle-in-tx sessions.'
END
FROM sev;
\pset tuples_only off

DROP TABLE _q1d;
DROP TABLE _q1_intsum;
DROP TABLE _q1_lw;
DROP TABLE _q1;
DROP TABLE _q1_raw;
