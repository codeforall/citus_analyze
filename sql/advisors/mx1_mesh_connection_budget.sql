-- =====================================================================
-- MX1 : Citus-MX mesh connection budget
-- ---------------------------------------------------------------------
-- Asks: in MX mode, each metadata-synced node opens outbound
--       connections to every other worker AND receives inbound
--       connections from every other MX node. Is there any node
--       whose max_connections is already close to exhausted purely
--       by this mesh traffic, before a single client connects?
--
-- Model (per MX peer n):
--   n_mx          = # primary nodes with hasmetadata=true
--   out_peers     = n_mx - 1   (one connection pool per other node)
--   mesh_inbound  = (n_mx - 1) * max_adaptive_executor_pool_size * k_reuse
--                   -- from each other MX peer, each entry-point session
--                   -- can fan out up to max_adaptive_executor_pool_size
--                   -- cached connections toward this node; we apply a
--                   -- k_reuse factor to reflect that not all sessions
--                   -- keep every pool slot warm.
--   reserved_for_mesh = mesh_inbound
--   system_reserve    = superuser_reserved_connections
--                     + max_wal_senders
--                     + autovacuum_max_workers
--                     + max_logical_replication_workers
--                     + 5
--   client_slots  = max_connections - reserved_for_mesh - system_reserve
--
--   Severity:
--     CRITICAL  client_slots < :mx1_min_reserve
--     WARN      client_slots < :mx1_warn_reserve
--     OK        otherwise
--
-- Inputs
--   :mx1_k_reuse        default 0.6
--   :mx1_min_reserve    default 10
--   :mx1_warn_reserve   default 20
-- =====================================================================

\if :{?mx1_k_reuse}      \else \set mx1_k_reuse       0.6 \endif
\if :{?mx1_min_reserve}  \else \set mx1_min_reserve   10  \endif
\if :{?mx1_warn_reserve} \else \set mx1_warn_reserve  20  \endif

\pset pager off
\pset border 2
\pset format aligned

\echo
\echo '==================== MX1 : MX mesh connection budget ===================='

-- Collect per-node settings from all active primaries.
DROP TABLE IF EXISTS _mx1_raw;
CREATE TEMP TABLE _mx1_raw AS
SELECT n.nodeid, n.nodename, n.nodeport, n.noderole,
       n.hasmetadata, n.metadatasynced,
       r.success, r.result::jsonb AS settings
FROM pg_dist_node n
LEFT JOIN LATERAL run_command_on_workers($cmd$
  SELECT jsonb_build_object(
    'max_connections',                 current_setting('max_connections'),
    'superuser_reserved_connections',  current_setting('superuser_reserved_connections'),
    'max_wal_senders',                 current_setting('max_wal_senders'),
    'autovacuum_max_workers',          current_setting('autovacuum_max_workers'),
    'max_logical_replication_workers', current_setting('max_logical_replication_workers'),
    'max_adaptive_executor_pool_size', current_setting('citus.max_adaptive_executor_pool_size'),
    'max_shared_pool_size',            current_setting('citus.max_shared_pool_size')
  )::text
$cmd$) r ON r.nodename = n.nodename AND r.nodeport = n.nodeport
WHERE n.isactive AND n.noderole = 'primary';

-- Also pull the coordinator's own settings (run_command_on_workers
-- excludes the coordinator).
DROP TABLE IF EXISTS _mx1_coord;
CREATE TEMP TABLE _mx1_coord AS
SELECT
  jsonb_build_object(
    'max_connections',                 current_setting('max_connections'),
    'superuser_reserved_connections',  current_setting('superuser_reserved_connections'),
    'max_wal_senders',                 current_setting('max_wal_senders'),
    'autovacuum_max_workers',          current_setting('autovacuum_max_workers'),
    'max_logical_replication_workers', current_setting('max_logical_replication_workers'),
    'max_adaptive_executor_pool_size', current_setting('citus.max_adaptive_executor_pool_size'),
    'max_shared_pool_size',            current_setting('citus.max_shared_pool_size')
  ) AS settings;

-- Merge coordinator row into _mx1_raw.
INSERT INTO _mx1_raw
SELECT n.nodeid, n.nodename, n.nodeport, n.noderole,
       n.hasmetadata, n.metadatasynced,
       true, (SELECT settings FROM _mx1_coord)
FROM pg_dist_node n
WHERE n.groupid = 0 AND n.noderole = 'primary' AND n.isactive;

DROP TABLE IF EXISTS _mx1_calc;
CREATE TEMP TABLE _mx1_calc AS
WITH base AS (
  SELECT nodename, nodeport, hasmetadata,
         NULLIF(settings->>'max_connections','')::int                 AS max_conn,
         NULLIF(settings->>'superuser_reserved_connections','')::int  AS super_reserved,
         NULLIF(settings->>'max_wal_senders','')::int                 AS wal_senders,
         NULLIF(settings->>'autovacuum_max_workers','')::int          AS av_workers,
         NULLIF(settings->>'max_logical_replication_workers','')::int AS lrep_workers,
         NULLIF(settings->>'max_adaptive_executor_pool_size','')::int AS exec_pool,
         NULLIF(settings->>'max_shared_pool_size','')::int            AS shared_pool
  FROM _mx1_raw
  WHERE success
),
totals AS (
  SELECT count(*) FILTER (WHERE hasmetadata) AS n_mx,
         count(*)                             AS n_nodes
  FROM base
)
SELECT
  b.nodename || ':' || b.nodeport AS node,
  b.hasmetadata,
  b.max_conn, b.super_reserved, b.wal_senders, b.av_workers,
  b.lrep_workers, b.exec_pool, b.shared_pool,
  t.n_mx,
  -- mesh_inbound: other MX peers times their exec_pool slot cap.
  -- We use this node's exec_pool as a proxy (uniform cluster), which
  -- is the case the user should be running anyway; we flag drift in
  -- GUC1 if it's not.
  CASE WHEN b.hasmetadata AND t.n_mx > 1
       THEN ceil( (t.n_mx - 1)::numeric
                  * COALESCE(b.exec_pool, 16)
                  * (:mx1_k_reuse)::numeric )::int
       ELSE 0
  END AS mesh_inbound,
  (COALESCE(b.super_reserved,0) + COALESCE(b.wal_senders,0)
   + COALESCE(b.av_workers,0)   + COALESCE(b.lrep_workers,0) + 5) AS system_reserve
FROM base b, totals t;

DROP TABLE IF EXISTS _mx1_verdict;
CREATE TEMP TABLE _mx1_verdict AS
SELECT node, hasmetadata, max_conn, mesh_inbound, system_reserve,
       GREATEST(max_conn - mesh_inbound - system_reserve, 0) AS client_slots,
       CASE
         WHEN NOT hasmetadata THEN 'OK'   -- non-MX nodes do not receive mesh fan-in
         WHEN (max_conn - mesh_inbound - system_reserve) < (:mx1_min_reserve)::int
              THEN 'CRITICAL'
         WHEN (max_conn - mesh_inbound - system_reserve) < (:mx1_warn_reserve)::int
              THEN 'WARN'
         ELSE 'OK'
       END AS severity
FROM _mx1_calc;

\echo
\echo '-- MX1a. Per-node mesh budget --'
SELECT severity, node,
       CASE WHEN hasmetadata THEN 'yes' ELSE 'no' END AS mx,
       max_conn, mesh_inbound, system_reserve, client_slots
FROM _mx1_verdict
ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
         client_slots ASC;

\echo
\echo '-- MX1b. Summary --'
SELECT (SELECT n_mx FROM _mx1_calc LIMIT 1) AS mx_nodes,
       count(*) FILTER (WHERE severity='CRITICAL') AS critical_nodes,
       count(*) FILTER (WHERE severity='WARN')     AS warn_nodes,
       count(*) FILTER (WHERE severity='OK')       AS ok_nodes
FROM _mx1_verdict;

\echo
\pset tuples_only on
SELECT CASE
  WHEN (SELECT n_mx FROM _mx1_calc LIMIT 1) <= 1 THEN
    'INFO : cluster is not in MX mode (0 or 1 metadata-synced nodes); MX mesh budget does not apply.'
  WHEN (SELECT count(*) FROM _mx1_verdict WHERE severity='CRITICAL') > 0 THEN
    format('CRITICAL : %s MX node(s) have fewer than %s client slots after subtracting mesh inbound + system reserve. Raise max_connections or lower citus.max_adaptive_executor_pool_size.',
           (SELECT count(*) FROM _mx1_verdict WHERE severity='CRITICAL'),
           :mx1_min_reserve)
  WHEN (SELECT count(*) FROM _mx1_verdict WHERE severity='WARN') > 0 THEN
    format('WARN : %s MX node(s) have fewer than %s client slots once mesh inbound is reserved. Plan for growth in max_connections.',
           (SELECT count(*) FROM _mx1_verdict WHERE severity='WARN'),
           :mx1_warn_reserve)
  ELSE
    'OK : every MX node has headroom for client connections after accounting for mesh fan-in.'
END AS "MX1 headline";
\pset tuples_only off

DROP TABLE _mx1_verdict;
DROP TABLE _mx1_calc;
DROP TABLE _mx1_coord;
DROP TABLE _mx1_raw;
