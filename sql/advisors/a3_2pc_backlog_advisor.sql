\set advisor_id A3
\ir ../capabilities.sql
-- =====================================================================
-- citus_analyze / A3 : prepared-transaction age and capacity
-- ---------------------------------------------------------------------
-- Fans out to every active primary to collect pg_prepared_xacts, then
-- reports recovery-record visibility only for this database and origin.
-- A snapshot cannot distinguish an active transaction from one requiring
-- recovery. Citus recovery checks origin, active transactions and repeated
-- snapshots; absence of a local recovery record is not a rollback decision.
--
-- Also reports per-node max_prepared_transactions headroom and the age
-- of the oldest prepared xact (old transactions can retain locks and xmin).
--
-- Citus 2PC gid prefix convention: 'citus_...'  (see prepare.c)
--
-- Inputs (override with -v):
--   age_warn_min          WARN if oldest prepared xact > N min; default 10
--   age_crit_min          CRITICAL if > N min; default 60
--   headroom_warn_pct     WARN if count/max_prepared > N%; default 50
--   headroom_crit_pct     CRITICAL if count/max_prepared > N%; default 80
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?age_warn_min}      \else \set age_warn_min       10 \endif
\if :{?age_crit_min}      \else \set age_crit_min       60 \endif
\if :{?headroom_warn_pct} \else \set headroom_warn_pct  50 \endif
\if :{?headroom_crit_pct} \else \set headroom_crit_pct  80 \endif

\echo
\echo '==================== A3 : prepared-transaction age and capacity ===================='

-- ---------------------------------------------------------------------
-- Collect prepared xacts from every worker + headroom counts
-- (The coord participates in 2PC too, so we scan its own catalog.)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._a3_raw;
CREATE TEMP TABLE _a3_raw AS
SELECT * FROM run_command_on_all_nodes($CMD$
  SELECT jsonb_build_object(
    'max_prepared', current_setting('max_prepared_transactions')::int,
    'current_prepared', (SELECT count(*) FROM pg_prepared_xacts),
    'transactions', (SELECT coalesce(jsonb_agg(to_jsonb(prepared)), '[]'::jsonb)
                     FROM pg_prepared_xacts prepared)
  )::text
$CMD$, parallel := true);

DROP TABLE IF EXISTS pg_temp._a3_prep;
CREATE TEMP TABLE _a3_prep AS
SELECT node.nodename, node.nodeport, node.groupid,
       item->>'gid' AS gid,
       (item->>'prepared')::timestamptz AS prepared_at,
       item->>'owner' AS owner, item->>'database' AS database
FROM _a3_raw raw
JOIN pg_dist_node node USING (nodeid)
CROSS JOIN LATERAL jsonb_array_elements(
  CASE WHEN raw.success THEN raw.result::jsonb->'transactions'
       ELSE '[]'::jsonb END) item;

DROP TABLE IF EXISTS pg_temp._a3_headroom;
CREATE TEMP TABLE _a3_headroom AS
SELECT node.nodename, node.nodeport, node.groupid = 0 AS is_coord,
       (raw.result::jsonb->>'max_prepared')::int AS max_prepared,
       (raw.result::jsonb->>'current_prepared')::int AS current_prepared
FROM _a3_raw raw JOIN pg_dist_node node USING (nodeid)
WHERE raw.success;

-- ---------------------------------------------------------------------
-- A3a. Classification per prepared xact
-- ---------------------------------------------------------------------
\echo
\echo '-- A3a. Per-node prepared-xact summary --'

WITH classified AS (
    SELECT p.*,
           (p.gid LIKE 'citus\_%' ESCAPE '\') AS is_citus,
           round(extract(epoch FROM (now() - p.prepared_at))/60, 1) AS age_min
    FROM _a3_prep p
)
SELECT nodename, nodeport,
      count(*) FILTER (WHERE is_citus)                         AS citus_prepared,
       count(*) FILTER (WHERE NOT is_citus)                      AS foreign_xacts,
       count(*)                                                  AS total,
       coalesce(max(age_min), 0)                                 AS oldest_min
FROM classified
GROUP BY nodename, nodeport
ORDER BY nodename, nodeport;

\echo
\echo '-- A3b. Citus prepared transactions (record absence does not establish an orphan) --'
SELECT nodename, nodeport, gid, owner, database,
       prepared_at,
       round(extract(epoch FROM (now() - prepared_at))/60, 1)  AS age_min,
       CASE WHEN extract(epoch FROM (now() - prepared_at))/60 >= :age_crit_min THEN 'CRITICAL'
            WHEN extract(epoch FROM (now() - prepared_at))/60 >= :age_warn_min THEN 'WARN'
            ELSE 'INFO' END                                     AS age_verdict,
       CASE WHEN database <> current_database()
                  OR split_part(gid, '_', 2) <>
                     (SELECT groupid::text FROM pg_dist_local_group)
            THEN 'outside this database/origin; not assessed'
            WHEN EXISTS (SELECT 1 FROM pg_dist_transaction dt
                         WHERE dt.gid = p.gid AND dt.groupid = p.groupid)
            THEN 'local recovery record visible'
            ELSE 'no local record visible; active/recovery state unknown'
       END AS recovery_observation
FROM _a3_prep p
WHERE p.gid LIKE 'citus\_%' ESCAPE '\'
ORDER BY prepared_at;

\echo
\echo '-- A3c. Coord-side pg_dist_transaction entries with NO matching worker prepared xact --'
\echo '   (snapshot mismatch only; may reflect concurrent recovery or incomplete coverage)'
SELECT dt.groupid, dt.gid
FROM pg_dist_transaction dt
WHERE NOT EXISTS (
    SELECT 1 FROM _a3_prep p
      JOIN pg_dist_node n ON n.nodename=p.nodename AND n.nodeport=p.nodeport
     WHERE n.groupid = dt.groupid AND p.gid = dt.gid
       AND p.database = current_database()
)
LIMIT 50;

\echo
\echo '-- A3d. max_prepared_transactions headroom per node --'
SELECT nodename, nodeport,
       CASE WHEN is_coord THEN 'coord' ELSE 'worker' END AS role,
       max_prepared, current_prepared,
       CASE WHEN max_prepared = 0 THEN 0
            ELSE round(100.0 * current_prepared / max_prepared, 1) END AS pct_used,
       CASE
         WHEN max_prepared = 0 THEN 'CRITICAL : max_prepared_transactions=0 -> Citus 2PC DISABLED on this node'
         WHEN current_prepared * 100 >= max_prepared * :headroom_crit_pct THEN 'CRITICAL'
         WHEN current_prepared * 100 >= max_prepared * :headroom_warn_pct THEN 'WARN'
         ELSE 'OK'
       END AS verdict
FROM _a3_headroom
ORDER BY pct_used DESC;

-- ---------------------------------------------------------------------
-- A3 Headline
-- ---------------------------------------------------------------------
\echo
\echo '-- A3 headline --'
\pset tuples_only on

WITH summary AS (
    SELECT
      (SELECT max(extract(epoch FROM (now() - prepared_at))/60) FROM _a3_prep
         WHERE gid LIKE 'citus\_%' ESCAPE '\')
          AS oldest_citus_min,
      (SELECT max(current_prepared * 100.0 / NULLIF(max_prepared,0)) FROM _a3_headroom)
          AS max_pct_used,
      (SELECT count(*) FROM _a3_headroom WHERE max_prepared = 0)
          AS zero_mpx
)
SELECT
  CASE
    WHEN zero_mpx > 0
      THEN format('CRITICAL : %s node(s) have max_prepared_transactions=0. Citus 2PC cannot function. Set max_prepared_transactions >= max_connections.', zero_mpx)
    WHEN coalesce(max_pct_used,0) >= :headroom_crit_pct
      THEN format('CRITICAL : max_prepared_transactions usage %s%% on one or more nodes.', round(max_pct_used::numeric,1))
    WHEN coalesce(oldest_citus_min,0) >= :age_crit_min
      THEN format('WARN : oldest Citus prepared transaction is %s min; investigate its origin and recovery state. Snapshot cannot determine a safe recovery action.', round(oldest_citus_min::numeric,1))
    WHEN coalesce(max_pct_used,0) >= :headroom_warn_pct
      THEN format('WARN : max_prepared_transactions usage %s%% on one or more nodes.', round(max_pct_used::numeric,1))
    WHEN coalesce(oldest_citus_min,0) >= :age_warn_min
      THEN format('WARN : oldest Citus prepared transaction is %s min; inspect recovery and retained locks/xmin.', round(oldest_citus_min::numeric,1))
    WHEN EXISTS (SELECT 1 FROM _a3_raw WHERE NOT success)
      THEN 'WARN : prepared-transaction snapshot incomplete; one or more node probes failed.'
    ELSE 'OK : no prepared-transaction age or capacity policy thresholds exceeded on responding nodes.'
  END
FROM summary;

\pset tuples_only off

DROP TABLE pg_temp._a3_prep;
DROP TABLE pg_temp._a3_headroom;
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._a3_raw;
