-- =====================================================================
-- citus_analyze / A3 : 2PC backlog & orphan prepared transactions
-- ---------------------------------------------------------------------
-- Fans out to every active primary to collect pg_prepared_xacts, then
-- cross-checks against the coordinator's pg_dist_transaction to find:
--   * normal Citus prepared xacts (await recovery by the maintenance
--     daemon via RecoverTwoPhaseCommits)
--   * orphan Citus prepared xacts (worker has them, coordinator does
--     NOT — these will NOT auto-recover; they pin WAL + xmin horizon)
--   * foreign (non-Citus) prepared xacts (out of scope, reported but
--     not flagged as Citus issues)
--   * coord-side entries in pg_dist_transaction with no matching worker
--     prepared xact (usually means the worker already committed/rolled
--     back; coord row is just stale metadata awaiting maintenance)
--
-- Also reports per-node max_prepared_transactions headroom and the age
-- of the oldest prepared xact (old ones pin WAL & block VACUUM).
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
\echo '==================== A3 : 2PC backlog & orphan prepared xacts ===================='

-- ---------------------------------------------------------------------
-- Collect prepared xacts from every worker + headroom counts
-- (The coord participates in 2PC too, so we scan its own catalog.)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _a3_prep;
CREATE TEMP TABLE _a3_prep (
    nodename text, nodeport int,
    gid text, prepared_at timestamptz, owner name, database name
);

-- workers
INSERT INTO _a3_prep
SELECT r.nodename, r.nodeport,
       split_part(s, E'\t', 1),
       split_part(s, E'\t', 2)::timestamptz,
       split_part(s, E'\t', 3)::name,
       split_part(s, E'\t', 4)::name
FROM run_command_on_workers(
    $$ SELECT string_agg(gid || E'\t' || prepared || E'\t' || owner || E'\t' || database, E'\n')
         FROM pg_prepared_xacts $$
) r
JOIN LATERAL regexp_split_to_table(coalesce(r.result,''), E'\n') AS s ON s <> ''
WHERE r.success;

-- coord
INSERT INTO _a3_prep
SELECT (SELECT nodename FROM pg_dist_node WHERE groupid=0 AND isactive LIMIT 1),
       (SELECT nodeport FROM pg_dist_node WHERE groupid=0 AND isactive LIMIT 1),
       gid, prepared, owner::name, database::name
FROM pg_prepared_xacts;

-- headroom per node (workers via run_command, coord directly)
DROP TABLE IF EXISTS _a3_headroom;
CREATE TEMP TABLE _a3_headroom (
    nodename text, nodeport int, is_coord boolean,
    max_prepared int, current_prepared int
);

INSERT INTO _a3_headroom
SELECT r.nodename, r.nodeport, false,
       (regexp_match(r.result, 'max=([0-9]+)'))[1]::int,
       (regexp_match(r.result, 'cur=([0-9]+)'))[1]::int
FROM run_command_on_workers(
    $$ SELECT format('max=%s;cur=%s',
                     current_setting('max_prepared_transactions'),
                     (SELECT count(*) FROM pg_prepared_xacts)) $$
) r
WHERE r.success;

INSERT INTO _a3_headroom
SELECT n.nodename, n.nodeport, true,
       current_setting('max_prepared_transactions')::int,
       (SELECT count(*)::int FROM pg_prepared_xacts)
FROM pg_dist_node n
WHERE n.groupid = 0 AND n.isactive;

-- ---------------------------------------------------------------------
-- A3a. Classification per prepared xact
-- ---------------------------------------------------------------------
\echo
\echo '-- A3a. Per-node prepared-xact summary --'

WITH classified AS (
    SELECT p.*,
           (p.gid LIKE 'citus\_%' ESCAPE '\') AS is_citus,
           EXISTS (
               SELECT 1 FROM pg_dist_transaction dt WHERE dt.gid = p.gid
           )                                  AS known_to_coord,
           round(extract(epoch FROM (now() - p.prepared_at))/60, 1) AS age_min
    FROM _a3_prep p
)
SELECT nodename, nodeport,
       count(*) FILTER (WHERE is_citus AND known_to_coord)      AS normal_citus,
       count(*) FILTER (WHERE is_citus AND NOT known_to_coord)  AS orphan_citus,
       count(*) FILTER (WHERE NOT is_citus)                      AS foreign_xacts,
       count(*)                                                  AS total,
       coalesce(max(age_min), 0)                                 AS oldest_min
FROM classified
GROUP BY nodename, nodeport
ORDER BY nodename, nodeport;

\echo
\echo '-- A3b. Orphan Citus prepared xacts (worker-only; will NOT auto-recover) --'
SELECT nodename, nodeport, gid, owner, database,
       prepared_at,
       round(extract(epoch FROM (now() - prepared_at))/60, 1)  AS age_min,
       CASE WHEN extract(epoch FROM (now() - prepared_at))/60 >= :age_crit_min THEN 'CRITICAL'
            WHEN extract(epoch FROM (now() - prepared_at))/60 >= :age_warn_min THEN 'WARN'
            ELSE 'INFO' END                                     AS age_verdict,
       -- manual recovery hint: coord has no record -> ROLLBACK PREPARED on the worker
       format('On %s:%s run:  ROLLBACK PREPARED %L;', nodename, nodeport, gid) AS remediation
FROM _a3_prep p
WHERE p.gid LIKE 'citus\_%' ESCAPE '\'
  AND NOT EXISTS (SELECT 1 FROM pg_dist_transaction dt WHERE dt.gid = p.gid)
ORDER BY prepared_at;

\echo
\echo '-- A3c. Coord-side pg_dist_transaction entries with NO matching worker prepared xact --'
\echo '   (usually stale; maintenance daemon will clean them)'
SELECT dt.groupid, dt.gid, dt.outer_xid
FROM pg_dist_transaction dt
WHERE NOT EXISTS (
    SELECT 1 FROM _a3_prep p
      JOIN pg_dist_node n ON n.nodename=p.nodename AND n.nodeport=p.nodeport
     WHERE n.groupid = dt.groupid AND p.gid = dt.gid
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
      (SELECT count(*) FROM _a3_prep
         WHERE gid LIKE 'citus\_%' ESCAPE '\'
           AND NOT EXISTS (SELECT 1 FROM pg_dist_transaction dt WHERE dt.gid = _a3_prep.gid))
          AS orphans,
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
    WHEN orphans > 0 AND coalesce(oldest_citus_min,0) >= :age_crit_min
      THEN format('CRITICAL : %s orphan Citus prepared xact(s), oldest %s min. WAL & xmin horizon are pinned. Resolve with ROLLBACK PREPARED.',
                  orphans, round(oldest_citus_min::numeric,1))
    WHEN orphans > 0
      THEN format('WARN : %s orphan Citus prepared xact(s); maintenance daemon will NOT recover these.', orphans)
    WHEN coalesce(max_pct_used,0) >= :headroom_crit_pct
      THEN format('CRITICAL : max_prepared_transactions usage %s%% on one or more nodes.', round(max_pct_used::numeric,1))
    WHEN coalesce(oldest_citus_min,0) >= :age_warn_min
      THEN format('WARN : oldest Citus prepared xact is %s min (pins WAL). Check maintenance daemon.', round(oldest_citus_min::numeric,1))
    ELSE 'OK : no 2PC backlog.'
  END
FROM summary;

\pset tuples_only off

DROP TABLE _a3_prep;
DROP TABLE _a3_headroom;
