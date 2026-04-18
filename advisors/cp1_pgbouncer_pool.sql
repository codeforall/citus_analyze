-- =====================================================================
-- citus_analyze / CP1 : pgbouncer pool sizing for a Citus front
-- ---------------------------------------------------------------------
-- Tells the operator exactly what `pool_size`, `max_client_conn`,
-- `reserve_pool_size`, `min_pool_size`, and timeouts to set on a
-- pgbouncer that fronts a Citus cluster, given the safe-external cap
-- already computed by C3 plus observed concurrency.
--
-- Why this matters
-- ----------------
-- * pgbouncer's `pool_size` is the number of server-side connections
--   it will hold open per (database, user) pair, per pgbouncer
--   instance. If pool_size > the C3 cap on the entry node it points
--   at, the cluster will reject sessions under load.
-- * For Citus the only sane mode is **transaction**. Session mode ties
--   one Citus backend per client, defeating the pool, AND breaks the
--   adaptive executor's outbound connection pool because intermediate
--   results may be left behind. Statement mode breaks Citus 2PC.
-- * pgbouncer 1.21+ on PG14+ supports prepared statements in
--   transaction mode. We surface that compatibility check.
-- * Per-database pool budgets must SUM to the entry node's safe cap.
--
-- Sections
-- --------
--   CP1a  Per entry node recommended pool_size, reserve_pool_size,
--         max_client_conn, min_pool_size with the formula breakdown.
--   CP1b  Observed peak active client backends per node vs the cap
--         (to detect already-existing under-pooling or over-pooling).
--   CP1c  Per-database pool budget if the database has > 1 distributed
--         database; the pool sum across DBs must not exceed the cap.
--   CP1d  Copy-pasteable pgbouncer.ini snippet for each entry node.
--   CP1e  Connectivity & version compatibility notes (transaction mode,
--         prepared statements, idle timeouts vs Citus expectations).
--
-- Inputs (override with -v)
--   overhead_per_node   Reserve for bg workers/AV/daemons (default 15).
--   k_reuse             Outbound conn occupancy fraction (default 0.6).
--                       (Same semantics as C3.)
--   headroom_pct        Plan-for-this-percent-of-cap (default 80).
--   client_multiplier   max_client_conn = pool_size * this (default 10).
--   reserve_pct         reserve_pool_size = pool_size * this/100
--                       (default 20).
--   min_pool_pct        min_pool_size = pool_size * this/100
--                       (default 25).
--   surge_factor        observed -> recommended buffer (default 1.3).
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?overhead_per_node} \else \set overhead_per_node 15  \endif
\if :{?k_reuse}           \else \set k_reuse           0.6 \endif
\if :{?headroom_pct}      \else \set headroom_pct      80  \endif
\if :{?client_multiplier} \else \set client_multiplier 10  \endif
\if :{?reserve_pct}       \else \set reserve_pct       20  \endif
\if :{?min_pool_pct}      \else \set min_pool_pct      25  \endif
\if :{?surge_factor}      \else \set surge_factor      1.3 \endif

\echo '==================== CP1 : pgbouncer pool sizing ===================='

-- ---------------------------------------------------------------------
-- 1) Per-node GUCs + observed peak external backends.
--    Reuses the same fan-out shape as C3 to keep formulas aligned.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _cp1_nodes;
CREATE TEMP TABLE _cp1_nodes (
    nodename text, nodeport int, is_coord boolean, is_mx boolean,
    max_connections int,
    superuser_reserved int,
    citus_max_shared_pool int,
    server_version_num int,
    n_active_external int,    -- snapshot peak from pg_stat_activity
    n_databases int           -- # distributed databases (with citus extension)
);

-- coordinator
INSERT INTO _cp1_nodes
SELECT
    n.nodename, n.nodeport, true, n.hasmetadata,
    current_setting('max_connections')::int,
    current_setting('superuser_reserved_connections')::int,
    NULLIF(current_setting('citus.max_shared_pool_size')::int, -1),
    current_setting('server_version_num')::int,
    (SELECT count(*)::int FROM pg_stat_activity
       WHERE backend_type='client backend'
         AND application_name NOT LIKE 'citus%'
         AND application_name NOT LIKE 'Citus%'),
    (SELECT count(*)::int FROM pg_database d
       WHERE NOT d.datistemplate AND d.datallowconn)
FROM pg_dist_node n
WHERE n.groupid = 0 AND n.isactive;

-- workers via fan-out
INSERT INTO _cp1_nodes
SELECT
    r.nodename, r.nodeport, false,
    (SELECT hasmetadata FROM pg_dist_node dn
       WHERE dn.nodename=r.nodename AND dn.nodeport=r.nodeport),
    (regexp_match(r.result, 'mc=([0-9-]+)'))[1]::int,
    (regexp_match(r.result, 'sr=([0-9-]+)'))[1]::int,
    NULLIF((regexp_match(r.result, 'sp=([0-9-]+)'))[1]::int, -1),
    (regexp_match(r.result, 'sv=([0-9]+)'))[1]::int,
    (regexp_match(r.result, 'ne=([0-9]+)'))[1]::int,
    (regexp_match(r.result, 'nd=([0-9]+)'))[1]::int
FROM run_command_on_workers(
    $$ SELECT format('mc=%s;sr=%s;sp=%s;sv=%s;ne=%s;nd=%s',
        current_setting('max_connections'),
        current_setting('superuser_reserved_connections'),
        current_setting('citus.max_shared_pool_size'),
        current_setting('server_version_num'),
        (SELECT count(*) FROM pg_stat_activity
           WHERE backend_type='client backend'
             AND application_name NOT LIKE 'citus%'
             AND application_name NOT LIKE 'Citus%'),
        (SELECT count(*) FROM pg_database d
           WHERE NOT d.datistemplate AND d.datallowconn)) $$
) r
WHERE r.success;

-- ---------------------------------------------------------------------
-- 2) Compute per-node safe entry cap (mirrors C3's per-entry inbound)
--    and derive recommended pgbouncer settings.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _cp1_recs;
CREATE TEMP TABLE _cp1_recs AS
WITH params AS (
    SELECT
        (:'overhead_per_node')::int   AS overhead,
        (:'headroom_pct')::numeric    AS headroom,
        (:'client_multiplier')::int   AS client_mult,
        (:'reserve_pct')::numeric     AS reserve_pct,
        (:'min_pool_pct')::numeric    AS min_pool_pct,
        (:'surge_factor')::numeric    AS surge
),
caps AS (
    SELECT
        n.*,
        GREATEST(
            n.max_connections - n.superuser_reserved - p.overhead,
            0
        )                                                 AS inbound_budget,
        GREATEST(
            (LEAST(n.max_connections - n.superuser_reserved - p.overhead,
                   COALESCE(n.citus_max_shared_pool,
                            n.max_connections))::numeric
              * p.headroom / 100.0)::int,
            0
        )                                                 AS safe_external_cap,
        p.surge, p.client_mult, p.reserve_pct, p.min_pool_pct
    FROM _cp1_nodes n, params p
),
recs AS (
    SELECT
        nodename, nodeport, is_coord, is_mx,
        max_connections, superuser_reserved, citus_max_shared_pool,
        n_active_external, n_databases,
        safe_external_cap,
        -- pool_size: fits within safe cap, divided by # databases on
        -- this node (since pgbouncer pool is per-database). Also
        -- bounded below by surge*observed so live load doesn't starve.
        GREATEST(
          (safe_external_cap / GREATEST(n_databases,1))::int,
          (n_active_external * surge)::int
        )                                                  AS demand_pool,
        -- but not above the per-db share of safe cap
        (safe_external_cap / GREATEST(n_databases,1))::int AS budget_pool
    FROM caps
)
SELECT
    nodename, nodeport, is_coord, is_mx,
    max_connections, superuser_reserved, citus_max_shared_pool,
    n_active_external, n_databases, safe_external_cap,
    LEAST(demand_pool, budget_pool)                                       AS pool_size,
    GREATEST(LEAST(demand_pool, budget_pool) * (:'reserve_pct')::int / 100, 1) AS reserve_pool_size,
    GREATEST(LEAST(demand_pool, budget_pool) * (:'min_pool_pct')::int / 100, 0) AS min_pool_size,
    LEAST(demand_pool, budget_pool) * (:'client_multiplier')::int             AS max_client_conn,
    -- flag if observed already exceeds budget
    CASE WHEN n_active_external > safe_external_cap
         THEN 'OVER-CAPACITY' ELSE 'ok' END                               AS observed_state
FROM recs;

-- ---------------------------------------------------------------------
-- CP1a : per-entry-node recommendations
-- ---------------------------------------------------------------------
\echo
\echo '-- CP1a. Per-entry-node pgbouncer recommendations --'
SELECT
    nodename || ':' || nodeport     AS entry_node,
    CASE WHEN is_coord THEN 'coord' ELSE 'worker' END
      || CASE WHEN is_mx THEN ' (MX)' ELSE '' END    AS role,
    max_connections, superuser_reserved,
    safe_external_cap                AS safe_cap,
    n_databases                      AS dbs,
    pool_size, reserve_pool_size, min_pool_size, max_client_conn,
    observed_state
FROM _cp1_recs
ORDER BY is_coord DESC, nodename, nodeport;

-- ---------------------------------------------------------------------
-- CP1b : observed external concurrency vs safe cap
-- ---------------------------------------------------------------------
\echo
\echo '-- CP1b. Observed external concurrency vs safe cap --'
SELECT
    nodename || ':' || nodeport     AS entry_node,
    n_active_external               AS observed_now,
    safe_external_cap               AS safe_cap,
    CASE WHEN safe_external_cap > 0
         THEN round(100.0 * n_active_external / safe_external_cap, 0)
         ELSE NULL END              AS pct_used,
    CASE
      WHEN n_active_external > safe_external_cap
        THEN 'CRITICAL: already above safe cap'
      WHEN n_active_external > safe_external_cap * 0.8
        THEN 'WARN: > 80% of safe cap, scale pool now'
      ELSE 'ok'
    END                              AS verdict
FROM _cp1_recs
ORDER BY pct_used DESC NULLS LAST;

-- ---------------------------------------------------------------------
-- CP1c : multi-database pool budget breakdown (only if any node has
--        more than one user database). Pool budget is per (db,user) so
--        the SUM across DBs must respect the safe cap.
-- ---------------------------------------------------------------------
\echo
\echo '-- CP1c. Multi-database pool budget (per-node) --'
SELECT
    nodename || ':' || nodeport     AS entry_node,
    n_databases                     AS databases,
    safe_external_cap               AS safe_cap,
    pool_size                       AS per_db_pool_size,
    pool_size * n_databases         AS sum_pools,
    CASE
      WHEN pool_size * n_databases > safe_external_cap
        THEN 'WARN: per-db pools sum above cap; partition the budget'
      ELSE 'ok'
    END                              AS verdict
FROM _cp1_recs
WHERE n_databases > 1
ORDER BY nodename, nodeport;

-- ---------------------------------------------------------------------
-- CP1d : copy-paste pgbouncer.ini snippet
-- ---------------------------------------------------------------------
\echo
\echo '-- CP1d. pgbouncer.ini snippet (one [databases] entry per node) --'
\pset format unaligned
\pset tuples_only on
SELECT '
;; --- pgbouncer.ini snippet for ' || nodename || ':' || nodeport || ' ---
[databases]
;; one stanza per app-database; replace <db> and <user> as needed
* = host=' || nodename || ' port=' || nodeport || E'\n' ||
'
[pgbouncer]
listen_port      = 6432
listen_addr      = *
auth_type        = scram-sha-256
auth_file        = /etc/pgbouncer/userlist.txt
pool_mode        = transaction        ;; REQUIRED for Citus
default_pool_size = ' || pool_size || '
min_pool_size    = ' || min_pool_size || '
reserve_pool_size = ' || reserve_pool_size || '
reserve_pool_timeout = 5
max_client_conn  = ' || max_client_conn || '
server_idle_timeout = 600
server_lifetime  = 3600
query_wait_timeout = 120
;; PG ' ||
  CASE WHEN server_version_num >= 140000
       THEN '14+, pgbouncer 1.21+: prepared statements OK in tx mode'
       ELSE '<14: prepared statements UNSUPPORTED in tx mode -- disable in app'
  END || E'\n'
FROM _cp1_recs r
JOIN _cp1_nodes n USING (nodename, nodeport)
ORDER BY r.is_coord DESC, nodename, nodeport;
\pset tuples_only off
\pset format aligned

-- ---------------------------------------------------------------------
-- CP1e : compatibility & operational notes
-- ---------------------------------------------------------------------
\echo
\echo '-- CP1e. Notes for pgbouncer in front of Citus --'
SELECT
    nodename || ':' || nodeport AS entry_node,
    server_version_num          AS pg_version_num,
    CASE
      WHEN server_version_num >= 140000
        THEN 'OK: PG14+, pgbouncer 1.21+ tx-mode supports prepared stmts'
      ELSE 'WARN: PG<14 cannot use prepared statements in tx-mode pgbouncer'
    END                         AS prepared_stmts,
    CASE
      WHEN citus_max_shared_pool IS NOT NULL
        THEN 'NOTE: citus.max_shared_pool_size set; pool_size already accounts for it'
      ELSE 'NOTE: citus.max_shared_pool_size = -1 (uncapped); pool_size limited only by max_connections'
    END                         AS shared_pool_note
FROM _cp1_nodes
ORDER BY is_coord DESC, nodename, nodeport;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sig AS (
  SELECT
    (SELECT count(*) FROM _cp1_recs WHERE observed_state = 'OVER-CAPACITY') AS over_n,
    (SELECT count(*) FROM _cp1_recs
       WHERE n_active_external > 0
         AND safe_external_cap > 0
         AND n_active_external > safe_external_cap * 0.8
         AND n_active_external <= safe_external_cap)                          AS warn_n,
    (SELECT min(pool_size) FROM _cp1_recs)                                    AS min_pool,
    (SELECT max(pool_size) FROM _cp1_recs)                                    AS max_pool,
    (SELECT count(*) FROM _cp1_recs)                                          AS n_entries,
    (SELECT count(*) FROM _cp1_nodes WHERE server_version_num < 140000)       AS old_pg_n,
    (SELECT count(*) FROM _cp1_recs WHERE n_databases > 1
                                      AND pool_size * n_databases > safe_external_cap) AS multidb_warn
)
SELECT CASE
  WHEN over_n > 0 THEN
    format('CRITICAL : %s entry node(s) ALREADY over the safe external cap. Reduce client traffic or scale pool down. See CP1b.', over_n)
  WHEN warn_n > 0 THEN
    format('WARN : %s entry node(s) > 80%% of safe cap. Adopt CP1a recommendations or scale pool size before peak. See CP1a/CP1b.', warn_n)
  WHEN multidb_warn > 0 THEN
    format('WARN : %s node(s) have per-db pool sums above the safe cap. Partition the budget across databases. See CP1c.', multidb_warn)
  WHEN old_pg_n > 0 THEN
    format('INFO : %s node(s) on PG <14: pgbouncer transaction-mode + prepared statements is unsupported. Disable prepared statements in app or upgrade. Recommended pool_size range: %s..%s. See CP1a.',
           old_pg_n, min_pool, max_pool)
  ELSE
    format('OK : recommended pool_size range %s..%s; max_client_conn = pool_size * %s. See CP1a/CP1d.',
           min_pool, max_pool, (:'client_multiplier'))
END
FROM sig;
\pset tuples_only off

DROP TABLE IF EXISTS _cp1_recs;
DROP TABLE IF EXISTS _cp1_nodes;
