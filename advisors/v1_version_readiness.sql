-- =====================================================================
-- citus_analyze / V1 : version & upgrade readiness
-- ---------------------------------------------------------------------
-- Verifies that every node in the cluster is running compatible
-- PostgreSQL and Citus versions and that no upgrade step is pending.
-- Citus clusters go sideways when:
--   * Two nodes run DIFFERENT Citus binaries (worker & coord) — any
--     SQL that crosses the wire can fail with "this Citus version is
--     not supported" or silently return wrong results.
--   * A node had its Citus PACKAGE upgraded but ALTER EXTENSION citus
--     UPDATE was never run. `citus_version()` reports the new binary
--     but pg_extension.extversion still shows the old SQL layer; the
--     two are inconsistent and features behave unpredictably.
--   * `shared_preload_libraries` is missing `citus` on a node (Citus
--     will be partially non-functional there).
--   * A coord-side extension (pg_stat_statements, fdws, types) is
--     absent on a worker — distributed functions referring to the
--     missing extension will fail.
--   * The PG major version differs across nodes (logical replication
--     for shard moves may still work, but physical / pg_upgrade paths
--     assume homogeneous major).
--
-- Inputs (override with -v):
--   top_n                rows per listing; default 20.
--   require_same_pg_major  1 = CRITICAL if PG major differs; 0 = WARN.
--                          default 1 (homogeneous majors is the norm).
--
-- Caveats
--   * Detecting "ALTER EXTENSION citus UPDATE is pending" uses
--     citus_version() (binary library) vs pg_extension.extversion.
--     On very first install these can differ transiently during a
--     package swap window; rerun after the upgrade completes.
--   * Required-extension drift (V1b) compares the extensions present
--     on the coordinator against each worker. It does NOT know which
--     extensions your schema actually depends on — surfacing the diff
--     is informative but a missing extension is not always fatal.
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?top_n}                 \else \set top_n                 20 \endif
\if :{?require_same_pg_major} \else \set require_same_pg_major 1  \endif

\echo
\echo '==================== V1 : version & upgrade readiness ===================='

-- ---------------------------------------------------------------------
-- Per-node version snapshot.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _v1_raw;
CREATE TEMP TABLE _v1_raw (
    nodeid int, success boolean, result text
);

INSERT INTO _v1_raw
SELECT nodeid, success, result
FROM run_command_on_all_nodes(
$CMD$
SELECT jsonb_build_object(
  'server_version',      current_setting('server_version'),
  'server_version_num',  current_setting('server_version_num'),
  'citus_ext_version',   (SELECT extversion FROM pg_extension
                            WHERE extname='citus'),
  'citus_lib_version',   CASE WHEN (SELECT count(*) FROM pg_extension
                                     WHERE extname='citus') > 0
                              THEN citus_version() END,
  'citus_default_version', (SELECT default_version FROM pg_available_extensions
                              WHERE name='citus'),
  -- Semver-aware max: parse numeric runs (tolerates suffixes like "15.0devel").
  'citus_max_available',   (SELECT version
                              FROM pg_available_extension_versions
                              WHERE name='citus'
                              ORDER BY ARRAY(
                                SELECT t::int
                                FROM regexp_split_to_table(version, '[^0-9]+') t
                                WHERE t ~ '^[0-9]+$'
                              ) DESC
                              LIMIT 1),
  'columnar_ext_version',  (SELECT extversion FROM pg_extension
                              WHERE extname='citus_columnar'),
  'columnar_default_version', (SELECT default_version FROM pg_available_extensions
                              WHERE name='citus_columnar'),
  'shared_preload_libraries', current_setting('shared_preload_libraries'),
  'extensions', (SELECT jsonb_object_agg(extname, extversion)
                   FROM pg_extension)
)::text
$CMD$
, parallel := true);

-- ---------------------------------------------------------------------
-- Parse payload into one row per node.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _v1;
CREATE TEMP TABLE _v1 AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata  THEN 'worker (MX)'
         ELSE 'worker' END                                  AS role,
    n.nodename || ':' || n.nodeport                         AS node,
    n.groupid,
    r.success,
    CASE WHEN r.success AND r.result <> '' THEN r.result::jsonb
         ELSE '{}'::jsonb END                               AS p
FROM _v1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE n.isactive;

-- Derived scalars.
-- cmp_* = int[] form of a version, computed once, NULL-safe,
--         tolerant of suffixes like "15.0devel" via regex extraction.
-- Convention: NULL int[] means "could not parse", treated as "unknown".
DROP TABLE IF EXISTS _v1_calc;
CREATE TEMP TABLE _v1_calc AS
SELECT
    role, node, groupid, success,
    p->>'server_version'                                    AS pg_ver,
    NULLIF(p->>'server_version_num','')::int                AS pg_ver_num,
    (p->>'server_version_num')::int / 10000                 AS pg_major,
    p->>'citus_ext_version'                                 AS citus_ext,
    p->>'citus_lib_version'                                 AS citus_lib,
    p->>'citus_default_version'                             AS citus_default,
    p->>'citus_max_available'                               AS citus_max_avail,
    p->>'columnar_ext_version'                              AS columnar_ext,
    p->>'columnar_default_version'                          AS columnar_default,
    ARRAY(SELECT t::int
            FROM regexp_split_to_table(
                 COALESCE(p->>'citus_ext_version',''), '[^0-9]+') t
            WHERE t ~ '^[0-9]+$')                           AS cmp_ext,
    ARRAY(SELECT t::int
            FROM regexp_split_to_table(
                 COALESCE(p->>'citus_default_version',''), '[^0-9]+') t
            WHERE t ~ '^[0-9]+$')                           AS cmp_default,
    ARRAY(SELECT t::int
            FROM regexp_split_to_table(
                 COALESCE(p->>'citus_max_available',''), '[^0-9]+') t
            WHERE t ~ '^[0-9]+$')                           AS cmp_max,
    ARRAY(SELECT t::int
            FROM regexp_split_to_table(
                 COALESCE(p->>'columnar_ext_version',''), '[^0-9]+') t
            WHERE t ~ '^[0-9]+$')                           AS cmp_cext,
    ARRAY(SELECT t::int
            FROM regexp_split_to_table(
                 COALESCE(p->>'columnar_default_version',''), '[^0-9]+') t
            WHERE t ~ '^[0-9]+$')                           AS cmp_cdefault,
    p->>'shared_preload_libraries'                          AS spl,
    p->'extensions'                                         AS extensions
FROM _v1;

-- ---------------------------------------------------------------------
-- V1a : per-node version snapshot + drift flags
-- ---------------------------------------------------------------------
\echo
\echo '-- V1a. Per-node PG & Citus versions --'
SELECT
    role,
    node,
    pg_ver                                                  AS pg_version,
    citus_ext                                               AS citus_ext,
    citus_default                                           AS binary_default,
    CASE
      WHEN citus_ext IS NULL OR citus_default IS NULL       THEN ''
      WHEN cmp_default > cmp_ext                            THEN 'UPDATE-PENDING'
      WHEN cmp_default < cmp_ext                            THEN 'BINARY-OLDER'
      ELSE ''
    END                                                     AS ext_vs_binary,
    citus_max_avail                                         AS max_available,
    CASE WHEN 'citus' = ANY(string_to_array(
            regexp_replace(COALESCE(spl,''),'\s','','g'),','))
         THEN 'yes' ELSE 'MISSING' END                      AS in_spl
FROM _v1_calc
ORDER BY groupid, node;

-- ---------------------------------------------------------------------
-- V1b : extension drift (coord vs each worker)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _v1_ext;
CREATE TEMP TABLE _v1_ext AS
SELECT
    c.node,
    kv.key                                                  AS ext_name,
    kv.value #>> '{}'                                       AS ext_version,
    c.groupid
FROM _v1_calc c
LEFT JOIN LATERAL jsonb_each(COALESCE(c.extensions, '{}'::jsonb)) kv ON true
WHERE c.success;

DROP TABLE IF EXISTS _v1_ext_drift;
CREATE TEMP TABLE _v1_ext_drift AS
WITH coord_ext AS (
  SELECT ext_name, ext_version FROM _v1_ext WHERE groupid = 0
),
worker_ext AS (
  SELECT node, ext_name, ext_version FROM _v1_ext WHERE groupid <> 0
)
SELECT
    COALESCE(c.ext_name, w.ext_name)                        AS ext_name,
    w.node                                                  AS worker_node,
    c.ext_version                                           AS coord_ver,
    w.ext_version                                           AS worker_ver,
    CASE
      WHEN c.ext_name IS NULL                        THEN 'worker-only'
      WHEN w.ext_version IS NULL                     THEN 'missing-on-worker'
      WHEN c.ext_version <> w.ext_version            THEN 'version-mismatch'
      ELSE 'ok'
    END                                                     AS status
FROM coord_ext c
FULL JOIN worker_ext w USING (ext_name)
WHERE c.ext_version IS DISTINCT FROM w.ext_version
   OR (c.ext_name IS NULL OR w.ext_name IS NULL);

\echo
\echo '-- V1b. Extension drift (coordinator vs workers; ok rows suppressed) --'
SELECT worker_node, ext_name, coord_ver, worker_ver, status
FROM _v1_ext_drift
WHERE status <> 'ok'
ORDER BY status, ext_name, worker_node
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- V1c : upgrade opportunities (extversion < max available)
-- ---------------------------------------------------------------------
\echo
\echo '-- V1c. Citus upgrade opportunities --'
SELECT
    node,
    citus_ext                       AS currently_installed,
    citus_default                   AS default_available,
    citus_max_avail                 AS max_available,
    CASE
      WHEN citus_ext IS NULL                                 THEN 'extension not installed'
      WHEN cmp_max IS NULL OR array_length(cmp_max,1) IS NULL
                                                             THEN 'cannot parse available'
      WHEN cmp_max > cmp_ext                                 THEN 'newer version available'
      WHEN cmp_max < cmp_ext                                 THEN 'installed is AHEAD of on-disk files (control drift)'
      ELSE 'at latest available'
    END                             AS status
FROM _v1_calc
ORDER BY groupid, node;

-- Citus Columnar upgrade opportunities (only when present somewhere).
\echo
\echo '-- V1c2. citus_columnar pending updates --'
SELECT
    node,
    columnar_ext                    AS currently_installed,
    columnar_default                AS binary_default,
    CASE
      WHEN columnar_ext IS NULL                              THEN 'not installed'
      WHEN columnar_default IS NULL                          THEN 'binary default unknown'
      WHEN cmp_cdefault > cmp_cext                           THEN 'UPDATE pending (run ALTER EXTENSION citus_columnar UPDATE)'
      WHEN cmp_cdefault < cmp_cext                           THEN 'BINARY-OLDER (control drift)'
      ELSE 'at binary default'
    END                             AS status
FROM _v1_calc
WHERE EXISTS (SELECT 1 FROM _v1_calc c2
                WHERE c2.columnar_ext IS NOT NULL)
ORDER BY groupid, node;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sig AS (
  SELECT
    count(DISTINCT pg_major)                                     AS pg_majors,
    min(pg_major)                                                AS min_pg_major,
    max(pg_major)                                                AS max_pg_major,
    count(DISTINCT citus_ext)      FILTER (WHERE citus_ext     IS NOT NULL) AS ext_variants,
    count(DISTINCT citus_default)  FILTER (WHERE citus_default IS NOT NULL) AS binary_variants,
    count(*) FILTER (WHERE cmp_default > cmp_ext
                       AND array_length(cmp_ext,1)     IS NOT NULL
                       AND array_length(cmp_default,1) IS NOT NULL)
                                                                 AS update_pending_n,
    count(*) FILTER (WHERE cmp_default < cmp_ext
                       AND array_length(cmp_ext,1)     IS NOT NULL
                       AND array_length(cmp_default,1) IS NOT NULL)
                                                                 AS binary_older_n,
    count(*) FILTER (WHERE cmp_cdefault > cmp_cext
                       AND array_length(cmp_cext,1)     IS NOT NULL
                       AND array_length(cmp_cdefault,1) IS NOT NULL)
                                                                 AS columnar_update_pending_n,
    count(*) FILTER (WHERE NOT ('citus' = ANY(string_to_array(
            regexp_replace(COALESCE(spl,''),'\s','','g'),','))))
                                                                 AS spl_missing_n,
    count(*) FILTER (WHERE cmp_max > cmp_ext
                       AND array_length(cmp_ext,1) IS NOT NULL
                       AND array_length(cmp_max,1) IS NOT NULL)  AS upgradeable_n,
    (SELECT count(*) FROM _v1_ext_drift
       WHERE status = 'missing-on-worker')                       AS ext_missing_worker,
    (SELECT count(*) FROM _v1_ext_drift
       WHERE status = 'worker-only')                             AS ext_worker_only,
    (SELECT count(*) FROM _v1_ext_drift
       WHERE status = 'version-mismatch')                        AS ext_version_mismatch,
    (SELECT count(*) FROM _v1_raw WHERE NOT success)             AS unreachable
  FROM _v1_calc
)
SELECT CASE
  -- CRITICAL signals first, so that a partially-unreachable cluster
  -- with known critical drift among reachable nodes still surfaces it.
  WHEN binary_variants > 1 THEN
    format('CRITICAL : Citus BINARY package version differs across nodes (%s distinct values). Distributed queries may fail. Install the same Citus package on every node.%s',
           binary_variants,
           CASE WHEN unreachable > 0 THEN format(' [%s node(s) also unreachable]', unreachable) ELSE '' END)
  WHEN ext_variants > 1 THEN
    format('CRITICAL : Citus extension SQL version differs across nodes (%s distinct values). Run ALTER EXTENSION citus UPDATE on lagging nodes.%s',
           ext_variants,
           CASE WHEN unreachable > 0 THEN format(' [%s node(s) also unreachable]', unreachable) ELSE '' END)
  WHEN spl_missing_n > 0 THEN
    format('CRITICAL : %s node(s) missing ''citus'' from shared_preload_libraries. Fix postgresql.conf and restart.',
           spl_missing_n)
  WHEN binary_older_n > 0 THEN
    format('CRITICAL : %s node(s) have binary package OLDER than the installed SQL version. Control drift or package downgrade. Reinstall the correct Citus package.',
           binary_older_n)
  WHEN pg_majors > 1 AND :require_same_pg_major::int = 1 THEN
    format('CRITICAL : PostgreSQL MAJOR version differs across nodes (%s..%s). pg_upgrade the lagging node(s). Override with -v require_same_pg_major=0 during planned upgrades.',
           min_pg_major, max_pg_major)
  WHEN unreachable > 0 THEN
    format('WARN : %s node(s) unreachable during V1 snapshot. Version drift cannot be fully verified.', unreachable)
  WHEN update_pending_n > 0 THEN
    format('WARN : %s node(s) have a pending ALTER EXTENSION citus UPDATE (binary default > installed SQL).',
           update_pending_n)
  WHEN columnar_update_pending_n > 0 THEN
    format('WARN : %s node(s) have a pending ALTER EXTENSION citus_columnar UPDATE.',
           columnar_update_pending_n)
  WHEN ext_missing_worker > 0 THEN
    format('WARN : %s extension(s) present on coord but missing on at least one worker. See V1b.',
           ext_missing_worker)
  WHEN ext_version_mismatch > 0 THEN
    format('WARN : %s extension(s) version-mismatched between coord and workers. See V1b.',
           ext_version_mismatch)
  WHEN ext_worker_only > 0 THEN
    format('WARN : %s extension(s) present on a worker but missing on coord. See V1b.',
           ext_worker_only)
  WHEN pg_majors > 1 THEN
    format('WARN : PostgreSQL MAJOR version differs across nodes (%s..%s).',
           min_pg_major, max_pg_major)
  WHEN upgradeable_n > 0 THEN
    format('INFO : %s node(s) could upgrade Citus to a newer available version. See V1c.',
           upgradeable_n)
  ELSE 'OK : all nodes on matching PG major and Citus versions, no pending upgrades.'
END
FROM sig;
\pset tuples_only off

DROP TABLE _v1_ext_drift;
DROP TABLE _v1_ext;
DROP TABLE _v1_calc;
DROP TABLE _v1;
DROP TABLE _v1_raw;
