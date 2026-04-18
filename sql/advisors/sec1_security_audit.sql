-- SEC1: Security & role audit.
-- Three Citus-specific angles:
--   1. Role drift across nodes. In MX every metadata-synced worker
--      holds the full role catalog; drift means an application role
--      that works on the coord silently fails on a worker (or has
--      wrong password hash). Role propagation is gated by
--      citus.enable_create_role_propagation.
--   2. Authentication posture on every entry point -- in MX, clients
--      can connect to any metadata-synced worker, so password_
--      encryption / ssl / pg_hba effective behavior must be uniform.
--   3. Privilege surface: superuser count, bypassrls, PUBLIC grants
--      on distributed tables, and md5-hashed passwords in
--      pg_authid (pre-scram).
--
-- Inputs (override via psql -v):
--   sec1_max_superusers    = 3    -- warn if cluster has > N superusers
--   sec1_password_valid_days = 365  -- warn if valid_until within N days
--   top_n                  = 50

\pset pager off
\pset border 2
\pset format aligned

\if :{?sec1_max_superusers}        \else \set sec1_max_superusers        3   \endif
\if :{?sec1_password_valid_days}   \else \set sec1_password_valid_days   365 \endif
\if :{?top_n}                      \else \set top_n                      50  \endif

\echo '==================== SEC1 : security & role audit ===================='

-- Fan-out: per-node snapshot.
DROP TABLE IF EXISTS _sec1_raw;
CREATE TEMP TABLE _sec1_raw (nodeid int, success boolean, result text);

INSERT INTO _sec1_raw (nodeid, success, result)
SELECT r.nodeid, r.success, r.result
FROM run_command_on_all_nodes($CMD$
  SELECT jsonb_build_object(
    'roles', (
      SELECT jsonb_agg(jsonb_build_object(
        'rolname',        r.rolname,
        'rolsuper',       r.rolsuper,
        'rolcanlogin',    r.rolcanlogin,
        'rolbypassrls',   r.rolbypassrls,
        'rolcreaterole',  r.rolcreaterole,
        'rolcreatedb',    r.rolcreatedb,
        'rolreplication', r.rolreplication,
        'rolvaliduntil',  r.rolvaliduntil::text,
        'has_pwd',        (a.rolpassword IS NOT NULL),
        'pwd_method',
          CASE WHEN a.rolpassword IS NULL            THEN 'none'
               WHEN a.rolpassword LIKE 'md5%'        THEN 'md5'
               WHEN a.rolpassword LIKE 'SCRAM-SHA-256$%'
                                                     THEN 'scram-sha-256'
               ELSE 'other' END,
        'rolconfig',      r.rolconfig
      ))
      FROM pg_roles r
      LEFT JOIN pg_authid a ON a.rolname = r.rolname
      WHERE r.rolname NOT LIKE 'pg\_%'
    ),
    'settings', jsonb_build_object(
      'password_encryption',    current_setting('password_encryption'),
      'ssl',                    current_setting('ssl'),
      'ssl_ciphers',            current_setting('ssl_ciphers'),
      'row_security',           current_setting('row_security'),
      'log_connections',        current_setting('log_connections'),
      'log_disconnections',     current_setting('log_disconnections'),
      'citus.node_conninfo',    current_setting('citus.node_conninfo'),
      'citus.enable_create_role_propagation',
          current_setting('citus.enable_create_role_propagation'),
      'citus.enable_ddl_propagation',
          current_setting('citus.enable_ddl_propagation')
    ),
    'hba_summary', (
      SELECT jsonb_agg(jsonb_build_object(
        'type', type, 'database', database,
        'user_name', user_name, 'address', address,
        'auth_method', auth_method))
      FROM pg_hba_file_rules
      WHERE auth_method IN ('trust','password')
         OR (type='host' AND auth_method NOT IN ('scram-sha-256','cert','peer','reject'))
    ),
    'hba_nonpw_methods', (
      -- Methods that do NOT require a database-stored password hash.
      -- Used by SEC1b to suppress false-positive "login role without
      -- password" warnings when pg_hba authenticates via OS/identity.
      SELECT jsonb_agg(DISTINCT auth_method)
      FROM pg_hba_file_rules
      WHERE auth_method IN ('peer','trust','cert','ident','gss','sspi')
    ),
    'pub_grants', (
      SELECT jsonb_agg(jsonb_build_object(
        'schema', n.nspname,
        'rel',    c.relname,
        'privs',  has_table_privilege('public', c.oid,
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')))
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind IN ('r','p','v','m')
        AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        AND (has_table_privilege('public', c.oid, 'SELECT')
          OR has_table_privilege('public', c.oid, 'INSERT')
          OR has_table_privilege('public', c.oid, 'UPDATE')
          OR has_table_privilege('public', c.oid, 'DELETE'))
    ),
    'extensions', (
      SELECT jsonb_agg(jsonb_build_object(
        'extname', extname, 'extversion', extversion))
      FROM pg_extension
    )
  )::text
$CMD$, parallel := true) r;

DROP TABLE IF EXISTS _sec1;
CREATE TEMP TABLE _sec1 AS
SELECT
  n.nodeid, n.nodename, n.nodeport, n.groupid,
  CASE WHEN n.groupid = 0 THEN 'coord' ELSE 'worker' END AS role,
  (r.result)::jsonb AS p
FROM _sec1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE r.success;

-- Flatten roles per node
DROP TABLE IF EXISTS _sec1_roles;
CREATE TEMP TABLE _sec1_roles AS
SELECT
  s.role AS node_role,
  s.nodename||':'||s.nodeport AS node,
  s.nodeid,
  item->>'rolname'                                AS rolname,
  (item->>'rolsuper')::bool                       AS rolsuper,
  (item->>'rolcanlogin')::bool                    AS rolcanlogin,
  (item->>'rolbypassrls')::bool                   AS rolbypassrls,
  (item->>'rolcreaterole')::bool                  AS rolcreaterole,
  (item->>'rolcreatedb')::bool                    AS rolcreatedb,
  (item->>'rolreplication')::bool                 AS rolreplication,
  NULLIF(item->>'rolvaliduntil','')::timestamptz  AS rolvaliduntil,
  (item->>'has_pwd')::bool                        AS has_pwd,
  item->>'pwd_method'                             AS pwd_method
FROM _sec1 s,
     jsonb_array_elements(
       CASE WHEN jsonb_typeof(s.p->'roles')='array'
            THEN s.p->'roles' ELSE '[]'::jsonb END
     ) item;

-- ---------------------------------------------------------------------
-- SEC1a: Superuser inventory (cluster-wide)
-- ---------------------------------------------------------------------
\echo
\echo '-- SEC1a. Superuser inventory (cluster-wide) --'
SELECT
  rolname,
  count(*) FILTER (WHERE rolsuper) AS superuser_on_nodes,
  count(*)                         AS total_nodes_seen,
  bool_or(rolcanlogin)             AS can_login,
  bool_or(rolreplication)          AS can_replicate,
  string_agg(DISTINCT pwd_method, ', ' ORDER BY pwd_method) AS pwd_methods
FROM _sec1_roles
WHERE rolsuper
GROUP BY rolname
ORDER BY rolname
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- SEC1b: Password hash method distribution
-- ---------------------------------------------------------------------
\echo
\echo '-- SEC1b. Password hash method distribution per role --'
-- A role with no database-stored password is only problematic when the
-- cluster's pg_hba requires a password hash (scram/md5). If pg_hba routes
-- authentication through non-password methods (peer/trust/cert/ident/
-- gss/sspi), the absence of rolpassword is by design.
WITH hba_nonpw AS (
  SELECT bool_or(
           (s.p ? 'hba_nonpw_methods')
           AND jsonb_typeof(s.p->'hba_nonpw_methods') = 'array'
           AND jsonb_array_length(s.p->'hba_nonpw_methods') > 0
         ) AS has_nonpw_auth
  FROM _sec1 s
)
SELECT
  rolname,
  string_agg(DISTINCT pwd_method, ', ') AS methods_seen,
  bool_or(rolcanlogin)                  AS can_login,
  count(DISTINCT pwd_method)            AS drift_count,
  CASE
    WHEN count(DISTINCT pwd_method) > 1
      THEN 'WARN: password hash differs across nodes'
    WHEN 'md5' = ANY(array_agg(DISTINCT pwd_method))
      THEN 'WARN: md5 is deprecated -- re-set password to upgrade to scram-sha-256'
    WHEN 'none' = ANY(array_agg(DISTINCT pwd_method)) AND bool_or(rolcanlogin)
         AND (SELECT has_nonpw_auth FROM hba_nonpw)
      THEN 'INFO: login role has no password hash; pg_hba uses non-password auth (peer/trust/cert/ident) -- confirm this role is authorised via that path'
    WHEN 'none' = ANY(array_agg(DISTINCT pwd_method)) AND bool_or(rolcanlogin)
      THEN 'WARN: login role without password'
    ELSE 'ok'
  END AS verdict
FROM _sec1_roles
WHERE rolcanlogin
GROUP BY rolname
HAVING count(DISTINCT pwd_method) > 1
    OR 'md5' = ANY(array_agg(DISTINCT pwd_method))
    OR ('none' = ANY(array_agg(DISTINCT pwd_method)) AND bool_or(rolcanlogin))
ORDER BY drift_count DESC, rolname
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- SEC1c: Role drift across nodes
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _sec1_role_drift;
CREATE TEMP TABLE _sec1_role_drift AS
WITH node_ct AS (SELECT count(*) AS n FROM _sec1)
SELECT
  rolname,
  count(*)                                  AS nodes_with_role,
  (SELECT n FROM node_ct)                   AS total_nodes,
  bool_or(rolsuper)                         AS any_super,
  bool_and(rolsuper)                        AS all_super,
  array_agg(DISTINCT node_role||':'||node)  AS seen_on
FROM _sec1_roles
GROUP BY rolname
HAVING count(*) <> (SELECT n FROM node_ct)
    OR bool_or(rolsuper) <> bool_and(rolsuper);

\echo
\echo '-- SEC1c. Role drift across cluster (missing or inconsistent flags) --'
SELECT
  rolname, nodes_with_role, total_nodes,
  any_super, all_super,
  seen_on,
  CASE
    WHEN nodes_with_role <> total_nodes
      THEN format('CRITICAL: role missing on %s node(s) -- MX clients hitting those nodes will fail',
                  total_nodes - nodes_with_role)
    ELSE 'WARN: rolsuper differs across nodes'
  END AS verdict
FROM _sec1_role_drift
ORDER BY (nodes_with_role <> total_nodes) DESC, rolname
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- SEC1d: Per-node security GUCs
-- ---------------------------------------------------------------------
\echo
\echo '-- SEC1d. Per-node security GUCs --'
SELECT
  role, nodename||':'||nodeport AS node,
  p->'settings'->>'password_encryption'    AS password_encryption,
  p->'settings'->>'ssl'                    AS ssl,
  p->'settings'->>'row_security'           AS row_security,
  p->'settings'->>'log_connections'        AS log_connections,
  p->'settings'->>'citus.node_conninfo'    AS node_conninfo,
  p->'settings'->>'citus.enable_create_role_propagation' AS role_prop,
  p->'settings'->>'citus.enable_ddl_propagation'         AS ddl_prop,
  CASE
    WHEN p->'settings'->>'password_encryption' <> 'scram-sha-256'
      THEN 'WARN: password_encryption should be scram-sha-256'
    WHEN p->'settings'->>'ssl' = 'off'
         AND p->'settings'->>'citus.node_conninfo' NOT LIKE '%sslmode=require%'
         AND p->'settings'->>'citus.node_conninfo' NOT LIKE '%sslmode=verify%'
      THEN 'WARN: ssl=off AND node_conninfo has no sslmode -- inter-node traffic unencrypted'
    WHEN p->'settings'->>'citus.enable_create_role_propagation' = 'off'
      THEN 'NOTICE: role propagation disabled -- roles must be pre-created on new nodes'
    WHEN p->'settings'->>'citus.enable_ddl_propagation' = 'off'
      THEN 'WARN: DDL propagation disabled -- schemas will drift'
    ELSE 'ok'
  END AS verdict
FROM _sec1
ORDER BY role DESC, node;

-- ---------------------------------------------------------------------
-- SEC1e: PUBLIC grants on distributed tables (coord only)
-- ---------------------------------------------------------------------
\echo
\echo '-- SEC1e. PUBLIC has grants on these distributed / reference tables (coord) --'
WITH coord AS (
  SELECT p FROM _sec1 WHERE role = 'coord' LIMIT 1
),
grants AS (
  SELECT
    item->>'schema' AS schema,
    item->>'rel'    AS rel,
    item->'privs'   AS privs
  FROM coord,
       jsonb_array_elements(
         CASE WHEN jsonb_typeof(coord.p->'pub_grants')='array'
              THEN coord.p->'pub_grants' ELSE '[]'::jsonb END
       ) item
)
SELECT
  g.schema, g.rel,
  dp.partmethod AS citus_kind,
  g.privs
FROM grants g
LEFT JOIN pg_class     c  ON c.relname = g.rel
LEFT JOIN pg_namespace n  ON n.oid = c.relnamespace AND n.nspname = g.schema
LEFT JOIN pg_dist_partition dp ON dp.logicalrelid = c.oid
WHERE dp.logicalrelid IS NOT NULL   -- only Citus-managed tables
ORDER BY g.schema, g.rel
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- SEC1f: Extension version drift across cluster
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _sec1_ext;
CREATE TEMP TABLE _sec1_ext AS
SELECT
  s.role, s.nodename||':'||s.nodeport AS node,
  item->>'extname'     AS extname,
  item->>'extversion'  AS extversion
FROM _sec1 s,
     jsonb_array_elements(
       CASE WHEN jsonb_typeof(s.p->'extensions')='array'
            THEN s.p->'extensions' ELSE '[]'::jsonb END
     ) item;

\echo
\echo '-- SEC1f. Extension version drift across nodes --'
SELECT
  extname,
  count(DISTINCT extversion) AS versions_seen,
  string_agg(DISTINCT extversion, ', ') AS versions,
  count(*)                   AS present_on_nodes,
  CASE
    WHEN count(DISTINCT extversion) > 1
      THEN 'CRITICAL: version drift across cluster -- run ALTER EXTENSION ... UPDATE'
    ELSE 'ok'
  END AS verdict
FROM _sec1_ext
GROUP BY extname
HAVING count(DISTINCT extversion) > 1
ORDER BY extname
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- SEC1g: pg_hba rules using trust / plain password
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _sec1_hba;
CREATE TEMP TABLE _sec1_hba AS
SELECT
  s.role, s.nodename||':'||s.nodeport AS node,
  item->>'type'        AS type,
  item->>'database'    AS db,
  item->>'user_name'   AS user_name,
  item->>'address'     AS address,
  item->>'auth_method' AS auth_method
FROM _sec1 s,
     jsonb_array_elements(
       CASE WHEN jsonb_typeof(s.p->'hba_summary')='array'
            THEN s.p->'hba_summary' ELSE '[]'::jsonb END
     ) item;

\echo
\echo '-- SEC1g. pg_hba rules with weak auth (trust / password / md5 over network) --'
SELECT role, node, type, db, user_name, address, auth_method
FROM _sec1_hba
ORDER BY role DESC, node, auth_method, type
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
SELECT (
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM _sec1_role_drift WHERE nodes_with_role <> total_nodes)
      THEN format('CRITICAL : %s role(s) missing on some cluster nodes. Apps will fail in MX.',
                  (SELECT count(*) FROM _sec1_role_drift WHERE nodes_with_role <> total_nodes))
    WHEN EXISTS (SELECT 1 FROM _sec1_ext GROUP BY extname HAVING count(DISTINCT extversion) > 1)
      THEN format('CRITICAL : %s extension(s) at different versions across nodes. Run ALTER EXTENSION ... UPDATE.',
                  (SELECT count(*) FROM (SELECT 1 FROM _sec1_ext GROUP BY extname HAVING count(DISTINCT extversion) > 1) _))
    WHEN (SELECT count(*) FROM (
           SELECT rolname FROM _sec1_roles
           WHERE rolsuper GROUP BY rolname) _) > (:sec1_max_superusers)::int
      THEN format('WARN : %s superuser role(s) (> %s allowed).',
                  (SELECT count(*) FROM (SELECT rolname FROM _sec1_roles WHERE rolsuper GROUP BY rolname) _),
                  (:sec1_max_superusers)::text)
    WHEN EXISTS (SELECT 1 FROM _sec1 WHERE p->'settings'->>'password_encryption' <> 'scram-sha-256')
      THEN 'WARN : password_encryption is not scram-sha-256 on one or more nodes.'
    WHEN EXISTS (
      SELECT 1 FROM _sec1_roles WHERE rolcanlogin AND pwd_method = 'md5'
    )
      THEN 'WARN : login role(s) still using md5. See SEC1b.'
    WHEN EXISTS (
      SELECT 1 FROM _sec1_roles r
      WHERE r.rolcanlogin AND r.pwd_method = 'none'
        -- only escalate to WARN if pg_hba actually requires passwords;
        -- otherwise SEC1b will report INFO and operator can verify.
        AND NOT EXISTS (
          SELECT 1 FROM _sec1 s
          WHERE jsonb_typeof(s.p->'hba_nonpw_methods') = 'array'
            AND jsonb_array_length(s.p->'hba_nonpw_methods') > 0
        )
    )
      THEN 'WARN : login role(s) with no password hash AND pg_hba lacks peer/trust/cert/ident rules. See SEC1b.'
    WHEN EXISTS (SELECT 1 FROM _sec1_hba WHERE auth_method IN ('trust','password'))
      THEN 'WARN : pg_hba rules use trust or plain password. See SEC1g.'
    WHEN EXISTS (
      SELECT 1 FROM _sec1
      WHERE p->'settings'->>'ssl' = 'off'
        AND p->'settings'->>'citus.node_conninfo' NOT LIKE '%sslmode=require%'
        AND p->'settings'->>'citus.node_conninfo' NOT LIKE '%sslmode=verify%'
    )
      THEN 'WARN : inter-node traffic may be unencrypted. See SEC1d.'
    ELSE 'OK : no role / auth drift; hash methods and extensions consistent.'
  END
) AS "Advisor SEC1 headline";

DROP TABLE _sec1_raw;
DROP TABLE _sec1;
DROP TABLE _sec1_roles;
DROP TABLE _sec1_role_drift;
DROP TABLE _sec1_ext;
DROP TABLE _sec1_hba;
