-- SEC1: inventory and scoped policy checks; not a complete security assessment.
\set advisor_id SEC1
\ir ../capabilities.sql
\pset pager off
\pset border 2
\pset format aligned
\if :{?sec1_max_superusers} \else \set sec1_max_superusers 2 \endif
\if :{?top_n} \else \set top_n 50 \endif
\echo '==================== SEC1 : scoped role and authentication inventory ===================='
DROP TABLE IF EXISTS pg_temp._sec1_raw;
CREATE TEMP TABLE _sec1_raw AS
SELECT * FROM run_command_on_all_nodes($CMD$
 SELECT jsonb_build_object(
  'roles', (SELECT jsonb_agg(jsonb_build_object('name', rolname, 'super', rolsuper,
       'login', rolcanlogin, 'bypassrls', rolbypassrls, 'createrole', rolcreaterole,
       'replication', rolreplication, 'validuntil', rolvaliduntil)) FROM pg_roles),
  'settings', (SELECT jsonb_object_agg(name, setting) FROM pg_settings
       WHERE name IN ('ssl', 'password_encryption', 'row_security', 'log_connections',
                     'citus.enable_create_role_propagation', 'citus.enable_ddl_propagation'))
 )::text
$CMD$, parallel := true);
DROP TABLE IF EXISTS pg_temp._sec1_roles;
CREATE TEMP TABLE _sec1_roles AS
SELECT node.nodeid, node.nodename || ':' || node.nodeport AS node,
       role->>'name' AS rolname, (role->>'super')::bool AS rolsuper,
       (role->>'login')::bool AS canlogin, role - 'validuntil' AS flags,
       nullif(role->>'validuntil', '')::timestamptz AS validuntil
FROM _sec1_raw raw JOIN pg_dist_node node USING (nodeid)
CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN raw.success THEN raw.result::jsonb->'roles' ELSE '[]'::jsonb END) role;
SELECT node, rolname, rolsuper, canlogin, validuntil FROM _sec1_roles
WHERE rolsuper OR (canlogin AND validuntil < now()) ORDER BY node, rolname LIMIT :top_n;
SELECT node, count(*) FILTER (WHERE rolsuper) AS superusers,
       'WARN : superuser count exceeds configured policy; review intended administrative roles' AS finding
FROM _sec1_roles GROUP BY node HAVING count(*) FILTER (WHERE rolsuper) > :sec1_max_superusers::int;

WITH distributed_roles AS (
 SELECT roles.rolname FROM pg_roles roles JOIN pg_dist_object objects
   ON objects.classid='pg_authid'::regclass AND objects.objid=roles.oid
), expected AS (
 SELECT roles.rolname, node.nodeid FROM distributed_roles roles CROSS JOIN pg_dist_node node
 WHERE node.isactive AND node.noderole='primary'
)
SELECT expected.rolname, count(*) FILTER (WHERE actual.rolname IS NULL) AS missing_nodes,
       count(DISTINCT actual.flags) AS flag_variants,
       'WARN : distributed role missing or flags differ; verify propagation and intended permissions' AS finding
FROM expected LEFT JOIN _sec1_roles actual USING (nodeid, rolname)
GROUP BY expected.rolname HAVING count(*) FILTER (WHERE actual.rolname IS NULL) > 0 OR count(DISTINCT actual.flags) > 1;

SELECT node.nodename || ':' || node.nodeport AS node,
       raw.result::jsonb->'settings' AS settings,
       CASE WHEN raw.result::jsonb->'settings'->>'password_encryption' <> 'scram-sha-256'
            THEN 'INFO : password encryption default differs from SCRAM; assess server/client version support and password migration'
            WHEN raw.result::jsonb->'settings'->>'ssl' = 'off'
            THEN 'INFO : server TLS disabled; inspect transport/proxy architecture and actual connection encryption'
            ELSE 'INFO : settings inventory only; HBA routing and transport encryption require separate verification'
       END AS finding
FROM _sec1_raw raw JOIN pg_dist_node node USING (nodeid) WHERE raw.success;

DROP TABLE IF EXISTS pg_temp._sec1_auth_raw;
CREATE TEMP TABLE _sec1_auth_raw AS
SELECT * FROM run_command_on_all_nodes($CMD$
 SELECT jsonb_build_object(
   'methods', (SELECT jsonb_agg(jsonb_build_object('role', rolname,
        'method', CASE WHEN rolpassword IS NULL THEN 'none'
                       WHEN rolpassword LIKE 'SCRAM-SHA-256$%' THEN 'scram-sha-256'
                       WHEN rolpassword LIKE 'md5%' THEN 'md5' ELSE 'other' END))
        FROM pg_authid WHERE rolcanlogin),
   'hba', (SELECT jsonb_agg(jsonb_build_object('line', line_number, 'type', type,
        'database', database, 'user', user_name, 'address', address, 'method', auth_method,
        'parse_error', error IS NOT NULL)) FROM pg_hba_file_rules)
 )::text
$CMD$, parallel := true);
SELECT node.nodename || ':' || node.nodeport AS node, method->>'role' AS role,
       method->>'method' AS hash_method,
       CASE WHEN method->>'method'='md5' THEN 'INFO : legacy password hash; verify SCRAM compatibility before migration'
            WHEN method->>'method'='none' THEN 'INFO : no stored password; determine this role actual ordered HBA/identity path; no security failure inferred'
            ELSE 'INFO : password method inventory; password equality across nodes not assessed' END AS finding
FROM _sec1_auth_raw raw JOIN pg_dist_node node USING (nodeid)
CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN raw.success THEN raw.result::jsonb->'methods' ELSE '[]'::jsonb END) method;
SELECT node.nodename || ':' || node.nodeport AS node, rule AS hba_rule,
       CASE WHEN (rule->>'parse_error')::bool THEN 'WARN : HBA parse error; inspect server configuration'
            WHEN rule->>'method'='trust' AND rule->>'type'<>'local' THEN 'WARN : network trust rule; review ordered address/database/user scope'
            WHEN rule->>'method'='password' AND rule->>'type'='hostnossl' THEN 'WARN : password authentication over an explicitly non-TLS rule'
            ELSE 'INFO : inspect HBA first-match semantics and role/address scope before changing authentication' END AS finding
FROM _sec1_auth_raw raw JOIN pg_dist_node node USING (nodeid)
CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN raw.success THEN raw.result::jsonb->'hba' ELSE '[]'::jsonb END) rule
WHERE rule->>'method' IN ('trust', 'password') OR (rule->>'parse_error')::bool;

SELECT relation.oid::regclass AS distributed_relation, privilege.privilege_type,
       'INFO : PUBLIC grant; review against application access policy' AS finding
FROM pg_class relation JOIN pg_dist_partition partition ON partition.logicalrelid=relation.oid
CROSS JOIN LATERAL aclexplode(coalesce(relation.relacl, acldefault('r', relation.relowner))) privilege
WHERE privilege.grantee=0 ORDER BY distributed_relation, privilege_type LIMIT :top_n;
\echo 'INFO : security inventory collected from responding nodes. Local-only roles need not match; hash methods do not prove password equality. This report does not establish end-to-end TLS or effective HBA authorization.'
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._sec1_roles;
DROP TABLE pg_temp._sec1_auth_raw;
DROP TABLE pg_temp._sec1_raw;