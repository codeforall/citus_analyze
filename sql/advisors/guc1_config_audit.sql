\set advisor_id GUC1
\ir ../capabilities.sql
-- =====================================================================
-- citus_analyze / GUC1 : Citus + PostgreSQL configuration audit
-- ---------------------------------------------------------------------
-- Inspects a curated set of GUCs across coordinator and every worker.
-- Flags three classes of problem:
--
--   GUC1a  CROSS-NODE DRIFT
--          A GUC that governs cluster-wide behaviour (connection pool
--          sizing, shard defaults, 2PC protocol, local execution) must
--          agree on every node. One out-of-sync worker becomes a
--          throughput bottleneck or a correctness risk.
--
--   GUC1b  DANGEROUS VALUES
--          Absolute rule violations (e.g. max_prepared_transactions=0
--          on a multi-node cluster → all distributed writes will fail
--          to COMMIT PREPARED) and strong red-flags (e.g.
--          citus.recover_2pc_interval=0 → 2PC orphans never cleaned).
--
--   GUC1c  FULL AUDIT LISTING
--          Every audited setting, per node, with the boot-time default
--          marked so operators can quickly see what has been changed.
--
-- Inputs (override with -v):
--   warn_drift_severity       'warn' or 'info'; default 'warn'.
--                             Some shops run heterogeneous worker RAM
--                             so e.g. shared_buffers legitimately
--                             differs; setting this to 'info' suppresses
--                             those rows to INFO severity.
--   top_n                     rows per section (default 20).
--
-- Caveats
--   * Snapshot is point-in-time. A running ALTER SYSTEM plus pending
--     reload will not appear until pg_reload_conf() fires; this
--     advisor reads the effective in-memory setting, not the file.
--   * `wal_level`, `max_connections`, `max_prepared_transactions` and
--     a handful of others require a RESTART; the advisor flags drift
--     but cannot tell you when the restart will happen.
--   * Rules are conservative and are expressed as the Citus docs
--     recommend for a single standard deployment topology. If you
--     deliberately run MX with asymmetric pool sizing or with
--     sequential modify mode for a specific workload, the INFO
--     messages are expected and safe to ignore.
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?warn_drift_severity} \else \set warn_drift_severity 'warn' \endif
\if :{?top_n}               \else \set top_n                20     \endif

-- Clamp user-supplied severity to {WARN, INFO}; anything else -> WARN.
SELECT CASE WHEN lower(:'warn_drift_severity') = 'info' THEN 'INFO'
            ELSE 'WARN' END AS _clamped_drift_sev \gset

\echo
\echo '==================== GUC1 : configuration audit ===================='

-- ---------------------------------------------------------------------
-- Per-node snapshot of audited GUCs.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._guc_raw;
CREATE TEMP TABLE _guc_raw (
    nodeid int, success boolean, result text
);

INSERT INTO _guc_raw
SELECT nodeid, success, result
FROM run_command_on_all_nodes(
$CMD$
SELECT jsonb_build_object(
  'settings', (SELECT jsonb_object_agg(name, jsonb_build_object(
                 'setting', setting, 'unit', unit,
                 'boot_val', boot_val, 'source', source))
               FROM pg_settings
               WHERE name IN (
                 -- PostgreSQL availability knobs
                 'max_connections',
                 'max_prepared_transactions',
                 'max_locks_per_transaction',
                 'max_worker_processes',
                 'max_wal_senders',
                 'max_replication_slots',
                 'shared_preload_libraries',
                 'server_version_num',
                 'lc_collate','lc_ctype','lc_monetary','lc_numeric','lc_time',
                 'wal_level',
                 'hot_standby_feedback',
                 'idle_in_transaction_session_timeout',
                 'statement_timeout',
                 'lock_timeout',
                 'deadlock_timeout',
                 'track_activities',
                 'track_counts',
                 'autovacuum',
                 'fsync',
                 'full_page_writes',
                 'log_min_duration_statement',
                 'shared_buffers',
                 'work_mem',
                 'maintenance_work_mem',
                 -- Citus essentials
                 'citus.max_shared_pool_size',
                 'citus.local_shared_pool_size',
                 'citus.max_adaptive_executor_pool_size',
                 'citus.node_connection_timeout',
                 'citus.recover_2pc_interval',
                 'citus.multi_shard_modify_mode',
                 'citus.enable_local_execution',
                 'citus.enable_repartition_joins',
                 'citus.shard_count',
                 'citus.shard_replication_factor',
                 'citus.task_executor_type',
                 'citus.log_remote_commands',
                 'citus.writable_standby_coordinator',
                 'citus.defer_drop_after_shard_move',
                 'citus.defer_drop_after_shard_split'
               )),
  'version', (SELECT extversion FROM pg_extension WHERE extname='citus')
)::text
$CMD$
, parallel := true);

-- Un-pivot per-node JSON into (node, name, value, ...).
DROP TABLE IF EXISTS pg_temp._guc;
CREATE TEMP TABLE _guc AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata THEN 'worker (MX)'
         ELSE 'worker' END                                  AS role,
    n.nodename || ':' || n.nodeport                         AS node,
    n.groupid,
    kv.key                                                  AS name,
    kv.value->>'setting'                                    AS value,
    kv.value->>'unit'                                       AS unit,
    kv.value->>'boot_val'                                   AS boot_val,
    kv.value->>'source'                                     AS source,
    CASE WHEN r.success THEN r.result::jsonb ->> 'version' END AS citus_version
FROM _guc_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
LEFT JOIN LATERAL jsonb_each(
    CASE WHEN r.success AND r.result <> ''
         THEN COALESCE(r.result::jsonb -> 'settings', '{}'::jsonb)
         ELSE '{}'::jsonb END) kv ON true
WHERE n.isactive;

-- Pivot subset used by cross-setting rules. node -> (name -> value).
-- Use a lookup helper via a materialised per-(node,name) map.
DROP TABLE IF EXISTS pg_temp._guc_map;
CREATE TEMP TABLE _guc_map AS
SELECT node, role, groupid, name, value FROM _guc WHERE name IS NOT NULL;

CREATE INDEX ON _guc_map (node, name);

-- ---------------------------------------------------------------------
-- GUC1a : cross-node drift detection
-- A setting drifts if distinct values observed > 1 across active nodes.
-- Some settings (shared_buffers, work_mem, max_wal_senders) are
-- legitimately per-node; we still surface them but at INFO severity by
-- default (or WARN via -v warn_drift_severity=warn).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._guc_drift;
CREATE TEMP TABLE _guc_drift AS
WITH agg AS (
  SELECT name,
         count(DISTINCT value)                                AS variants,
         string_agg(DISTINCT node || '=' || COALESCE(value,'null'),
                    ', ' ORDER BY node || '=' || COALESCE(value,'null'))
                                                              AS layout
  FROM _guc_map GROUP BY name
)
SELECT name,
       variants,
       layout,
       CASE
         WHEN name IN ('lc_collate', 'lc_ctype') THEN 'WARN'
         ELSE :'_clamped_drift_sev'
       END                                                    AS severity
FROM agg
WHERE variants > 1;

\echo
\echo '-- GUC1a. Cross-node drift (settings that differ across nodes) --'
SELECT severity, name, variants, layout
FROM _guc_drift
ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
         name
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- GUC1b : dangerous values (absolute rules, evaluated per node)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._guc_rules;
CREATE TEMP TABLE _guc_rules (
    rule_id   text,
    severity  text,
    node      text,
    detail    text
);

-- Rule R1: max_prepared_transactions must be > 0 on every node
INSERT INTO _guc_rules
SELECT 'R1:max_prepared_transactions',
       'CRITICAL',
       node,
       format('max_prepared_transactions=0 on %s; distributed 2PC writes will fail. Set to >= max_connections and restart.', node)
FROM _guc_map
WHERE name = 'max_prepared_transactions' AND value = '0';

-- Rule R2: max_prepared_transactions >= max_connections (per node; minimum floor)
INSERT INTO _guc_rules
SELECT 'R2:mpt_floor',
       'WARN',
       m1.node,
      format('max_prepared_transactions=%s < max_connections=%s on %s (conservative capacity policy; compare with peak concurrent 2PC demand, not evidence of exhausted slots).',
              m1.value, m2.value, m1.node)
FROM _guc_map m1 JOIN _guc_map m2 USING (node)
WHERE m1.name='max_prepared_transactions'
  AND m2.name='max_connections'
  AND m1.value ~ '^\d+$' AND m2.value ~ '^\d+$'
  AND m1.value::int > 0
  AND m1.value::int < m2.value::int;

-- Rule R3: citus.recover_2pc_interval must be > 0 in a multi-node cluster
INSERT INTO _guc_rules
SELECT 'R3:recover_2pc_off',
  'INFO',
       node,
  format('Non-positive citus.recover_2pc_interval on %s; verify scheduling semantics for the installed version and inspect A3 age/capacity evidence.', node)
FROM _guc_map
WHERE name='citus.recover_2pc_interval' AND value IN ('0', '-1')
  AND (SELECT count(*) FROM pg_dist_node WHERE isactive) > 1;

-- Rule R4: idle_in_transaction_session_timeout=0 → unbounded bloat risk.
-- Note: 0 is the PG-shipped default. Q1 owns the real signal (observed
-- idle-in-tx sessions pinning xmin). We emit INFO here so policy-driven
-- hardening reports see it, but we don't fail a run on the default value.
INSERT INTO _guc_rules
SELECT 'R4:idle_in_tx_timeout',
       'INFO',
       node,
       format('idle_in_transaction_session_timeout=0 on %s (PG default); stuck idle-in-tx sessions can pin xmin/WAL. See Q1 for actual observed backlog.', node)
FROM _guc_map
WHERE name='idle_in_transaction_session_timeout' AND value='0';

-- Rule R5: citus.enable_local_execution=off
INSERT INTO _guc_rules
SELECT 'R5:local_exec_off',
       'WARN',
       node,
       format('citus.enable_local_execution=off on %s; coord-local shards will round-trip unnecessarily.', node)
FROM _guc_map
WHERE name='citus.enable_local_execution' AND value='off';

-- Rule R6: citus.task_executor_type != adaptive (legacy 'real-time'/'task-tracker' dropped)
INSERT INTO _guc_rules
SELECT 'R6:executor_type',
       'WARN',
       node,
       format('citus.task_executor_type=%s on %s; only ''adaptive'' is supported in modern Citus.',
              value, node)
FROM _guc_map
WHERE name='citus.task_executor_type' AND value <> 'adaptive';

-- Rule R7: citus.log_remote_commands=on (noisy + perf hit, usually forgotten)
INSERT INTO _guc_rules
SELECT 'R7:log_remote_commands',
       'INFO',
       node,
       format('citus.log_remote_commands=on on %s; expect heavy log volume and per-query overhead.', node)
FROM _guc_map
WHERE name='citus.log_remote_commands' AND value='on';

-- Rule R8: wal_level not at least 'replica'
INSERT INTO _guc_rules
SELECT 'R8:wal_level',
       'WARN',
       node,
       format('wal_level=%s on %s; shard moves / streaming replication require at least ''replica''.',
              value, node)
FROM _guc_map
WHERE name='wal_level' AND value NOT IN ('replica', 'logical');

-- Rule R9: coord's max_locks_per_transaction < any worker's value
--   Distributed DDL plans are built on coord and then propagated; if
--   coord's lock table is smaller than a worker's, the coord will be
--   the bottleneck. See GR1 for deeper modelling.
INSERT INTO _guc_rules
SELECT 'R9:coord_lpt_below_worker',
  'INFO',
       (SELECT node FROM _guc_map
         WHERE name='max_locks_per_transaction' AND groupid=0 LIMIT 1),
      format('coord max_locks_per_transaction=%s < min(worker)=%s; this alone does not determine lock capacity or a bottleneck. See GR1 scenario assumptions.',
              coord_lpt, min_worker_lpt)
FROM (
  SELECT
    (SELECT value::int FROM _guc_map
       WHERE name='max_locks_per_transaction' AND groupid=0
         AND value ~ '^\d+$' LIMIT 1)              AS coord_lpt,
    (SELECT min(value::int) FROM _guc_map
       WHERE name='max_locks_per_transaction' AND groupid<>0
         AND value ~ '^\d+$')                      AS min_worker_lpt
) s
WHERE coord_lpt IS NOT NULL
  AND min_worker_lpt IS NOT NULL
  AND coord_lpt < min_worker_lpt;

-- Rule R10: citus.max_shared_pool_size = 0 or -1 (unlimited/sentinel)
--   Both values mean "do not cap". Capacity analysis belongs in C3;
--   surface as INFO so users know to look there.
INSERT INTO _guc_rules
SELECT 'R10:pool_uncapped',
       'INFO',
       node,
      format('citus.max_shared_pool_size=%s on %s: -1 disables throttling; 0 selects automatic max_connections. See C3 for per-peer budgets.',
              value, node)
FROM _guc_map
WHERE name='citus.max_shared_pool_size' AND value IN ('0','-1')
  AND (SELECT count(*) FROM pg_dist_node WHERE isactive AND groupid<>0) > 0;

-- Rule R11: citus.multi_shard_modify_mode=sequential — dramatically reduces
-- write throughput if left on cluster-wide. Report as INFO (may be intentional).
INSERT INTO _guc_rules
SELECT 'R11:sequential_mode',
       'INFO',
       node,
       format('citus.multi_shard_modify_mode=sequential on %s; multi-shard writes will be serialised.', node)
FROM _guc_map
WHERE name='citus.multi_shard_modify_mode' AND value='sequential';

-- Rule R12: autovacuum=off cluster-wide → catastrophic bloat
INSERT INTO _guc_rules
SELECT 'R12:autovacuum_off',
       'CRITICAL',
       node,
       format('autovacuum=off on %s; bloat will grow unbounded, especially on shards. Re-enable.', node)
FROM _guc_map
WHERE name='autovacuum' AND value='off';

-- Rule R13: fsync=off → durability loss on crash
INSERT INTO _guc_rules
SELECT 'R13:fsync_off',
       'CRITICAL',
       node,
      format('fsync=off on %s; crash corruption risk. Restore durability unless this is an explicitly disposable workload.', node)
FROM _guc_map
WHERE name='fsync' AND value='off';

-- Rule R14: full_page_writes=off → torn-page corruption on crash
INSERT INTO _guc_rules
SELECT 'R14:fpw_off',
       'WARN',
       node,
       format('full_page_writes=off on %s; torn-page corruption possible on crash unless the filesystem guarantees atomic 8kB writes.', node)
FROM _guc_map
WHERE name='full_page_writes' AND value='off';

-- Rule R15: track_counts=off → autovacuum cannot decide when to run
INSERT INTO _guc_rules
SELECT 'R15:track_counts_off',
       'WARN',
       node,
       format('track_counts=off on %s; autovacuum cannot see row churn and will skip vacuum/analyze. Turn on.', node)
FROM _guc_map
WHERE name='track_counts' AND value='off';

-- Rule R16: a GUC was not reported by at least one node (missing extension,
--   renamed setting, or version drift between binaries).
INSERT INTO _guc_rules
SELECT 'R16:missing_guc',
  'INFO',
       string_agg(missing_on.node, ', ' ORDER BY missing_on.node),
       format('Setting "%s" was not returned by node(s) %s; binary/extension version drift or renamed GUC across cluster.',
              expected.name,
              string_agg(missing_on.node, ', ' ORDER BY missing_on.node))
FROM (
  SELECT DISTINCT name FROM _guc_map
) AS expected(name)
CROSS JOIN (
  SELECT DISTINCT node FROM _guc_map
) AS nodes
LEFT JOIN _guc_map m
       ON m.node = nodes.node AND m.name = expected.name
LEFT JOIN LATERAL (SELECT nodes.node WHERE m.name IS NULL) AS missing_on(node) ON true
WHERE m.name IS NULL
GROUP BY expected.name;

\echo
\echo '-- GUC1b. Dangerous values (per node) --'
SELECT severity, rule_id, node, detail
FROM _guc_rules
ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
         rule_id, node
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- GUC1c : full audit listing (per-node values + non-default marker)
-- ---------------------------------------------------------------------
\echo
\echo '-- GUC1c. Audited settings (per node; * = non-default) --'
SELECT
    name,
    node,
    value || CASE WHEN unit IS NOT NULL AND unit <> ''
                  THEN ' ' || unit ELSE '' END                AS value,
    CASE WHEN value IS DISTINCT FROM boot_val THEN '*' ELSE '' END AS non_default,
    source
FROM _guc
WHERE name IS NOT NULL
ORDER BY name, groupid, node;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sev AS (
  SELECT
    (SELECT count(*) FROM _guc_rules WHERE severity='CRITICAL')                AS crit_n,
    (SELECT count(*) FROM _guc_rules WHERE severity='WARN')                    AS warn_n,
    (SELECT count(*) FROM _guc_rules WHERE severity='INFO')                    AS info_n,
    (SELECT count(*) FROM _guc_drift WHERE severity='CRITICAL')                AS drift_crit,
    (SELECT count(*) FROM _guc_drift WHERE severity='WARN')                    AS drift_warn,
    (SELECT count(*) FROM _guc_drift WHERE severity='INFO')                    AS drift_info,
    (SELECT count(*) FROM _guc_raw  WHERE NOT success)                         AS unreachable
)
SELECT CASE
  WHEN crit_n + drift_crit > 0 THEN
    format('CRITICAL : %s rule violation(s); %s critical drift(s); %s warning(s). Review individual findings.',
           crit_n, drift_crit, warn_n + drift_warn)
  WHEN warn_n + drift_warn > 0 THEN
    format('WARN : %s rule warning(s); %s cross-node drift(s). Review GUC1a/GUC1b.',
           warn_n, drift_warn)
  WHEN unreachable > 0 THEN
    format('INCOMPLETE : %s node(s) unreachable during GUC1 snapshot.', unreachable)
  WHEN info_n + drift_info > 0 THEN
    format('INFO : %s advisory finding(s); %s informational drift(s). Cluster is functional; review at your leisure.',
           info_n, drift_info)
  ELSE 'OK : no drift and no rule violations on audited settings.'
END
FROM sev;
\pset tuples_only off

DROP TABLE pg_temp._guc_rules;
DROP TABLE pg_temp._guc_drift;
DROP TABLE pg_temp._guc_map;
DROP TABLE pg_temp._guc;
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._guc_raw;
