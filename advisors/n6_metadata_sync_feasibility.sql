-- =====================================================================
-- citus_analyze / N6 : metadata-sync feasibility advisor
-- ---------------------------------------------------------------------
-- Predicts whether a metadata sync to a (new) MX node will succeed,
-- under which citus.metadata_sync_mode, and what the likely failure
-- signature would be if it does not. Answers the "adding a new node
-- OOMs / times out / fails on large metadata" class of reports.
--
-- Modeled after:
--   * SyncDistributedObjects() in src/backend/distributed/metadata/metadata_sync.c
--   * metadata_sync_mode GUC (METADATA_SYNC_TRANSACTIONAL default)
--
-- Sync payload formula (bytes):
--   role_commands              : pg_roles × 500
--   dependency_ddl_non_rel     : (pg_dist_object rows in non-relation classes) × 1024
--   shell_table_ddl            : dist_tables × 2048
--   partition_ddl              : partition_children × 400
--   pg_dist_shard rows         : total_shards × 200
--   pg_dist_placement rows     : placements × 120
--   pg_dist_object rows        : pg_dist_object × 300
--   fkey ddl                   : fkeys_on_dist × 500
--   pg_dist_schema rows        : schema_sharding_count × 400
--
-- Decision matrix (see output "Verdict" row):
--   payload < 50 MB          -> either mode OK (transactional is fine)
--   50 MB <= payload < 500 MB -> prefer nontransactional if mx_nodes >= 3
--   500 MB <= payload < 2 GB  -> nontransactional REQUIRED
--   payload >= 2 GB           -> nontransactional + maintenance window
--
-- Inputs (override with -v):
--   candidate_max_conn          (target node's planned max_connections)
--   candidate_max_lpt           (target node's planned max_locks_per_transaction)
--   candidate_work_mem_mb       (target node's work_mem in MB)
--   candidate_maint_mb          (target node's maintenance_work_mem in MB)
--   candidate_ram_mb            (target node's total RAM in MB)
--   link_mbps                   (effective coord<->candidate bandwidth in MB/s)
--   k_role                      (bytes per role command; default 500)
--   k_dep_non_rel               (bytes per non-relation dependency; default 1024)
--   k_shell_table               (bytes per shell CREATE TABLE; default 2048)
--   k_partition                 (bytes per partition attach; default 400)
--   k_shard_row                 (bytes per pg_dist_shard row insert; default 200)
--   k_placement_row             (bytes per pg_dist_placement row insert; default 120)
--   k_obj_row                   (bytes per pg_dist_object row insert; default 300)
--   k_fkey                      (bytes per fkey recreate; default 500)
--   k_schema_row                (bytes per pg_dist_schema row; default 400)
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?candidate_max_conn}     \else \set candidate_max_conn      100 \endif
\if :{?candidate_max_lpt}      \else \set candidate_max_lpt        64 \endif
\if :{?candidate_work_mem_mb}  \else \set candidate_work_mem_mb     4 \endif
\if :{?candidate_maint_mb}     \else \set candidate_maint_mb       64 \endif
\if :{?candidate_ram_mb}       \else \set candidate_ram_mb       4096 \endif
\if :{?link_mbps}              \else \set link_mbps                50 \endif
\if :{?k_role}                 \else \set k_role                  500 \endif
\if :{?k_dep_non_rel}          \else \set k_dep_non_rel           1024 \endif
\if :{?k_shell_table}          \else \set k_shell_table           2048 \endif
\if :{?k_partition}            \else \set k_partition             400 \endif
\if :{?k_shard_row}            \else \set k_shard_row             200 \endif
\if :{?k_placement_row}        \else \set k_placement_row         120 \endif
\if :{?k_obj_row}              \else \set k_obj_row               300 \endif
\if :{?k_fkey}                 \else \set k_fkey                  500 \endif
\if :{?k_schema_row}           \else \set k_schema_row            400 \endif

-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _n6;
CREATE TEMP TABLE _n6 AS
WITH facts AS (
    SELECT
        (SELECT count(*)::int FROM pg_roles)                                          AS n_roles,
        (SELECT count(*)::int FROM pg_dist_partition
            WHERE partmethod IN ('h','r'))                                            AS n_dist_tables,
        (SELECT count(*)::int FROM pg_dist_partition
            WHERE partmethod = 'n')                                                   AS n_ref_tables,
        (SELECT count(*)::int FROM pg_dist_shard)                                     AS n_shards,
        (SELECT count(*)::int FROM pg_dist_placement)                                 AS n_placements,
        (SELECT count(*)::int FROM pg_dist_object)                                    AS n_dist_objects,
        (SELECT count(*)::int FROM pg_dist_object
            WHERE classid NOT IN ('pg_class'::regclass, 'pg_authid'::regclass))       AS n_dep_non_rel,
        (SELECT count(*)::int FROM pg_inherits i
            JOIN pg_dist_partition d ON d.logicalrelid = i.inhparent)                 AS n_partitions,
        (SELECT count(*)::int FROM pg_constraint c
            JOIN pg_dist_partition d ON d.logicalrelid = c.conrelid
            WHERE c.contype = 'f')                                                    AS n_fkeys,
        (SELECT CASE WHEN to_regclass('pg_catalog.pg_dist_schema') IS NULL THEN 0
                     ELSE (SELECT count(*) FROM pg_catalog.pg_dist_schema) END)::int  AS n_schema_sharded,
        (SELECT count(*)::int FROM pg_dist_node
            WHERE noderole='primary' AND isactive AND hasmetadata)                    AS mx_nodes,
        current_setting('citus.shard_replication_factor')::int                        AS rf,
        coalesce((SELECT current_setting('citus.metadata_sync_mode', true)),
                 'transactional')                                                     AS sync_mode
)
SELECT f.*,
    -- payload components (bytes)
    (n_roles::bigint          * :k_role)           AS b_role,
    (n_dep_non_rel::bigint    * :k_dep_non_rel)    AS b_dep,
    (n_dist_tables::bigint    * :k_shell_table)    AS b_shell,
    (n_partitions::bigint     * :k_partition)      AS b_part,
    (n_shards::bigint         * :k_shard_row)      AS b_shard,
    (n_placements::bigint     * :k_placement_row)  AS b_plac,
    (n_dist_objects::bigint   * :k_obj_row)        AS b_obj,
    (n_fkeys::bigint          * :k_fkey)           AS b_fkey,
    (n_schema_sharded::bigint * :k_schema_row)     AS b_schema,
    -- total
    (n_roles::bigint*:k_role + n_dep_non_rel::bigint*:k_dep_non_rel
     + n_dist_tables::bigint*:k_shell_table + n_partitions::bigint*:k_partition
     + n_shards::bigint*:k_shard_row + n_placements::bigint*:k_placement_row
     + n_dist_objects::bigint*:k_obj_row + n_fkeys::bigint*:k_fkey
     + n_schema_sharded::bigint*:k_schema_row)    AS payload_b,
    -- lock-slot demand on candidate: worst-case one txn creates every shell +
    -- partition + shard placement; divide by 64 and round up for lock-partition bucket
    ((n_dist_tables + n_partitions + n_shards + n_ref_tables) * 2)::int AS lpt_needed_candidate_raw
FROM facts f;

-- ---------------------------------------------------------------------
\pset tuples_only on

SELECT line FROM (
  SELECT 0 AS ord, '==================== N6 : metadata-sync feasibility ====================' AS line FROM _n6
  UNION ALL SELECT 1,
    format('Source cluster       : %s dist tables (+%s ref), %s shards, %s placements, RF=%s, %s partitions, %s fkeys, %s pg_dist_object rows, %s roles, %s schema-sharded, %s MX nodes',
           n_dist_tables, n_ref_tables, n_shards, n_placements, rf, n_partitions, n_fkeys, n_dist_objects, n_roles, n_schema_sharded, mx_nodes) FROM _n6
  UNION ALL SELECT 2,
    format('Current sync mode    : citus.metadata_sync_mode = %s', sync_mode) FROM _n6
  UNION ALL SELECT 10, '' FROM _n6
  UNION ALL SELECT 11, '-- Payload breakdown (bytes) --' FROM _n6
  UNION ALL SELECT 12,
    format('  roles                 : %s kB', round(b_role/1024.0, 2)) FROM _n6
  UNION ALL SELECT 13,
    format('  non-relation deps     : %s kB', round(b_dep/1024.0, 2)) FROM _n6
  UNION ALL SELECT 14,
    format('  shell tables (CREATE) : %s kB', round(b_shell/1024.0, 2)) FROM _n6
  UNION ALL SELECT 15,
    format('  partition attach      : %s kB', round(b_part/1024.0, 2)) FROM _n6
  UNION ALL SELECT 16,
    format('  pg_dist_shard rows    : %s kB', round(b_shard/1024.0, 2)) FROM _n6
  UNION ALL SELECT 17,
    format('  pg_dist_placement     : %s kB', round(b_plac/1024.0, 2)) FROM _n6
  UNION ALL SELECT 18,
    format('  pg_dist_object rows   : %s kB', round(b_obj/1024.0, 2)) FROM _n6
  UNION ALL SELECT 19,
    format('  foreign keys          : %s kB', round(b_fkey/1024.0, 2)) FROM _n6
  UNION ALL SELECT 20,
    format('  schema-sharded        : %s kB', round(b_schema/1024.0, 2)) FROM _n6
  UNION ALL SELECT 21,
    format('  -------- TOTAL payload: %s MB', round(payload_b/1048576.0, 2)) FROM _n6

  UNION ALL SELECT 30, '' FROM _n6
  UNION ALL SELECT 31, '-- Candidate risk predictions --' FROM _n6
  UNION ALL SELECT 32,
    format('Lock-slot demand (transactional) : %s slots per backend; candidate max_locks_per_transaction=%s -> %s',
           lpt_needed_candidate_raw, :candidate_max_lpt,
           CASE WHEN lpt_needed_candidate_raw > :candidate_max_lpt
                THEN '*** WILL FAIL: out of shared memory; raise candidate.max_locks_per_transaction to ' || (((lpt_needed_candidate_raw/64)+1)*64)
                ELSE 'OK' END) FROM _n6
  UNION ALL SELECT 33,
    format('Candidate backend peak mem (txnal): ~%s MB  (payload x1.5)  vs work_mem=%s MB, maintenance_work_mem=%s MB, RAM=%s MB',
           round(payload_b*1.5/1048576.0, 0),
           :candidate_work_mem_mb, :candidate_maint_mb, :candidate_ram_mb) FROM _n6
  UNION ALL SELECT 34,
    format('Coordinator peak mem (txnal)     : ~%s MB  (materializes full command list before sending)',
           round(payload_b*2.0/1048576.0, 0)) FROM _n6
  UNION ALL SELECT 35,
    format('Estimated wall time (any mode)   : ~%s sec  (payload %s MB over %s MB/s link)  vs citus.node_connection_timeout',
           round(payload_b/1048576.0 / NULLIF(:link_mbps,0), 1),
           round(payload_b/1048576.0, 2), :link_mbps) FROM _n6

  UNION ALL SELECT 40, '' FROM _n6
  UNION ALL SELECT 41, '-- Mode recommendation --' FROM _n6
  UNION ALL SELECT 42,
    CASE
      WHEN payload_b < 50  * 1048576
        THEN format('OK : payload %s MB < 50 MB. Either mode works; transactional (default) is fine.', round(payload_b/1048576.0, 1))
      WHEN payload_b < 500 * 1048576 AND mx_nodes < 3
        THEN format('OK : payload %s MB. Transactional acceptable with %s MX nodes. Consider nontransactional if candidate RAM is tight.',
                    round(payload_b/1048576.0, 1), mx_nodes)
      WHEN payload_b < 500 * 1048576
        THEN format('WARN : payload %s MB with %s MX nodes. Prefer citus.metadata_sync_mode = nontransactional to reduce peak memory & lock footprint on each worker.',
                    round(payload_b/1048576.0, 1), mx_nodes)
      WHEN payload_b < 2 * 1024::bigint * 1048576
        THEN format('CRITICAL : payload %s MB. Transactional WILL risk OOM/lock-exhaustion on candidate. REQUIRE citus.metadata_sync_mode = nontransactional before citus_add_node.',
                    round(payload_b/1048576.0, 1))
      ELSE
        format('CRITICAL : payload %s MB >= 2 GB. Use nontransactional + maintenance window. Expect >10 min sync time and pg_dist_object catch-up. Also consider splitting pg_dist_object propagation if any custom extensions are involved.',
                    round(payload_b/1048576.0, 1))
    END FROM _n6

  UNION ALL SELECT 50, '' FROM _n6
  UNION ALL SELECT 51, '-- Failure-signature predictions if you add a node AS-IS --' FROM _n6
  UNION ALL SELECT 52,
    CASE WHEN lpt_needed_candidate_raw > :candidate_max_lpt
         THEN format('  * ERROR on candidate: "out of shared memory; You might need to increase max_locks_per_transaction" -- raise to %s before adding.',
                     (((lpt_needed_candidate_raw/64)+1)*64))
         ELSE '  * Lock table: OK.' END FROM _n6
  UNION ALL SELECT 53,
    CASE WHEN payload_b*1.5/1048576.0 > :candidate_ram_mb * 0.25
         THEN format('  * RISK: candidate backend peak ~%s MB, >25%% of candidate RAM (%s MB). Use nontransactional or raise RAM.',
                     round(payload_b*1.5/1048576.0,0), :candidate_ram_mb)
         ELSE '  * Candidate RAM: OK.' END FROM _n6
  UNION ALL SELECT 54,
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='max_connections')
              * (payload_b/1048576.0) > 0  -- placeholder; keep check cheap
              AND payload_b*2.0/1048576.0 > 1024
         THEN format('  * RISK: coordinator peak ~%s MB while building command list (transactional only). Switch mode or increase coord RAM.',
                     round(payload_b*2.0/1048576.0,0))
         ELSE '  * Coordinator RAM during build: OK.' END FROM _n6
  UNION ALL SELECT 55,
    format('  * Wall-time vs citus.node_connection_timeout (%s ms): sync estimate %s sec. %s',
           (SELECT setting FROM pg_settings WHERE name='citus.node_connection_timeout'),
           round(payload_b/1048576.0/NULLIF(:link_mbps,0), 1),
           CASE WHEN (payload_b/1048576.0/NULLIF(:link_mbps,0)) * 1000
                     > (SELECT setting::numeric FROM pg_settings WHERE name='citus.node_connection_timeout')
                THEN '*** EXCEEDS timeout -- raise citus.node_connection_timeout or use nontransactional chunking.'
                ELSE 'OK.' END) FROM _n6

  UNION ALL SELECT 60, '' FROM _n6
  UNION ALL SELECT 61, '-- N6 headline --' FROM _n6
  UNION ALL SELECT 62,
    CASE
      WHEN lpt_needed_candidate_raw > :candidate_max_lpt
        THEN format('CRITICAL : add-node WILL fail on lock-slot exhaustion (needs %s, candidate has %s). Raise max_locks_per_transaction to %s before adding.',
                    lpt_needed_candidate_raw, :candidate_max_lpt, (((lpt_needed_candidate_raw/64)+1)*64))
      WHEN payload_b >= 2 * 1024::bigint * 1048576
        THEN format('CRITICAL : sync payload %s MB >= 2 GB; add-node requires nontransactional mode + maintenance window.',
                    round(payload_b/1048576.0, 1))
      WHEN payload_b*1.5/1048576.0 > :candidate_ram_mb * 0.25
        THEN format('CRITICAL : predicted candidate backend peak ~%s MB exceeds 25%% of candidate RAM (%s MB). Use nontransactional or raise RAM.',
                    round(payload_b*1.5/1048576.0,0), :candidate_ram_mb)
      WHEN (payload_b/1048576.0/NULLIF(:link_mbps,0)) * 1000
           > (SELECT setting::numeric FROM pg_settings WHERE name='citus.node_connection_timeout')
        THEN 'CRITICAL : estimated sync wall-time exceeds citus.node_connection_timeout; raise timeout or use nontransactional chunking.'
      WHEN payload_b >= 500 * 1048576 AND mx_nodes > 3
        THEN format('WARN : payload %s MB with %s MX nodes; prefer citus.metadata_sync_mode=nontransactional.',
                    round(payload_b/1048576.0,1), mx_nodes)
      WHEN payload_b >= 50 * 1048576
        THEN format('WARN : payload %s MB is non-trivial; raise timeouts and lock budget before adding.',
                    round(payload_b/1048576.0,1))
      ELSE format('OK : add-node is safe under current config (payload %s MB, lock-slot need %s <= %s).',
                  round(payload_b/1048576.0,2), lpt_needed_candidate_raw, :candidate_max_lpt)
    END FROM _n6

  ORDER BY ord
) q;

\pset tuples_only off
DROP TABLE _n6;
