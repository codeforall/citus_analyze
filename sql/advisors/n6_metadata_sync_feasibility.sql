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
--   candidate_max_conn          (target node's planned max_connections; default 100)
--   candidate_max_lpt           (target node's planned max_locks_per_transaction; default 64)
--   candidate_max_prep          (target node's planned max_prepared_transactions; default 0, PG's actual default)
--   candidate_av_workers        (target node's autovacuum_max_workers; default 3)
--   candidate_wal_senders       (target node's max_wal_senders; default 10)
--   candidate_worker_procs      (target node's max_worker_processes; default 8)
--   avg_indexes_per_shard       (int; auto-detected; default 1 implicit PK)
--   lock_safety_factor          (numeric; default 1.3 — per plan spec)
--   candidate_work_mem_mb       (target node's work_mem in MB)
--   candidate_maint_mb          (target node's maintenance_work_mem in MB)
--   candidate_ram_mb            (target node's total RAM in MB)
--   link_mbps                   (effective coord<->candidate bandwidth in MB/s)
--   k_role / k_dep_non_rel / k_shell_table / k_partition / k_shard_row /
--   k_placement_row / k_obj_row / k_fkey / k_schema_row  (payload size constants)
--
-- LOCK-TABLE MODEL (see PG storage/lmgr/lock.c, NLOCKENTS()):
--   Total shared-hash capacity on the CANDIDATE is
--       candidate_max_lpt * (candidate_MaxBackends + candidate_max_prepared_xacts)
--   where candidate_MaxBackends = max_connections + autovacuum_max_workers
--                                 + max_wal_senders + max_worker_processes.
--   In transactional metadata sync, the *one* backend applying the snapshot
--   on the candidate holds, simultaneously, relation locks on every shell
--   table + every index + every partition + a handful of catalog entries,
--   all until COMMIT. A single backend CAN exceed candidate_max_lpt as long
--   as the TOTAL hash still has room. We therefore require:
--     candidate_hash_capacity  >=  peak_backend_locks × lock_safety_factor
--   which yields:
--     candidate_max_lpt_needed = ceil(peak_backend_locks × safety
--                                     / (MaxBackends + mpx) / 64) × 64
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?candidate_max_conn}     \else \set candidate_max_conn      100 \endif
\if :{?candidate_max_lpt}      \else \set candidate_max_lpt        64 \endif
\if :{?candidate_max_prep}     \else \set candidate_max_prep        0 \endif
\if :{?candidate_av_workers}   \else \set candidate_av_workers      3 \endif
\if :{?candidate_wal_senders}  \else \set candidate_wal_senders    10 \endif
\if :{?candidate_worker_procs} \else \set candidate_worker_procs    8 \endif
\if :{?avg_indexes_per_shard}  \else \set avg_indexes_per_shard    -1 \endif
\if :{?lock_safety_factor}     \else \set lock_safety_factor       1.3 \endif
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
                 'transactional')                                                     AS sync_mode,
        -- avg indexes per distributed table (>=1 implicit PK if override not given)
        GREATEST(
          CASE WHEN :avg_indexes_per_shard >= 0 THEN :avg_indexes_per_shard
               ELSE COALESCE(
                 (SELECT ceil(count(i.*)::numeric
                              / NULLIF(count(DISTINCT p.logicalrelid), 0))::int
                    FROM pg_dist_partition p
                    LEFT JOIN pg_index i ON i.indrelid = p.logicalrelid
                    WHERE p.partmethod IN ('h','r')), 1)
          END, 1)                                                                    AS avg_idx_per_shard
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
    -- ---- Lock-table demand on the candidate ----
    -- One backend in a single transaction creates every shell table + its
    -- indexes + every partition (+ its indexes) + reference tables, and
    -- briefly locks catalog relations (10 slack for pg_dist_* catalogs).
    -- peak_backend_locks = (dist_tables + ref_tables + partitions) * (1 + avg_idx)
    --                    + 10
    ( (n_dist_tables + n_ref_tables + n_partitions)::bigint
        * (1 + f.avg_idx_per_shard) + 10
    )                                               AS peak_backend_locks,
    -- candidate hash capacity = lpt * (MaxBackends + mpx)
    ( :candidate_max_lpt::bigint
        * ( (:candidate_max_conn + :candidate_av_workers
             + :candidate_wal_senders + :candidate_worker_procs)
            + :candidate_max_prep )
    )                                               AS candidate_hash_capacity,
    -- required lpt on candidate for transactional mode
    -- honest math: smallest integer lpt s.t. lpt * (MaxBackends + mpx) >= demand * safety
    -- rounded UP to the nearest multiple of 64 for operational convenience
    (CEIL( ( (n_dist_tables + n_ref_tables + n_partitions)::numeric
              * (1 + f.avg_idx_per_shard) + 10 )
           * :lock_safety_factor::numeric
           / NULLIF((:candidate_max_conn + :candidate_av_workers
                     + :candidate_wal_senders + :candidate_worker_procs
                     + :candidate_max_prep)::numeric, 0)
           / 64.0 )::int * 64
    )                                               AS lpt_needed_candidate_raw,
    -- will transactional add-node actually fail? true iff demand*safety > capacity
    ( ( (n_dist_tables + n_ref_tables + n_partitions)::numeric
         * (1 + f.avg_idx_per_shard) + 10 ) * :lock_safety_factor::numeric
      > ( :candidate_max_lpt::bigint
          * ( (:candidate_max_conn + :candidate_av_workers
               + :candidate_wal_senders + :candidate_worker_procs)
              + :candidate_max_prep ) )::numeric
    )                                               AS lock_exhaustion_predicted
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
    format('Candidate hash capacity (planned): lpt(%s) * (MaxBackends(%s) + mpx(%s)) = %s slots',
           :candidate_max_lpt,
           (:candidate_max_conn + :candidate_av_workers + :candidate_wal_senders + :candidate_worker_procs),
           :candidate_max_prep, candidate_hash_capacity) FROM _n6
  UNION ALL SELECT 33,
    format('Peak locks held by ONE sync txn  : %s  (= (%s dist+%s ref+%s parts) * (1 + avg_idx=%s) + catalog slack)',
           peak_backend_locks, n_dist_tables, n_ref_tables, n_partitions, avg_idx_per_shard) FROM _n6
  UNION ALL SELECT 34,
    format('Lock-slot feasibility (txnal)    : demand %s x safety(%s) vs capacity %s -> %s',
           peak_backend_locks, :lock_safety_factor, candidate_hash_capacity,
           CASE WHEN lock_exhaustion_predicted
                THEN format('*** WILL FAIL: raise candidate.max_locks_per_transaction to %s', lpt_needed_candidate_raw)
                ELSE format('OK (suggested lpt=%s at 64-slot granularity, planned %s)', lpt_needed_candidate_raw, :candidate_max_lpt)
           END) FROM _n6
  UNION ALL SELECT 35,
    format('Candidate backend peak mem (txnal): ~%s MB  (payload x1.5)  vs work_mem=%s MB, maintenance_work_mem=%s MB, RAM=%s MB',
           round(payload_b*1.5/1048576.0, 0),
           :candidate_work_mem_mb, :candidate_maint_mb, :candidate_ram_mb) FROM _n6
  UNION ALL SELECT 36,
    format('Coordinator peak mem (txnal)     : ~%s MB  (materializes full command list before sending)',
           round(payload_b*2.0/1048576.0, 0)) FROM _n6
  UNION ALL SELECT 37,
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
    CASE WHEN lock_exhaustion_predicted
         THEN format('  * ERROR on candidate: "out of shared memory; You might need to increase max_locks_per_transaction" -- raise to %s before adding.',
                     lpt_needed_candidate_raw)
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
      WHEN lock_exhaustion_predicted
        THEN format('CRITICAL : add-node WILL fail on lock-slot exhaustion (demand %s x safety %s > capacity %s). Raise candidate max_locks_per_transaction to %s before adding.',
                    peak_backend_locks, :lock_safety_factor, candidate_hash_capacity, lpt_needed_candidate_raw)
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
      ELSE format('OK : add-node is safe under current config (payload %s MB, lock demand %s x %s <= capacity %s).',
                  round(payload_b/1048576.0,2), peak_backend_locks, :lock_safety_factor, candidate_hash_capacity)
    END FROM _n6

  ORDER BY ord
) q;

\pset tuples_only off
DROP TABLE _n6;
