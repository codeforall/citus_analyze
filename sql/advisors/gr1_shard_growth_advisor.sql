\set advisor_id GR1
\ir ../capabilities.sql
-- =====================================================================
-- citus_analyze / GR1 : shard & partition growth memory advisor
-- ---------------------------------------------------------------------
-- Predicts the marginal RAM cost (per-backend Citus metadata cache,
-- PG relcache) and lock-table headroom (GR2) of changing shard_count,
-- partition count, replication_factor, or adding distributed tables.
--
-- Inputs (override with -v on the psql command line; defaults keep current):
--   target_total_shards            (int)
--   target_partitions_per_parent   (int)
--   target_replication_factor      (int)
--   additional_dist_tables         (int; default 0)
--   k_meta_per_shard               (bytes; default 160 at R=1)
--   k_meta_per_replica             (bytes; default 40)
--   k_relcache                     (bytes / relation / backend; default 51200)
--   k_lockslot                     (bytes / lock slot; default 270)
--   avg_indexes_per_shard          (int; auto-detected from pg_index, default 1)
--   fraction_cross_shard           (0..1; share of backends concurrently running
--                                    multi-shard xacts; default 0.10)
--   ordinary_locks_per_backend     (int; lock budget for normal OLTP backends;
--                                    default 10 — PG's historical sizing assumption)
--   lock_safety_factor             (multiplier on peak cluster demand; default 1.5)
--   write_mode                     (1 = cross-shard DML path is the peak, adds
--                                    LockShardDistributionMetadata + LockShardResource
--                                    per shard; 0 = SELECT-only peak; default 1)
--
-- LOCK-TABLE MODEL (see PG storage/lmgr/lock.c, NLOCKENTS()):
--   The shared hash capacity is
--       max_locks_per_transaction * (MaxBackends + max_prepared_xacts)
--   where MaxBackends = max_connections + autovacuum_max_workers
--                       + max_wal_senders + max_worker_processes.
--   A single backend can exceed max_locks_per_transaction as long as the
--   TOTAL hash still has room. Failure ("out of shared memory; You might
--   need to increase max_locks_per_transaction") fires when the hash
--   cannot admit a new entry. GR1 therefore sizes by peak cluster demand,
--   not per-backend demand.
--
-- Usage:
--   psql citus -f gr1_shard_growth_advisor.sql \
--        -v target_total_shards=1000 \
--        -v target_partitions_per_parent=12
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

-- defaults
\if :{?target_total_shards}          \else \set target_total_shards          -1 \endif
\if :{?target_partitions_per_parent} \else \set target_partitions_per_parent -1 \endif
\if :{?target_replication_factor}    \else \set target_replication_factor   -1 \endif
\if :{?additional_dist_tables}       \else \set additional_dist_tables       0 \endif
\if :{?k_meta_per_shard}             \else \set k_meta_per_shard           160 \endif
\if :{?k_meta_per_replica}           \else \set k_meta_per_replica          40 \endif
\if :{?k_relcache}                   \else \set k_relcache               51200 \endif
\if :{?k_lockslot}                   \else \set k_lockslot                 270 \endif
\if :{?avg_indexes_per_shard}        \else \set avg_indexes_per_shard       -1 \endif
\if :{?fraction_cross_shard}         \else \set fraction_cross_shard       0.10 \endif
\if :{?ordinary_locks_per_backend}   \else \set ordinary_locks_per_backend  10 \endif
\if :{?lock_safety_factor}           \else \set lock_safety_factor         1.5 \endif
\if :{?write_mode}                   \else \set write_mode                   1 \endif

-- ---------------------------------------------------------------------
-- Gather facts + projections into a single-row temp table
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._gr1;
CREATE TEMP TABLE _gr1 AS
WITH
facts AS (
    SELECT
          (SELECT count(*)::int FROM pg_dist_partition WHERE partmethod IN ('h','r')
            AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=logicalrelid)) AS dist_tables,
          (SELECT count(*)::int FROM pg_dist_partition WHERE partmethod = 'n' AND repmodel='t') AS ref_tables,
        (SELECT count(*)::int FROM pg_dist_shard)                                    AS shards_now,
        (SELECT count(*)::int FROM pg_dist_placement)                                AS placements_now,
        (SELECT count(DISTINCT inhparent)::int
           FROM pg_inherits i JOIN pg_dist_partition d ON d.logicalrelid = i.inhparent) AS part_parents_now,
        (SELECT count(*)::int
           FROM pg_inherits i JOIN pg_dist_partition d ON d.logicalrelid = i.inhparent) AS part_children_now,
        current_setting('citus.shard_replication_factor')::int                       AS rf_now,
        (SELECT setting::int FROM pg_settings WHERE name='max_connections')          AS max_conn,
        (SELECT setting::int FROM pg_settings WHERE name='max_prepared_transactions') AS max_prep,
        (SELECT setting::int FROM pg_settings WHERE name='max_locks_per_transaction') AS max_lpt,
        (SELECT setting::int FROM pg_settings WHERE name='autovacuum_max_workers')    AS av_workers,
        (SELECT setting::int FROM pg_settings WHERE name='max_wal_senders')           AS wal_senders,
        (SELECT setting::int FROM pg_settings WHERE name='max_worker_processes')      AS worker_procs,
        -- Auto-detected avg indexes per distributed table (>=1 implicit PK assumed
        -- when override not provided). The number of indexes propagates to every
        -- shard, so peak single-backend lock count scales with (1 + avg_idx).
        GREATEST(
          CASE WHEN :avg_indexes_per_shard >= 0 THEN :avg_indexes_per_shard
               ELSE COALESCE(
                 (SELECT ceil(count(i.*)::numeric
                              / NULLIF(count(DISTINCT p.logicalrelid), 0))::int
                    FROM pg_dist_partition p
                    LEFT JOIN pg_index i ON i.indrelid = p.logicalrelid
                    WHERE p.partmethod IN ('h','r')), 1)
          END, 1)                                                                    AS avg_idx_per_shard,
        (SELECT count(*)::int FROM pg_stat_activity
           WHERE backend_type='client backend' AND pid <> pg_backend_pid())          AS backends_now,
        -- Lock-table pressure is per-node. Size the budget for the HOTTEST node —
        -- in a balanced cluster that's ~shards/workers placements; in a skewed
        -- cluster (or a 1-worker cluster) one node holds everything. For the
        -- coordinator or MX entry point the model uses total shards.
        GREATEST(
          COALESCE((SELECT max(c) FROM (
             SELECT count(*) AS c FROM pg_dist_placement p GROUP BY p.groupid
          ) x), 1),
          1
        )                                                                            AS max_placements_per_node,
        GREATEST(
          COALESCE((SELECT count(*) FROM pg_dist_node
                     WHERE isactive AND shouldhaveshards AND noderole='primary'), 1),
          1
        )                                                                            AS n_workers_eff
),
tgt AS (
    SELECT
        CASE WHEN :target_total_shards >= 0 THEN :target_total_shards ELSE f.shards_now END AS t_shards_total,
        CASE WHEN :target_partitions_per_parent >= 0 THEN :target_partitions_per_parent
             WHEN f.part_parents_now > 0 THEN f.part_children_now / f.part_parents_now
             ELSE 0 END                                                              AS t_parts_per_parent,
        CASE WHEN :target_replication_factor >= 0 THEN :target_replication_factor ELSE f.rf_now END AS t_rf,
          :additional_dist_tables AS t_extra_tables,
          CASE WHEN :target_replication_factor::int < 0 AND f.shards_now > 0
           THEN ceil((CASE WHEN :target_total_shards >= 0 THEN :target_total_shards ELSE f.shards_now END)
                * f.placements_now::numeric / f.shards_now)::bigint
           ELSE (CASE WHEN :target_total_shards >= 0 THEN :target_total_shards ELSE f.shards_now END)
             * (CASE WHEN :target_replication_factor >= 0 THEN :target_replication_factor ELSE f.rf_now END)
           END AS t_placements_total
    FROM facts f
)
SELECT f.*,
       t.t_shards_total,
       t.t_parts_per_parent,
       t.t_rf,
       t.t_extra_tables,
       t.t_placements_total,
       (f.dist_tables + t.t_extra_tables
           + CASE WHEN f.part_parents_now > 0
                  THEN f.part_parents_now * t.t_parts_per_parent
                  ELSE f.part_children_now END
           + f.ref_tables)::int                                                      AS t_relations,
       (f.dist_tables + f.part_children_now + f.ref_tables)::int                     AS c_relations,
       -- per-backend cache bytes (now)
       (f.shards_now * :k_meta_per_shard + f.placements_now * :k_meta_per_replica)::bigint AS c_meta_b,
       -- per-backend cache bytes (projected)
      (t.t_shards_total * :k_meta_per_shard + t.t_placements_total * :k_meta_per_replica)::bigint AS t_meta_b,
       -- relcache bytes
       ((f.dist_tables + f.part_children_now + f.ref_tables) * :k_relcache)::bigint  AS c_relc_b,
       ((f.dist_tables + t.t_extra_tables
           + CASE WHEN f.part_parents_now > 0
                  THEN f.part_parents_now * t.t_parts_per_parent
                  ELSE f.part_children_now END
           + f.ref_tables) * :k_relcache)::bigint                                    AS t_relc_b,
       -- ---- Lock table (shared memory) — see PG lock.c NLOCKENTS() ----
       --   NLOCKENTS() = max_locks_per_xact * (MaxBackends + max_prepared_xacts)
       --   MaxBackends = max_connections + autovacuum_max_workers
       --               + max_wal_senders + max_worker_processes
       lm.max_backends,
       lm.hash_capacity_now,
       lm.lock_bytes_now,
       lm.peak_locks_per_xshard_backend,
       lm.n_xshard_backends,
       lm.peak_cluster_locks,
       lm.lpt_needed,
       lm.lock_bytes_needed
FROM facts f, tgt t,
LATERAL (
  SELECT
    (f.max_conn + f.av_workers + f.wal_senders + f.worker_procs)::int AS max_backends,
    (f.max_lpt::bigint
       * ((f.max_conn + f.av_workers + f.wal_senders + f.worker_procs) + f.max_prep)
    )                                                                 AS hash_capacity_now,
    (:k_lockslot::bigint * f.max_lpt
       * ((f.max_conn + f.av_workers + f.wal_senders + f.worker_procs) + f.max_prep)
    )                                                                 AS lock_bytes_now,
    -- Per-node peak lock pressure from a cross-shard backend. The lock table
    -- is per-node, so we size by the HOTTEST node:
    --   * Coordinator (or MX entry) backend planning a cross-shard xact holds
    --     LockShardDistributionMetadata on every target shard -> total shards.
    --   * Worker backend executing that xact only holds locks on its local
    --     placements -> projected max placements on any single worker.
    -- Take the max so lpt_needed is safe for whichever node is hottest.
    (GREATEST(
        t.t_shards_total,
        CEIL(
          -- projected max placements per worker after the change. If the user
          -- hasn't grown shards, keep the currently-observed maximum; otherwise
          -- assume balanced distribution to the worker count.
          CASE WHEN t.t_shards_total = f.shards_now
               THEN f.max_placements_per_node
               ELSE t.t_placements_total::numeric / f.n_workers_eff
          END
        )
     )::bigint
       * (1 + f.avg_idx_per_shard
            + CASE WHEN :write_mode = 1 THEN 2 ELSE 0 END)
    )                                                                 AS peak_locks_per_xshard_backend,
    GREATEST(1, ceil(f.max_conn * :fraction_cross_shard::numeric)::int) AS n_xshard_backends
) lm0,
LATERAL (
  SELECT
    lm0.max_backends,
    lm0.hash_capacity_now,
    lm0.lock_bytes_now,
    lm0.peak_locks_per_xshard_backend,
    lm0.n_xshard_backends,
    CEIL(
      ( lm0.n_xshard_backends::bigint * lm0.peak_locks_per_xshard_backend
        + GREATEST(0, f.max_conn - lm0.n_xshard_backends)::bigint
             * :ordinary_locks_per_backend::bigint
      ) * :lock_safety_factor::numeric
    )::bigint                                                         AS peak_cluster_locks
) lm1,
LATERAL (
  SELECT
    lm1.max_backends, lm1.hash_capacity_now, lm1.lock_bytes_now,
    lm1.peak_locks_per_xshard_backend, lm1.n_xshard_backends,
    lm1.peak_cluster_locks,
    GREATEST(
      f.max_lpt,
      CEIL( lm1.peak_cluster_locks::numeric
            / NULLIF((lm1.max_backends + f.max_prep), 0)
            / 64.0 )::int * 64
    )                                                                 AS lpt_needed
) lm2,
LATERAL (
  SELECT
    lm2.max_backends, lm2.hash_capacity_now, lm2.lock_bytes_now,
    lm2.peak_locks_per_xshard_backend, lm2.n_xshard_backends,
    lm2.peak_cluster_locks, lm2.lpt_needed,
    (:k_lockslot::bigint * lm2.lpt_needed
       * (lm2.max_backends + f.max_prep))                             AS lock_bytes_needed
) lm;

-- ---------------------------------------------------------------------
-- Main report
-- ---------------------------------------------------------------------
\pset tuples_only on

SELECT line FROM (
  SELECT 0 AS ord, '==================== GR1 : shard/partition growth advisor ====================' AS line FROM _gr1
  UNION ALL SELECT 1,
    format('Cluster now : %s dist tables (+%s ref), %s shards, %s placements, RF=%s, %s partition children under %s parents',
           dist_tables, ref_tables, shards_now, placements_now, rf_now, part_children_now, part_parents_now) FROM _gr1
  UNION ALL SELECT 2,
    format('Targets     : %s total shards, %s partitions/parent, RF=%s, +%s new dist tables',
           t_shards_total, t_parts_per_parent, t_rf, t_extra_tables) FROM _gr1
  UNION ALL SELECT 3,
    format('Backends    : %s connected now, max_connections=%s, max_prepared_transactions=%s, max_locks_per_transaction=%s',
           backends_now, max_conn, max_prep, max_lpt) FROM _gr1
  UNION ALL SELECT 10, '' FROM _gr1
  UNION ALL SELECT 11, '-- Per-backend memory (coordinator; each worker sees only its placements) --' FROM _gr1
  UNION ALL SELECT 12,
    format('Citus metadata cache :  now %s MB   ->  projected %s MB   (delta %s MB)',
           round(c_meta_b/1048576.0, 2), round(t_meta_b/1048576.0, 2),
           round((t_meta_b-c_meta_b)/1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 13,
    format('PG relcache+catcache :  now %s MB   ->  projected %s MB   (delta %s MB)',
           round(c_relc_b/1048576.0, 2), round(t_relc_b/1048576.0, 2),
           round((t_relc_b-c_relc_b)/1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 14,
    format('PER-BACKEND TOTAL    :  now %s MB   ->  projected %s MB   (delta %s MB)',
           round((c_meta_b+c_relc_b)/1048576.0, 2),
           round((t_meta_b+t_relc_b)/1048576.0, 2),
           round(((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))/1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 20, '' FROM _gr1
  UNION ALL SELECT 21, '-- Node-level extra RAM = delta_per_backend * backend_count --' FROM _gr1
  UNION ALL SELECT 22,
    format('At current %s connected backends: +%s MiB assumed cache residency on coordinator',
           backends_now,
           round((backends_now * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 23,
    format('At max_connections = %s         : +%s MB extra RSS on coordinator (worst case)',
           max_conn,
           round((max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 30, '' FROM _gr1
  UNION ALL SELECT 31, '-- Lock table (PG NLOCKENTS = lpt * (MaxBackends + max_prepared_xacts); shared) --' FROM _gr1
  UNION ALL SELECT 32,
    format('MaxBackends          : %s  = max_conn(%s) + av(%s) + wal_sender(%s) + worker_proc(%s)',
           max_backends, max_conn, av_workers, wal_senders, worker_procs) FROM _gr1
  UNION ALL SELECT 33,
    format('Current hash capacity: lpt(%s) * (MaxBackends(%s) + mpx(%s)) = %s slots  (%s MB)',
           max_lpt, max_backends, max_prep, hash_capacity_now,
           round(lock_bytes_now/1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 34,
    format('Peak demand model    : %s cross-shard backend(s) × %s locks/backend + %s ordinary × %s  → %s cluster-wide (×%s safety)',
           n_xshard_backends,
           peak_locks_per_xshard_backend,
           GREATEST(0, max_conn - n_xshard_backends),
           :ordinary_locks_per_backend,
           peak_cluster_locks,
           :lock_safety_factor) FROM _gr1
  UNION ALL SELECT 35,
    format('                       (avg_indexes_per_shard=%s, fraction_cross_shard=%s, write_mode=%s)',
           avg_idx_per_shard, :fraction_cross_shard, :write_mode) FROM _gr1
  UNION ALL SELECT 36,
    format('Needed               : max_locks_per_transaction >= %s  (current %s, %s)%s',
           lpt_needed, max_lpt,
           CASE WHEN peak_cluster_locks > hash_capacity_now
                THEN format('saturation = %s%%', round(100.0 * peak_cluster_locks::numeric / NULLIF(hash_capacity_now,0), 0))
                ELSE format('headroom = %s%%', round(100.0 * (1 - peak_cluster_locks::numeric / NULLIF(hash_capacity_now,0)), 0))
           END,
           CASE WHEN lpt_needed > max_lpt
                THEN '   scenario exceeds estimated slots; validate distinct locks and PROCLOCK demand'
                ELSE '   scenario within estimate; actual per-node lock demand not assessed' END) FROM _gr1
  UNION ALL SELECT 37,
    format('Projected lock table : %s MB  (delta %s MB shared)',
           round(lock_bytes_needed/1048576.0, 2),
           round((lock_bytes_needed-lock_bytes_now)/1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 40, '' FROM _gr1
  UNION ALL SELECT 41, '-- Verdict --' FROM _gr1
  UNION ALL SELECT 42,
    CASE
      WHEN (t_meta_b+t_relc_b) - (c_meta_b+c_relc_b) <= 0
           AND lpt_needed <= max_lpt
        THEN 'OK : target is at or below current footprint.'
      WHEN lpt_needed > 2000
        THEN format('INFO : lock scenario suggests max_locks_per_transaction=%s. There is no universal 2000-setting limit; validate actual shared-memory cost and lock workload per node.', lpt_needed)
      WHEN (lock_bytes_needed - lock_bytes_now) / 1048576.0 > 512
        THEN format('INFO : estimated lock memory grows by %s MiB at scenario max_locks_per_transaction=%s. Validate version-specific shared memory before changing settings.',
                    round((lock_bytes_needed - lock_bytes_now)/1048576.0, 0), lpt_needed)
      WHEN (max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0 > 2048
        THEN format('INFO : hypothetical all-connected-backend metadata growth totals %s MiB on the coordinator; not a measured RSS requirement.',
                    round((max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0, 0))
      WHEN (max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0 > 512
        THEN 'INFO : hypothetical metadata growth exceeds 512 MiB; compare measured cache residency and active concurrency.'
      WHEN lpt_needed > max_lpt
        THEN format('INFO : scenario max_locks_per_transaction changes from %s to %s; verify distinct locks, PROCLOCK entries and concurrency before tuning.', max_lpt, lpt_needed)
      ELSE 'INFO : modeled growth only; per-node settings and memory headroom not validated.'
    END FROM _gr1
  ORDER BY ord
) q;

\pset tuples_only off
\echo
\echo '-- Per-node placement distribution (scaling ratio applied for projection) --'

SELECT
    n.nodename                                               AS node,
    n.nodeport                                               AS port,
    CASE WHEN n.groupid = 0 THEN 'coordinator' ELSE 'worker' END AS role,
    COALESCE(COUNT(p.placementid), 0)                        AS placements_now,
    CASE
      WHEN (SELECT placements_now FROM _gr1) = 0 THEN 0
      ELSE ROUND( COUNT(p.placementid)::numeric
                  * (SELECT t_placements_total FROM _gr1)::numeric
                  / NULLIF((SELECT placements_now FROM _gr1), 0) )
    END                                                      AS placements_projected,
    pg_size_pretty(
      (COUNT(DISTINCT p.shardid) * :k_meta_per_shard
       + COUNT(p.placementid) * :k_meta_per_replica
      )::bigint
    )                                                        AS local_placement_metadata_approx
FROM pg_dist_node n
LEFT JOIN pg_dist_placement p ON p.groupid = n.groupid
WHERE n.isactive AND n.noderole = 'primary'
GROUP BY n.nodename, n.nodeport, n.groupid
ORDER BY (n.groupid = 0) DESC, COUNT(p.placementid) DESC;

DROP TABLE pg_temp._gr1;
