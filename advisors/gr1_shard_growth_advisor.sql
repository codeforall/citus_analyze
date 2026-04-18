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

-- ---------------------------------------------------------------------
-- Gather facts + projections into a single-row temp table
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _gr1;
CREATE TEMP TABLE _gr1 AS
WITH
facts AS (
    SELECT
        (SELECT count(*)::int FROM pg_dist_partition WHERE partmethod IN ('h','r'))  AS dist_tables,
        (SELECT count(*)::int FROM pg_dist_partition WHERE partmethod = 'n')         AS ref_tables,
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
        (SELECT count(*)::int FROM pg_stat_activity
           WHERE backend_type='client backend' AND pid <> pg_backend_pid())          AS backends_now
),
tgt AS (
    SELECT
        CASE WHEN :target_total_shards >= 0 THEN :target_total_shards ELSE f.shards_now END AS t_shards_total,
        CASE WHEN :target_partitions_per_parent >= 0 THEN :target_partitions_per_parent
             WHEN f.part_parents_now > 0 THEN f.part_children_now / f.part_parents_now
             ELSE 0 END                                                              AS t_parts_per_parent,
        CASE WHEN :target_replication_factor >= 0 THEN :target_replication_factor ELSE f.rf_now END AS t_rf,
        :additional_dist_tables                                                      AS t_extra_tables
    FROM facts f
)
SELECT f.*,
       t.t_shards_total,
       t.t_parts_per_parent,
       t.t_rf,
       t.t_extra_tables,
       (t.t_shards_total * t.t_rf)::bigint                                           AS t_placements_total,
       (f.dist_tables + t.t_extra_tables
           + CASE WHEN f.part_parents_now > 0
                  THEN f.part_parents_now * t.t_parts_per_parent
                  ELSE f.part_children_now END
           + f.ref_tables)::int                                                      AS t_relations,
       (f.dist_tables + f.part_children_now + f.ref_tables)::int                     AS c_relations,
       -- per-backend cache bytes (now)
       (f.shards_now * :k_meta_per_shard + f.placements_now * :k_meta_per_replica)::bigint AS c_meta_b,
       -- per-backend cache bytes (projected)
       (t.t_shards_total * :k_meta_per_shard + (t.t_shards_total * t.t_rf) * :k_meta_per_replica)::bigint AS t_meta_b,
       -- relcache bytes
       ((f.dist_tables + f.part_children_now + f.ref_tables) * :k_relcache)::bigint  AS c_relc_b,
       ((f.dist_tables + t.t_extra_tables
           + CASE WHEN f.part_parents_now > 0
                  THEN f.part_parents_now * t.t_parts_per_parent
                  ELSE f.part_children_now END
           + f.ref_tables) * :k_relcache)::bigint                                    AS t_relc_b,
       -- lock table (shared memory)
       (:k_lockslot::bigint * f.max_lpt * (f.max_conn + f.max_prep))                 AS lock_bytes_now,
       GREATEST(f.max_lpt,
                CEIL((t.t_shards_total * 1.5) / 64.0)::int * 64)                     AS lpt_needed,
       (:k_lockslot::bigint *
           GREATEST(f.max_lpt, CEIL((t.t_shards_total * 1.5) / 64.0)::int * 64) *
           (f.max_conn + f.max_prep))                                                AS lock_bytes_needed
FROM facts f, tgt t;

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
    format('Backends    : %s active now, max_connections=%s, max_prepared_transactions=%s, max_locks_per_transaction=%s',
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
    format('At current %s active backends   : +%s MB extra RSS on coordinator',
           backends_now,
           round((backends_now * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 23,
    format('At max_connections = %s         : +%s MB extra RSS on coordinator (worst case)',
           max_conn,
           round((max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 30, '' FROM _gr1
  UNION ALL SELECT 31, '-- Lock table (GR2 tie-in; shared memory, set at postmaster start) --' FROM _gr1
  UNION ALL SELECT 32,
    format('Current lock table   : %s * %s * (%s+%s)  =  %s MB',
           :k_lockslot, max_lpt, max_conn, max_prep, round(lock_bytes_now/1048576.0, 2)) FROM _gr1
  UNION ALL SELECT 33,
    format('Needed (worst-case)  : max_locks_per_transaction >= %s  (current %s)%s',
           lpt_needed, max_lpt,
           CASE WHEN lpt_needed > max_lpt
                THEN '   *** ACTION: raise max_locks_per_transaction ***'
                ELSE '   OK' END) FROM _gr1
  UNION ALL SELECT 34,
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
        THEN format('CRITICAL : worst-case max_locks_per_transaction would need to be %s — unworkable. Reduce target shard count; raising the lock table this high is not viable in PostgreSQL.', lpt_needed)
      WHEN (lock_bytes_needed - lock_bytes_now) / 1048576.0 > 512
        THEN format('CRITICAL : lock table would grow by %s MB of shared memory (needs postmaster restart with raised max_locks_per_transaction to %s). Validate shared_buffers + kernel SHMMAX headroom.',
                    round((lock_bytes_needed - lock_bytes_now)/1048576.0, 0), lpt_needed)
      WHEN (max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0 > 2048
        THEN format('CRITICAL : worst-case extra per-backend RSS %s MB > 2 GB on coordinator. Reduce shard/partition target or lower max_connections.',
                    round((max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0, 0))
      WHEN (max_conn * ((t_meta_b+t_relc_b)-(c_meta_b+c_relc_b))) / 1048576.0 > 512
        THEN 'WARN : worst-case extra per-backend RSS > 512 MB on coordinator. Validate node RAM headroom.'
      WHEN lpt_needed > max_lpt
        THEN format('WARN : max_locks_per_transaction must rise from %s to at least %s (requires postmaster restart).', max_lpt, lpt_needed)
      ELSE 'OK : projected growth fits within typical headroom.'
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
      (COUNT(p.placementid) * :k_meta_per_shard
       + COUNT(p.placementid) * (SELECT t_rf FROM _gr1) * :k_meta_per_replica
      )::bigint
    )                                                        AS meta_cache_per_backend_approx
FROM pg_dist_node n
LEFT JOIN pg_dist_placement p ON p.groupid = n.groupid
WHERE n.isactive AND n.noderole = 'primary'
GROUP BY n.nodename, n.nodeport, n.groupid
ORDER BY (n.groupid = 0) DESC, COUNT(p.placementid) DESC;

DROP TABLE _gr1;
