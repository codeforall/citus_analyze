-- =====================================================================
-- SC1 : shard-count right-sizing per colocation group
-- ---------------------------------------------------------------------
-- Asks: is each colocation group using the right number of shards
--       given its current + projected data volume?
--
-- Why this matters:
--   Citus' metadata cache is per-backend and scales with the number
--   of shards. Over-sharding a small colocation group costs every
--   backend extra RAM (see M1 / GR1), extra planning time (more
--   plan variants), extra fan-out fan-in (more connections per
--   multi-shard query), and extra rebalance work. Under-sharding
--   a large one leaves you unable to parallelise and you hit
--   single-backend work_mem / disk limits.
--
-- Model:
--   per-colocation recommended_shards =
--       clamp( ceil( total_bytes(group) / target_shard_bytes ),
--              min_shards = n_workers,
--              max_shards = max_shards_cap )
--
--   where:
--     target_shard_bytes : community guidance is "1-10 GiB per shard"
--                          for typical OLTP; 10-50 GiB for analytical
--                          / columnar. Default here: 10 GiB.
--     min_shards         : at least one shard per data-holding worker
--                          so every worker participates.
--     max_shards_cap     : safety ceiling to avoid metadata-cache
--                          runaway. Default 4096.
--
-- Inputs
--   :target_shard_bytes   default 10737418240  (10 GiB)
--   :min_shard_bytes      default 1048576      (1 MiB; below this, shard is "too small")
--   :max_shards_cap       default 4096
--   :sc1_significant_ratio default 0.5   (recommend change if current/recommended is off by >2x)
--   :sc1_top_n            default 40
-- =====================================================================

\if :{?target_shard_bytes} \else \set target_shard_bytes 10737418240 \endif
\if :{?min_shard_bytes}    \else \set min_shard_bytes    1048576     \endif
\if :{?max_shards_cap}     \else \set max_shards_cap     4096        \endif
\if :{?sc1_significant_ratio} \else \set sc1_significant_ratio 0.5   \endif
\if :{?sc1_top_n}          \else \set sc1_top_n          40          \endif

\pset pager off
\pset border 2
\pset format aligned

\echo
\echo '==================== SC1 : shard-count right-sizing ===================='

-- Per-colocation facts: total size, shard count, member tables.
-- We use citus_tables for sizes because it rolls up all shards+partitions
-- per distributed parent, which is exactly what "data in the group" means.
DROP TABLE IF EXISTS _sc1_g;
CREATE TEMP TABLE _sc1_g AS
WITH dist AS (
  SELECT p.logicalrelid::regclass::text AS table_name,
         p.colocationid,
         p.partmethod,
         c.reloptions,
         COALESCE(
           (SELECT count(*) FROM pg_dist_shard s WHERE s.logicalrelid = p.logicalrelid),
           0) AS shard_count,
         -- citus_total_relation_size sums across all shards+placements; this
         -- is the only way to get the *data* size from the coordinator.
         -- It can throw if the table was just dropped; swallow with NULL.
         COALESCE(
           (SELECT citus_total_relation_size(p.logicalrelid, fail_on_error := false)),
           0) AS bytes_total
  FROM pg_dist_partition p
  JOIN pg_class c ON c.oid = p.logicalrelid
  WHERE p.partmethod = 'h'          -- hash-distributed only
    AND p.colocationid > 0
)
SELECT colocationid,
       count(*)                                        AS n_tables,
       string_agg(table_name, ', ' ORDER BY table_name) AS tables_list,
       -- Every member of a colocation group shares the same shard count;
       -- max() just picks that value from any member.
       max(shard_count)                                AS shard_count,
       sum(bytes_total)::bigint                        AS total_bytes,
       max(bytes_total)::bigint                        AS largest_member_bytes
FROM dist
GROUP BY colocationid;

-- Worker count (only data-holding primaries count; coordinators marked
-- should_have_shards=false are correctly excluded from the min-shards
-- floor).
DROP TABLE IF EXISTS _sc1_ctx;
CREATE TEMP TABLE _sc1_ctx AS
SELECT
  (SELECT count(*) FROM pg_dist_node
     WHERE isactive AND shouldhaveshards AND noderole='primary') AS n_workers,
  (:target_shard_bytes)::bigint AS target_shard_bytes,
  (:min_shard_bytes)::bigint    AS min_shard_bytes,
  (:max_shards_cap)::int        AS max_shards_cap;

-- Recommendation.
DROP TABLE IF EXISTS _sc1_rec;
CREATE TEMP TABLE _sc1_rec AS
SELECT
  g.colocationid,
  g.n_tables,
  g.tables_list,
  g.shard_count                                      AS current_shards,
  g.total_bytes,
  g.largest_member_bytes,
  c.n_workers,
  -- raw ideal ignoring floor/ceiling
  GREATEST(1,
           ceil(g.total_bytes::numeric / NULLIF(c.target_shard_bytes, 0))::int
  )                                                  AS ideal_shards,
  -- clamped recommendation (at least n_workers, at most cap)
  LEAST(
    GREATEST(
      GREATEST(1,
               ceil(g.total_bytes::numeric / NULLIF(c.target_shard_bytes, 0))::int),
      GREATEST(1, c.n_workers)
    ),
    c.max_shards_cap
  )                                                  AS recommended_shards,
  -- average shard size
  CASE WHEN g.shard_count > 0
       THEN (g.total_bytes / g.shard_count)::bigint
       ELSE 0 END                                    AS avg_shard_bytes
FROM _sc1_g g CROSS JOIN _sc1_ctx c;

-- Classify each group.
DROP TABLE IF EXISTS _sc1_verdict;
CREATE TEMP TABLE _sc1_verdict AS
SELECT
  r.*,
  CASE
    WHEN r.current_shards = r.recommended_shards                   THEN 'OK'
    -- Over-sharded: shards are tiny AND recommended is much smaller.
    WHEN r.avg_shard_bytes < (SELECT min_shard_bytes FROM _sc1_ctx)
         AND r.recommended_shards <= r.current_shards
                                    * (:sc1_significant_ratio)::numeric
      THEN 'WARN'
    -- Under-sharded: recommended is much larger than current.
    WHEN r.recommended_shards >= r.current_shards / NULLIF((:sc1_significant_ratio)::numeric, 0)
      THEN 'CRITICAL'
    ELSE 'INFO'
  END AS severity,
  CASE
    WHEN r.current_shards = r.recommended_shards THEN 'no-op'
    WHEN r.recommended_shards < r.current_shards THEN 'decrease'
    ELSE 'increase'
  END AS direction
FROM _sc1_rec r;

\echo
\echo '-- SC1a. Per-colocation recommendation --'
SELECT severity, direction,
       colocationid AS coloc,
       n_tables,
       current_shards,
       recommended_shards AS rec_shards,
       pg_size_pretty(total_bytes)       AS total_size,
       pg_size_pretty(avg_shard_bytes)   AS avg_shard_size,
       pg_size_pretty((:target_shard_bytes)::bigint) AS target_size,
       -- Trim table list to keep the row readable.
       CASE WHEN length(tables_list) > 80
            THEN substr(tables_list, 1, 77) || '...'
            ELSE tables_list END         AS tables
FROM _sc1_verdict
ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1
                       WHEN 'INFO' THEN 2 ELSE 3 END,
         abs(recommended_shards - current_shards) DESC,
         colocationid
LIMIT :sc1_top_n;

-- Cluster-wide summary.
\echo
\echo '-- SC1b. Cluster-wide roll-up --'
SELECT
  sum(current_shards)                                 AS current_total_shards,
  sum(recommended_shards)                             AS recommended_total_shards,
  count(*) FILTER (WHERE severity='CRITICAL')         AS critical_groups,
  count(*) FILTER (WHERE severity='WARN')             AS warn_groups,
  count(*) FILTER (WHERE severity='INFO')             AS info_groups,
  count(*) FILTER (WHERE severity='OK')               AS ok_groups
FROM _sc1_verdict;

-- Headline.
\echo
\pset tuples_only on
SELECT CASE
  WHEN (SELECT count(*) FROM _sc1_verdict WHERE severity='CRITICAL') > 0 THEN
    format('CRITICAL : %s colocation group(s) under-sharded for their data volume; SELECT citus_alter_distributed_table(<table>, shard_count => <n>) will block writes briefly. See SC1a.',
           (SELECT count(*) FROM _sc1_verdict WHERE severity='CRITICAL'))
  WHEN (SELECT count(*) FROM _sc1_verdict WHERE severity='WARN') > 0 THEN
    format('WARN : %s colocation group(s) over-sharded (avg shard < %s); reducing shard count saves per-backend metadata cache + planning cost. Plan a maintenance window.',
           (SELECT count(*) FROM _sc1_verdict WHERE severity='WARN'),
           pg_size_pretty((:min_shard_bytes)::bigint))
  WHEN (SELECT count(*) FROM _sc1_verdict WHERE severity='INFO') > 0 THEN
    format('INFO : %s colocation group(s) at a non-ideal shard count but within tolerance; review SC1a.',
           (SELECT count(*) FROM _sc1_verdict WHERE severity='INFO'))
  WHEN (SELECT count(*) FROM _sc1_g) = 0 THEN
    'INFO : no hash-distributed colocation groups found.'
  ELSE
    'OK : every colocation group is sized within ' ||
    ((:sc1_significant_ratio)::numeric * 100)::int || '% of target.'
END AS "SC1 headline";
\pset tuples_only off

DROP TABLE _sc1_verdict;
DROP TABLE _sc1_rec;
DROP TABLE _sc1_ctx;
DROP TABLE _sc1_g;
