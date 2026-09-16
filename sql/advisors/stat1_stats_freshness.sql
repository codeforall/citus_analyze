\set advisor_id STAT1
\ir ../capabilities.sql
-- STAT1: Statistics freshness for the distributed planner.
-- Stale planner statistics produce bad plans (wrong join order, wrong
-- index, wrong shard pruning). On Citus this is amplified: every
-- worker plans its own sub-query using its own shard stats, and the
-- coord plans distributed joins using the LOCAL PARENT statistics.
-- Three failure modes we check:
--   1. Distributed tables where some shards are analyzed and others
--      aren't -- workers build different plans for "identical" shards.
--   2. Tables with substantial churn since last analyze -- row-count
--      estimates drift, cost model picks wrong plan.
--   3. Tables never analyzed at all -- planner uses hard-coded
--      fallback estimates (10 rows, random_page_cost linear scan).
--
-- Inputs (override via psql -v):
--   stat1_stale_days    = 7       -- warn threshold
--   stat1_crit_days     = 30      -- critical threshold
--   stat1_min_rows      = 10000   -- ignore trivially small tables
--   stat1_churn_pct     = 20      -- warn if n_mod since analyze
--                                    exceeds this fraction of reltuples
--   stat1_default_target_low = 100   -- warn if default_statistics_target
--                                      below this on any node

\pset pager off
\pset border 2
\pset format aligned

\if :{?stat1_stale_days}         \else \set stat1_stale_days         7     \endif
\if :{?stat1_crit_days}          \else \set stat1_crit_days          30    \endif
\if :{?stat1_min_rows}           \else \set stat1_min_rows           10000 \endif
\if :{?stat1_churn_pct}          \else \set stat1_churn_pct          20    \endif
\if :{?stat1_default_target_low} \else \set stat1_default_target_low 100   \endif
\if :{?top_n}                    \else \set top_n                   50    \endif

\echo '==================== STAT1 : statistics freshness ===================='

-- Fan-out: per-node snapshot of pg_stat_user_tables + default_statistics_target.
DROP TABLE IF EXISTS pg_temp._stat1_raw;
CREATE TEMP TABLE _stat1_raw (nodeid int, success boolean, result text);

INSERT INTO _stat1_raw (nodeid, success, result)
SELECT r.nodeid, r.success, r.result
FROM (
  SELECT format($CMD$
    WITH _vis AS MATERIALIZED (
      -- Make shards visible to pg_class / pg_stat_* scans even if the
      -- internal-backend bypass is stripped in a hardened deployment.
      SELECT set_config('citus.override_table_visibility','off', true)
    ),
    per_rel AS (
      SELECT jsonb_agg(jsonb_build_object(
               'schema',         n.nspname,
               'rel',            c.relname,
               'rel_oid',        c.oid::int8,
               'reltuples',      c.reltuples::bigint,
               'relpages',       c.relpages::bigint,
               'n_live',         s.n_live_tup,
               'n_dead',         s.n_dead_tup,
               'n_mod_since_analyze',
                                 COALESCE(s.n_mod_since_analyze, 0),
               'last_analyze',   s.last_analyze::text,
               'last_autoanalyze', s.last_autoanalyze::text,
               'relkind',        c.relkind::text,
               'n_ext_stats',
                 (SELECT count(*) FROM pg_statistic_ext se
                  WHERE se.stxrelid = c.oid)
             )) AS items
      FROM pg_stat_user_tables s
      JOIN pg_class c      ON c.oid = s.relid
      JOIN pg_namespace n  ON n.oid = c.relnamespace
      WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        AND c.relkind IN ('r','p')
           AND (greatest(c.reltuples, s.n_live_tup, s.n_mod_since_analyze) >= %s
             OR (c.reltuples < 0 AND pg_relation_size(c.oid) > 0))
    ),
    settings AS (
      SELECT jsonb_build_object(
        'default_statistics_target',
          current_setting('default_statistics_target')::int
      ) AS g
    )
    SELECT jsonb_build_object(
      'per_rel',   COALESCE((SELECT items FROM per_rel), '[]'::jsonb),
      'settings',  (SELECT g FROM settings),
      'now',       now()::text
    )::text
  $CMD$, (:'stat1_min_rows')::bigint) AS c
) cmd,
run_command_on_all_nodes(cmd.c, parallel := true) r;

-- Parse + tag with role
DROP TABLE IF EXISTS pg_temp._stat1;
CREATE TEMP TABLE _stat1 AS
SELECT
  n.nodeid,
  n.nodename, n.nodeport,
  CASE WHEN n.groupid = 0 THEN 'coord' ELSE 'worker' END AS role,
  (r.result)::jsonb AS p
FROM _stat1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE r.success;

-- Coord-side shard-map for matching shard relnames back to parents.
DROP TABLE IF EXISTS pg_temp._stat1_shard_map;
CREATE TEMP TABLE _stat1_shard_map AS
SELECT
  pn.nspname                      AS parent_schema,
  pc.relname                      AS parent_rel,
  pc.oid                          AS parent_oid,
  ds.shardid                      AS shardid,
  pc.relname || '_' || ds.shardid AS shard_rel
FROM pg_dist_shard ds
JOIN pg_class     pc ON pc.oid = ds.logicalrelid
JOIN pg_namespace pn ON pn.oid = pc.relnamespace;

CREATE INDEX ON _stat1_shard_map (parent_schema, shard_rel);

-- Flatten per-rel stream
DROP TABLE IF EXISTS pg_temp._stat1_rels;
CREATE TEMP TABLE _stat1_rels AS
SELECT
  s.role, s.nodename||':'||s.nodeport AS node, s.nodeid,
  item->>'schema'                                    AS schema,
  item->>'rel'                                       AS rel,
  (item->>'reltuples')::bigint                       AS reltuples,
  (item->>'n_live')::bigint                          AS n_live,
  (item->>'n_dead')::bigint                          AS n_dead,
  (item->>'n_mod_since_analyze')::bigint             AS n_mod,
  NULLIF(item->>'last_analyze','')::timestamptz      AS last_manual,
  NULLIF(item->>'last_autoanalyze','')::timestamptz  AS last_auto,
  (item->>'n_ext_stats')::int                        AS n_ext_stats,
  sm.parent_schema, sm.parent_rel
FROM _stat1 s,
     jsonb_array_elements(
       CASE WHEN jsonb_typeof(s.p->'per_rel')='array'
            THEN s.p->'per_rel' ELSE '[]'::jsonb END
     ) item
LEFT JOIN _stat1_shard_map sm
       ON sm.parent_schema = item->>'schema'
      AND sm.shard_rel     = item->>'rel';

-- Derive most-recent-analyze as GREATEST(manual, auto) per row.
ALTER TABLE _stat1_rels ADD COLUMN last_any timestamptz;
UPDATE _stat1_rels
SET last_any = GREATEST(last_manual, last_auto);

-- ---------------------------------------------------------------------
-- STAT1a: Distributed tables aggregated by parent
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._stat1_dist;
CREATE TEMP TABLE _stat1_dist AS
SELECT
  r.parent_schema                                     AS schema,
  r.parent_rel                                        AS parent_table,
  count(*)                                            AS n_shards,
  count(*) FILTER (WHERE last_any IS NULL AND reltuples < 0) AS shards_never_analyzed,
  min(last_any)                                       AS oldest_analyze,
  max(last_any)                                       AS newest_analyze,
  EXTRACT(EPOCH FROM (max(last_any) - min(last_any))) / 86400 AS analyze_drift_days,
  sum(n_mod)::bigint                                  AS total_mods_since_analyze,
  sum(greatest(reltuples, n_live, 0))::bigint            AS total_reltuples,
  avg(n_ext_stats)::int                               AS avg_ext_stats
FROM _stat1_rels r
WHERE r.parent_rel IS NOT NULL
GROUP BY 1,2;

SELECT 'INFO : some relations have stored row estimates but no recorded ANALYZE timestamp; statistics reset may explain the missing history' AS finding
WHERE EXISTS (SELECT 1 FROM _stat1_rels WHERE last_any IS NULL AND reltuples >= 0);

\echo
\echo '-- STAT1a. Distributed-table stats summary --'
SELECT
  schema, parent_table,
  n_shards,
  shards_never_analyzed,
  COALESCE(to_char(oldest_analyze, 'YYYY-MM-DD HH24:MI'), 'never') AS oldest,
  COALESCE(to_char(newest_analyze, 'YYYY-MM-DD HH24:MI'), 'never') AS newest,
  ROUND(analyze_drift_days::numeric, 2)                            AS drift_days,
  total_reltuples,
  total_mods_since_analyze,
  CASE WHEN total_reltuples > 0
       THEN ROUND((total_mods_since_analyze::numeric / total_reltuples) * 100, 1)
       ELSE 0 END                                                  AS churn_pct,
  CASE
    WHEN shards_never_analyzed = n_shards
      THEN 'WARN: no collected shard has a row estimate or recorded ANALYZE; inspect nonempty relations and statistics visibility'
    WHEN shards_never_analyzed > 0
      THEN format('WARN: %s of %s shards never analyzed -- run ANALYZE', shards_never_analyzed, n_shards)
    WHEN oldest_analyze IS NOT NULL AND total_mods_since_analyze > 0
         AND now() - oldest_analyze > ((:stat1_crit_days)::int * interval '1 day')
      THEN format('CRITICAL: oldest shard analyze is %s days old -- run ANALYZE',
                  ROUND(EXTRACT(EPOCH FROM now()-oldest_analyze)::numeric / 86400, 1))
    WHEN oldest_analyze IS NOT NULL AND total_mods_since_analyze > 0
         AND now() - oldest_analyze > ((:stat1_stale_days)::int * interval '1 day')
      THEN format('WARN: oldest shard analyze is %s days old',
                  ROUND(EXTRACT(EPOCH FROM now()-oldest_analyze)::numeric / 86400, 1))
    WHEN total_reltuples > 0
         AND (total_mods_since_analyze::numeric / total_reltuples) * 100
             >= (:stat1_churn_pct)::numeric
      THEN format('WARN: %s%% rows changed since last analyze (>= %s%%)',
                  ROUND((total_mods_since_analyze::numeric / total_reltuples) * 100, 1),
                  (:stat1_churn_pct)::text)
    WHEN analyze_drift_days > 1
      THEN format('INFO: analyze timestamps differ by %s days; this alone does not establish inconsistent estimates',
                  ROUND(analyze_drift_days::numeric, 1))
    ELSE 'ok'
  END AS verdict
FROM _stat1_dist
ORDER BY
  (CASE WHEN shards_never_analyzed = n_shards THEN 0
        WHEN shards_never_analyzed > 0 THEN 1
        ELSE 2 END),
  oldest_analyze NULLS FIRST,
  parent_table
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- STAT1b: Shards that are significantly staler than their siblings
-- (analyze_drift > 1 day within a parent family).
-- ---------------------------------------------------------------------
\echo
\echo '-- STAT1b. Per-shard outliers with heavy analyze drift --'
WITH parent_newest AS (
  SELECT parent_schema, parent_rel, max(last_any) AS newest
  FROM _stat1_rels
  WHERE parent_rel IS NOT NULL
  GROUP BY 1,2
)
SELECT
  r.node, r.parent_schema AS schema, r.parent_rel AS parent_table,
  r.rel AS shard,
  r.last_any AS shard_last_analyze,
  p.newest   AS sibling_newest_analyze,
  ROUND(EXTRACT(EPOCH FROM (p.newest - r.last_any))::numeric / 86400, 1) AS lag_days
FROM _stat1_rels r
JOIN parent_newest p USING (parent_schema, parent_rel)
WHERE r.parent_rel IS NOT NULL
  AND p.newest IS NOT NULL
  AND (r.last_any IS NULL
       OR p.newest - r.last_any > interval '1 day')
ORDER BY lag_days DESC NULLS FIRST, parent_table, shard
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- STAT1c: Non-shard local tables never analyzed (hygiene on coord)
-- ---------------------------------------------------------------------
\echo
\echo '-- STAT1c. Local (non-shard) tables never analyzed --'
SELECT
  r.role, r.node, r.schema, r.rel,
  r.reltuples,
  r.n_live, r.n_mod
FROM _stat1_rels r
WHERE r.parent_rel IS NULL
  AND r.last_any IS NULL
  AND r.reltuples >= (:stat1_min_rows)::bigint
ORDER BY r.reltuples DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- STAT1d: Wide tables that might benefit from CREATE STATISTICS
-- (heuristic: > 6 columns and no extended statistics object).
-- ---------------------------------------------------------------------
\echo
\echo '-- STAT1d. Distributed tables without extended statistics --'
WITH cols AS (
  SELECT
    pn.nspname    AS schema,
    pc.relname    AS table_name,
    pc.oid        AS rel_oid,
    count(*)      AS n_columns
  FROM pg_dist_partition dp
  JOIN pg_class     pc ON pc.oid = dp.logicalrelid
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attnum > 0 AND NOT a.attisdropped
  WHERE dp.partmethod = 'h'
  GROUP BY 1,2,3
)
SELECT
  c.schema, c.table_name, c.n_columns,
  (SELECT count(*) FROM pg_statistic_ext se WHERE se.stxrelid = c.rel_oid) AS ext_stats_defined,
  'consider CREATE STATISTICS on correlated columns (e.g. tenant_id, created_at)' AS recommendation
FROM cols c
WHERE c.n_columns >= 6
  AND NOT EXISTS (SELECT 1 FROM pg_statistic_ext se WHERE se.stxrelid = c.rel_oid)
ORDER BY c.n_columns DESC, c.table_name
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- STAT1e: default_statistics_target drift / low values
-- ---------------------------------------------------------------------
\echo
\echo '-- STAT1e. default_statistics_target per node --'
SELECT
  role, nodename||':'||nodeport AS node,
  (p->'settings'->>'default_statistics_target')::int AS default_statistics_target,
  CASE
    WHEN (p->'settings'->>'default_statistics_target')::int < (:stat1_default_target_low)::int
      THEN format('WARN: < %s -- planner histograms are coarse', (:stat1_default_target_low)::text)
    ELSE 'ok'
  END AS verdict
FROM _stat1
ORDER BY role DESC, node;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
SELECT (
  SELECT CASE
    WHEN EXISTS (
           SELECT 1 FROM _stat1_dist
           WHERE shards_never_analyzed = n_shards
         )
      THEN format(
        'WARN : %s distributed table(s) lack row estimates and ANALYZE history on every collected shard. Review nonempty relations and statistics visibility.',
        (SELECT COUNT(*) FROM _stat1_dist WHERE shards_never_analyzed = n_shards)
      )
    WHEN EXISTS (
           SELECT 1 FROM _stat1_dist
           WHERE oldest_analyze IS NOT NULL AND total_mods_since_analyze > 0
             AND now() - oldest_analyze > ((:stat1_crit_days)::int * interval '1 day')
         )
      THEN format(
        'CRITICAL : %s distributed table(s) have shard analyze older than %s days.',
        (SELECT COUNT(*) FROM _stat1_dist
          WHERE oldest_analyze IS NOT NULL AND total_mods_since_analyze > 0
            AND now() - oldest_analyze > ((:stat1_crit_days)::int * interval '1 day')),
        (:stat1_crit_days)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _stat1_dist
           WHERE shards_never_analyzed > 0
         )
      THEN format(
        'WARN : %s distributed table(s) have some un-analyzed shards -- plans will differ per worker.',
        (SELECT COUNT(*) FROM _stat1_dist WHERE shards_never_analyzed > 0)
      )
    WHEN EXISTS (
           SELECT 1 FROM _stat1_dist
           WHERE oldest_analyze IS NOT NULL AND total_mods_since_analyze > 0
             AND now() - oldest_analyze > ((:stat1_stale_days)::int * interval '1 day')
         )
      THEN format(
        'WARN : %s distributed table(s) have shards older than %s days since analyze.',
        (SELECT COUNT(*) FROM _stat1_dist
          WHERE oldest_analyze IS NOT NULL AND total_mods_since_analyze > 0
            AND now() - oldest_analyze > ((:stat1_stale_days)::int * interval '1 day')),
        (:stat1_stale_days)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _stat1_dist
           WHERE total_reltuples > 0
             AND (total_mods_since_analyze::numeric / total_reltuples) * 100
                 >= (:stat1_churn_pct)::numeric
         )
      THEN format(
        'WARN : %s distributed table(s) have >= %s%% churn since last ANALYZE.',
        (SELECT COUNT(*) FROM _stat1_dist
          WHERE total_reltuples > 0
            AND (total_mods_since_analyze::numeric / total_reltuples) * 100
                >= (:stat1_churn_pct)::numeric),
        (:stat1_churn_pct)::text
      )
    WHEN EXISTS (
           SELECT 1 FROM _stat1
           WHERE (p->'settings'->>'default_statistics_target')::int
                 < (:stat1_default_target_low)::int
         )
      THEN 'WARN : default_statistics_target too low on one or more nodes. See STAT1e.'
    ELSE 'OK : no statistics age-and-churn policy thresholds exceeded in collected evidence; estimates not independently validated.'
  END
) AS "Advisor STAT1 headline";

\ir ../advisor_coverage.sql
DROP TABLE pg_temp._stat1_raw;
DROP TABLE pg_temp._stat1;
DROP TABLE pg_temp._stat1_shard_map;
DROP TABLE pg_temp._stat1_rels;
DROP TABLE pg_temp._stat1_dist;
