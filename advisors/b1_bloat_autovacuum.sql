-- =====================================================================
-- citus_analyze / B1 : table bloat & autovacuum lag (per-shard aware)
-- ---------------------------------------------------------------------
-- Bloat on a single distributed shard is invisible from the coordinator
-- unless you look at every shard on every worker. Citus distributes a
-- write load N-ways, so dead-tuple accumulation on the busiest shard
-- often accelerates faster than `autovacuum_vacuum_scale_factor` defaults
-- can keep up with -- and the symptoms (slow index scans, growing index
-- bloat, RAM pressure on workers) only show up via per-shard digging.
--
-- This advisor reports, per worker:
--
--   B1a  Top bloated shards by ESTIMATED dead-tuple ratio (n_dead_tup /
--        (n_live_tup + n_dead_tup)) above a threshold AND large enough
--        to matter.
--   B1b  Shards whose dead-tuple count exceeds the per-shard autovacuum
--        threshold (av_threshold + av_scale_factor * n_live_tup) -- the
--        formula PostgreSQL itself uses to decide when to vacuum -- but
--        autovacuum has not run yet (last_autovacuum stale or null).
--   B1c  Currently running autovacuum / autoanalyze workers per node
--        (saturation indicator).
--   B1d  Per-table aggregate (sum across shards) view of dead tuples
--        and the suggested per-table autovacuum_vacuum_scale_factor
--        override that would converge dead tuples without thrashing.
--   B1e  Tables with stale ANALYZE -- not bloat per se but a sibling
--        symptom of autovacuum starvation. Stale stats on distributed
--        tables produces wrong row estimates and bad plans.
--
-- Why estimated, not pgstattuple:
--   pgstattuple has an O(N) scan cost per relation and is only available
--   when the extension is installed. We default to the cheap statistical
--   estimate and surface a NOTICE recommending pgstattuple if any shard
--   crosses the WARN line.
--
-- Inputs (override with -v):
--   warn_dead_pct        Dead-tuple ratio % at WARN (default 20).
--   crit_dead_pct        Dead-tuple ratio % at CRITICAL (default 40).
--   min_bytes_for_bloat  Skip relations smaller than this in bytes
--                        (default 100 MB; tiny tables are noise).
--   stale_analyze_days   ANALYZE older than this is "stale"
--                        (default 7 days).
--   top_n                Row cap on listings (default 20).
--
-- Caveats
--   * `n_live_tup` and `n_dead_tup` are estimates maintained by
--     autovacuum; they reset on stats reset and drift between vacuums.
--     We surface the estimate's freshness via stats_reset.
--   * On non-MX workers `pg_dist_shard` isn't available; we still emit
--     a per-shard view because `pg_stat_user_tables` gives us the
--     relation name and we map shards to parents on the COORDINATOR.
--   * "Suggested scale_factor" is a lower-bound recommendation; if a
--     table is genuinely write-heavy and bloat is a function of vacuum
--     IOPS rather than threshold, lowering the factor may not help.
--     We surface that distinction in the headline.
-- =====================================================================
\if :{?warn_dead_pct}        \else \set warn_dead_pct        20 \endif
\if :{?crit_dead_pct}        \else \set crit_dead_pct        40 \endif
\if :{?min_bytes_for_bloat}  \else \set min_bytes_for_bloat 104857600 \endif
\if :{?stale_analyze_days}   \else \set stale_analyze_days  7  \endif
\if :{?top_n}                \else \set top_n               20 \endif

\echo '==================== B1 : table bloat & autovacuum lag ===================='

-- ---------------------------------------------------------------------
-- Fan-out per-relation autovacuum & dead-tuple stats from every node.
-- The remote payload deliberately does NOT reference pg_dist_shard --
-- shard mapping happens on the coordinator (always has metadata).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _b1_raw;
CREATE TEMP TABLE _b1_raw (nodeid int, success boolean, result text);

-- Build the remote command outside any dollar-quoted block so psql
-- can interpolate :min_bytes_for_bloat normally; the inner SQL is
-- dollar-quoted ($CMD$) which is opaque to psql -- format() fills %s.
INSERT INTO _b1_raw (nodeid, success, result)
SELECT r.nodeid, r.success, r.result
FROM (
  SELECT format($CMD$
    WITH _vis AS MATERIALIZED (
      -- Defensive: ensure shards are visible to pg_class / pg_stat_*
      -- scans even in deployments where the internal-backend bypass is
      -- stripped or citus.override_table_visibility is forced on.
      SELECT set_config('citus.override_table_visibility','off', true)
    ),
    stats_age AS (
      SELECT EXTRACT(EPOCH FROM (now() - min(stats_reset)))::bigint AS sec
      FROM pg_stat_database WHERE stats_reset IS NOT NULL
    ),
    relopts AS (
      SELECT c.oid AS relid,
             COALESCE((SELECT option_value::float
                         FROM pg_options_to_table(c.reloptions)
                         WHERE option_name='autovacuum_vacuum_scale_factor'),
                       current_setting('autovacuum_vacuum_scale_factor')::float) AS scale_factor,
             COALESCE((SELECT option_value::float
                         FROM pg_options_to_table(c.reloptions)
                         WHERE option_name='autovacuum_vacuum_threshold'),
                       current_setting('autovacuum_vacuum_threshold')::float)    AS av_threshold
      FROM pg_class c
      WHERE c.relkind IN ('r','p','m')
    ),
    rels AS (
      SELECT
          n.nspname                                  AS schema,
          c.relname                                  AS rel,
          s.n_live_tup                               AS live,
          s.n_dead_tup                               AS dead,
          s.last_vacuum,
          s.last_autovacuum,
          s.last_analyze,
          s.last_autoanalyze,
          pg_relation_size(s.relid)                  AS bytes,
          ro.scale_factor                            AS av_sf,
          ro.av_threshold                            AS av_thr,
          (ro.av_threshold + ro.scale_factor * s.n_live_tup)::bigint AS av_trigger
      FROM pg_stat_user_tables s
      JOIN pg_class c ON c.oid = s.relid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN relopts ro ON ro.relid = s.relid
      WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        AND pg_relation_size(s.relid) >= %s
    ),
    rels_json AS (
      SELECT jsonb_agg(jsonb_build_object(
               'schema',          schema,
               'rel',             rel,
               'live',            live,
               'dead',            dead,
               'bytes',           bytes,
               'last_vacuum',     last_vacuum,
               'last_autovacuum', last_autovacuum,
               'last_analyze',    last_analyze,
               'last_autoanalyze',last_autoanalyze,
               'av_sf',           av_sf,
               'av_thr',          av_thr,
               'av_trigger',      av_trigger
             )) AS items
      FROM rels
    ),
    av_active AS (
      SELECT count(*)::int AS n
      FROM pg_stat_activity
      WHERE backend_type = 'autovacuum worker'
    ),
    pgstattuple_avail AS (
      SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgstattuple') AS yes
    )
    SELECT jsonb_build_object(
      'stats_reset_age_sec', (SELECT sec FROM stats_age),
      'rels',                (SELECT items FROM rels_json),
      'av_active',           (SELECT n FROM av_active),
      'pgstattuple',         (SELECT yes FROM pgstattuple_avail),
      'av_max_workers',      current_setting('autovacuum_max_workers')::int
    )::text
  $CMD$, (:'min_bytes_for_bloat')::bigint) AS c
) cmd, run_command_on_all_nodes(cmd.c, parallel := true) r;

-- ---------------------------------------------------------------------
-- Parse per-node payload.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _b1;
CREATE TEMP TABLE _b1 AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata  THEN 'worker (MX)'
         ELSE 'worker' END                                  AS role,
    n.nodename || ':' || n.nodeport                         AS node,
    n.groupid,
    r.success,
    CASE WHEN r.success AND r.result <> '' THEN r.result::jsonb
         ELSE '{}'::jsonb END                               AS p
FROM _b1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE n.isactive;

-- ---------------------------------------------------------------------
-- Coord-side shard map -- same trick as I1 for schema-safe matching.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _b1_shard_map;
CREATE TEMP TABLE _b1_shard_map AS
SELECT
    pn.nspname                                          AS parent_schema,
    pc.relname                                          AS parent_rel,
    pc.oid                                              AS parent_oid,
    ds.shardid                                          AS shardid,
    pc.relname || '_' || ds.shardid                     AS shard_rel
FROM pg_dist_shard ds
JOIN pg_class     pc ON pc.oid = ds.logicalrelid
JOIN pg_namespace pn ON pn.oid = pc.relnamespace;

CREATE INDEX ON _b1_shard_map (parent_schema, shard_rel);

-- ---------------------------------------------------------------------
-- Flatten + classify (shard vs local).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _b1_flat;
CREATE TEMP TABLE _b1_flat AS
SELECT
    b.role, b.node, b.groupid,
    item->>'schema'                              AS schema,
    item->>'rel'                                 AS rel,
    COALESCE((item->>'live')::bigint,0)          AS live,
    COALESCE((item->>'dead')::bigint,0)          AS dead,
    COALESCE((item->>'bytes')::bigint,0)         AS bytes,
    NULLIF(item->>'last_vacuum','')::timestamptz      AS last_vacuum,
    NULLIF(item->>'last_autovacuum','')::timestamptz  AS last_autovacuum,
    NULLIF(item->>'last_analyze','')::timestamptz     AS last_analyze,
    NULLIF(item->>'last_autoanalyze','')::timestamptz AS last_autoanalyze,
    COALESCE((item->>'av_sf')::float,0.2)        AS av_sf,
    COALESCE((item->>'av_thr')::bigint,50)       AS av_thr,
    COALESCE((item->>'av_trigger')::bigint,0)    AS av_trigger,
    sm.parent_schema, sm.parent_rel,
    CASE WHEN sm.parent_rel IS NOT NULL THEN 'shard' ELSE 'local' END AS kind
FROM _b1 b,
     jsonb_array_elements(CASE WHEN jsonb_typeof(b.p->'rels')='array' THEN b.p->'rels' ELSE '[]'::jsonb END) item
LEFT JOIN _b1_shard_map sm
       ON sm.parent_schema = item->>'schema'
      AND sm.shard_rel     = item->>'rel'
WHERE b.success;

-- ---------------------------------------------------------------------
-- B1a : top bloated shards / tables by ESTIMATED dead-tuple ratio.
-- ---------------------------------------------------------------------
\echo
\echo '-- B1a. Top relations by estimated dead-tuple ratio --'
SELECT
    f.role, f.node, f.kind,
    f.schema,
    COALESCE(f.parent_rel, f.rel)             AS parent_or_local,
    f.rel                                     AS relation,
    pg_size_pretty(f.bytes)                   AS size,
    f.live, f.dead,
    CASE WHEN (f.live + f.dead) > 0
         THEN round(100.0 * f.dead / (f.live + f.dead), 1)
         ELSE 0 END                           AS dead_pct,
    f.last_autovacuum
FROM _b1_flat f
WHERE (f.live + f.dead) > 0
  AND (100.0 * f.dead / (f.live + f.dead)) >= (:'warn_dead_pct')::int
ORDER BY (1.0 * f.dead / NULLIF(f.live + f.dead,0)) DESC, f.bytes DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- B1b : Shards/tables whose dead tuples already exceed the per-relation
--       autovacuum trigger but autovacuum has not run since.
--       (Last vacuum / autovacuum is stale relative to the threshold
--       crossing => autovacuum is starved or misconfigured.)
-- ---------------------------------------------------------------------
\echo
\echo '-- B1b. Past-due relations (dead > av_trigger and AV has not caught up) --'
SELECT
    f.role, f.node, f.kind,
    f.schema,
    COALESCE(f.parent_rel, f.rel)            AS parent_or_local,
    f.rel                                    AS relation,
    f.dead,
    f.av_trigger,
    f.av_sf                                  AS scale_factor,
    pg_size_pretty(f.bytes)                  AS size,
    GREATEST(f.last_autovacuum, f.last_vacuum) AS last_vac
FROM _b1_flat f
WHERE f.dead > f.av_trigger
  AND f.av_trigger > 0
ORDER BY (f.dead::numeric / NULLIF(f.av_trigger,0)) DESC, f.bytes DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- B1c : autovacuum worker saturation per node.
-- ---------------------------------------------------------------------
\echo
\echo '-- B1c. Active autovacuum workers per node (saturation indicator) --'
SELECT
    role, node,
    (p->>'av_active')::int       AS active_workers,
    (p->>'av_max_workers')::int  AS max_workers,
    CASE WHEN (p->>'av_max_workers')::int > 0
         THEN round(100.0 * (p->>'av_active')::int
                          / (p->>'av_max_workers')::int, 0)
         ELSE NULL END           AS pct_busy
FROM _b1
WHERE success
ORDER BY groupid;

-- ---------------------------------------------------------------------
-- B1d : Per-distributed-table aggregate dead tuples + recommended
--       autovacuum_vacuum_scale_factor override.
--
-- Recommendation rationale:
--   The default scale_factor is 0.2 (20% of live tuples must be dead
--   before AV runs). For distributed tables sharded N ways and seeing
--   high write rates per shard, this can mean each shard accumulates
--   millions of dead tuples between AV passes. We recommend:
--
--     suggested_sf = max(0.02, av_sf * (current_dead / 2*av_trigger))
--
--   capped at 0.20 (don't recommend a HIGHER value than current; only
--   lower it, since dead > av_trigger is the trigger condition for B1d).
-- ---------------------------------------------------------------------
\echo
\echo '-- B1d. Per-distributed-table aggregate bloat + suggested AV scale_factor --'
WITH agg AS (
  SELECT
      f.parent_schema AS schema,
      f.parent_rel    AS parent_table,
      sum(f.live)     AS live,
      sum(f.dead)     AS dead,
      sum(f.bytes)    AS bytes,
      count(*)        AS shard_n,
      max(f.av_sf)    AS current_sf,
      max(f.av_trigger) AS max_av_trigger,
      max(f.dead)     AS worst_shard_dead
  FROM _b1_flat f
  WHERE f.kind = 'shard'
  GROUP BY 1, 2
)
SELECT
    schema, parent_table, shard_n,
    live, dead, pg_size_pretty(bytes) AS total_size,
    CASE WHEN (live + dead) > 0
         THEN round(100.0 * dead / (live + dead), 1)
         ELSE 0 END                      AS dead_pct,
    current_sf,
    -- Recommend lowering scale_factor proportional to overshoot.
    CASE
      WHEN max_av_trigger > 0 AND worst_shard_dead > max_av_trigger
        THEN GREATEST(0.02,
               LEAST(current_sf,
                     round((current_sf * max_av_trigger
                            / NULLIF(worst_shard_dead,0))::numeric, 3)))
      ELSE NULL
    END                                  AS suggested_sf
FROM agg
WHERE (live + dead) > 0
ORDER BY dead DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- B1e : stale ANALYZE on distributed tables (per-shard, sibling symptom)
-- ---------------------------------------------------------------------
\echo
\echo '-- B1e. Shards/tables with stale ANALYZE --'
SELECT
    f.role, f.node, f.kind,
    f.schema,
    COALESCE(f.parent_rel, f.rel) AS parent_or_local,
    f.rel                         AS relation,
    pg_size_pretty(f.bytes)       AS size,
    GREATEST(f.last_analyze, f.last_autoanalyze) AS last_analyzed,
    CASE WHEN GREATEST(f.last_analyze, f.last_autoanalyze) IS NULL
         THEN 'never'
         ELSE EXTRACT(DAY FROM (now() -
                GREATEST(f.last_analyze, f.last_autoanalyze)))::text
              || ' days' END      AS staleness
FROM _b1_flat f
WHERE GREATEST(f.last_analyze, f.last_autoanalyze) IS NULL
   OR GREATEST(f.last_analyze, f.last_autoanalyze)
        < now() - ((:'stale_analyze_days')::int || ' days')::interval
ORDER BY GREATEST(f.last_analyze, f.last_autoanalyze) NULLS FIRST, f.bytes DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sig AS (
  SELECT
    (SELECT count(*) FROM _b1_flat
       WHERE (live + dead) > 0
         AND (100.0 * dead / (live + dead)) >= (:'crit_dead_pct')::int) AS crit_n,
    (SELECT count(*) FROM _b1_flat
       WHERE (live + dead) > 0
         AND (100.0 * dead / (live + dead)) >= (:'warn_dead_pct')::int) AS warn_n,
    (SELECT count(*) FROM _b1_flat
       WHERE dead > av_trigger AND av_trigger > 0)                       AS pastdue_n,
    (SELECT bool_or((p->>'av_active')::int = (p->>'av_max_workers')::int)
       FROM _b1 WHERE success)                                            AS av_saturated,
    (SELECT count(*) FROM _b1_flat
       WHERE GREATEST(last_analyze, last_autoanalyze) IS NULL
          OR GREATEST(last_analyze, last_autoanalyze)
              < now() - ((:'stale_analyze_days')::int || ' days')::interval) AS stale_n,
    (SELECT bool_or((p->>'pgstattuple')::bool) FROM _b1 WHERE success)   AS has_pgstat,
    (SELECT min((p->>'stats_reset_age_sec')::bigint)
       FROM _b1 WHERE p ? 'stats_reset_age_sec'
                 AND (p->>'stats_reset_age_sec') ~ '^[0-9]+$')           AS min_stats_age,
    (SELECT count(*) FROM _b1_raw WHERE NOT success)                     AS unreachable
)
SELECT CASE
  WHEN crit_n > 0 THEN
    format('CRITICAL : %s relation(s) >= %s%% dead tuples (estimated). %s past-due autovacuum target(s). See B1a/B1b.%s',
           crit_n, (:'crit_dead_pct'),
           pastdue_n,
           CASE WHEN NOT has_pgstat
                THEN ' Install pgstattuple for exact bloat numbers.'
                ELSE '' END)
  WHEN warn_n > 0 THEN
    format('WARN : %s relation(s) >= %s%% dead tuples (estimated). %s past-due autovacuum target(s). See B1a/B1b/B1d for suggested scale_factor.',
           warn_n, (:'warn_dead_pct'), pastdue_n)
  WHEN pastdue_n > 0 THEN
    format('WARN : %s relation(s) past their autovacuum trigger but not yet vacuumed. Autovacuum may be falling behind; consider lowering autovacuum_vacuum_scale_factor (see B1d) or raising autovacuum_max_workers / autovacuum_naptime.',
           pastdue_n)
  WHEN av_saturated THEN
    'WARN : autovacuum_max_workers fully busy on at least one node. Consider raising autovacuum_max_workers or autovacuum_vacuum_cost_limit.'
  WHEN stale_n > 0 THEN
    format('WARN : %s relation(s) have stale ANALYZE (> %s days). Plans on distributed queries may use wrong row estimates.',
           stale_n, (:'stale_analyze_days'))
  WHEN min_stats_age IS NOT NULL AND min_stats_age < 3600 THEN
    format('INFO : pg_stat counters reset only %s s ago on at least one node; bloat estimate is unreliable until autovacuum updates n_dead_tup. Re-run later.',
           min_stats_age)
  WHEN unreachable > 0 THEN
    format('WARN : %s node(s) unreachable; bloat partially verified.', unreachable)
  ELSE
    'OK : no significant bloat, autovacuum keeping up, ANALYZE fresh.'
END
FROM sig;
\pset tuples_only off

DROP TABLE IF EXISTS _b1_flat;
DROP TABLE IF EXISTS _b1_shard_map;
DROP TABLE IF EXISTS _b1;
DROP TABLE IF EXISTS _b1_raw;
