-- =====================================================================
-- citus_analyze / C3 : maximum safe external client concurrency (MX-aware)
-- ---------------------------------------------------------------------
-- Computes the largest number of concurrent external client sessions the
-- cluster can absorb without any node breaching a PG or Citus connection
-- constraint. Considers both MX entry points (any node with hasmetadata)
-- and every inbound target (every worker + coord-as-target for MX).
--
-- Model
-- -----
-- Let  MX = { nodes where hasmetadata = true AND isactive }
-- Let  W  = { active primaries that receive shard traffic }           // |W| = n_workers
-- Let  S  = total concurrent external sessions across the whole cluster
-- Assume traffic is evenly split across MX entry points: each entry
-- handles S / |MX| sessions. Worst-case: each session's plan touches
-- every worker (pessimistic, gives a lower bound).
--
-- K_reuse is the effective per-session outbound-connection occupancy to
-- a single target worker (fraction of the transaction lifetime a
-- connection to that target is actively held). Default 0.6, overridable.
--
-- Constraints
--   (1) Per-entry-point INBOUND PG slot availability:
--         S / |MX| + internal_inbound(E)  <=  max_connections(E) - overhead
--   (2) Per-entry-point OUTBOUND budget to each target worker:
--         (S / |MX|) * K_reuse  <=  max_shared_pool_size(E)
--       (citus.max_shared_pool_size throttles per (source, target))
--   (3) Per-target-worker AGGREGATE inbound (from all entry points
--       except itself):
--         ((|MX|-1) / |MX|) * S * K_reuse  <=  max_connections(W) - overhead
--
-- The headline "max safe S" is the minimum over (1)(2)(3); the
-- constraint that binds is the reported bottleneck.
--
-- Inputs (override with -v):
--   overhead_per_node   reserve for bg workers / autovac / daemons; default 15
--   k_reuse             outbound reuse factor 0..1; default 0.6
--   headroom_pct        safety margin (e.g. 80 = plan for 80% of limit); default 80
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?overhead_per_node}  \else \set overhead_per_node  15   \endif
\if :{?k_reuse}            \else \set k_reuse            0.6  \endif
\if :{?headroom_pct}       \else \set headroom_pct       80   \endif

-- ---------------------------------------------------------------------
-- 1) Gather per-node GUCs (coord reads its own; workers via run_command)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _c3_nodes;
CREATE TEMP TABLE _c3_nodes (
    nodename text, nodeport int, is_coord boolean, is_mx boolean,
    max_connections int, max_shared_pool int, max_adaptive int,
    local_shared_pool int, inbound_budget int, outbound_budget int
);

-- coordinator row
INSERT INTO _c3_nodes
SELECT
    n.nodename, n.nodeport,
    true                                                       AS is_coord,
    n.hasmetadata                                              AS is_mx,
    current_setting('max_connections')::int                    AS max_connections,
    CASE WHEN current_setting('citus.max_shared_pool_size')::int = -1
         THEN current_setting('max_connections')::int
         ELSE current_setting('citus.max_shared_pool_size')::int END,
    current_setting('citus.max_adaptive_executor_pool_size')::int,
    current_setting('citus.local_shared_pool_size')::int,
    current_setting('max_connections')::int - :overhead_per_node,
    CASE WHEN current_setting('citus.max_shared_pool_size')::int = -1
         THEN current_setting('max_connections')::int
         ELSE current_setting('citus.max_shared_pool_size')::int END
FROM pg_dist_node n
WHERE n.groupid = 0 AND n.isactive;

-- worker rows
INSERT INTO _c3_nodes
SELECT
    r.nodename, r.nodeport, false,
    (SELECT hasmetadata FROM pg_dist_node dn
       WHERE dn.nodename=r.nodename AND dn.nodeport=r.nodeport),
    (regexp_match(r.result, 'mc=([0-9-]+)'))[1]::int              AS max_connections,
    CASE WHEN (regexp_match(r.result, 'sp=([0-9-]+)'))[1]::int = -1
         THEN (regexp_match(r.result, 'mc=([0-9-]+)'))[1]::int
         ELSE (regexp_match(r.result, 'sp=([0-9-]+)'))[1]::int END AS max_shared_pool,
    (regexp_match(r.result, 'ae=([0-9-]+)'))[1]::int              AS max_adaptive,
    (regexp_match(r.result, 'lp=([0-9-]+)'))[1]::int              AS local_shared_pool,
    (regexp_match(r.result, 'mc=([0-9-]+)'))[1]::int - :overhead_per_node AS inbound_budget,
    CASE WHEN (regexp_match(r.result, 'sp=([0-9-]+)'))[1]::int = -1
         THEN (regexp_match(r.result, 'mc=([0-9-]+)'))[1]::int
         ELSE (regexp_match(r.result, 'sp=([0-9-]+)'))[1]::int END AS outbound_budget
FROM run_command_on_workers(
    $$ SELECT format('mc=%s;sp=%s;ae=%s;lp=%s',
           current_setting('max_connections'),
           current_setting('citus.max_shared_pool_size'),
           current_setting('citus.max_adaptive_executor_pool_size'),
           current_setting('citus.local_shared_pool_size')) $$
) r
WHERE r.success;

-- ---------------------------------------------------------------------
-- 2) Compute caps and bottleneck
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _c3_summary;
CREATE TEMP TABLE _c3_summary AS
WITH
sizes AS (
    SELECT
        (SELECT count(*) FROM _c3_nodes WHERE is_mx)                 AS n_mx,
        (SELECT count(*) FROM pg_dist_node
           WHERE noderole='primary' AND isactive AND shouldhaveshards) AS n_workers,
        :k_reuse::numeric        AS k_reuse,
        (:headroom_pct::numeric / 100.0) AS safety
),
-- per-entry-point caps
per_entry AS (
    SELECT
        n.nodename, n.nodeport, n.is_coord, n.is_mx,
        n.max_connections, n.max_shared_pool, n.inbound_budget, n.outbound_budget,
        s.n_mx, s.n_workers, s.k_reuse, s.safety,
        -- (1) inbound limit -> max S if evenly distributed across MX entries
        CASE WHEN n.is_mx THEN (n.inbound_budget::numeric * s.safety)::int * s.n_mx
             ELSE NULL END AS cap_from_inbound,
        -- (2) outbound to a single worker target (worst-case target)
        CASE WHEN n.is_mx THEN
             floor((n.outbound_budget::numeric * s.safety / s.k_reuse))::int * s.n_mx
             ELSE NULL END AS cap_from_outbound
    FROM _c3_nodes n, sizes s
),
-- per-target-worker inbound aggregation
per_target AS (
    SELECT n.nodename, n.nodeport, n.is_coord, n.is_mx,
           n.inbound_budget, s.n_mx, s.n_workers, s.k_reuse, s.safety,
           (s.n_mx - CASE WHEN n.is_mx THEN 1 ELSE 0 END) AS n_mx_others,
           CASE
             WHEN (s.n_mx - CASE WHEN n.is_mx THEN 1 ELSE 0 END) = 0
               THEN 2147483647
             ELSE floor(
               (n.inbound_budget::numeric * s.safety) * s.n_mx
               / ((s.n_mx - CASE WHEN n.is_mx THEN 1 ELSE 0 END) * s.k_reuse)
             )::int
           END AS cap_from_aggregate_inbound
    FROM _c3_nodes n, sizes s
)
SELECT
    (SELECT min(LEAST(cap_from_inbound, cap_from_outbound))
       FROM per_entry WHERE is_mx)                    AS cap_entry_min,
    (SELECT entry_bottleneck FROM (
       SELECT format('%s:%s (%s)', nodename, nodeport,
              CASE WHEN cap_from_inbound <= cap_from_outbound
                   THEN 'inbound max_connections'
                   ELSE 'outbound max_shared_pool_size' END) AS entry_bottleneck,
              LEAST(cap_from_inbound, cap_from_outbound) AS cap
       FROM per_entry WHERE is_mx
       ORDER BY cap ASC LIMIT 1
     ) z)                                             AS entry_bottleneck_label,
    (SELECT min(cap_from_aggregate_inbound) FROM per_target)    AS cap_target_min,
    (SELECT format('%s:%s (aggregate inbound from %s MX entries)',
                   nodename, nodeport, n_mx)
       FROM per_target
       WHERE cap_from_aggregate_inbound = (SELECT min(cap_from_aggregate_inbound) FROM per_target)
       LIMIT 1)                                       AS target_bottleneck_label,
    (SELECT n_mx FROM sizes),
    (SELECT n_workers FROM sizes),
    (SELECT k_reuse FROM sizes),
    (SELECT safety  FROM sizes);

-- ---------------------------------------------------------------------
-- 3) Report
-- ---------------------------------------------------------------------
\echo
\echo '======================= C3 : max safe external client concurrency ======================='

SELECT
    format('Topology          : %s MX entry points, %s workers (shouldhaveshards), K_reuse=%s, safety=%s%%',
           n_mx, n_workers, k_reuse, (safety*100)::int) AS info
FROM _c3_summary;

\echo
\echo '-- Per-entry-point caps (if this node is the only receiver of external traffic, evenly divided across MX) --'
SELECT
    nodename                           AS node,
    nodeport                           AS port,
    CASE WHEN is_coord THEN 'coord' ELSE 'worker' END AS role,
    CASE WHEN is_mx THEN 'yes' ELSE 'no' END            AS mx,
    max_connections                    AS max_conn,
    inbound_budget                     AS inbound_slots,
    max_shared_pool                    AS outbound_pool,
    cap_from_inbound                   AS cap_inbound_S,
    cap_from_outbound                  AS cap_outbound_S,
    LEAST(cap_from_inbound, cap_from_outbound) AS cap_S,
    CASE WHEN cap_from_inbound <= cap_from_outbound
         THEN 'inbound max_connections'
         ELSE 'outbound max_shared_pool_size' END       AS bottleneck
FROM (
    SELECT n.*, s.n_mx, s.n_workers, s.k_reuse, s.safety,
           (n.inbound_budget::numeric * s.safety)::int * s.n_mx         AS cap_from_inbound,
           floor((n.outbound_budget::numeric * s.safety / s.k_reuse))::int * s.n_mx AS cap_from_outbound
    FROM _c3_nodes n,
         (SELECT (SELECT count(*) FROM _c3_nodes WHERE is_mx) n_mx,
                 (SELECT count(*) FROM pg_dist_node
                    WHERE noderole='primary' AND isactive AND shouldhaveshards) n_workers,
                 :k_reuse::numeric k_reuse,
                 :headroom_pct::numeric/100.0 safety) s
    WHERE n.is_mx
) x
ORDER BY cap_S;

\echo
\echo '-- Per-target aggregate inbound caps (ALL MX entries share load onto this target) --'
SELECT
    nodename AS node, nodeport AS port,
    CASE WHEN is_coord THEN 'coord' ELSE 'worker' END AS role,
    CASE WHEN is_mx   THEN 'yes'   ELSE 'no'     END AS mx,
    inbound_budget AS inbound_slots,
    n_mx_others,
    cap_S_aggregate
FROM (
    SELECT n.nodename, n.nodeport, n.is_coord, n.is_mx, n.inbound_budget,
           (s.n_mx - CASE WHEN n.is_mx THEN 1 ELSE 0 END) AS n_mx_others,
           CASE
             WHEN (s.n_mx - CASE WHEN n.is_mx THEN 1 ELSE 0 END) = 0
               THEN 2147483647  -- not bottlenecked by fan-in from other MX
             ELSE floor(
               (n.inbound_budget::numeric * s.safety) * s.n_mx
               / ((s.n_mx - CASE WHEN n.is_mx THEN 1 ELSE 0 END) * s.k_reuse)
             )::int
           END AS cap_S_aggregate
    FROM _c3_nodes n,
         (SELECT (SELECT count(*) FROM _c3_nodes WHERE is_mx) n_mx,
                 :k_reuse::numeric k_reuse,
                 :headroom_pct::numeric/100.0 safety) s
) y
ORDER BY cap_S_aggregate;

\echo
\echo '-- Headline --'
\pset tuples_only on
SELECT format('MAX SAFE EXTERNAL CONCURRENCY : %s  (bottleneck: %s)',
              LEAST(cap_entry_min, cap_target_min),
              CASE WHEN cap_entry_min <= cap_target_min
                   THEN entry_bottleneck_label
                   ELSE target_bottleneck_label END)
FROM _c3_summary;

SELECT format('   - Entry-point min cap   : %s  (%s)',   cap_entry_min,  entry_bottleneck_label)  FROM _c3_summary;
SELECT format('   - Target-aggregate min  : %s  (%s)',   cap_target_min, target_bottleneck_label) FROM _c3_summary;

SELECT
  CASE
    WHEN LEAST(cap_entry_min, cap_target_min) <= 0
      THEN 'CRITICAL : no headroom. Raise max_connections or max_shared_pool_size before accepting external traffic.'
    WHEN LEAST(cap_entry_min, cap_target_min) < 50
      THEN format('WARN : only %s concurrent external sessions safe. Consider pooling (pgbouncer) in front of entry points.',
                  LEAST(cap_entry_min, cap_target_min))
    ELSE format('OK : safe up to %s concurrent external sessions under current config.',
                  LEAST(cap_entry_min, cap_target_min))
  END
FROM _c3_summary;

\pset tuples_only off

DROP TABLE _c3_nodes;
DROP TABLE _c3_summary;
