-- NET1: Node reachability & inter-node latency.
-- In Citus MX every metadata-synced node opens outbound libpq to
-- every other node. A partial netsplit, firewall rule, or wrong
-- citus.node_conninfo on one node is enough to break DDL propagation
-- and the distributed-deadlock detector, yet it's invisible until a
-- real query tries to fan out. NET1 produces three things:
--
--  NET1a  NxN connectivity matrix via
--         citus_check_cluster_node_health()  -- every nodeid probes
--         every other active nodeid. Any row with result=false is
--         CRITICAL.
--  NET1b  Asymmetry detector -- pairs where A->B works but B->A
--         fails. Usually means one side's pg_hba or firewall is
--         missing a rule.
--  NET1c  Round-trip latency from coord to each node (3 samples
--         per node). Reports min/avg and flags WARN at > 50 ms or
--         > 3x the median across peers.
--
-- Inputs (psql -v):
--   net1_samples       = 3    -- number of latency samples per node
--   net1_warn_ms       = 50   -- WARN if avg > this
--   net1_stddev_factor = 3    -- WARN if a node's avg is > N x median

\pset pager off
\pset border 2
\pset format aligned

\if :{?net1_samples}        \else \set net1_samples        3  \endif
\if :{?net1_warn_ms}        \else \set net1_warn_ms        50 \endif
\if :{?net1_stddev_factor}  \else \set net1_stddev_factor  3  \endif
\if :{?top_n}               \else \set top_n               50 \endif

\echo '==================== NET1 : node reachability & latency ===================='

-- ---------------------------------------------------------------------
-- NET1a: connectivity matrix via citus_check_cluster_node_health
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _net1_health;
CREATE TEMP TABLE _net1_health AS
SELECT * FROM citus_check_cluster_node_health();

\echo
\echo '-- NET1a. Cluster connectivity matrix (failed edges only) --'
SELECT
  from_nodename||':'||from_nodeport AS source,
  to_nodename||':'||to_nodeport     AS target,
  result                             AS ok
FROM _net1_health
WHERE result IS NOT TRUE
ORDER BY source, target
LIMIT :top_n;

\echo
\echo '-- NET1a. Edge count summary --'
SELECT
  count(*)                                  AS total_edges,
  count(*) FILTER (WHERE result IS TRUE)    AS ok_edges,
  count(*) FILTER (WHERE result IS NOT TRUE) AS failed_edges
FROM _net1_health;

-- ---------------------------------------------------------------------
-- NET1b: asymmetric edges
-- ---------------------------------------------------------------------
\echo
\echo '-- NET1b. Asymmetric edges (A->B differs from B->A) --'
SELECT
  LEAST(from_nodename||':'||from_nodeport, to_nodename||':'||to_nodeport) AS node_a,
  GREATEST(from_nodename||':'||from_nodeport, to_nodename||':'||to_nodeport) AS node_b,
  bool_or(CASE WHEN (from_nodename,from_nodeport) < (to_nodename,to_nodeport)
               THEN result ELSE NULL END)                  AS ab,
  bool_or(CASE WHEN (from_nodename,from_nodeport) > (to_nodename,to_nodeport)
               THEN result ELSE NULL END)                  AS ba
FROM _net1_health
WHERE from_nodename <> to_nodename OR from_nodeport <> to_nodeport
GROUP BY node_a, node_b
HAVING bool_or(CASE WHEN (from_nodename,from_nodeport) < (to_nodename,to_nodeport)
                    THEN result ELSE NULL END)
    IS DISTINCT FROM
       bool_or(CASE WHEN (from_nodename,from_nodeport) > (to_nodename,to_nodeport)
                    THEN result ELSE NULL END)
ORDER BY node_a, node_b
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- NET1c: coord -> node latency sampling
-- ---------------------------------------------------------------------
-- Latency via N serial fan-outs. We use a row-returning expression
-- so we can gather clock_timestamp() on each pass.

-- Make the target sample count available to the DO block.
SELECT set_config('citus_analyze.net1_samples', (:net1_samples)::text, false);

DROP TABLE IF EXISTS _net1_lat;
CREATE TEMP TABLE _net1_lat (
  sample_no int,
  nodeid    int,
  ok        boolean,
  elapsed_ms numeric
);

-- Warm-up (discarded; first connection is cold).
INSERT INTO _net1_lat
SELECT 0, r.nodeid, r.success, NULL::numeric
FROM run_command_on_all_nodes('SELECT 1') r;

DO $$
DECLARE
  i int;
  s timestamptz;
BEGIN
  FOR i IN 1..current_setting('citus_analyze.net1_samples')::int LOOP
    s := clock_timestamp();
    INSERT INTO _net1_lat (sample_no, nodeid, ok, elapsed_ms)
    SELECT i, r.nodeid, r.success,
           EXTRACT(EPOCH FROM (clock_timestamp() - s)) * 1000
    FROM run_command_on_all_nodes('SELECT 1') r;
  END LOOP;
END $$;

\echo
\echo '-- NET1c. Round-trip latency (coord -> node, ms) --'
WITH per_node AS (
  SELECT
    l.nodeid,
    count(*) FILTER (WHERE l.sample_no > 0) AS n_samples,
    min(l.elapsed_ms)  FILTER (WHERE l.sample_no > 0) AS min_ms,
    avg(l.elapsed_ms)  FILTER (WHERE l.sample_no > 0) AS avg_ms,
    max(l.elapsed_ms)  FILTER (WHERE l.sample_no > 0) AS max_ms,
    bool_and(l.ok)                                     AS all_ok
  FROM _net1_lat l
  GROUP BY l.nodeid
),
med AS (
  SELECT
    percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_ms) AS median_avg
  FROM per_node
  WHERE avg_ms IS NOT NULL
)
SELECT
  CASE WHEN n.groupid = 0 THEN 'coord' ELSE 'worker' END AS role,
  n.nodename||':'||n.nodeport AS node,
  p.n_samples,
  ROUND(p.min_ms::numeric, 2) AS min_ms,
  ROUND(p.avg_ms::numeric, 2) AS avg_ms,
  ROUND(p.max_ms::numeric, 2) AS max_ms,
  p.all_ok,
  CASE
    WHEN NOT p.all_ok
      THEN 'CRITICAL: node unreachable during sampling'
    WHEN p.avg_ms > (:net1_warn_ms)::numeric
      THEN format('WARN: avg RTT %s ms > %s ms', ROUND(p.avg_ms::numeric,2), (:net1_warn_ms)::text)
    WHEN (SELECT median_avg FROM med) > 0
         AND p.avg_ms > (SELECT median_avg FROM med) * (:net1_stddev_factor)::numeric
      THEN format('WARN: RTT %sx higher than cluster median',
                  ROUND((p.avg_ms / NULLIF((SELECT median_avg FROM med),0))::numeric, 1))
    ELSE 'ok'
  END AS verdict
FROM per_node p
JOIN pg_dist_node n ON n.nodeid = p.nodeid
ORDER BY n.groupid, n.nodename, n.nodeport;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
SELECT (
  SELECT CASE
    WHEN (SELECT count(*) FROM _net1_health WHERE result IS NOT TRUE) > 0
      THEN format('CRITICAL : %s connectivity edge(s) failed in the cluster mesh.',
                  (SELECT count(*) FROM _net1_health WHERE result IS NOT TRUE))
    WHEN EXISTS (
      SELECT 1 FROM _net1_lat
      WHERE sample_no > 0 AND NOT ok
    )
      THEN 'CRITICAL : at least one node unreachable during latency sampling.'
    WHEN (SELECT max(avg_ms) FROM (
            SELECT avg(elapsed_ms) AS avg_ms FROM _net1_lat
            WHERE sample_no > 0 GROUP BY nodeid) _) > (:net1_warn_ms)::numeric
      THEN format('WARN : at least one node has avg RTT > %s ms.',
                  (:net1_warn_ms)::text)
    ELSE format('OK : %s-node mesh reachable, all RTTs under %s ms.',
                (SELECT count(DISTINCT from_nodename||from_nodeport::text) FROM _net1_health),
                (:net1_warn_ms)::text)
  END
) AS "Advisor NET1 headline";

DROP TABLE _net1_health;
DROP TABLE _net1_lat;
