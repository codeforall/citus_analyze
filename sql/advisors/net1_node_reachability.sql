\set advisor_id NET1
\ir ../capabilities.sql
\pset pager off
\pset border 2
\pset format aligned
\if :{?net1_samples} \else \set net1_samples 3 \endif
\echo '==================== NET1 : node reachability and batch probe timing ===================='
DROP TABLE IF EXISTS pg_temp._net1_health;
CREATE TEMP TABLE _net1_health AS SELECT * FROM citus_check_cluster_node_health();
SELECT from_nodename || ':' || from_nodeport AS source,
       to_nodename || ':' || to_nodeport AS target, result AS reachable
FROM _net1_health ORDER BY source, target;
SELECT CASE WHEN count(*) FILTER (WHERE result IS NOT TRUE) > 0
            THEN format('CRITICAL : %s mesh connectivity edge(s) failed', count(*) FILTER (WHERE result IS NOT TRUE))
            WHEN count(*) = 0 THEN 'INCOMPLETE : health function returned no edges'
            ELSE 'OK : reported mesh connectivity edges succeeded; this is not a latency measurement'
       END AS finding FROM _net1_health;
SELECT 'INCOMPLETE : connectivity matrix does not cover every active primary pair' AS finding
WHERE EXISTS (
  SELECT 1 FROM pg_dist_node source CROSS JOIN pg_dist_node target
  WHERE source.isactive AND target.isactive AND source.noderole='primary' AND target.noderole='primary'
    AND NOT EXISTS (SELECT 1 FROM _net1_health health
        WHERE health.from_nodename=source.nodename AND health.from_nodeport=source.nodeport
          AND health.to_nodename=target.nodename AND health.to_nodeport=target.nodeport)
);
SELECT set_config('citus_analyze.net1_samples', least(greatest(:net1_samples::int, 1), 10)::text, false);
DROP TABLE IF EXISTS pg_temp._net1_raw;
CREATE TEMP TABLE _net1_raw (sample int, nodeid int, success boolean, result text, batch_ms numeric);
DO $timing$
DECLARE
  sample_index int;
  started timestamptz;
BEGIN
  FOR sample_index IN 0..current_setting('citus_analyze.net1_samples')::int LOOP
    started := clock_timestamp();
    INSERT INTO _net1_raw SELECT sample_index, response.*, NULL::numeric FROM run_command_on_all_nodes('SELECT 1', parallel := true) response;
    UPDATE _net1_raw SET batch_ms=extract(epoch FROM (clock_timestamp()-started))*1000 WHERE sample=sample_index;
  END LOOP;
END
$timing$;
SELECT sample, round(max(batch_ms), 2) AS batch_elapsed_ms,
       count(*) AS nodes, bool_and(success) AS all_succeeded,
       'INFO : complete fan-out duration including scheduling/SQL overhead; not per-node RTT' AS observation
FROM _net1_raw WHERE sample > 0 GROUP BY sample ORDER BY sample;
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._net1_raw;
DROP TABLE pg_temp._net1_health;