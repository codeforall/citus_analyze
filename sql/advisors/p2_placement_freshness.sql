-- Recorded placement length is metadata, not necessarily the active cost UDF input.
\set advisor_id P2
\ir ../capabilities.sql
\pset pager off
\pset border 2
\pset format aligned
\if :{?p2_drift_pct} \else \set p2_drift_pct 25 \endif
\if :{?p2_min_bytes} \else \set p2_min_bytes 1048576 \endif
\if :{?p2_zero_floor} \else \set p2_zero_floor 0 \endif
\if :{?p2_top_n} \else \set p2_top_n 40 \endif
\echo '==================== P2 : recorded vs live placement sizes ===================='
DROP TABLE IF EXISTS pg_temp._p2_raw;
CREATE TEMP TABLE _p2_raw AS
SELECT response.*
FROM (SELECT DISTINCT logicalrelid FROM pg_dist_shard) tables
CROSS JOIN LATERAL run_command_on_placements(tables.logicalrelid,
  'SELECT pg_total_relation_size(''%s'')::text') response;
DROP TABLE IF EXISTS pg_temp._p2_sizes;
CREATE TEMP TABLE _p2_sizes AS
SELECT shard.logicalrelid::regclass::text AS table_name, placement.shardid,
       node.nodename || ':' || node.nodeport AS node,
       placement.shardlength AS recorded_bytes,
       CASE WHEN raw.success AND raw.result ~ '^[0-9]+$' THEN raw.result::bigint END AS live_bytes
FROM pg_dist_shard shard JOIN pg_dist_placement placement USING (shardid)
JOIN pg_dist_node node ON node.groupid=placement.groupid AND node.isactive AND node.noderole='primary'
LEFT JOIN _p2_raw raw ON raw.shardid=placement.shardid AND raw.nodename=node.nodename AND raw.nodeport=node.nodeport;
SELECT table_name, shardid, node, recorded_bytes, live_bytes,
       CASE WHEN live_bytes IS NULL THEN 'INCOMPLETE : placement probe unavailable'
            ELSE 'INFO : recorded size differs from live size; review consumers of this metadata'
       END AS verdict
FROM _p2_sizes
WHERE live_bytes IS NULL OR (recorded_bytes=0 AND live_bytes > :p2_zero_floor::numeric)
   OR (live_bytes > :p2_min_bytes::numeric AND abs(live_bytes-recorded_bytes) / greatest(live_bytes, 1)::numeric * 100 > :p2_drift_pct::numeric)
ORDER BY live_bytes DESC NULLS FIRST LIMIT :p2_top_n;
SELECT CASE WHEN count(*) FILTER (WHERE live_bytes IS NULL) > 0
            THEN 'INCOMPLETE : placement-size coverage is incomplete'
            ELSE format('INFO : %s placements measured; %s nonempty placements have zero recorded length. This does not establish an incorrect rebalance plan.',
                 count(*), count(*) FILTER (WHERE recorded_bytes=0 AND live_bytes > :p2_zero_floor::numeric))
       END AS finding FROM _p2_sizes;
\echo 'INFO : upstream Citus 13.3 disk-size cost reads live colocated shard sizes. Other versions/custom strategies must be inspected before attributing behavior to shardlength. Do not stop a rebalance solely on this discrepancy.'
DROP TABLE pg_temp._p2_sizes;
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._p2_raw;