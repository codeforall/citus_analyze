\set advisor_id S3
\ir ../capabilities.sql
\pset pager off
\pset border 2
\pset format aligned
\if :{?warn_ratio} \else \set warn_ratio 2.0 \endif
\if :{?min_group_bytes} \else \set min_group_bytes 104857600 \endif
\echo '==================== S3 : per-table shard size and physical node balance ===================='
DROP TABLE IF EXISTS pg_temp._s3_sizes;
CREATE TEMP TABLE _s3_sizes AS SELECT * FROM citus_shards;
WITH logical_shards AS (
  SELECT table_name, shardid, max(shard_size) AS bytes
  FROM _s3_sizes GROUP BY table_name, shardid
), tables AS (
  SELECT table_name, count(*) AS shards, sum(bytes) AS total_bytes,
         max(bytes) AS max_bytes, avg(bytes) AS avg_bytes
  FROM logical_shards GROUP BY table_name
)
SELECT table_name, shards, pg_size_pretty(total_bytes) AS total_size,
       round(max_bytes / nullif(avg_bytes, 0), 2) AS max_avg_ratio,
       CASE WHEN total_bytes >= :min_group_bytes::numeric
                  AND max_bytes / nullif(avg_bytes, 0) > :warn_ratio::numeric
            THEN 'WARN : table shard-size ratio exceeds policy; inspect distribution and workload before choosing an action'
            ELSE 'INFO : table shard-size policy not exceeded; size alone does not establish workload balance'
       END AS verdict
FROM tables ORDER BY max_bytes / nullif(avg_bytes, 0) DESC NULLS LAST;
WITH physical AS (
  SELECT node.nodeid, node.nodename, node.nodeport,
         coalesce(sum(size.shard_size), 0) AS bytes
  FROM pg_dist_node node LEFT JOIN _s3_sizes size
       ON size.nodename=node.nodename AND size.nodeport=node.nodeport
  WHERE node.isactive AND node.noderole='primary' AND node.shouldhaveshards
  GROUP BY node.nodeid, node.nodename, node.nodeport
)
SELECT nodename || ':' || nodeport AS node, pg_size_pretty(bytes) AS physical_size,
       round(bytes / nullif(avg(bytes) OVER (), 0), 2) AS relative_to_mean,
       'INFO : physical bytes include replicas; compare with each node capacity and planned placement policy' AS verdict
FROM physical ORDER BY bytes DESC;
SELECT 'INCOMPLETE : one or more shard sizes were unavailable' AS finding
WHERE EXISTS (SELECT 1 FROM _s3_sizes WHERE shard_size IS NULL);
DROP TABLE pg_temp._s3_sizes;