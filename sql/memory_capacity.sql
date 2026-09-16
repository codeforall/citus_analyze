\if :{?m1_capacity_active_pct} \else \set m1_capacity_active_pct 100 \endif
\if :{?m1_capacity_headroom_pct} \else \set m1_capacity_headroom_pct 20 \endif
\if :{?m1_growth_cache_pct} \else \set m1_growth_cache_pct -1 \endif
\if :{?m1_growth_shard_mb} \else \set m1_growth_shard_mb -1 \endif
\if :{?m1_growth_shards_per_table} \else \set m1_growth_shards_per_table -1 \endif

DROP TABLE IF EXISTS pg_temp._m1_data;
CREATE TEMP TABLE _m1_data (nodeid int, distributed_mib numeric, local_copies numeric);
DO $sizes$
BEGIN
  IF to_regclass('citus_shards') IS NOT NULL THEN
    BEGIN
      EXECUTE $query$
        INSERT INTO _m1_data
        SELECT node.nodeid, sum(size.shard_size) / 1048576.0, count(*)
        FROM citus_shards size JOIN pg_dist_node node
          ON node.nodename=size.nodename AND node.nodeport=size.nodeport
        JOIN pg_dist_shard shard ON shard.shardid=size.shardid
        JOIN pg_dist_partition partition ON partition.logicalrelid=shard.logicalrelid
        JOIN pg_class relation ON relation.oid=partition.logicalrelid
        WHERE node.isactive AND node.noderole='primary' AND partition.partmethod IN ('h','r','a')
          AND relation.relkind <> 'p'
        GROUP BY node.nodeid HAVING count(size.shard_size)=count(*)
      $query$;
      INSERT INTO _m1_data
      SELECT node.nodeid, 0, 0 FROM pg_dist_node node
      WHERE node.isactive AND node.noderole='primary' AND NOT EXISTS (
        SELECT 1 FROM pg_dist_placement placement JOIN pg_dist_shard shard USING (shardid)
        JOIN pg_dist_partition partition ON partition.logicalrelid=shard.logicalrelid
        JOIN pg_class relation ON relation.oid=partition.logicalrelid
        WHERE placement.groupid=node.groupid AND partition.partmethod IN ('h','r','a') AND relation.relkind <> 'p');
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Distributed data sizes unavailable; table/shard growth will not be estimated.';
    END;
  END IF;
END
$sizes$;

DROP TABLE IF EXISTS pg_temp._m1_capacity_input;
CREATE TEMP TABLE _m1_capacity_input AS
WITH layout AS (
  SELECT count(DISTINCT relation.oid)::numeric AS tables,
         count(shard.shardid)::numeric AS shards,
         (SELECT avg(index_count) FROM (
           SELECT count(indexes.indexrelid)::numeric AS index_count
           FROM pg_dist_partition partition JOIN pg_class relation ON relation.oid=partition.logicalrelid
           LEFT JOIN pg_index indexes ON indexes.indrelid=relation.oid
           WHERE partition.partmethod IN ('h','r','a') AND relation.relkind <> 'p'
           GROUP BY relation.oid) indexes) AS indexes_per_table
  FROM pg_dist_partition partition JOIN pg_class relation ON relation.oid=partition.logicalrelid
  LEFT JOIN pg_dist_shard shard ON shard.logicalrelid=relation.oid
  WHERE partition.partmethod IN ('h','r','a') AND relation.relkind <> 'p'
), source AS (
  SELECT model.*, layout.*, data.distributed_mib, data.local_copies,
         (SELECT count(*) FROM pg_dist_placement placement JOIN pg_dist_shard shard USING (shardid)
          JOIN pg_dist_partition partition ON partition.logicalrelid=shard.logicalrelid
          JOIN pg_class relation ON relation.oid=partition.logicalrelid
          WHERE partition.partmethod IN ('h','r','a') AND relation.relkind <> 'p')::numeric / nullif(shards, 0) AS copies_per_shard,
         CASE WHEN :m1_growth_shard_mb::numeric >= 0 THEN :m1_growth_shard_mb::numeric
              ELSE (SELECT sum(distributed_mib) / nullif(sum(local_copies), 0) FROM _m1_data) END AS new_shard_mib,
         CASE WHEN :m1_growth_shards_per_table::numeric > 0 THEN :m1_growth_shards_per_table::numeric
              ELSE ceil(shards / nullif(tables, 0)) END AS new_table_shards,
         supplied_ram_mib * (1 - :m1_capacity_headroom_pct::numeric / 100) AS planning_ram_mib,
         greatest(0, (settings->>'max_connections')::int - (settings->>'reserved_connections')::int) AS connection_slots,
         :baseline_backend_mb::numeric + metadata_mib AS per_connection_mib,
         CASE WHEN :m1_growth_cache_pct::numeric >= 0 AND data.distributed_mib IS NOT NULL
              THEN greatest(0, data.distributed_mib * :m1_growth_cache_pct::numeric / 100 - (settings->>'shared')::numeric)
              ELSE 0 END AS current_extra_cache_mib,
         (SELECT count(*) FROM pg_dist_node node WHERE node.isactive AND node.noderole='primary'
             AND NOT EXISTS (SELECT 1 FROM _m1_raw raw WHERE raw.nodeid=node.nodeid AND raw.success)) AS failed_nodes,
         (SELECT count(*) FROM pg_dist_node node WHERE node.isactive AND node.noderole='primary'
             AND NOT EXISTS (SELECT 1 FROM _m1_data measured WHERE measured.nodeid=node.nodeid)) AS missing_size_nodes
  FROM _m1_calc model CROSS JOIN layout LEFT JOIN _m1_data data USING (nodeid)
), costs AS (
  SELECT *, least(planning_ram_mib - :os_reserve_mb::numeric,
                  planning_ram_mib / nullif(1 + :os_reserve_pct::numeric / 100, 0)) AS budget_mib,
    shared_mib + maintenance_mib + other_mib + current_extra_cache_mib AS fixed_mib,
    connected * (
      CASE WHEN hasmetadata OR groupid=0
           THEN :k_meta_per_shard::numeric + copies_per_shard * :k_meta_per_placement::numeric
           ELSE local_copies / nullif(shards, 0) * (:k_meta_per_shard::numeric + :k_meta_per_placement::numeric) END
      + local_copies / nullif(shards, 0) * :k_relation_cache_bytes::numeric * (1 + coalesce(indexes_per_table, 0))
    ) / 1048576 AS new_shard_tracking_mib,
    new_shard_mib * local_copies / nullif(shards, 0) * greatest(:m1_growth_cache_pct::numeric, 0) / 100 AS new_shard_cache_mib,
    connected * :k_relation_cache_bytes::numeric * (1 + coalesce(indexes_per_table, 0)) / 1048576 AS new_table_tracking_mib,
    :m1_capacity_active_pct::numeric / 100 AS active_fraction,
    supplied_ram_mib > 0 AND failed_nodes=0 AND connected >= active AND active >= 0
      AND connected <= (settings->>'max_connections')::int
      AND :m1_peak_connected::numeric >= -1 AND :m1_peak_active::numeric >= -1
      AND :m1_temp_sessions::numeric BETWEEN 0 AND connected
      AND :m1_capacity_active_pct::numeric > 0 AND :m1_capacity_active_pct::numeric <= 100
      AND :m1_capacity_headroom_pct::numeric >= 0 AND :m1_capacity_headroom_pct::numeric < 100
      AND (:m1_growth_cache_pct::numeric = -1 OR :m1_growth_cache_pct::numeric BETWEEN 0 AND 100)
      AND :m1_growth_shard_mb::numeric >= -1
      AND (:m1_growth_cache_pct::numeric = -1 OR missing_size_nodes=0)
       AND (:m1_growth_shards_per_table::numeric = -1 OR (:m1_growth_shards_per_table::numeric >= 1
         AND :m1_growth_shards_per_table::numeric = floor(:m1_growth_shards_per_table::numeric)))
      AND least(:baseline_backend_mb::numeric, :k_relation_cache_bytes::numeric,
                :k_meta_per_shard::numeric, :k_meta_per_placement::numeric,
                :os_reserve_mb::numeric, :os_reserve_pct::numeric,
                :m1_sort_ops::numeric, :m1_hash_ops::numeric,
                :m1_other_memory_mb::numeric, :m1_outbound_connections::numeric,
                :outbound_per_conn_mb::numeric, :burst_maint_ops::numeric) >= 0 AS inputs_valid
  FROM source
)
SELECT *, modeled_mib + current_extra_cache_mib AS growth_baseline_mib,
       new_shard_tracking_mib + new_shard_cache_mib AS per_new_shard_mib,
       new_table_shards * (new_shard_tracking_mib + new_shard_cache_mib) + new_table_tracking_mib AS per_new_table_mib
FROM costs;

DROP TABLE IF EXISTS pg_temp._m1_capacity;
CREATE TEMP TABLE _m1_capacity AS
WITH RECURSIVE search(nodeid, low, high) AS (
  SELECT nodeid, 0::bigint, connection_slots::bigint + 1
  FROM _m1_capacity_input WHERE inputs_valid
  UNION ALL
  SELECT search.nodeid,
         CASE WHEN cost.mib <= input.budget_mib THEN midpoint.connections ELSE low END,
         CASE WHEN cost.mib <= input.budget_mib THEN high ELSE midpoint.connections END
  FROM search JOIN _m1_capacity_input input USING (nodeid)
  CROSS JOIN LATERAL (SELECT (low+high)/2 AS connections) midpoint
  CROSS JOIN LATERAL (SELECT ceil(midpoint.connections * input.active_fraction) AS busy) workload
  CROSS JOIN LATERAL (SELECT input.fixed_mib + midpoint.connections * input.per_connection_mib
      + (workload.busy + least(workload.busy * (input.settings->>'per_gather')::numeric,
                              (input.settings->>'parallel')::numeric)) * input.operator_mib_per_process AS mib) cost
  WHERE high-low > 1
), limits AS (
  SELECT nodeid, low AS connection_limit FROM search WHERE high-low=1
)
SELECT input.*, limits.connection_limit,
      CASE WHEN limits.connection_limit IS NOT NULL THEN greatest(0, limits.connection_limit-connected) END AS extra_connections,
       CASE WHEN inputs_valid AND missing_size_nodes=0 AND shards>0 AND tables>0
                  AND :m1_peak_connected::numeric > 0 AND :m1_peak_active::numeric >= 0
                  AND :m1_growth_cache_pct::numeric >= 0 AND per_new_shard_mib>0
            THEN greatest(0, floor((budget_mib-growth_baseline_mib) / per_new_shard_mib)) END AS extra_shards,
       CASE WHEN inputs_valid AND missing_size_nodes=0 AND shards>0 AND tables>0
                  AND :m1_peak_connected::numeric > 0 AND :m1_peak_active::numeric >= 0
                  AND :m1_growth_cache_pct::numeric >= 0 AND per_new_table_mib>0
            THEN greatest(0, floor((budget_mib-growth_baseline_mib) / per_new_table_mib)) END AS extra_tables
FROM _m1_capacity_input input LEFT JOIN limits USING (nodeid);

\echo '-- Memory planning: connections and growth are separate alternatives --'
SELECT nodename || ':' || nodeport AS server,
       round((settings->>'database_mib')::numeric / 1024, 2) AS database_on_disk_gib,
       round(supplied_ram_mib / 1024, 2) AS provided_ram_gib,
       connection_limit AS total_connections_within_budget,
       extra_connections AS more_than_selected_workload,
       extra_shards AS additional_cluster_shards,
       extra_tables AS additional_cluster_tables,
       CASE WHEN NOT inputs_valid THEN 'INFO : estimate unavailable; provide RAM, valid inputs and results from all servers'
            WHEN growth_baseline_mib > budget_mib THEN 'INFO : the selected workload already uses the planning budget; no growth room remains in this estimate'
            WHEN extra_shards IS NULL THEN 'INFO : connection estimate available; growth needs peak connected/active counts, a data-cache percentage and data sizes'
            ELSE 'INFO : memory-only planning estimates; confirm with a load test before increasing limits'
       END AS explanation
FROM _m1_capacity ORDER BY groupid, nodeport;
SELECT format('INFO : connection estimate assumes %s%% busy at once and leaves %s%% of RAM unused, plus the operating-system reserve.',
              :m1_capacity_active_pct::numeric, :m1_capacity_headroom_pct::numeric) AS planning_assumptions;
\echo 'INFO : connections include application and internal database sessions, not concurrent application users. CPU, disk, locks and inter-server connection limits may be reached first. Default: all connections busy, plus 20% of RAM left unused for growth planning in addition to the operating-system reserve. Selected percentages are printed below.'
\echo 'INFO : table/shard growth keeps the selected connection and query workload fixed, assumes new shards match the selected average size/index count and placement pattern, and reserves extra data-cache memory separately. More connections and more tables are alternatives, not allowances that can be added together. This is not an exact point where memory runs out.'

\pset format unaligned
\pset tuples_only on
SELECT 'M1_CAPACITY_JSON=' || jsonb_build_object(
  'schema_version', 1,
  'active_pct', :m1_capacity_active_pct::numeric,
  'headroom_pct', :m1_capacity_headroom_pct::numeric,
  'cache_pct', CASE WHEN :m1_growth_cache_pct::numeric >= 0 THEN :m1_growth_cache_pct::numeric END,
  'os_reserve_mib', :os_reserve_mb::numeric, 'os_reserve_pct', :os_reserve_pct::numeric,
  'growth_workload_provided', :m1_peak_connected::numeric > 0 AND :m1_peak_active::numeric >= 0,
  'all_nodes_measured', coalesce(bool_and(failed_nodes=0), false),
  'cluster_extra_shards', CASE WHEN count(extra_shards)=count(*) THEN min(extra_shards) END,
  'cluster_extra_tables', CASE WHEN count(extra_tables)=count(*) THEN min(extra_tables) END,
  'nodes', coalesce(jsonb_agg(jsonb_build_object(
    'server', nodename || ':' || nodeport, 'role', CASE WHEN groupid=0 THEN 'Coordinator' ELSE 'Worker' END,
    'ram_mib', supplied_ram_mib, 'database_mib', (settings->>'database_mib')::numeric,
    'distributed_mib', distributed_mib, 'planning_budget_mib', round(budget_mib, 2),
    'selected_workload_mib', round(growth_baseline_mib, 2),
    'selected_connected', connected, 'selected_active', active,
    'current_tables', tables, 'current_shards', shards,
    'new_shard_mib', round(new_shard_mib, 2), 'new_table_shards', new_table_shards,
    'connection_limit', connection_limit, 'connection_slots', connection_slots,
    'extra_connections', extra_connections, 'extra_shards', extra_shards, 'extra_tables', extra_tables,
    'inputs_valid', inputs_valid,
    'limiting_factor', CASE WHEN connection_limit IS NULL THEN 'unknown'
                           WHEN connection_limit=connection_slots THEN 'configured connection limit' ELSE 'memory budget' END
  ) ORDER BY groupid, nodeport), '[]'::jsonb)
)::text FROM _m1_capacity;
\pset tuples_only off
\pset format aligned
DROP TABLE pg_temp._m1_capacity;
DROP TABLE pg_temp._m1_capacity_input;
DROP TABLE pg_temp._m1_data;