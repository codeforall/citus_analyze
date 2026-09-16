WITH requirements(advisor, relations, routines, columns) AS (VALUES
 ('M1', 'pg_dist_partition,pg_dist_shard,pg_dist_placement', 'run_command_on_all_nodes', ''),
 ('C3', '', 'run_command_on_all_nodes', ''),
 ('CP1', '', 'run_command_on_all_nodes', ''),
 ('MX1', '', 'run_command_on_all_nodes', ''),
 ('D1', '', 'run_command_on_all_nodes', ''),
 ('Q1', 'citus_lock_waits', 'run_command_on_all_nodes', 'pg_locks.waitstart'),
 ('GUC1', '', 'run_command_on_all_nodes', ''),
 ('V1', 'pg_available_extension_versions', 'run_command_on_all_nodes', ''),
 ('P1', 'pg_partitioned_table,time_partitions', '', ''),
 ('I1', '', 'run_command_on_all_nodes', 'pg_index.indnkeyatts'),
 ('B1', '', 'run_command_on_all_nodes', ''),
 ('R2', 'pg_dist_rebalance_strategy,citus_shards', 'get_rebalance_table_shards_plan', 'pg_dist_rebalance_strategy.improvement_threshold'),
 ('REF1', 'citus_shards', '', ''),
 ('W1', '', 'run_command_on_all_nodes', ''),
 ('STAT1', 'pg_statistic_ext', 'run_command_on_all_nodes', ''),
 ('SEC1', 'pg_dist_object', 'run_command_on_all_nodes', ''),
 ('NET1', '', 'run_command_on_all_nodes,citus_check_cluster_node_health', ''),
 ('REP1', '', 'run_command_on_all_nodes', ''),
 ('GR1', 'pg_dist_shard,pg_dist_placement', '', ''),
 ('S3', 'citus_shards', '', ''),
 ('R1', 'pg_dist_background_job,pg_dist_background_task', 'get_rebalance_progress', 'pg_dist_background_task.retry_count'),
 ('N6', 'pg_dist_schema,pg_dist_object', '', ''),
 ('A3', 'pg_dist_transaction,pg_dist_local_group', 'run_command_on_all_nodes', ''),
 ('SC1', 'pg_dist_colocation', 'citus_total_relation_size', ''),
 ('P2', 'pg_dist_placement', 'run_command_on_placements', 'pg_dist_placement.shardlength')
), missing AS (
 SELECT item AS capability FROM requirements,
        unnest(string_to_array(relations, ',')) item
 WHERE advisor = :'advisor_id' AND item <> '' AND to_regclass(item) IS NULL
 UNION ALL
 SELECT item FROM requirements, unnest(string_to_array(routines, ',')) item
 WHERE advisor = :'advisor_id' AND item <> '' AND NOT EXISTS (
   SELECT 1 FROM pg_proc JOIN pg_namespace ON pg_namespace.oid=pronamespace
   WHERE proname=item AND nspname IN ('pg_catalog', 'public'))
 UNION ALL
 SELECT item FROM requirements, unnest(string_to_array(columns, ',')) item
 WHERE advisor = :'advisor_id' AND item <> '' AND NOT EXISTS (
   SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(split_part(item, '.', 1))
   AND attname=split_part(item, '.', 2) AND NOT attisdropped)
 UNION ALL
 SELECT 'pg_dist_node.' || required FROM unnest(ARRAY['nodeid','groupid','noderole','isactive','hasmetadata','shouldhaveshards']) required
 WHERE NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('pg_dist_node')
                   AND attname=required AND NOT attisdropped)
)
SELECT NOT EXISTS (SELECT 1 FROM missing) AS advisor_supported,
       coalesce((SELECT string_agg(capability, ', ' ORDER BY capability) FROM missing), '') AS missing_capabilities
\gset
\if :advisor_supported
\else
\echo INCOMPLETE : advisor :advisor_id unsupported by available capabilities: :missing_capabilities
\quit
\endif

SELECT 'INCOMPLETE : no active primary nodes in the topology; collection scope is unknown' AS finding
WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE isactive AND noderole='primary');