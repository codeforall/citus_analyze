-- =====================================================================
-- citus_analyze / citus_gather.sql
-- ---------------------------------------------------------------------
-- pg_gather-style one-shot collector. Dumps all inputs required by
-- the citus_analyze advisors (GR1, C3/C4/C5/C6, N6, S3, R1, A3 ...)
-- plus a baseline PG snapshot as a series of CSV-with-header sections.
--
-- Output format
--   Each section is bracketed by two marker lines:
--     ### BEGIN: <section_id>
--     <CSV with header>
--     ### END : <section_id>
--   So a downstream tool can extract a section with:
--     awk '/^### BEGIN: cluster_topology$/,/^### END : cluster_topology$/'
--
-- Usage
--   psql citus -X -A -q -f citus_gather.sql > citus_gather.out 2>&1
--   tar czf citus_gather_$(hostname)_$(date +%s).tgz citus_gather.out
--
-- Runs only on the coordinator. Cross-node data is collected via
-- run_command_on_workers() fan-out.
-- =====================================================================

\pset pager off
\pset footer off
\set ON_ERROR_STOP off

-- Detect optional extensions/views once so sections can \if them
SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements') AS have_pgss \gset
SELECT EXISTS (SELECT 1 FROM pg_views WHERE viewname='citus_stat_statements' AND schemaname='public') AS have_css \gset

-- ---------------------------------------------------------------------
\echo ### BEGIN: meta
\copy (SELECT now() AS collected_at, current_database() AS db, inet_server_addr()::text AS host, inet_server_port() AS port, current_user AS collected_by, version() AS pg_version, citus_version() AS citus_version) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : meta

-- ---------------------------------------------------------------------
\echo ### BEGIN: cluster_topology
\copy (SELECT nodeid, groupid, nodename, nodeport, noderole, nodecluster, isactive, hasmetadata, metadatasynced, shouldhaveshards FROM pg_dist_node ORDER BY groupid, nodeid) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : cluster_topology

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_partition
\copy (SELECT logicalrelid::text AS table_name, partmethod, colocationid, repmodel, autoconverted FROM pg_dist_partition ORDER BY logicalrelid::text) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_partition

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_colocation
\copy (SELECT colocationid, shardcount, replicationfactor, distributioncolumntype::regtype::text AS dist_type, distributioncolumncollation FROM pg_dist_colocation ORDER BY colocationid) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_colocation

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_shard_summary
\copy (SELECT logicalrelid::text AS table_name, count(*) AS shards FROM pg_dist_shard GROUP BY 1 ORDER BY 1) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_shard_summary

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_placement_summary
\copy (SELECT n.nodename, n.nodeport, n.noderole, count(p.placementid) AS placements FROM pg_dist_node n LEFT JOIN pg_dist_placement p ON p.groupid=n.groupid WHERE n.isactive AND n.noderole='primary' GROUP BY 1,2,3 ORDER BY 4 DESC) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_placement_summary

-- ---------------------------------------------------------------------
\echo ### BEGIN: citus_shards_sizes
\copy (SELECT table_name::text, shardid, shard_name, citus_table_type, colocation_id, nodename, nodeport, shard_size FROM citus_shards ORDER BY colocation_id, shardid, nodename) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : citus_shards_sizes

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_object
\copy (SELECT classid::regclass::text AS classname, objid, objsubid, type, object_names, object_args, distribution_argument_index, colocationid, force_delegation FROM pg_dist_object ORDER BY classid, objid) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_object

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_schema
\copy (SELECT schemaid::regnamespace::text AS schema_name, colocationid FROM pg_dist_schema ORDER BY 1) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_schema

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_transaction
\copy (SELECT groupid, gid, outer_xid::text FROM pg_dist_transaction ORDER BY groupid, gid) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_transaction

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_background_job
\copy (SELECT job_id, state::text, job_type::text, description, started_at, finished_at FROM pg_dist_background_job ORDER BY job_id) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_background_job

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_background_task
\copy (SELECT job_id, task_id, owner::text, pid, status::text, retry_count, not_before, left(command, 400) AS command, left(message, 400) AS message, nodes_involved FROM pg_dist_background_task ORDER BY job_id, task_id) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_background_task

-- ---------------------------------------------------------------------
\echo ### BEGIN: get_rebalance_progress
\copy (SELECT * FROM get_rebalance_progress()) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : get_rebalance_progress

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_dist_cleanup
\copy (SELECT record_id, operation_id, object_type, object_name, node_group_id, policy_type FROM pg_dist_cleanup ORDER BY record_id) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_dist_cleanup

-- ---------------------------------------------------------------------
\echo ### BEGIN: citus_gucs_coord
\copy (SELECT name, setting, unit, category, short_desc, context, vartype, source, min_val, max_val, reset_val FROM pg_settings WHERE name LIKE 'citus.%' ORDER BY name) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : citus_gucs_coord

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_gucs_coord
\copy (SELECT name, setting, unit, context, source FROM pg_settings WHERE name IN ('max_connections','max_prepared_transactions','max_locks_per_transaction','max_worker_processes','shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','wal_level','max_wal_size','min_wal_size','max_wal_senders','max_replication_slots','hot_standby_feedback','statement_timeout','lock_timeout','idle_in_transaction_session_timeout','autovacuum','autovacuum_max_workers','autovacuum_naptime','checkpoint_timeout','max_parallel_workers','max_parallel_workers_per_gather','jit','random_page_cost','default_statistics_target') ORDER BY name) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_gucs_coord

-- ---------------------------------------------------------------------
\echo ### BEGIN: per_node_gucs
\copy (SELECT nodename, nodeport, success, result FROM run_command_on_workers($$ SELECT string_agg(name || '=' || setting, ';') FROM pg_settings WHERE name IN ('max_connections','max_prepared_transactions','max_locks_per_transaction','max_worker_processes','shared_buffers','work_mem','maintenance_work_mem','citus.max_shared_pool_size','citus.max_adaptive_executor_pool_size','citus.local_shared_pool_size','citus.max_client_connections','citus.metadata_sync_mode','citus.node_connection_timeout','citus.max_background_task_executors_per_node','citus.defer_shard_delete_interval','citus.enable_cluster_clock') $$)) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : per_node_gucs

-- ---------------------------------------------------------------------
\echo ### BEGIN: per_node_prepared_xacts
\copy (SELECT nodename, nodeport, success, result FROM run_command_on_workers($$ SELECT string_agg(gid || '|' || prepared::text || '|' || owner || '|' || database, E'\n') FROM pg_prepared_xacts $$)) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : per_node_prepared_xacts

-- ---------------------------------------------------------------------
\echo ### BEGIN: coord_prepared_xacts
\copy (SELECT gid, prepared, owner::text, database::text FROM pg_prepared_xacts) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : coord_prepared_xacts

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_stat_activity_coord
\copy (SELECT datname, usename, application_name, client_addr::text, state, wait_event_type, wait_event, backend_type, query_start, state_change, backend_xmin::text, left(query, 400) AS query FROM pg_stat_activity WHERE pid <> pg_backend_pid()) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_stat_activity_coord

-- ---------------------------------------------------------------------
\echo ### BEGIN: citus_dist_stat_activity
\copy (SELECT global_pid, nodeid, pid, datname, usename, application_name, client_addr::text, state, wait_event_type, wait_event, backend_type, query_start, left(query, 400) AS query FROM citus_dist_stat_activity) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : citus_dist_stat_activity

-- ---------------------------------------------------------------------
\echo ### BEGIN: partition_inventory
\copy (SELECT i.inhparent::regclass::text AS parent, i.inhrelid::regclass::text AS child, EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = i.inhrelid) AS child_is_distributed, EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = i.inhparent) AS parent_is_distributed FROM pg_inherits i ORDER BY 1, 2) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : partition_inventory

-- ---------------------------------------------------------------------
\echo ### BEGIN: fkeys_on_distributed
\copy (SELECT c.conrelid::regclass::text AS parent_rel, c.conname, c.confrelid::regclass::text AS referenced_rel, pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c JOIN pg_dist_partition d ON d.logicalrelid = c.conrelid WHERE c.contype = 'f' ORDER BY 1, 2) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : fkeys_on_distributed

-- ---------------------------------------------------------------------
\echo ### BEGIN: extensions
\copy (SELECT extname, extversion, nspname AS schema FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace ORDER BY extname) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : extensions

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_roles
\copy (SELECT rolname, rolsuper, rolcanlogin, rolreplication, rolconnlimit, rolvaliduntil FROM pg_roles ORDER BY rolname) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_roles

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_stat_database
\copy (SELECT datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, deadlocks, stats_reset FROM pg_stat_database WHERE datname IS NOT NULL) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_stat_database

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_stat_bgwriter
\copy (SELECT * FROM pg_stat_bgwriter) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_stat_bgwriter

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_replication_slots
\copy (SELECT slot_name, plugin, slot_type, database, temporary, active, active_pid, xmin::text, catalog_xmin::text, restart_lsn::text, confirmed_flush_lsn::text FROM pg_replication_slots) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : pg_replication_slots

-- ---------------------------------------------------------------------
\echo ### BEGIN: pg_stat_statements_top
\if :have_pgss
-- Top 30 by total_exec_time
\copy (SELECT queryid::text, calls, total_exec_time::numeric(20,2) AS total_ms, mean_exec_time::numeric(20,2) AS mean_ms, rows, shared_blks_hit, shared_blks_read, left(query, 400) AS query FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 30) TO STDOUT WITH (FORMAT csv, HEADER)
\else
\echo -- skipped: pg_stat_statements extension not installed
\endif
\echo ### END : pg_stat_statements_top

-- ---------------------------------------------------------------------
\echo ### BEGIN: citus_stat_statements_top
\if :have_css
\copy (SELECT queryid::text, left(query,400) AS query, executor, partition_key, calls FROM public.citus_stat_statements ORDER BY calls DESC LIMIT 30) TO STDOUT WITH (FORMAT csv, HEADER)
\else
\echo -- skipped: citus_stat_statements view not present (citus_stat_statements extension not installed)
\endif
\echo ### END : citus_stat_statements_top

-- ---------------------------------------------------------------------
\echo ### BEGIN: end_of_gather
\copy (SELECT now() AS finished_at) TO STDOUT WITH (FORMAT csv, HEADER)
\echo ### END : end_of_gather
