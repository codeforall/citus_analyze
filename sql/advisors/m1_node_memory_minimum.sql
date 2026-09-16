\set advisor_id M1
\ir ../capabilities.sql
\ir ../client_limit.sql
-- M1: per-node memory scenarios, not an RSS measurement or an OOM prediction.
-- MiB inputs; -1 means use the observed concurrency (not a peak estimate).
\pset pager off
\pset border 2
\pset format aligned
\if :{?coord_ram_mb} \else \set coord_ram_mb 0 \endif
\if :{?worker_ram_mb} \else \set worker_ram_mb 0 \endif
\if :{?m1_peak_connected} \else \set m1_peak_connected -1 \endif
\if :{?m1_peak_active} \else \set m1_peak_active -1 \endif
\if :{?m1_sort_ops} \else \set m1_sort_ops 1 \endif
\if :{?m1_hash_ops} \else \set m1_hash_ops 1 \endif
\if :{?m1_temp_sessions} \else \set m1_temp_sessions 0 \endif
\if :{?m1_outbound_connections} \else \set m1_outbound_connections 0 \endif
\if :{?m1_other_memory_mb} \else \set m1_other_memory_mb 0 \endif
\if :{?baseline_backend_mb} \else \set baseline_backend_mb 10 \endif
\if :{?outbound_per_conn_mb} \else \set outbound_per_conn_mb 0.5 \endif
\if :{?os_reserve_mb} \else \set os_reserve_mb 1024 \endif
\if :{?os_reserve_pct} \else \set os_reserve_pct 10 \endif
\if :{?burst_maint_ops} \else \set burst_maint_ops 0 \endif
\if :{?k_meta_per_shard} \else \set k_meta_per_shard 160 \endif
\if :{?k_meta_per_placement} \else \set k_meta_per_placement 40 \endif
\if :{?k_relation_cache_bytes} \else \set k_relation_cache_bytes 8192 \endif

\echo '==================== M1 : memory and room to grow ===================='
DROP TABLE IF EXISTS pg_temp._m1_raw;
CREATE TEMP TABLE _m1_raw AS
SELECT * FROM run_command_on_all_nodes($CMD$
  SELECT jsonb_build_object(
    'shared', pg_size_bytes(current_setting('shared_buffers')) / 1048576.0,
    'work', pg_size_bytes(current_setting('work_mem')) / 1048576.0,
    'maintenance', pg_size_bytes(current_setting('maintenance_work_mem')) / 1048576.0,
    'autovacuum', pg_size_bytes(CASE WHEN current_setting('autovacuum_work_mem') = '-1'
                    THEN current_setting('maintenance_work_mem') ELSE current_setting('autovacuum_work_mem') END) / 1048576.0,
    'wal', pg_size_bytes(current_setting('wal_buffers')) / 1048576.0,
    'temp', pg_size_bytes(current_setting('temp_buffers')) / 1048576.0,
    'hash', current_setting('hash_mem_multiplier')::numeric,
    'av_workers', current_setting('autovacuum_max_workers')::int,
    'parallel', least(current_setting('max_parallel_workers')::int, current_setting('max_worker_processes')::int),
    'per_gather', current_setting('max_parallel_workers_per_gather')::int,
    'max_connections', current_setting('max_connections')::int,
    'max_client_connections', current_setting('citus.max_client_connections', true),
    'client_limit_default', (SELECT boot_val FROM pg_settings WHERE name='citus.max_client_connections'),
    'citus_version', current_setting('citus.version', true),
    'reserved_connections', current_setting('superuser_reserved_connections')::int
      + coalesce(current_setting('reserved_connections', true), '0')::int,
    'database_mib', pg_database_size(current_database()) / 1048576.0,
    'user_relations', (SELECT count(*) FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
      WHERE namespace.nspname NOT LIKE 'pg_%' AND namespace.nspname <> 'information_schema'
        AND relation.relkind IN ('r', 'p', 'i', 'I', 'm')),
    'connected', (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()),
    'active', (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND state = 'active' AND pid <> pg_backend_pid())
  )::text
$CMD$, parallel := true);

DROP TABLE IF EXISTS pg_temp._m1_calc;
CREATE TEMP TABLE _m1_calc AS
WITH nodes AS (
  SELECT node.*, raw.result::jsonb AS settings,
    CASE WHEN node.groupid = 0 THEN :coord_ram_mb::numeric ELSE :worker_ram_mb::numeric END AS supplied_ram_mib,
    CASE WHEN node.hasmetadata OR node.groupid = 0
         THEN ((SELECT count(*) FROM pg_dist_shard) * :k_meta_per_shard::numeric
              + (SELECT count(*) FROM pg_dist_placement) * :k_meta_per_placement::numeric) / 1048576.0
         ELSE ((SELECT count(*) FROM pg_dist_placement WHERE groupid = node.groupid)
                * (:k_meta_per_shard::numeric + :k_meta_per_placement::numeric)) / 1048576.0 END
      + (raw.result::jsonb->>'user_relations')::numeric * :k_relation_cache_bytes::numeric / 1048576.0 AS metadata_mib
  FROM _m1_raw raw JOIN pg_dist_node node USING (nodeid) WHERE raw.success
), scenario AS (
  SELECT *,
    CASE WHEN :m1_peak_connected::int < 0 THEN (settings->>'connected')::int ELSE :m1_peak_connected::int END AS connected,
    CASE WHEN :m1_peak_active::int < 0 THEN (settings->>'active')::int ELSE :m1_peak_active::int END AS active
  FROM nodes
), components AS (
  SELECT *, least(active * (settings->>'per_gather')::numeric, (settings->>'parallel')::numeric) AS parallel_workers,
    (settings->>'shared')::numeric + (settings->>'wal')::numeric AS shared_mib,
    connected * (:baseline_backend_mb::numeric + metadata_mib) AS backend_mib,
    (settings->>'work')::numeric * (:m1_sort_ops::numeric + :m1_hash_ops::numeric * (settings->>'hash')::numeric) AS operator_mib_per_process,
    (settings->>'av_workers')::numeric * (settings->>'autovacuum')::numeric
      + :burst_maint_ops::numeric * (settings->>'maintenance')::numeric AS maintenance_mib,
    :m1_temp_sessions::numeric * (settings->>'temp')::numeric
      + :m1_outbound_connections::numeric * :outbound_per_conn_mb::numeric
      + :m1_other_memory_mb::numeric AS other_mib
  FROM scenario
), totals AS (
  SELECT *, shared_mib + backend_mib + (active + parallel_workers) * operator_mib_per_process
                   + maintenance_mib + other_mib AS modeled_mib
  FROM components
)
SELECT *, modeled_mib + greatest(:os_reserve_mb::numeric, modeled_mib * :os_reserve_pct::numeric / 100) AS scenario_mib
FROM totals;

SELECT nodename || ':' || nodeport AS node, settings->>'max_connections' AS configured_connections,
       settings->>'connected' AS observed_connected, settings->>'active' AS observed_active,
       connected AS scenario_connected, active AS scenario_active, parallel_workers,
       round(shared_mib, 2) AS shared_mib, round(backend_mib, 2) AS backend_mib,
       round((active + parallel_workers) * operator_mib_per_process, 2) AS operators_mib,
       round(maintenance_mib, 2) AS maintenance_mib, round(other_mib, 2) AS other_mib,
       round(scenario_mib, 2) AS scenario_mib, round(scenario_mib / 1024, 2) AS scenario_gib,
       supplied_ram_mib,
       CASE WHEN connected < active OR active < 0 OR connected > (settings->>'max_connections')::int
                   OR :m1_temp_sessions::int > connected OR :m1_temp_sessions::int < 0
                   OR least(:m1_sort_ops::numeric, :m1_hash_ops::numeric, :baseline_backend_mb::numeric,
                            :m1_other_memory_mb::numeric, :burst_maint_ops::numeric, :os_reserve_pct::numeric,
                            :m1_outbound_connections::numeric, :outbound_per_conn_mb::numeric, :os_reserve_mb::numeric) < 0
            THEN 'INCOMPLETE : memory inputs are invalid; check the selected connection counts and allowances'
            WHEN :m1_peak_active::int < 0 OR :m1_peak_connected::int < 0
            THEN 'INFO : uses current connections only; provide peak workload counts for growth planning'
            WHEN supplied_ram_mib <= 0 THEN 'INFO : RAM was not provided; memory use cannot be compared with server capacity'
            WHEN scenario_mib > supplied_ram_mib THEN 'WARN : estimated workload memory is above provided RAM; check peak demand before increasing load'
            ELSE 'INFO : the selected workload fits the estimated memory budget; confirm under peak load'
       END AS verdict
FROM _m1_calc ORDER BY groupid, nodeport;
\echo 'INFO : memory use is calculated, not measured. Each connection has assumed overhead. Temporary tables, outgoing connections, replication and other programs need their own allowances. Query-worker memory is capped by the server total; hash and sort operations are counted separately.'

\ir ../memory_capacity.sql
DROP TABLE pg_temp._m1_calc;
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._m1_raw;