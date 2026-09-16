\set advisor_id C3
\ir ../capabilities.sql
-- C3: direct and distributed connections under an explicit routing scenario.
\pset pager off
\pset border 2
\pset format aligned
\echo '==================== C3 : connection budget scenario ===================='
\ir ../connection_budget.sql
SELECT nodename || ':' || nodeport AS node, is_entry, shouldhaveshards AS shard_target,
       available_slots, observed_clients, observed_internal, scenario_sessions,
       shared_pool_per_peer, requested_fanin, throttled_fanin, external_budget,
     config->>'max_client_connections' AS citus_client_setting, client_limit, client_limit_status,
       CASE WHEN requested_fanin + CASE WHEN is_entry THEN scenario_sessions ELSE 0 END > available_slots
            THEN 'WARN : scenario demand exceeds backend slots; review routing and concurrency'
            WHEN (is_entry AND scenario_sessions > external_budget) OR observed_clients > external_budget
            THEN 'WARN : proposed or observed application connections exceed the external connection budget; review the Citus client limit and internal demand'
            WHEN client_limit_status='unknown'
            THEN 'INCOMPLETE : application connection capacity is unknown because the Citus client limit could not be resolved'
            ELSE 'INFO : scenario fits connection slots; workload and pool behavior not certified'
       END AS verdict
FROM _connection_budget ORDER BY groupid, nodeport;
SELECT 'WARN : requested per-peer connections exceed a source pool limit; queueing is possible, not a hard query concurrency ceiling' AS finding
WHERE EXISTS (SELECT 1 FROM _connection_edges WHERE requested > shared_pool_per_peer);
DROP TABLE pg_temp._connection_budget;
DROP TABLE pg_temp._connection_edges;
DROP TABLE pg_temp._connection_nodes;
DROP TABLE pg_temp._connections_raw;