\set advisor_id MX1
\ir ../capabilities.sql
-- MX1: fan-in applies to every shard target, including non-MX workers.
\pset pager off
\pset border 2
\pset format aligned
\echo '==================== MX1 : mesh connection scenario ===================='
\ir ../connection_budget.sql
SELECT nodename || ':' || nodeport AS node, is_entry, shouldhaveshards AS shard_target,
     available_slots, requested_fanin, throttled_fanin, scenario_sessions, external_budget,
     client_limit, client_limit_status,
       CASE WHEN requested_fanin + CASE WHEN is_entry THEN scenario_sessions ELSE 0 END > available_slots
            THEN 'WARN : incoming and direct connections exceed target connection slots'
            WHEN (is_entry AND scenario_sessions > external_budget) OR observed_clients > external_budget
            THEN 'WARN : proposed or observed application connections exceed the external connection budget; review the Citus client limit and internal demand'
            WHEN client_limit_status='unknown'
            THEN 'INCOMPLETE : application connection capacity is unknown because the Citus client limit could not be resolved'
            ELSE 'INFO : review residual slots against direct traffic and measured peak fan-in'
       END AS verdict
FROM _connection_budget ORDER BY groupid, nodeport;
DROP TABLE pg_temp._connection_budget;
DROP TABLE pg_temp._connection_edges;
DROP TABLE pg_temp._connection_nodes;
DROP TABLE pg_temp._connections_raw;