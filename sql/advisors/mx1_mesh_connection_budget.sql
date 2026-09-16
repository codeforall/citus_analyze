\set advisor_id MX1
\ir ../capabilities.sql
-- MX1: fan-in applies to every shard target, including non-MX workers.
\pset pager off
\pset border 2
\pset format aligned
\echo '==================== MX1 : mesh connection scenario ===================='
\ir ../connection_budget.sql
SELECT nodename || ':' || nodeport AS node, is_entry, shouldhaveshards AS shard_target,
       available_slots, requested_fanin, throttled_fanin, external_budget,
       CASE WHEN requested_fanin > available_slots
            THEN 'WARN : scenario fan-in exceeds target connection slots'
            ELSE 'INFO : review residual slots against direct traffic and measured peak fan-in'
       END AS verdict
FROM _connection_budget ORDER BY groupid, nodeport;
DROP TABLE pg_temp._connection_budget;
DROP TABLE pg_temp._connection_edges;
DROP TABLE pg_temp._connection_nodes;
DROP TABLE pg_temp._connections_raw;