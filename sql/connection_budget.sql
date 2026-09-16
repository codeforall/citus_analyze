\if :{?connection_sessions_per_entry} \else \set connection_sessions_per_entry -1 \endif
\if :{?connections_per_session} \else \set connections_per_session 1 \endif
\if :{?cached_connections_per_peer} \else \set cached_connections_per_peer 0 \endif
\if :{?overhead_per_node} \else \set overhead_per_node 0 \endif
\if :{?headroom_pct} \else \set headroom_pct 80 \endif

DROP TABLE IF EXISTS pg_temp._connections_raw;
CREATE TEMP TABLE _connections_raw AS
SELECT * FROM run_command_on_all_nodes($CMD$
  SELECT jsonb_build_object(
    'max_connections', current_setting('max_connections')::int,
    'superuser_reserved', current_setting('superuser_reserved_connections')::int,
    'reserved', coalesce(current_setting('reserved_connections', true), '0')::int,
    'shared_pool', current_setting('citus.max_shared_pool_size', true)::int,
    'adaptive_pool', current_setting('citus.max_adaptive_executor_pool_size', true)::int,
    'max_client_connections', current_setting('citus.max_client_connections', true),
    'observed_clients', (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend'
        AND pid <> pg_backend_pid() AND application_name !~* '^citus'),
    'observed_internal', (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend'
        AND pid <> pg_backend_pid() AND application_name ~* '^citus')
  )::text
$CMD$, parallel := true);

DROP TABLE IF EXISTS pg_temp._connection_nodes;
CREATE TEMP TABLE _connection_nodes AS
WITH settings AS (
  SELECT node.*, raw.result::jsonb AS config
  FROM _connections_raw raw JOIN pg_dist_node node USING (nodeid) WHERE raw.success
)
SELECT *, groupid = 0 OR hasmetadata AS is_entry,
       (config->>'observed_clients')::int AS observed_clients,
       (config->>'observed_internal')::int AS observed_internal,
       greatest(0, (config->>'max_connections')::int - (config->>'superuser_reserved')::int
          - (config->>'reserved')::int - :overhead_per_node::int) AS available_slots,
       CASE WHEN (config->>'shared_pool')::int = 0 THEN (config->>'max_connections')::int
            WHEN (config->>'shared_pool')::int > 0 THEN (config->>'shared_pool')::int
            ELSE NULL END AS shared_pool_per_peer,
       CASE WHEN :connection_sessions_per_entry::int < 0 THEN (config->>'observed_clients')::int
            ELSE :connection_sessions_per_entry::int END AS scenario_sessions
FROM settings;

DROP TABLE IF EXISTS pg_temp._connection_edges;
CREATE TEMP TABLE _connection_edges AS
SELECT source.nodeid AS source_id, target.nodeid AS target_id,
       source.scenario_sessions * :connections_per_session::numeric + :cached_connections_per_peer::numeric AS requested,
       source.shared_pool_per_peer,
       least(source.scenario_sessions * :connections_per_session::numeric + :cached_connections_per_peer::numeric,
             source.shared_pool_per_peer) AS throttled
FROM _connection_nodes source CROSS JOIN _connection_nodes target
WHERE source.is_entry AND target.shouldhaveshards AND source.nodeid <> target.nodeid;

DROP TABLE IF EXISTS pg_temp._connection_budget;
CREATE TEMP TABLE _connection_budget AS
SELECT node.*,
       coalesce((SELECT sum(requested) FROM _connection_edges WHERE target_id=node.nodeid), 0) AS requested_fanin,
       coalesce((SELECT sum(throttled) FROM _connection_edges WHERE target_id=node.nodeid), 0) AS throttled_fanin,
         greatest(0, least(floor(available_slots * :headroom_pct::numeric / 100)
           - coalesce((SELECT sum(requested) FROM _connection_edges WHERE target_id=node.nodeid), 0),
           CASE WHEN (config->>'max_client_connections')::int > 0
            THEN (config->>'max_client_connections')::int END)) AS external_budget
FROM _connection_nodes node;

\ir advisor_coverage.sql

SELECT 'INCOMPLETE : invalid connection scenario inputs' AS finding
WHERE :connections_per_session::numeric < 0 OR :cached_connections_per_peer::numeric < 0
   OR :headroom_pct::numeric <= 0 OR :headroom_pct::numeric > 100 OR :overhead_per_node::int < 0
   OR :connection_sessions_per_entry::int < -1;
\echo 'INFO : scenario only, not a certified concurrency ceiling. Each source can open multiple connections per target. Cached pools, uneven routing, repartition queries, local execution and application-specific limits require measurement. Zero shared_pool means automatic max_connections; -1 disables throttling. Client classification uses application_name and may be incomplete.'