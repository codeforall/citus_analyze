\set advisor_id CP1
\ir ../capabilities.sql
-- CP1: pool groups are actual (database,user) pairs, not pg_database rows.
\pset pager off
\pset border 2
\pset format aligned
\if :{?pool_groups} \else \set pool_groups 0 \endif
\if :{?pooler_instances} \else \set pooler_instances 1 \endif
\if :{?cp1_peak_backend_demand} \else \set cp1_peak_backend_demand -1 \endif
\if :{?surge_factor} \else \set surge_factor 1.5 \endif
\if :{?reserve_pct} \else \set reserve_pct 20 \endif
\echo '==================== CP1 : PgBouncer pool budget scenario ===================='
\ir ../connection_budget.sql
WITH budgets AS (
  SELECT *, floor(external_budget / nullif(:pool_groups::numeric * :pooler_instances::numeric, 0)) AS group_budget,
         ceil((CASE WHEN :cp1_peak_backend_demand::int < 0 THEN observed_clients
                    ELSE :cp1_peak_backend_demand::int END) * :surge_factor::numeric
                    / nullif(:pool_groups::numeric * :pooler_instances::numeric, 0)) AS demand
  FROM _connection_budget WHERE is_entry
), pools AS (
  SELECT *, CASE WHEN group_budget IS NOT NULL
                 THEN greatest(0, least(demand, floor(group_budget / (1 + :reserve_pct::numeric / 100)))) END AS pool_size
  FROM budgets
)
SELECT nodename || ':' || nodeport AS entry_node, external_budget, client_limit, client_limit_status,
       :pool_groups::int AS database_user_pairs, :pooler_instances::int AS pooler_instances,
       CASE WHEN group_budget IS NOT NULL THEN pool_size END AS pool_size,
       CASE WHEN group_budget IS NOT NULL THEN floor(pool_size * :reserve_pct::numeric / 100) END AS reserve_pool_size,
       CASE WHEN group_budget IS NOT NULL THEN (pool_size + floor(pool_size * :reserve_pct::numeric / 100))
            * :pool_groups::int * :pooler_instances::int END AS all_pools_including_reserve,
        CASE WHEN scenario_sessions > external_budget OR observed_clients > external_budget
          THEN 'WARN : proposed or observed application connections exceed the external connection budget; review the Citus client limit and internal demand'
            WHEN client_limit_status='unknown'
            THEN 'INCOMPLETE : pool sizes cannot be calculated without a known Citus client limit'
          WHEN :pool_groups::int <= 0 THEN 'INFO : supply actual pool_groups before calculating pool sizes'
            WHEN :pooler_instances::int <= 0 OR :reserve_pct::numeric < 0 OR :surge_factor::numeric < 0
            THEN 'INCOMPLETE : invalid pool scenario inputs'
            WHEN demand > pool_size THEN 'WARN : desired pool exceeds allocated backend budget; queueing or workload changes required'
            ELSE 'INFO : pool and reserve fit this scenario; verify load, routing and all other poolers'
       END AS verdict
FROM pools ORDER BY groupid, nodeport;
\echo 'INFO : choose session or transaction pooling based on application semantics. Protocol prepared statements in transaction mode require suitable PgBouncer version (1.21+) and max_prepared_statements configuration, not a PostgreSQL-major threshold. SQL PREPARE and session state need separate compatibility review. No deployable config is generated from an unmeasured workload.'
DROP TABLE pg_temp._connection_budget;
DROP TABLE pg_temp._connection_edges;
DROP TABLE pg_temp._connection_nodes;
DROP TABLE pg_temp._connections_raw;