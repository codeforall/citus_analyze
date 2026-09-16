-- N6: metadata planning scenarios. No snapshot can certify add-node success.
\set advisor_id N6
\ir ../capabilities.sql
\pset pager off
\pset border 2
\pset format aligned
\if :{?candidate_max_lpt} \else \set candidate_max_lpt 64 \endif
\if :{?candidate_max_conn} \else \set candidate_max_conn 100 \endif
\if :{?candidate_max_prep} \else \set candidate_max_prep 0 \endif
\if :{?candidate_av_workers} \else \set candidate_av_workers 3 \endif
\if :{?candidate_wal_senders} \else \set candidate_wal_senders 10 \endif
\if :{?candidate_worker_procs} \else \set candidate_worker_procs 8 \endif
\if :{?candidate_ram_mb} \else \set candidate_ram_mb 4096 \endif
\if :{?lock_safety_factor} \else \set lock_safety_factor 1.3 \endif
\if :{?link_mbps} \else \set link_mbps 50 \endif
\if :{?k_role} \else \set k_role 500 \endif
\if :{?k_dep_non_rel} \else \set k_dep_non_rel 1024 \endif
\if :{?k_shell_table} \else \set k_shell_table 2048 \endif
\if :{?k_partition} \else \set k_partition 400 \endif
\if :{?k_shard_row} \else \set k_shard_row 200 \endif
\if :{?k_placement_row} \else \set k_placement_row 120 \endif
\if :{?k_obj_row} \else \set k_obj_row 300 \endif
\if :{?k_fkey} \else \set k_fkey 500 \endif
\if :{?k_schema_row} \else \set k_schema_row 400 \endif
\echo '==================== N6 : metadata-sync planning scenario ===================='
WITH facts AS (
 SELECT (SELECT count(*) FROM pg_roles) AS roles,
        (SELECT count(*) FROM pg_dist_partition) AS relations,
        (SELECT count(*) FROM pg_dist_shard) AS shards,
        (SELECT count(*) FROM pg_dist_placement) AS placements,
        (SELECT count(*) FROM pg_dist_object) AS objects,
        (SELECT count(*) FROM pg_dist_object WHERE classid NOT IN ('pg_class'::regclass, 'pg_authid'::regclass)) AS dependencies,
        (SELECT count(*) FROM pg_inherits JOIN pg_dist_partition ON logicalrelid=inhrelid) AS attachments,
        (SELECT count(*) FROM pg_index JOIN pg_dist_partition ON logicalrelid=indrelid) AS indexes,
        (SELECT count(*) FROM pg_constraint JOIN pg_dist_partition ON logicalrelid=conrelid WHERE contype='f') AS foreign_keys,
        (SELECT count(*) FROM pg_dist_schema) AS schemas
), scenario AS (
 SELECT *, roles * :k_role::numeric + relations * :k_shell_table::numeric
      + shards * :k_shard_row::numeric + placements * :k_placement_row::numeric
      + objects * :k_obj_row::numeric + dependencies * :k_dep_non_rel::numeric
      + attachments * :k_partition::numeric + foreign_keys * :k_fkey::numeric
      + schemas * :k_schema_row::numeric AS payload_bytes,
      (relations + indexes + 10) * :lock_safety_factor::numeric AS lock_demand,
      :candidate_max_lpt::numeric * (:candidate_max_conn::numeric + :candidate_max_prep::numeric
        + :candidate_av_workers::numeric + :candidate_wal_senders::numeric + :candidate_worker_procs::numeric) AS estimated_slots
 FROM facts
)
SELECT *, round(payload_bytes / 1048576, 2) AS estimated_payload_mib,
       round(payload_bytes * 1.5 / 1048576, 2) AS assumed_backend_mib,
       :candidate_ram_mb::numeric AS supplied_candidate_ram_mib,
       round(payload_bytes / 1048576 / nullif(:link_mbps::numeric, 0), 2) AS transport_only_seconds,
       CASE WHEN :link_mbps::numeric <= 0 OR estimated_slots <= 0 OR :lock_safety_factor::numeric < 1
            THEN 'INCOMPLETE : invalid candidate or transport scenario'
            WHEN lock_demand > estimated_slots THEN 'WARN : heuristic lock demand exceeds estimated candidate slots; validate real object/lock workload and other sessions'
            ELSE 'INFO : estimated metadata scenario only; add-node feasibility is not established'
       END AS verdict
FROM scenario;
SELECT current_setting('citus.metadata_sync_mode', true) AS configured_sync_mode,
       current_setting('citus.node_connection_timeout', true) AS connection_establishment_timeout_ms,
       'INFO : connection timeout controls establishment, not metadata execution duration. Payload/transport coefficients omit CPU, round trips and DDL execution. Keep supported transactional defaults; consider nontransactional recovery only after diagnosing actual resource failures and accepting partial-state risks.' AS guidance;