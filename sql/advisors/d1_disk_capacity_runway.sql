-- D1: only supplied filesystem measurements support free-space/runway claims.
\set advisor_id D1
\ir ../capabilities.sql
\pset pager off
\pset border 2
\pset format aligned
\if :{?coord_disk_mb} \else \set coord_disk_mb 0 \endif
\if :{?worker_disk_mb} \else \set worker_disk_mb 0 \endif
\if :{?coord_disk_free_mb} \else \set coord_disk_free_mb -1 \endif
\if :{?worker_disk_free_mb} \else \set worker_disk_free_mb -1 \endif
\if :{?disk_growth_mb_per_day} \else \set disk_growth_mb_per_day 0 \endif
\if :{?warn_free_pct} \else \set warn_free_pct 20 \endif
\if :{?crit_free_pct} \else \set crit_free_pct 10 \endif
\if :{?warn_runway_days} \else \set warn_runway_days 90 \endif
\if :{?crit_runway_days} \else \set crit_runway_days 14 \endif
\echo '==================== D1 : database size and supplied disk measurements ===================='
DROP TABLE IF EXISTS pg_temp._d1_raw;
CREATE TEMP TABLE _d1_raw AS
SELECT * FROM run_command_on_all_nodes('SELECT pg_database_size(current_database())::text', parallel := true);
WITH measured AS (
 SELECT node.*, raw.result::bigint AS database_bytes,
        CASE WHEN groupid=0 THEN :coord_disk_mb::numeric ELSE :worker_disk_mb::numeric END AS total_mib,
        CASE WHEN groupid=0 THEN :coord_disk_free_mb::numeric ELSE :worker_disk_free_mb::numeric END AS free_mib
 FROM _d1_raw raw JOIN pg_dist_node node USING (nodeid) WHERE raw.success
), calculated AS (
 SELECT *, CASE WHEN total_mib > 0 AND free_mib >= 0 THEN free_mib / total_mib * 100 END AS free_pct,
        CASE WHEN free_mib >= 0 AND :disk_growth_mb_per_day::numeric > 0 THEN free_mib / :disk_growth_mb_per_day::numeric END AS runway_days
 FROM measured
)
SELECT nodename || ':' || nodeport AS node, pg_size_pretty(database_bytes) AS current_database_size,
       total_mib AS supplied_total_mib, free_mib AS supplied_free_mib, round(free_pct, 2) AS free_pct,
       round(runway_days, 1) AS scenario_runway_days,
       CASE WHEN free_mib < -1 OR total_mib < 0 OR (total_mib > 0 AND free_mib > total_mib)
            THEN 'INCOMPLETE : invalid supplied filesystem measurements'
            WHEN free_pct < :crit_free_pct::numeric THEN 'CRITICAL : supplied filesystem free space below policy threshold'
            WHEN runway_days < :crit_runway_days::numeric THEN 'WARN : supplied growth scenario has short runway; verify recent filesystem trend'
            WHEN free_pct < :warn_free_pct::numeric OR runway_days < :warn_runway_days::numeric THEN 'WARN : supplied disk measurements cross planning threshold'
            WHEN free_pct IS NULL THEN 'INFO : filesystem free space unknown; database size cannot establish disk usage'
            WHEN runway_days IS NULL THEN 'INFO : supplied free space available; growth and runway unknown'
            ELSE 'INFO : supplied disk scenario within policy; verify measurements and growth window'
       END AS verdict
FROM calculated ORDER BY groupid, nodeport;
\echo 'INFO : database size excludes other databases, WAL, logs, temporary files and filesystem overhead. Supply per-node filesystem free/total MiB and a measured growth rate; tuple update/delete counters are not physical growth. Worker inputs assume equal disks; use per-node measurements for heterogeneous fleets.'
\ir ../advisor_coverage.sql
DROP TABLE pg_temp._d1_raw;