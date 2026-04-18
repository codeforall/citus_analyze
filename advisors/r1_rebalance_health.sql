-- =====================================================================
-- citus_analyze / R1 : in-flight rebalance / background-job health
-- ---------------------------------------------------------------------
-- Surfaces the state of the Citus background-job queue, with
-- emphasis on the rebalancer. Answers:
--   * Is a rebalance running, and how far?
--   * Are any tasks in an error-retry loop that won't self-heal?
--   * Is anything wedged or long-running?
--   * ETA based on moved bytes + LSN apply rate.
--
-- Uses:
--   * pg_dist_background_job    (job-level state: scheduled/running/
--                                finished/cancelling/cancelled/
--                                failing/failed)
--   * pg_dist_background_task   (task-level status: blocked/runnable/
--                                running/done/cancelling/error/
--                                unscheduled/cancelled)
--   * get_rebalance_progress()  (per-move shard_size + source/target
--                                LSN lag)
--   * GUCs: max_background_task_executors_per_node,
--           background_task_queue_interval
--
-- Inputs (override with -v):
--   long_running_min   WARN if any task has been running > N minutes;
--                      default 60
--   stuck_min          CRITICAL if a task is running > N minutes;
--                      default 240 (4h)
--   error_retry_warn   WARN if any task has retry_count > N; default 3
--   error_retry_crit   CRITICAL if retry_count > N; default 10
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?long_running_min} \else \set long_running_min 60  \endif
\if :{?stuck_min}        \else \set stuck_min        240 \endif
\if :{?error_retry_warn} \else \set error_retry_warn 3   \endif
\if :{?error_retry_crit} \else \set error_retry_crit 10  \endif

\echo
\echo '==================== R1 : in-flight rebalance / job health ===================='

-- ---------------------------------------------------------------------
-- R1a. Job-level summary
-- ---------------------------------------------------------------------
\echo
\echo '-- R1a. Background jobs (by state) --'

SELECT state,
       count(*)                                              AS jobs,
       min(started_at)                                       AS oldest_started,
       max(coalesce(finished_at, now()))                     AS most_recent
FROM pg_dist_background_job
GROUP BY state
ORDER BY state;

\echo
\echo '-- Active / non-terminal jobs --'
SELECT job_id, state, job_type,
       started_at,
       round(extract(epoch FROM (now() - started_at))/60, 1) AS running_min,
       left(description, 80) AS description
FROM pg_dist_background_job
WHERE state IN ('scheduled','running','cancelling','failing')
ORDER BY started_at NULLS FIRST;

-- ---------------------------------------------------------------------
-- R1b. Task-level breakdown for active jobs
-- ---------------------------------------------------------------------
\echo
\echo '-- R1b. Tasks for active jobs (status histogram) --'

SELECT j.job_id,
       j.state     AS job_state,
       t.status    AS task_status,
       count(*)    AS n,
       max(t.retry_count) AS max_retries
FROM pg_dist_background_job j
LEFT JOIN pg_dist_background_task t USING (job_id)
WHERE j.state IN ('scheduled','running','cancelling','failing')
GROUP BY j.job_id, j.state, t.status
ORDER BY j.job_id, t.status;

\echo
\echo '-- Tasks in error / retry loop (across ALL jobs) --'
SELECT job_id, task_id, status, retry_count, pid,
       left(message, 120)                                      AS message,
       CASE
         WHEN retry_count >= :error_retry_crit THEN 'CRITICAL : persistent failure; investigate and citus_task_wait or cancel'
         WHEN retry_count >= :error_retry_warn THEN 'WARN     : retry loop'
         ELSE 'INFO'
       END                                                     AS verdict
FROM pg_dist_background_task
WHERE status = 'error' OR retry_count > 0
ORDER BY retry_count DESC
LIMIT 20;

\echo
\echo '-- Long-running tasks --'
SELECT t.job_id, t.task_id, t.status, t.pid,
       t.not_before                                            AS earliest_start,
       round(extract(epoch FROM (now() - j.started_at))/60, 1) AS job_running_min,
       CASE
         WHEN extract(epoch FROM (now() - j.started_at))/60 >= :stuck_min
              THEN format('CRITICAL : job has been running > %s min; investigate', :stuck_min::text)
         WHEN extract(epoch FROM (now() - j.started_at))/60 >= :long_running_min
              THEN format('WARN     : job has been running > %s min', :long_running_min::text)
         ELSE 'OK' END                                         AS verdict
FROM pg_dist_background_task t
JOIN pg_dist_background_job  j USING (job_id)
WHERE t.status = 'running'
ORDER BY j.started_at ASC
LIMIT 20;

-- ---------------------------------------------------------------------
-- R1c. Rebalance progress (per-move) + ETA
-- ---------------------------------------------------------------------
\echo
\echo '-- R1c. Per-move progress (get_rebalance_progress) --'

SELECT table_name, shardid,
       pg_size_pretty(shard_size)                              AS shard_size,
       format('%s:%s', sourcename, sourceport)                 AS source,
       format('%s:%s', targetname, targetport)                 AS target,
       pg_size_pretty(source_shard_size)                       AS src_sz,
       pg_size_pretty(target_shard_size)                       AS tgt_sz,
       CASE WHEN shard_size > 0
            THEN round(100.0 * target_shard_size / NULLIF(shard_size,0), 1)
            ELSE 0 END                                         AS pct_copied,
       CASE WHEN source_lsn IS NOT NULL AND target_lsn IS NOT NULL
            THEN pg_size_pretty(pg_wal_lsn_diff(source_lsn, target_lsn))
            ELSE NULL END                                      AS lsn_lag,
       status
FROM get_rebalance_progress()
ORDER BY table_name, shardid;

-- ---------------------------------------------------------------------
-- R1 Headline
-- ---------------------------------------------------------------------
\echo
\echo '-- R1 headline --'
\pset tuples_only on

WITH sums AS (
    SELECT
        (SELECT count(*) FROM pg_dist_background_job WHERE state IN ('scheduled','running')) AS active_jobs,
        (SELECT count(*) FROM pg_dist_background_job WHERE state IN ('failing','failed'))     AS failed_jobs,
        (SELECT count(*) FROM pg_dist_background_task WHERE status='error')                   AS error_tasks,
        (SELECT max(retry_count) FROM pg_dist_background_task)                                AS max_retries,
        (SELECT count(*) FROM pg_dist_background_task WHERE status='running')                 AS running_tasks,
        (SELECT count(*) FROM get_rebalance_progress())                                       AS moves_in_flight,
        (SELECT extract(epoch FROM (now() - min(started_at)))/60
           FROM pg_dist_background_job
           WHERE state IN ('running','failing','cancelling'))                                 AS oldest_active_min
)
SELECT
  CASE
    WHEN active_jobs = 0 AND failed_jobs = 0 AND error_tasks = 0
      THEN 'OK : background-job queue is idle and clean.'
    WHEN coalesce(max_retries,0) >= :error_retry_crit
      THEN format('CRITICAL : %s error task(s), max retry_count=%s. Manual intervention required.',
                  error_tasks, max_retries)
    WHEN oldest_active_min >= :stuck_min
      THEN format('CRITICAL : oldest active job has been running %s min. Investigate wedged task.',
                  round(oldest_active_min::numeric, 1))
    WHEN failed_jobs > 0
      THEN format('WARN : %s failed job(s). Inspect pg_dist_background_task.message.', failed_jobs)
    WHEN coalesce(max_retries,0) >= :error_retry_warn
      THEN format('WARN : %s task(s) with retry_count >= %s. Monitor closely.', error_tasks, :error_retry_warn::text)
    WHEN active_jobs > 0
      THEN format('OK : %s active job(s), %s task(s) running, %s move(s) in flight.',
                  active_jobs, running_tasks, moves_in_flight)
    ELSE 'OK'
  END
FROM sums;
\pset tuples_only off
