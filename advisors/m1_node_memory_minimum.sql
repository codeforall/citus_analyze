-- =====================================================================
-- M1 : node memory minimum (coordinator & workers)
-- ---------------------------------------------------------------------
-- Answers the most important sizing question: "how much RAM does each
-- node in this Citus cluster actually need, at minimum, to stay out of
-- OOM under realistic peak load?"
--
-- Model (per node)
--   min_ram = shared_buffers
--           + max_connections           * per_backend_steady_MB
--           + autovacuum_max_workers    * maintenance_work_mem
--           + max_parallel_workers      * work_mem    (parallel sort/hash)
--           + wal_buffers + temp_buffers * max_connections/4
--           + citus_outbound_pool       (MX entry nodes only)
--           + os_reserve                (1 GB or 10% of subtotal, whichever greater)
--
--   peak_ram (worst-case busy moment) =
--         shared_buffers
--       + max_connections * (per_backend_steady_MB + peak_query_multiplier * work_mem)
--       + autovacuum_max_workers * maintenance_work_mem
--       + 2 * maintenance_work_mem           (CREATE INDEX / VACUUM bursts)
--       + citus_outbound_pool
--       + os_reserve
--
--   per_backend_steady_MB =
--         baseline_backend_MB                (default 10 MB: palloc, catcache, relcache)
--       + citus_metadata_cache_MB_per_backend (scales with placements touched)
--       + work_mem                           (one working sort/hash per steady backend)
--
-- Inputs (override with `psql -v key=value`)
--   :coord_ram_mb           coordinator RAM in MB (default 0 = unknown, skip comparison)
--   :worker_ram_mb          per-worker RAM in MB  (default 0 = unknown, skip comparison)
--   :peak_query_mult        how many (work_mem)-sized operators a peak backend holds
--                           (default 3: a cross-shard SELECT with Sort + HashAgg + Hash Join)
--   :baseline_backend_mb    minimum per-backend RSS excluding work_mem (default 10)
--   :os_reserve_mb          floor on OS/kernel+filesystem reserve (default 1024)
--   :os_reserve_pct         percentage reserve if larger than os_reserve_mb (default 10)
--   :burst_maint_ops        concurrent CREATE INDEX / REINDEX / manual VACUUM bursts
--                           counted on top of autovacuum (default 2; heuristic)
--   :k_meta_per_shard       bytes per cached shard in Citus metadata cache (default 160)
--   :k_meta_per_placement   bytes per cached placement replica (default 40)
--
-- Verdict (gated on RECOMMENDED = PEAK + OS reserve, so OK means safe INCLUDING OS)
--   CRITICAL : measured RAM < peak_ram            (will OOM under load)
--   WARN     : measured RAM < recommended_ram     (fits peak but no OS headroom)
--   OK       : measured RAM >= recommended_ram    (safe with OS reserve)
--   INFO     : coord_ram_mb / worker_ram_mb not provided (comparison skipped)
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?coord_ram_mb}        \else \set coord_ram_mb 0         \endif
\if :{?worker_ram_mb}       \else \set worker_ram_mb 0        \endif
\if :{?peak_query_mult}     \else \set peak_query_mult 3      \endif
\if :{?baseline_backend_mb} \else \set baseline_backend_mb 10 \endif
\if :{?os_reserve_mb}       \else \set os_reserve_mb 1024     \endif
\if :{?os_reserve_pct}      \else \set os_reserve_pct 10      \endif
\if :{?burst_maint_ops}     \else \set burst_maint_ops 2      \endif
\if :{?k_meta_per_shard}    \else \set k_meta_per_shard 160   \endif
\if :{?k_meta_per_placement} \else \set k_meta_per_placement 40 \endif

\echo
\echo '==================== M1 : node memory minimum ===================='

-- pg_size_bytes-aware fetch of a GUC in bytes (handles kB/MB/GB/8kB-blocks)
DROP TABLE IF EXISTS _m1_gucs;
CREATE TEMP TABLE _m1_gucs AS
WITH g AS (
  SELECT name, setting, unit
  FROM pg_settings
  WHERE name IN (
      'shared_buffers', 'work_mem', 'maintenance_work_mem', 'wal_buffers',
      'temp_buffers', 'max_connections', 'max_prepared_transactions',
      'max_locks_per_transaction', 'autovacuum_max_workers',
      'max_parallel_workers', 'max_parallel_workers_per_gather',
      'max_worker_processes',
      'citus.max_shared_pool_size', 'citus.max_adaptive_executor_pool_size',
      'citus.local_shared_pool_size', 'citus.max_client_connections'
  )
)
SELECT name, setting, unit,
       -- normalise to MB where applicable
       CASE
         WHEN unit = '8kB'  THEN setting::bigint * 8 / 1024.0
         WHEN unit = 'kB'   THEN setting::bigint / 1024.0
         WHEN unit = 'MB'   THEN setting::bigint::numeric
         WHEN unit = 'GB'   THEN setting::bigint * 1024.0
         ELSE NULL
       END AS mb,
       CASE WHEN unit IS NULL OR unit = '' THEN setting::bigint ELSE NULL END AS n
FROM g;

-- quick accessors
DROP TABLE IF EXISTS _m1;
CREATE TEMP TABLE _m1 AS
SELECT
  (SELECT mb FROM _m1_gucs WHERE name='shared_buffers')        AS shbuf_mb,
  (SELECT mb FROM _m1_gucs WHERE name='work_mem')              AS work_mem_mb,
  (SELECT mb FROM _m1_gucs WHERE name='maintenance_work_mem')  AS maint_mem_mb,
  (SELECT mb FROM _m1_gucs WHERE name='wal_buffers')           AS wal_buffers_mb,
  (SELECT mb FROM _m1_gucs WHERE name='temp_buffers')          AS temp_buffers_mb,
  (SELECT n  FROM _m1_gucs WHERE name='max_connections')       AS max_conn,
  (SELECT n  FROM _m1_gucs WHERE name='autovacuum_max_workers') AS av_workers,
  (SELECT n  FROM _m1_gucs WHERE name='max_parallel_workers')  AS max_par_workers,
  (SELECT n  FROM _m1_gucs WHERE name='max_parallel_workers_per_gather') AS max_par_per_gather,
  (SELECT n  FROM _m1_gucs WHERE name='citus.max_shared_pool_size') AS cx_shared_pool,
  (SELECT n  FROM _m1_gucs WHERE name='citus.max_adaptive_executor_pool_size') AS cx_adaptive_pool,
  -- cluster shape
  (SELECT count(*) FROM pg_dist_shard)      AS total_shards,
  (SELECT count(*) FROM pg_dist_placement)  AS total_placements,
  (SELECT count(*) FROM pg_dist_partition)  AS total_dist_tables,
  (SELECT count(*) FROM pg_dist_node WHERE isactive AND shouldhaveshards AND noderole='primary') AS n_workers,
  (SELECT count(*) FROM pg_dist_node WHERE isactive AND hasmetadata AND noderole='primary')     AS n_mx_nodes;

-- Per-backend steady memory (MB) depends on node role:
--   coordinator / MX entry node : metadata cache ~ total_placements * 200 bytes
--   pure worker                 : metadata cache ~ (placements on this worker) * 200 bytes
-- We compute both; 200 bytes/placement is the field-observed upper bound
-- (Citus metadata cache entries: DistTableCacheEntry + ShardCacheEntry overhead).

DROP TABLE IF EXISTS _m1_nodes;
CREATE TEMP TABLE _m1_nodes AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata THEN 'worker (MX entry)'
         ELSE 'worker' END                                     AS role,
    n.nodename, n.nodeport, n.hasmetadata,
    COALESCE((SELECT count(*) FROM pg_dist_placement p WHERE p.groupid = n.groupid), 0) AS local_placements,
    -- cached_shards/placements on this node's backends: MX entry nodes see ALL shards
    -- (routing), a pure worker only needs its own local placements.
    CASE WHEN n.hasmetadata OR n.groupid = 0
         THEN (SELECT total_shards FROM _m1)
         ELSE COALESCE((SELECT count(DISTINCT shardid) FROM pg_dist_placement p WHERE p.groupid = n.groupid), 0)
    END                                                        AS cached_shards,
    CASE WHEN n.hasmetadata OR n.groupid = 0
         THEN (SELECT total_placements FROM _m1)
         ELSE COALESCE((SELECT count(*) FROM pg_dist_placement p WHERE p.groupid = n.groupid), 0)
    END                                                        AS cached_placements
FROM pg_dist_node n
WHERE n.isactive AND n.noderole='primary'
ORDER BY n.groupid;

-- Formula rows, one per node
DROP TABLE IF EXISTS _m1_calc;
CREATE TEMP TABLE _m1_calc AS
WITH base AS (
    SELECT nd.*,
           m.shbuf_mb, m.work_mem_mb, m.maint_mem_mb, m.wal_buffers_mb, m.temp_buffers_mb,
           m.max_conn, m.av_workers, m.max_par_workers,
           m.cx_shared_pool,
           -- citus.max_shared_pool_size = -1 means "no Citus-imposed limit" (falls
           -- back to max_connections on the target). Coerce to max_conn so the
           -- outbound-pool memory term doesn't go negative.
           CASE WHEN m.cx_shared_pool IS NULL OR m.cx_shared_pool < 0
                THEN m.max_conn
                ELSE m.cx_shared_pool
           END                                             AS cx_shared_pool_eff,
           -- Citus metadata cache: decomposed as shards + placements so the
           -- model scales correctly with replication_factor > 1.
           (nd.cached_shards    * (:k_meta_per_shard)::numeric
          + nd.cached_placements * (:k_meta_per_placement)::numeric) / 1048576.0
                                                           AS cx_meta_mb_per_backend,
           -- outbound pool: MX entry nodes hold up to max_shared_pool_size
           -- connections each costing ~1 MB of libpq + result-buffer overhead
           CASE WHEN nd.hasmetadata OR nd.role = 'coordinator'
                THEN (CASE WHEN m.cx_shared_pool IS NULL OR m.cx_shared_pool < 0
                           THEN m.max_conn ELSE m.cx_shared_pool END) * 1.0
                ELSE 0 END                                  AS cx_outbound_mb
    FROM _m1_nodes nd CROSS JOIN _m1 m
)
SELECT *,
       (:baseline_backend_mb)::numeric + cx_meta_mb_per_backend + work_mem_mb
                                                            AS per_backend_steady_mb,
       (:baseline_backend_mb)::numeric + cx_meta_mb_per_backend
           + (:peak_query_mult)::numeric * work_mem_mb      AS per_backend_peak_mb
FROM base;

-- Final roll-up with minimum + peak for each node
\pset tuples_only on
SELECT format(
'%s  %s:%s   (role: %s, local_placements=%s, cached_shards=%s, cached_placements=%s)
  shared_buffers              : %s MB
  per-backend steady          : %s MB  (baseline %s + citus_meta %s + work_mem %s)
  per-backend peak            : %s MB  (baseline %s + citus_meta %s + %s*work_mem %s)
  max_connections             : %s
  autovacuum slots            : %s  * maintenance_work_mem %s MB = %s MB
  parallel worker pool        : max_parallel_workers=%s * work_mem %s MB = %s MB
  wal_buffers + temp_buffers  : %s MB
  Citus outbound pool (MX)    : %s MB   (max_shared_pool_size=%s)
  ---------------------------------------------------------------
  MIN RAM (steady peak)       : %s MB  ~  %s GB
  PEAK RAM (worst-case burst) : %s MB  ~  %s GB
  OS/kernel reserve (added)   : %s MB  (max(%s MB, %s%% of subtotal))
  ===============================================================
  RECOMMENDED NODE RAM        : %s MB  ~  %s GB
  %s',
    '----',
    c.nodename, c.nodeport, c.role, c.local_placements, c.cached_shards, c.cached_placements,
    round(c.shbuf_mb, 1),
    round(c.per_backend_steady_mb, 2),
    :baseline_backend_mb, round(c.cx_meta_mb_per_backend, 3), round(c.work_mem_mb, 1),
    round(c.per_backend_peak_mb, 2),
    :baseline_backend_mb, round(c.cx_meta_mb_per_backend, 3),
    :peak_query_mult, round(c.work_mem_mb, 1),
    c.max_conn,
    c.av_workers, round(c.maint_mem_mb, 1), round(c.av_workers * c.maint_mem_mb, 1),
    c.max_par_workers, round(c.work_mem_mb, 1), round(c.max_par_workers * c.work_mem_mb, 1),
    round(c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25, 1),
    round(c.cx_outbound_mb, 1), c.cx_shared_pool,
    -- MIN RAM (steady)
    round(c.shbuf_mb + c.max_conn * c.per_backend_steady_mb
          + c.av_workers * c.maint_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb, 0),
    round((c.shbuf_mb + c.max_conn * c.per_backend_steady_mb
          + c.av_workers * c.maint_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb) / 1024.0, 2),
    -- PEAK RAM (burst)
    round(c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
          + c.av_workers * c.maint_mem_mb
          + (:burst_maint_ops)::numeric * c.maint_mem_mb
          + c.max_par_workers * c.work_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb, 0),
    round((c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
          + c.av_workers * c.maint_mem_mb
          + (:burst_maint_ops)::numeric * c.maint_mem_mb
          + c.max_par_workers * c.work_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb) / 1024.0, 2),
    -- OS reserve
    round(greatest(:os_reserve_mb::numeric,
                   (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                    + c.av_workers * c.maint_mem_mb
                    + (:burst_maint_ops)::numeric * c.maint_mem_mb
                    + c.max_par_workers * c.work_mem_mb
                    + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                    + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0)), 0),
    :os_reserve_mb, :os_reserve_pct,
    -- FINAL recommended RAM
    round(
      c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
      + c.av_workers * c.maint_mem_mb
      + (:burst_maint_ops)::numeric * c.maint_mem_mb
      + c.max_par_workers * c.work_mem_mb
      + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
      + c.cx_outbound_mb
      + greatest(:os_reserve_mb::numeric,
                 (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                  + c.av_workers * c.maint_mem_mb
                  + (:burst_maint_ops)::numeric * c.maint_mem_mb
                  + c.max_par_workers * c.work_mem_mb
                  + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                  + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0)),
      0),
    round((
      c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
      + c.av_workers * c.maint_mem_mb
      + (:burst_maint_ops)::numeric * c.maint_mem_mb
      + c.max_par_workers * c.work_mem_mb
      + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
      + c.cx_outbound_mb
      + greatest(:os_reserve_mb::numeric,
                 (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                  + c.av_workers * c.maint_mem_mb
                  + (:burst_maint_ops)::numeric * c.maint_mem_mb
                  + c.max_par_workers * c.work_mem_mb
                  + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                  + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0))
    ) / 1024.0, 2),
    -- verdict line per node: compare measured RAM against PEAK (CRITICAL threshold)
    -- and RECOMMENDED = PEAK + OS_reserve (WARN if between, OK if at or above).
    CASE
      WHEN (c.role = 'coordinator' AND :coord_ram_mb::int  = 0)
        OR (c.role LIKE 'worker%%'  AND :worker_ram_mb::int = 0)
      THEN format('INFO : provide -v %s_ram_mb=<measured RAM MB> to get a pass/fail verdict.',
                  CASE WHEN c.role='coordinator' THEN 'coord' ELSE 'worker' END)
      WHEN (c.role = 'coordinator' AND :coord_ram_mb::numeric < (
            c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
            + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
            + c.max_par_workers * c.work_mem_mb
            + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
            + c.cx_outbound_mb))
        OR (c.role LIKE 'worker%%' AND :worker_ram_mb::numeric < (
            c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
            + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
            + c.max_par_workers * c.work_mem_mb
            + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
            + c.cx_outbound_mb))
      THEN format('CRITICAL : measured RAM (%s MB) < PEAK RAM requirement. Node WILL OOM under load.',
                  CASE WHEN c.role='coordinator' THEN :coord_ram_mb ELSE :worker_ram_mb END)
      WHEN (c.role = 'coordinator' AND :coord_ram_mb::numeric < (
            c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
            + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
            + c.max_par_workers * c.work_mem_mb
            + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
            + c.cx_outbound_mb
            + greatest(:os_reserve_mb::numeric,
                       (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                        + c.av_workers * c.maint_mem_mb
                        + (:burst_maint_ops)::numeric*c.maint_mem_mb
                        + c.max_par_workers * c.work_mem_mb
                        + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                        + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0))))
        OR (c.role LIKE 'worker%%' AND :worker_ram_mb::numeric < (
            c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
            + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
            + c.max_par_workers * c.work_mem_mb
            + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
            + c.cx_outbound_mb
            + greatest(:os_reserve_mb::numeric,
                       (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                        + c.av_workers * c.maint_mem_mb
                        + (:burst_maint_ops)::numeric*c.maint_mem_mb
                        + c.max_par_workers * c.work_mem_mb
                        + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                        + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0))))
      THEN format('WARN : measured RAM (%s MB) fits PEAK but leaves no OS reserve; below RECOMMENDED.',
                  CASE WHEN c.role='coordinator' THEN :coord_ram_mb ELSE :worker_ram_mb END)
      ELSE format('OK : measured RAM (%s MB) >= RECOMMENDED (PEAK + OS reserve).',
                  CASE WHEN c.role='coordinator' THEN :coord_ram_mb ELSE :worker_ram_mb END)
    END
) AS "Per-node memory model"
FROM _m1_calc c ORDER BY c.role, c.nodeport;
\pset tuples_only off

-- Headline: worst verdict across all nodes
\pset tuples_only on
SELECT CASE
  WHEN bool_or(sev = 'CRITICAL') THEN
    format('CRITICAL : %s node(s) have less RAM than the peak-burst requirement. OOM is expected under load. See per-node RECOMMENDED NODE RAM values above.',
           count(*) FILTER (WHERE sev='CRITICAL'))
  WHEN bool_or(sev = 'WARN') THEN
    format('WARN : %s node(s) fit PEAK RAM but are below RECOMMENDED (no OS reserve).',
           count(*) FILTER (WHERE sev='WARN'))
  WHEN bool_or(sev = 'INFO') THEN
    'INFO : pass -v coord_ram_mb=... -v worker_ram_mb=... to get a pass/fail verdict against measured RAM.'
  ELSE
    'OK : all nodes have sufficient RAM for both steady and burst peaks.'
END AS "M1 headline"
FROM (
  SELECT CASE
    WHEN (c.role = 'coordinator' AND :coord_ram_mb::int = 0)
      OR (c.role LIKE 'worker%' AND :worker_ram_mb::int = 0)
      THEN 'INFO'
    WHEN (c.role = 'coordinator' AND :coord_ram_mb::numeric < (
          c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
          + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
          + c.max_par_workers * c.work_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb))
      OR (c.role LIKE 'worker%' AND :worker_ram_mb::numeric < (
          c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
          + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
          + c.max_par_workers * c.work_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb))
      THEN 'CRITICAL'
    WHEN (c.role = 'coordinator' AND :coord_ram_mb::numeric < (
          c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
          + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
          + c.max_par_workers * c.work_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb
          + greatest(:os_reserve_mb::numeric,
                     (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                      + c.av_workers * c.maint_mem_mb
                      + (:burst_maint_ops)::numeric*c.maint_mem_mb
                      + c.max_par_workers * c.work_mem_mb
                      + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                      + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0))))
      OR (c.role LIKE 'worker%' AND :worker_ram_mb::numeric < (
          c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
          + c.av_workers * c.maint_mem_mb + (:burst_maint_ops)::numeric*c.maint_mem_mb
          + c.max_par_workers * c.work_mem_mb
          + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
          + c.cx_outbound_mb
          + greatest(:os_reserve_mb::numeric,
                     (c.shbuf_mb + c.max_conn * c.per_backend_peak_mb
                      + c.av_workers * c.maint_mem_mb
                      + (:burst_maint_ops)::numeric*c.maint_mem_mb
                      + c.max_par_workers * c.work_mem_mb
                      + c.wal_buffers_mb + c.temp_buffers_mb * c.max_conn * 0.25
                      + c.cx_outbound_mb) * (:os_reserve_pct::numeric / 100.0))))
      THEN 'WARN'
    ELSE 'OK'
  END AS sev
  FROM _m1_calc c
) s;
\pset tuples_only off

DROP TABLE _m1_calc;
DROP TABLE _m1_nodes;
DROP TABLE _m1;
DROP TABLE _m1_gucs;
