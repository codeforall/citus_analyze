-- =====================================================================
-- citus_analyze / P1 : partition hygiene & maintenance runway
-- ---------------------------------------------------------------------
-- Partitioned distributed tables are the #1 source of Sunday-night
-- pages: writes start failing at midnight because no partition covers
-- "tomorrow". This advisor answers:
--
--   1. Which distributed/reference tables are partitioned, and how
--      many future days of coverage does each one have?
--   2. Are there gaps between partition ranges? (A gap means writes
--      that fall in the missing range will fail with "no partition
--      of relation X found for row".)
--   3. Are there partitions sitting beyond the sensible premake
--      horizon? (Not dangerous but often indicates a config mistake.)
--   4. Does a distributed partitioned table have an excessive number
--      of partitions? (Planning-time blow-up on coordinator; each
--      partition is further split into N shards on workers, so a 1000-
--      partition table at shard_count=32 lands 32,000 shards.)
--   5. If pg_partman is installed, does its config agree with reality?
--   6. Are there orphan child tables on workers that match a partition
--      naming pattern but are not attached via pg_inherits? (Happens
--      after failed drop_old_time_partitions.)
--
-- Modeled on Citus's partition helpers:
--   * time_partitions view (parent_table, partition, from_value, to_value)
--   * create_time_partitions(parent, interval, end_at, start_from)
--   * drop_old_time_partitions(parent, older_than)
--   * get_missing_time_partition_ranges(parent, interval, to_value, from_value)
--
-- Inputs (override with -v):
--   min_future_days    WARN threshold for remaining future coverage
--                      (default 14). Cluster is WARN if any distributed
--                      partitioned table has less than this many days
--                      of future partitions.
--   crit_future_days   CRITICAL threshold (default 3). Cluster is
--                      CRITICAL if any distributed partitioned table
--                      has less than this many days of future partitions.
--   far_future_days    Partitions further than this many days in the
--                      future are flagged as possibly over-premade
--                      (default 365).
--   partition_limit    Partition count beyond this on a single
--                      distributed parent is WARN (default 200).
--   shard_count_limit  (partitions × shards) beyond this is WARN
--                      (default 5000).
--   top_n              Row cap on listings (default 20).
--
-- Caveats
--   * from_value / to_value in time_partitions are text. We cast to
--     timestamptz at compare time, guarded with a regex (only try
--     the cast if the value looks like a timestamp). Non-time range
--     partitions (integer ranges, list partitions) are reported
--     separately in P1d and skipped from runway math.
--   * pg_partman sits on top of Citus; its premake setting is the
--     owner of future-partition creation for tables it manages. P1e
--     correlates pg_partman config vs time_partitions reality.
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?min_future_days}   \else \set min_future_days   14   \endif
\if :{?crit_future_days}  \else \set crit_future_days  3    \endif
\if :{?far_future_days}   \else \set far_future_days   365  \endif
\if :{?partition_limit}   \else \set partition_limit   200  \endif
\if :{?shard_count_limit} \else \set shard_count_limit 5000 \endif
\if :{?top_n}             \else \set top_n             20   \endif

\echo
\echo '==================== P1 : partition hygiene & runway ===================='

-- ---------------------------------------------------------------------
-- Availability checks. time_partitions was added in Citus 10.0.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _p1_env;
CREATE TEMP TABLE _p1_env AS
SELECT
    EXISTS (SELECT 1 FROM pg_class c
              JOIN pg_namespace n ON n.oid=c.relnamespace
              WHERE c.relname='time_partitions'
                AND n.nspname='pg_catalog') AS has_time_partitions_pgcatalog,
    EXISTS (SELECT 1 FROM pg_views WHERE viewname='time_partitions') AS has_time_partitions_view,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_partman') AS has_partman,
    (SELECT nspname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace
       WHERE e.extname='pg_partman')                               AS partman_schema;

-- ---------------------------------------------------------------------
-- Snapshot time_partitions into a local temp so we can join freely.
-- Temporal classification is TYPE-AWARE: we look up the partition key's
-- PostgreSQL type (pg_type.typcategory='D' means date/time family) and
-- only those families participate in runway / gap math. Integer-range
-- or text-range parents appearing in time_partitions surface in P1c
-- instead of being silently misinterpreted.
--
-- DEFAULT partitions (CREATE TABLE ... DEFAULT) have NULL bounds and
-- catch any row not matching a declared range. A parent with a DEFAULT
-- partition can tolerate range gaps without write failures, so we flag
-- it and adjust the headline accordingly.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _p1_tp;
CREATE TEMP TABLE _p1_tp (
    parent_table       text,
    parent_regclass    regclass,
    parent_citus_kind  text,        -- 'distributed'|'reference'|'other-citus'|'local'
    partition_col_type text,        -- e.g. 'timestamptz' / 'date' / 'int4'
    is_time            boolean,     -- typcategory 'D'
    shard_count        int,
    partition_name     text,
    partition_regclass regclass,
    is_default         boolean,     -- DEFAULT partition (both bounds NULL)
    from_text          text,
    to_text            text,
    from_ts            timestamptz,
    to_ts              timestamptz
);

DO $$
DECLARE
    v_has boolean;
BEGIN
    SELECT has_time_partitions_view INTO v_has FROM _p1_env;
    IF NOT v_has THEN
        RAISE NOTICE 'time_partitions view not available; skipping P1 time-partition analysis.';
        RETURN;
    END IF;

    EXECUTE $E$
    INSERT INTO _p1_tp
    SELECT
        tp.parent_table::text,
        tp.parent_table,
        CASE
          WHEN EXISTS (SELECT 1 FROM pg_dist_partition p
                         WHERE p.logicalrelid = tp.parent_table
                           AND p.partmethod IN ('h','r'))
               THEN 'distributed'
          WHEN EXISTS (SELECT 1 FROM pg_dist_partition p
                         WHERE p.logicalrelid = tp.parent_table
                           AND p.partmethod = 'n')
               THEN 'reference'
          WHEN EXISTS (SELECT 1 FROM pg_dist_partition p
                         WHERE p.logicalrelid = tp.parent_table)
               THEN 'other-citus'
          ELSE 'local'
        END,
        t.typname::text,
        (t.typcategory = 'D'),
        COALESCE((SELECT count(*)::int FROM pg_dist_shard s
                    WHERE s.logicalrelid = tp.parent_table), 0),
        tp.partition::text,
        tp.partition,
        (tp.from_value IS NULL AND tp.to_value IS NULL),
        tp.from_value,
        tp.to_value,
        -- Type-aware cast. Only attempt when partition key is a date/time type.
        CASE WHEN t.typcategory = 'D' AND tp.from_value IS NOT NULL
             THEN tp.from_value::timestamptz END,
        CASE WHEN t.typcategory = 'D' AND tp.to_value   IS NOT NULL
             THEN tp.to_value::timestamptz   END
    FROM time_partitions tp
    LEFT JOIN pg_attribute a
      ON  a.attrelid = tp.parent_table
      AND a.attname  = tp.partition_column
      AND a.attnum   > 0
    LEFT JOIN pg_type t ON t.oid = a.atttypid;
    $E$;
END $$;

-- ---------------------------------------------------------------------
-- P1a : per-parent summary (time-partitioned families only)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _p1_summary;
CREATE TEMP TABLE _p1_summary AS
SELECT
    parent_table,
    parent_citus_kind                                   AS kind,
    partition_col_type                                  AS key_type,
    shard_count                                         AS shards_per_partition,
    count(*) FILTER (WHERE NOT is_default)              AS partitions,
    count(*) FILTER (WHERE NOT is_default)
      * GREATEST(shard_count,1)                         AS total_shards,
    bool_or(is_default)                                 AS has_default,
    min(from_ts) FILTER (WHERE NOT is_default)          AS earliest_from,
    max(to_ts)   FILTER (WHERE NOT is_default)          AS latest_to,
    -- Does SOME non-default partition cover "now"? If no, writes that
    -- land in "now" hit the default partition (if any) or fail.
    bool_or(NOT is_default
            AND from_ts IS NOT NULL AND to_ts IS NOT NULL
            AND from_ts <= now() AND now() < to_ts)     AS covers_now,
    -- Future runway: days between latest partition's upper bound and now.
    CASE WHEN max(to_ts) FILTER (WHERE NOT is_default) IS NOT NULL
         THEN EXTRACT(EPOCH FROM (
               max(to_ts) FILTER (WHERE NOT is_default) - now())) / 86400.0
         END::numeric(12,2)                             AS future_runway_days,
    CASE WHEN max(to_ts) FILTER (WHERE NOT is_default) IS NOT NULL
              AND max(to_ts) FILTER (WHERE NOT is_default)
                  > now() + (:far_future_days || ' days')::interval
         THEN 'yes' ELSE '' END                         AS possibly_over_premade
FROM _p1_tp
WHERE is_time
GROUP BY parent_table, parent_citus_kind, partition_col_type, shard_count;

\echo
\echo '-- P1a. Time-partitioned tables: runway summary --'
SELECT
    parent_table,
    kind,
    key_type,
    partitions,
    shards_per_partition,
    total_shards,
    CASE WHEN has_default THEN 'yes' ELSE '' END   AS default_partition,
    CASE WHEN covers_now  THEN 'yes' ELSE 'NO'  END AS covers_now,
    earliest_from,
    latest_to,
    future_runway_days,
    possibly_over_premade
FROM _p1_summary
ORDER BY future_runway_days NULLS LAST, parent_table
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- P1b : gaps and overlaps between adjacent partition ranges.
--       We classify each anomaly:
--         kind='gap-future'    : gap extends into / past now() -> write risk
--         kind='gap-historical': gap lies fully in the past (only blocks
--                                 backfills; no current write impact)
--         kind='overlap'       : prev_to > next from  (catalog anomaly)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _p1_gaps;
CREATE TEMP TABLE _p1_gaps AS
WITH ordered AS (
  SELECT
      parent_table,
      partition_name,
      from_ts,
      to_ts,
      LAG(to_ts)           OVER w AS prev_to,
      LAG(partition_name)  OVER w AS prev_name
  FROM _p1_tp
  WHERE is_time AND NOT is_default
WINDOW w AS (PARTITION BY parent_table ORDER BY from_ts)
)
SELECT
    parent_table,
    prev_name                     AS lower_partition,
    partition_name                AS upper_partition,
    prev_to                       AS gap_from,
    from_ts                       AS gap_to,
    EXTRACT(EPOCH FROM (from_ts - prev_to)) / 86400.0 AS gap_days,
    CASE
      WHEN prev_to > from_ts                    THEN 'overlap'
      WHEN from_ts > now()                      THEN 'gap-future'
      ELSE                                           'gap-historical'
    END                           AS kind
FROM ordered
WHERE prev_to IS NOT NULL
  AND from_ts IS NOT NULL
  AND prev_to <> from_ts;

\echo
\echo '-- P1b. Gaps & overlaps in partition ranges --'
SELECT parent_table, lower_partition, upper_partition,
       gap_from, gap_to, gap_days::numeric(10,2) AS gap_days, kind
FROM _p1_gaps
ORDER BY
  CASE kind WHEN 'overlap' THEN 0
            WHEN 'gap-future' THEN 1
            WHEN 'gap-historical' THEN 2 END,
  gap_days DESC, parent_table
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- P1c : distributed partitioned tables that are NOT time-semantic
--       (list partitions, integer-range partitions, text-range).
--       Parents whose partition key is not a date/time type.
-- ---------------------------------------------------------------------
\echo
\echo '-- P1c. Distributed partitioned tables with NON-time key --'
SELECT
    c.oid::regclass::text                 AS parent_table,
    pt.partstrat                          AS strategy,
    count(i.inhrelid)                     AS partitions,
    (SELECT count(*) FROM pg_dist_shard s WHERE s.logicalrelid=c.oid) AS shards_per_partition
FROM pg_class c
JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
JOIN pg_dist_partition d     ON d.logicalrelid = c.oid
LEFT JOIN pg_inherits i      ON i.inhparent = c.oid
WHERE NOT EXISTS (
        SELECT 1 FROM _p1_tp tp
         WHERE tp.parent_regclass = c.oid
           AND tp.is_time
      )
GROUP BY c.oid, pt.partstrat
ORDER BY partitions DESC, parent_table
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- P1d : very wide partitioned families (planning-time risk)
-- ---------------------------------------------------------------------
\echo
\echo '-- P1d. Partition/shard count hotspots (planning-time risk) --'
SELECT
    parent_table,
    kind,
    partitions,
    shards_per_partition,
    total_shards,
    CASE
      WHEN total_shards > :shard_count_limit THEN 'WARN: total_shards > limit'
      WHEN partitions   > :partition_limit   THEN 'WARN: partition count > limit'
      ELSE ''
    END AS flag
FROM _p1_summary
WHERE partitions   > :partition_limit
   OR total_shards > :shard_count_limit
ORDER BY total_shards DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- P1e : pg_partman config vs reality (only when installed)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _p1_partman;
CREATE TEMP TABLE _p1_partman (
    parent_table         text,
    partition_interval   text,
    premake              int,
    retention            text,
    retention_keep_table boolean,
    actual_partitions    bigint,
    future_runway_days   numeric
);

DO $$
DECLARE
    v_has boolean;
    v_schema text;
BEGIN
    SELECT has_partman, partman_schema INTO v_has, v_schema FROM _p1_env;
    IF NOT v_has THEN
        RETURN;
    END IF;
    EXECUTE format($E$
      INSERT INTO _p1_partman
      SELECT
          pc.parent_table::text,
          pc.partition_interval::text,
          pc.premake,
          pc.retention::text,
          pc.retention_keep_table,
          s.partitions,
          s.future_runway_days
      FROM %I.part_config pc
      -- to_regclass() avoids aborting P1e if pg_partman has a stale row
      -- for a dropped table: such rows just filter out.
      LEFT JOIN _p1_summary s
        ON s.parent_table = to_regclass(pc.parent_table)::text
      WHERE to_regclass(pc.parent_table) IS NOT NULL
    $E$, v_schema);
END $$;

\echo
\echo '-- P1e. pg_partman config vs actual (empty if pg_partman not installed) --'
SELECT parent_table, partition_interval, premake, retention,
       retention_keep_table, actual_partitions, future_runway_days
FROM _p1_partman
ORDER BY future_runway_days NULLS LAST, parent_table
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Headline priority (most urgent first):
--   1. CRITICAL uncovered-now  : "now()" lies in no range AND no DEFAULT
--   2. CRITICAL future gap     : gap that starts in future, blocks writes
--   3. CRITICAL crit runway    : runs out in < crit_future_days
--   4. CRITICAL overlap        : catalog anomaly (should never happen)
--   5. WARN     warn runway    : runs out in < min_future_days
--   6. WARN     historical gap : backfills only, no current write impact
--   7. WARN     wide family    : planning-time/shard-count risk
--   8. INFO     over-premake   : far_future_days exceeded
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sig AS (
  SELECT
    (SELECT count(*) FROM _p1_summary
       WHERE covers_now = FALSE AND has_default = FALSE)       AS uncovered_now_n,
    (SELECT count(*) FROM _p1_gaps WHERE kind='gap-future')    AS future_gap_n,
    (SELECT count(*) FROM _p1_gaps WHERE kind='gap-historical')AS hist_gap_n,
    (SELECT count(*) FROM _p1_gaps WHERE kind='overlap')       AS overlap_n,
    (SELECT count(*) FROM _p1_summary
       WHERE future_runway_days IS NOT NULL
         AND future_runway_days < :crit_future_days)           AS crit_runway_n,
    (SELECT count(*) FROM _p1_summary
       WHERE future_runway_days IS NOT NULL
         AND future_runway_days < :min_future_days
         AND future_runway_days >= :crit_future_days)          AS warn_runway_n,
    (SELECT count(*) FROM _p1_summary
       WHERE partitions   > :partition_limit
          OR total_shards > :shard_count_limit)                AS wide_n,
    (SELECT count(*) FROM _p1_summary
       WHERE possibly_over_premade = 'yes')                    AS over_n,
    (SELECT count(*) FROM _p1_summary)                         AS total_n,
    (SELECT has_time_partitions_view FROM _p1_env)             AS has_view
)
SELECT CASE
  WHEN NOT has_view THEN
    'INFO : Citus time_partitions view not available (Citus < 10.0). P1 analysis skipped.'
  WHEN total_n = 0 THEN
    'OK : no time-partitioned tables found.'
  WHEN uncovered_now_n > 0 THEN
    format('CRITICAL : %s time-partitioned table(s) have NO partition covering now() AND no DEFAULT partition. New INSERTs WILL fail. See P1a.',
           uncovered_now_n)
  WHEN future_gap_n > 0 THEN
    format('CRITICAL : %s future partition gap(s). INSERTs landing in the gap WILL fail. See P1b.',
           future_gap_n)
  WHEN crit_runway_n > 0 THEN
    format('CRITICAL : %s table(s) have less than %s days of future partitions. Run create_time_partitions() NOW. See P1a.',
           crit_runway_n, :crit_future_days::text)
  WHEN overlap_n > 0 THEN
    format('CRITICAL : %s partition range overlap(s) detected. Catalog anomaly; investigate before DDL changes.',
           overlap_n)
  WHEN warn_runway_n > 0 THEN
    format('WARN : %s table(s) have less than %s days of future partitions. Schedule create_time_partitions(). See P1a.',
           warn_runway_n, :min_future_days::text)
  WHEN hist_gap_n > 0 THEN
    format('WARN : %s historical partition gap(s). No current write impact but backfills into the gap will fail. See P1b.',
           hist_gap_n)
  WHEN wide_n > 0 THEN
    format('WARN : %s partitioned table(s) exceed partition/shard limits (> %s partitions or > %s total shards). Planning-time and metadata cost may be heavy.',
           wide_n, :partition_limit::text, :shard_count_limit::text)
  WHEN over_n > 0 THEN
    format('INFO : %s table(s) have partitions further than %s days in the future. Possible over-premake.',
           over_n, :far_future_days::text)
  ELSE
    format('OK : %s time-partitioned table(s) checked; runways OK, no gaps, no width hotspots.', total_n)
END
FROM sig;
\pset tuples_only off

DROP TABLE IF EXISTS _p1_partman;
DROP TABLE IF EXISTS _p1_gaps;
DROP TABLE IF EXISTS _p1_summary;
DROP TABLE IF EXISTS _p1_tp;
DROP TABLE IF EXISTS _p1_env;
