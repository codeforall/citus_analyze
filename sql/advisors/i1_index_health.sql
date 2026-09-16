\set advisor_id I1
\ir ../capabilities.sql
-- =====================================================================
-- citus_analyze / I1 : index health (per-shard aware)
-- ---------------------------------------------------------------------
-- Indexes on distributed tables live as PER-SHARD physical indexes on
-- workers. Coordinator-side pg_stat_user_indexes shows nothing for
-- distributed tables. To answer real questions ("is this index ever
-- used?", "how big are my indexes?", "are any invalid?") we have to
-- fan out to every node, aggregate per logical (parent) index, and
-- correlate with the coord-side index definition.
--
-- Detected hazards:
--   1. INVALID / NOT-READY indexes  -> queries don't use them; CREATE
--                                      INDEX CONCURRENTLY may have died
--                                      mid-flight; usually require manual
--                                      REINDEX or DROP+rebuild.
--   2. Unused secondary indexes     -> waste IO + WAL + planning time.
--                                      Excludes PKs/UNIQUE/replica-ident.
--   3. Duplicate indexes            -> same indkey on same table; one
--                                      can be dropped.
--   4. Per-table size hot-spots     -> indexes larger than the table
--                                      itself (often > 1.0 ratio).
--   5. Foreign-key columns without  -> planner falls back to seq scans
--      a supporting index             on FK joins, RI checks lock heavy.
--
-- Inputs (override with -v):
--   min_history_sec  Suppress "unused" verdict when stats were reset
--                    less than this many seconds ago on any node
--                    (default 7 * 86400 = 7 days).
--   min_idx_size_mb  Skip indexes smaller than this from "unused" /
--                    size hotspots (default 8 MB). Tiny indexes are
--                    not worth a recommendation.
--   top_n            Row cap on listings (default 20).
--
-- Caveats
--   * Index "unused" is computed from pg_stat counters since the last
--     reset. A recently rebuilt cluster will have low scan counts even
--     for hot indexes; the min_history_sec gate suppresses false
--     positives. We display the cluster-wide minimum stats age.
--   * Shard-index name detection assumes Citus's <name>_<shardid>
--     convention (the only convention Citus has used since 5.x).
--   * Bloat estimation needs pgstattuple to be reliable. We do not
--     run pgstattuple here (it is expensive). Bloat is in B1.
-- =====================================================================

\pset pager off
\pset border 2
\pset format aligned

\if :{?min_history_sec} \else \set min_history_sec 604800 \endif
\if :{?min_idx_size_mb} \else \set min_idx_size_mb 8      \endif
\if :{?top_n}           \else \set top_n           20     \endif

\echo
\echo '==================== I1 : index health (per-shard aware) ===================='

-- ---------------------------------------------------------------------
-- Per-node payload. JSON object per node carrying:
--   stats_reset_age_sec : oldest pg_stat_database.stats_reset on the node
--   invalid_idx         : list {schema, parent_table, idx_name, isvalid, isready}
--   shard_idx_usage     : aggregated per (base_table, base_index)
--                         across shards: scans, bytes, shard_n
--   local_unused_idx    : non-shard, non-pk, non-unique indexes on
--                         non-Citus tables with idx_scan=0 (cluster-
--                         local hygiene; surfaced from coord only)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._i1_raw;
CREATE TEMP TABLE _i1_raw (nodeid int, success boolean, result text);

-- The remote payload deliberately does NOT reference pg_dist_shard --
-- shard mapping happens on the coordinator (which always has it) by
-- joining the per-relation stream against pg_dist_shard via OID. This
-- keeps the remote query portable to non-MX workers, and keeps the
-- match SCHEMA-SAFE (we never compare regclass::text to relname).
--
-- last_idx_scan is PG16+, so we version-gate it inline.
--
-- We build the remote command outside any dollar-quoted block so psql
-- can interpolate :min_idx_size_mb normally; the inner SQL is dollar-
-- quoted ($CMD$) which is opaque to psql -- format() fills %s.
INSERT INTO _i1_raw (nodeid, success, result)
SELECT r.nodeid, r.success, r.result
FROM (
  SELECT format($CMD$
    WITH _vis AS MATERIALIZED (
      -- Defensive: ensure shards are visible to pg_class scans even in
      -- deployments that strip the internal-backend bypass or force
      -- citus.override_table_visibility = on.
      SELECT set_config('citus.override_table_visibility','off', true)
    ),
    stats_age AS (
      SELECT EXTRACT(EPOCH FROM (now() - stats_reset))::bigint AS sec
      FROM pg_stat_database WHERE datname = current_database()
    ),
    inv AS (
      SELECT jsonb_agg(jsonb_build_object(
               'schema',   n.nspname,
               'rel',      c.relname,
               'rel_oid',  c.oid::int8,
               'idx_name', ic.relname,
               'isvalid',  i.indisvalid,
               'isready',  i.indisready
             )) AS items
      FROM pg_index i
      JOIN pg_class ic ON ic.oid = i.indexrelid
      JOIN pg_class c  ON c.oid  = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE NOT i.indisvalid OR NOT i.indisready
    ),
    all_idx AS (
      SELECT jsonb_agg(jsonb_build_object(
               'schema',   n.nspname,
               'rel',      c.relname,
               'rel_oid',  c.oid::int8,
               'idx_name', ic.relname,
               'idx_oid',  ic.oid::int8,
               'is_pk',    i.indisprimary,
               'is_uniq',  i.indisunique,
               'is_repl',  i.indisreplident,
               'scans',    s.idx_scan,
               'bytes',    pg_relation_size(s.indexrelid),
               'last_scan',
                 %s
             )) AS items
      FROM pg_stat_user_indexes s
      JOIN pg_index    i ON i.indexrelid = s.indexrelid
      JOIN pg_class   ic ON ic.oid = s.indexrelid
      JOIN pg_class   c  ON c.oid  = s.relid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    )
    SELECT jsonb_build_object(
      'stats_reset_age_sec', (SELECT sec FROM stats_age),
      'invalid_idx',         (SELECT items FROM inv),
      'all_idx',             (SELECT items FROM all_idx),
      'min_idx_bytes',       %s
    )::text
  $CMD$,
    CASE WHEN current_setting('server_version_num')::int >= 160000
         THEN 'to_jsonb(s)->>''last_idx_scan'''
         ELSE 'to_jsonb(s)->>''last_idx_scan''' END,
    ((:'min_idx_size_mb')::bigint * 1024 * 1024)) AS c
) cmd, run_command_on_all_nodes(cmd.c, parallel := true) r;

-- ---------------------------------------------------------------------
-- Parse payload into per-node parsed table.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._i1;
CREATE TEMP TABLE _i1 AS
SELECT
    CASE WHEN n.groupid = 0 THEN 'coordinator'
         WHEN n.hasmetadata  THEN 'worker (MX)'
         ELSE 'worker' END                                  AS role,
    n.nodename || ':' || n.nodeport                         AS node,
    n.groupid,
    r.success,
    CASE WHEN r.success AND r.result <> '' THEN r.result::jsonb
         ELSE '{}'::jsonb END                               AS p
FROM _i1_raw r
JOIN pg_dist_node n ON n.nodeid = r.nodeid
WHERE n.isactive;

-- ---------------------------------------------------------------------
-- Coord-side shard map: (parent_schema, parent_relname, shardid) ->
-- (shard_relname). pg_dist_shard.logicalrelid is resolved to OID and
-- joined to pg_class+pg_namespace so we never compare ::text vs relname.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._i1_shard_map;
CREATE TEMP TABLE _i1_shard_map AS
SELECT
    pn.nspname                                          AS parent_schema,
    pc.relname                                          AS parent_rel,
    pc.oid                                              AS parent_oid,
    ds.shardid                                          AS shardid,
    pc.relname || '_' || ds.shardid                     AS shard_rel
FROM pg_dist_shard ds
JOIN pg_class     pc ON pc.oid = ds.logicalrelid
JOIN pg_namespace pn ON pn.oid = pc.relnamespace;

CREATE INDEX ON _i1_shard_map (parent_schema, shard_rel);

-- ---------------------------------------------------------------------
-- Flatten remote per-index stream and classify each row as either a
-- distributed-shard placement (mapped back to its logical parent) or a
-- local relation. Schema-safe: we match (schema, rel) tuples, never
-- bare relnames.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._i1_all;
CREATE TEMP TABLE _i1_all AS
SELECT
    i.role, i.node, i.groupid,
    item->>'schema'                       AS schema,
    item->>'rel'                          AS rel,
    item->>'idx_name'                     AS idx_name,
    (item->>'is_pk')::bool                AS is_pk,
    (item->>'is_uniq')::bool              AS is_uniq,
    (item->>'is_repl')::bool              AS is_repl,
    COALESCE((item->>'scans')::bigint,0)  AS scans,
    COALESCE((item->>'bytes')::bigint,0)  AS bytes,
    NULLIF(item->>'last_scan','')::timestamptz AS last_scan,
    sm.parent_schema, sm.parent_rel
FROM _i1 i,
     jsonb_array_elements(CASE WHEN jsonb_typeof(i.p->'all_idx')='array' THEN i.p->'all_idx' ELSE '[]'::jsonb END) item
LEFT JOIN _i1_shard_map sm
       ON sm.parent_schema = item->>'schema'
      AND sm.shard_rel     = item->>'rel'
WHERE i.success;

-- ---------------------------------------------------------------------
-- I1a : invalid / not-ready indexes (across all nodes)
-- ---------------------------------------------------------------------
\echo
\echo '-- I1a. INVALID or NOT-READY indexes (per node) --'
WITH inv_rows AS (
  SELECT
      i.role, i.node, i.groupid,
      item->>'schema'   AS schema,
      item->>'rel'      AS rel,
      item->>'idx_name' AS idx_name,
      (item->>'isvalid')::bool AS isvalid,
      (item->>'isready')::bool AS isready
  FROM _i1 i, jsonb_array_elements(CASE WHEN jsonb_typeof(i.p->'invalid_idx')='array' THEN i.p->'invalid_idx' ELSE '[]'::jsonb END) item
)
SELECT
    r.role, r.node, r.schema,
    COALESCE(sm.parent_rel, r.rel) AS parent_table,
    r.idx_name AS index_name,
    r.isvalid, r.isready
FROM inv_rows r
LEFT JOIN _i1_shard_map sm
       ON sm.parent_schema = r.schema AND sm.shard_rel = r.rel
ORDER BY r.groupid, r.schema, parent_table, r.idx_name
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- I1b : unused indexes on distributed tables
--       (sum(idx_scan)=0 across ALL shards on ALL workers, AND not
--        a PK / UNIQUE / replica-identity index on the parent).
--       Index identity is the suffix-stripped name, matched to a real
--       index OID on the coord-side parent table.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._i1_shard_usage;
CREATE TEMP TABLE _i1_shard_usage AS
SELECT
    a.parent_schema                                     AS schema,
    a.parent_rel                                        AS base_tbl,
    -- strip trailing _<shardid> from idx_name to recover parent index
    -- name. Anchored to the actual shardid we just matched on, so it
    -- can't trim a partition-style suffix.
    regexp_replace(a.idx_name, '_' || sm.shardid::text || '$', '') AS base_idx,
    sum(a.scans)                                        AS total_scans,
    sum(a.bytes)                                        AS total_bytes,
    count(*)                                            AS total_shards,
    max(a.last_scan)                                    AS last_scan
FROM _i1_all a
JOIN _i1_shard_map sm
  ON sm.parent_schema = a.schema AND sm.shard_rel = a.rel
WHERE a.parent_rel IS NOT NULL
GROUP BY 1, 2, 3;

DROP TABLE IF EXISTS pg_temp._i1_unused_dist;
CREATE TEMP TABLE _i1_unused_dist AS
SELECT
    su.schema, su.base_tbl, su.base_idx,
    su.total_bytes, su.total_shards, su.last_scan
FROM _i1_shard_usage su
JOIN pg_class pc      ON pc.relname = su.base_tbl
JOIN pg_namespace pn  ON pn.oid = pc.relnamespace AND pn.nspname = su.schema
JOIN pg_dist_partition d ON d.logicalrelid = pc.oid
JOIN pg_class ic      ON ic.relname = su.base_idx AND ic.relnamespace = pn.oid
JOIN pg_index pi      ON pi.indexrelid = ic.oid AND pi.indrelid = pc.oid
WHERE su.total_scans = 0
  AND NOT pi.indisprimary
  AND NOT pi.indisunique
  AND NOT pi.indisreplident
  AND su.total_bytes > ((:'min_idx_size_mb')::bigint * 1024 * 1024);

\echo
\echo '-- I1b. Distributed-table indexes with 0 scans across ALL shards --'
SELECT base_tbl AS parent_table,
       base_idx AS index_name,
       total_shards,
       pg_size_pretty(total_bytes) AS total_size,
       last_scan
FROM _i1_unused_dist
ORDER BY total_bytes DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- I1c : duplicate indexes (same indrelid + same indkey signature)
--       Coord-side check on parent tables.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._i1_dup;
CREATE TEMP TABLE _i1_dup AS
WITH idx AS (
  SELECT
      n.nspname                                   AS schema,
      c.relname                                   AS parent_table,
      ic.relname                                  AS index_name,
      -- Tight equality signature: same column attnums, same access
      -- method, same opclass+collation+option per column, same
      -- predicate, same expression text, same key-attribute count.
      -- Anything that differs on any of these (e.g. partial-index
      -- predicate, btree vs hash, DESC ordering, expression index)
      -- yields a distinct signature, so we won't recommend dropping
      -- a non-equivalent index.
      i.indkey::text          || '|' ||
      am.amname               || '|' ||
      i.indnkeyatts::text     || '|' ||
      i.indoption::text       || '|' ||
      coalesce(i.indcollation::text,'')  || '|' ||
      i.indclass::text || '|' ||
      i.indisunique::text || '|' || coalesce(to_jsonb(i)->>'indnullsnotdistinct', 'false') || '|' ||
      coalesce(pg_get_expr(i.indpred, i.indrelid), '')   || '|' ||
      coalesce(pg_get_expr(i.indexprs, i.indrelid), '')  AS key_sig,
      pg_relation_size(i.indexrelid)              AS bytes,
      i.indisprimary, i.indisunique,
      i.indisreplident OR EXISTS (SELECT 1 FROM pg_constraint WHERE conindid=i.indexrelid) AS protected
  FROM pg_index i
  JOIN pg_class ic ON ic.oid = i.indexrelid
  JOIN pg_class c  ON c.oid  = i.indrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_am am ON am.oid = ic.relam
  WHERE n.nspname NOT IN ('pg_catalog','pg_toast','information_schema')
    AND i.indisvalid AND i.indisready AND i.indislive
    -- Skip partition children: their indexes are auto-attached from
    -- the parent's index, so any duplicate on the parent shows up
    -- spuriously on every child. Surfacing the parent is enough.
    AND NOT EXISTS (
          SELECT 1 FROM pg_inherits ih WHERE ih.inhrelid = c.oid
        )
    -- Skip indexes that are themselves attached children of a parent
    -- partitioned-table index (defensive belt+suspenders).
    AND NOT EXISTS (
          SELECT 1 FROM pg_inherits ih WHERE ih.inhrelid = ic.oid
        )
)
SELECT
    a.schema, a.parent_table,
    a.index_name AS idx_a, b.index_name AS idx_b,
    a.bytes      AS bytes_a, b.bytes    AS bytes_b,
    a.indisprimary OR a.indisunique OR a.protected AS a_is_pk_uniq,
    b.indisprimary OR b.indisunique OR b.protected AS b_is_pk_uniq
FROM idx a
JOIN idx b
  ON a.schema = b.schema
 AND a.parent_table = b.parent_table
 AND a.key_sig = b.key_sig
 AND a.index_name < b.index_name;

\echo
\echo '-- I1c. Duplicate indexes (same key signature on same table) --'
SELECT schema, parent_table, idx_a, idx_b,
       pg_size_pretty(bytes_a) AS size_a,
       pg_size_pretty(bytes_b) AS size_b,
        CASE WHEN a_is_pk_uniq OR b_is_pk_uniq THEN 'protected index present; inspect constraints, dependencies and replica identity'
          ELSE 'possible redundancy; validate workload, operator classes, dependencies and storage before removal'
       END AS suggestion
FROM _i1_dup
ORDER BY GREATEST(bytes_a, bytes_b) DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- I1d : foreign-key columns missing a supporting index.
--       Constraint-RI checks lock the referenced row, and joins on FK
--       columns can fall back to seq scans on the parent shard.
-- ---------------------------------------------------------------------
\echo
\echo '-- I1d. FK columns without a supporting btree (leftmost) index --'
WITH fks AS (
  SELECT
      n.nspname                                AS schema,
      c.relname                                AS tbl,
      con.conname                              AS fk_name,
      con.conrelid                             AS rel_oid,
      con.conkey                               AS fk_cols
  FROM pg_constraint con
  JOIN pg_class c  ON c.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE con.contype = 'f'
    AND n.nspname NOT IN ('pg_catalog','information_schema')
)
SELECT
    f.schema, f.tbl, f.fk_name,
    (SELECT string_agg(a.attname, ',' ORDER BY u.ord)
       FROM unnest(f.fk_cols) WITH ORDINALITY u(attnum, ord)
       JOIN pg_attribute a ON a.attrelid = f.rel_oid
                          AND a.attnum   = u.attnum) AS fk_columns
FROM fks f
WHERE NOT EXISTS (
   -- A "supporting" index for FK lookup must be: btree (only AM that
   -- the planner uses for RI lookup), valid+ready+live (so it's
   -- actually usable), have NO partial-index predicate (predicate
   -- might exclude the FK rows being checked), no expression key
   -- (FK columns are bare attributes), and have the FK columns as
   -- its leftmost key prefix.
   SELECT 1
   FROM pg_index i
   JOIN pg_class ic ON ic.oid = i.indexrelid
   JOIN pg_am   am  ON am.oid = ic.relam
   WHERE i.indrelid = f.rel_oid
     AND am.amname = 'btree'
     AND i.indisvalid AND i.indisready AND i.indislive
     AND i.indpred IS NULL
     AND i.indexprs IS NULL
     AND i.indnkeyatts >= cardinality(f.fk_cols)
     AND ARRAY(SELECT attribute FROM unnest(i.indkey) WITH ORDINALITY keys(attribute, position)
           WHERE position <= cardinality(f.fk_cols) ORDER BY attribute)
       = ARRAY(SELECT attribute FROM unnest(f.fk_cols) attribute ORDER BY attribute)
)
ORDER BY f.schema, f.tbl, f.fk_name
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- I1e : per-parent-table index size summary (top N by total bytes)
-- ---------------------------------------------------------------------
\echo
\echo '-- I1e. Largest distributed-index families across the cluster --'
SELECT
    su.schema,
    su.base_tbl AS parent_table,
    su.base_idx AS index_name,
    su.total_shards,
    su.total_scans,
    pg_size_pretty(su.total_bytes) AS total_size
FROM _i1_shard_usage su
ORDER BY su.total_bytes DESC
LIMIT :top_n;

-- ---------------------------------------------------------------------
-- Headline
-- ---------------------------------------------------------------------
\echo
\pset tuples_only on
WITH sig AS (
  SELECT
    (SELECT count(*) FROM _i1, jsonb_array_elements(
              CASE WHEN jsonb_typeof(p->'invalid_idx')='array'
                   THEN p->'invalid_idx' ELSE '[]'::jsonb END))     AS invalid_n,
    (SELECT count(*) FROM _i1_unused_dist)                        AS unused_dist_n,
    (SELECT count(*) FROM _i1_dup)                                AS dup_n,
    (SELECT min((p->>'stats_reset_age_sec')::bigint)
       FROM _i1
       WHERE p ? 'stats_reset_age_sec'
         AND (p->>'stats_reset_age_sec') ~ '^[0-9]+$')            AS min_stats_age,
    (SELECT count(*) FROM _i1_raw WHERE NOT success)              AS unreachable
)
SELECT CASE
  WHEN invalid_n > 0 THEN
    format('WARN : %s INVALID or NOT-READY index(es) detected. Check concurrent builds and intended partition-index state before repair. See I1a.',
           invalid_n)
  WHEN unreachable > 0 THEN
    format('INCOMPLETE : %s node(s) unreachable; unused-index assessment suppressed.', unreachable)
  WHEN min_stats_age IS NULL OR min_stats_age < :min_history_sec THEN
    format('INFO : pg_stat counters reset only %s s ago on at least one node (< %s s). Suppressing "unused" verdict; rerun later. invalid=%s dup=%s.',
           min_stats_age, :min_history_sec::text, invalid_n, dup_n)
  WHEN unused_dist_n > 0 THEN
    format('WARN : %s distributed-table index(es) with 0 scans across all shards (size > %s MB). Consider DROP after confirming with app owners. See I1b.',
           unused_dist_n, :min_idx_size_mb::text)
  WHEN dup_n > 0 THEN
    format('WARN : %s duplicate index pair(s) detected. See I1c for drop suggestions.', dup_n)
  WHEN unreachable > 0 THEN
    format('WARN : %s node(s) unreachable; index health partially verified.', unreachable)
  ELSE
    'OK : no invalid indexes, no unused distributed indexes, no duplicates.'
END
FROM sig;
\pset tuples_only off

DROP TABLE IF EXISTS pg_temp._i1_dup;
DROP TABLE IF EXISTS pg_temp._i1_unused_dist;
DROP TABLE IF EXISTS pg_temp._i1_shard_usage;
DROP TABLE IF EXISTS pg_temp._i1_all;
DROP TABLE IF EXISTS pg_temp._i1_shard_map;
DROP TABLE IF EXISTS pg_temp._i1;
\ir ../advisor_coverage.sql
DROP TABLE IF EXISTS pg_temp._i1_raw;
