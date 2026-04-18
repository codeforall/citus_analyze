# citus_analyze

A [`pg_gather`](https://github.com/codeforall/pg_gather)-style **one-stop
health, sizing, and capacity-planning utility for [Citus](https://github.com/citusdata/citus)
clusters**.

Run a single script against the coordinator and get:

1. A **cluster snapshot bundle** (`gather.out`) — CSV-sectioned dump of every
   catalog, GUC, background job, prepared xact, and activity view an analyst
   needs to triage a cluster offline (pg_gather-style).
2. A set of **quantitative advisors** that produce concrete numbers and
   remediation SQL — not just flags. Each advisor emits a traffic-light
   verdict (`OK` / `WARN` / `CRITICAL`).
3. An **executive summary** with a badge-panel of every advisor's headline
   and a single cluster-wide verdict.

The script exits `0` if everything is OK, `1` if any advisor is WARN, and
`2` if any advisor is CRITICAL — so it drops straight into CI/monitoring.

---

## Why

The Citus support inbox repeatedly sees the same failure modes: OOM after
raising shard or partition count; lock-slot exhaustion after `citus_add_node`
on a populated cluster; connection-storm meltdowns when MX workers start
taking traffic; stuck rebalance jobs; orphan 2PC records; "hot-tenant" shard
skew that no one notices until queries fall off a cliff. Each of these has
a *closed-form capacity model* that Citus itself doesn't surface. This tool
bakes those models into a runnable sketch so you can answer, before pushing
to prod, questions like:

- "If I go from 64 to 256 shards, how much more RAM and how many more lock
  slots do I actually need on every worker?"
- "How many concurrent external client sessions can this cluster really
  take today — before `citus.max_shared_pool_size` or worker
  `max_connections` starts rejecting?"
- "I'm about to `citus_add_node` this 3-year-old cluster. Will the metadata
  sync actually finish, or is it going to OOM / hit `node_connection_timeout`
  / exhaust `max_locks_per_transaction` on the new node?"
- "Where is my data-skew hot spot right now, and what's the exact SQL to
  isolate that tenant?"
- "Is any 2PC record stuck such that the maintenance daemon won't clean it?"

---

## Layout

```
citus_analyze/
├── citus_analyze.sh         # driver (bash; needs libpq/psql)
├── citus_gather.sql         # collector — 31 CSV-with-header sections
├── advisors/
│   ├── gr1_shard_growth_advisor.sql       GR1 — memory & lock budget for shard/partition growth
│   ├── c3_max_external_connections.sql    C3  — max safe external sessions (MX-aware)
│   ├── s3_data_skew_advisor.sql           S3  — per-colocation / per-table / per-worker skew
│   ├── r1_rebalance_health.sql            R1  — rebalance & background-job health
│   ├── n6_metadata_sync_feasibility.sql   N6  — can citus_add_node actually succeed?
│   └── a3_2pc_backlog_advisor.sql         A3  — 2PC backlog & orphan prepared xacts
└── README.md
```

Each advisor is a **self-contained `psql` script**. You can run any one of
them on its own against a coordinator — no setup, no extensions to install,
no schema to create. Inputs are overridable via `-v key=value`.

---

## Requirements

- PostgreSQL **12+** with Citus **10+** (tested against Citus `main`
  on PG 17; works on Citus 11/12/13 with minor catalog differences).
- `psql` on `PATH` (or set `PSQL_BIN=/full/path/to/psql` — useful on macOS
  where Postgres.app / Homebrew / source builds often aren't on the
  system `PATH`).
- Connect as a role that can read Citus catalogs and call
  `run_command_on_workers()`. For a hardened role, `pg_monitor` +
  `citus_monitoring` (if present) is the minimum; a superuser is
  sufficient everywhere. A few GUCs used by the advisors are flagged
  `GUC_SUPERUSER_ONLY` (e.g. `citus.metadata_sync_mode`) and will
  silently fall back to defaults for non-superusers.
- Bash 4+, `awk`, `sed`, `grep`, `gzip` (standard on every Linux/macOS).

---

## Quick start

```bash
git clone https://github.com/<you>/citus_analyze.git
cd citus_analyze

# Point at your coordinator (any psql-compatible flag set works):
./citus_analyze.sh -h coord.example.com -p 5432 -d citus -U admin

# or via URI:
./citus_analyze.sh --uri "postgres://admin@coord.example.com:5432/citus"

# or via PG* env vars:
PGHOST=coord PGDATABASE=citus ./citus_analyze.sh
```

Output (everything lands in `./citus_analyze_<UTC-timestamp>/` by default):

```
output directory: ./citus_analyze_20260418T005526Z
connected OK. citus_version: Citus 12.1.x on x86_64-pc-linux-gnu ...
running citus_gather.sql ...
  gather OK: 31 sections,      463 lines, ~7 KB gzipped
running advisors ...

============================================================
 CITUS_ANALYZE EXECUTIVE SUMMARY
============================================================
  SEVERITY   ID    HEADLINE
  ---------  ----  -------------------------------------------
  [i] INFO     M1   Node memory minimum  -- INFO : pass --coord-ram-mb / --worker-ram-mb for pass/fail verdict. RECOMMENDED NODE RAM 3.92 GB (see M1.out).
  [+] OK       GR1  Shard/partition growth memory model  -- OK : target is at or below current footprint.
  [+] OK       C3   Max safe external connections (MX-aware)  -- OK : safe up to 136 concurrent external sessions under current config.
  [X] CRITICAL S3   Data skew across shards & workers  -- CRITICAL : colocation 11 has max/avg = 128.67 (>= 5.0). Isolate hot tenant or re-pick distribution key.
  [+] OK       R1   Rebalance / background-job health  -- OK : background-job queue is idle and clean.
  [+] OK       N6   Metadata-sync feasibility for add-node  -- OK : payload 0.1 MB < 50 MB. Either mode works; transactional (default) is fine.
  [+] OK       A3   2PC backlog & orphan prepared xacts  -- OK : no 2PC backlog.

  OVERALL: CRITICAL - at least one advisor returned CRITICAL.
```

Per-advisor full output lives in `./<out-dir>/{GR1,C3,S3,R1,N6,A3}.out`.
The raw cluster snapshot is `./<out-dir>/gather.out`.

### HTML report (pg_gather-style)

Any run directory can be turned into a single self-contained HTML report
(collapsible sections, traffic-light badges, per-advisor drill-down, every
snapshot section rendered as a browsable table) with the bundled renderer:

```bash
./render_html.py ./citus_analyze_20260418T005000Z
# -> ./citus_analyze_20260418T005000Z/report.html
```

Open `report.html` in any browser. No web server, no DB, no dependencies
beyond Python 3 stdlib — it parses the `### BEGIN:` / `### END :` markers
in `gather.out` and the advisor `*.out` files directly.

---

## Driver flags

```
./citus_analyze.sh [options]

  -h, --host HOST             coordinator host
  -p, --port PORT             coordinator port
  -d, --dbname DB             database
  -U, --username USER         role
      --uri URI               full libpq URI (overrides -h/-p/-d/-U)
  -o, --out-dir DIR           output directory (default: ./citus_analyze_<ts>)
      --psql PATH             path to psql binary (overrides PSQL_BIN env, default: psql on PATH)
      --coord-ram-mb MB       coordinator RAM in MB (enables M1 pass/fail verdict)
      --worker-ram-mb MB      per-worker RAM in MB  (enables M1 pass/fail verdict)
      --gather-only           run citus_gather.sql, skip advisors
      --advisors-only         run advisors, skip citus_gather.sql
      --help
```

Exit code:

| code | meaning                                   |
|------|-------------------------------------------|
| 0    | all advisors OK                           |
| 1    | at least one advisor returned WARN        |
| 2    | at least one advisor returned CRITICAL, or a fatal error occurred |

---

## The advisors

All advisors can be run standalone:

```bash
psql -X -f advisors/m1_node_memory_minimum.sql \
       -v coord_ram_mb=4096 -v worker_ram_mb=8192
psql -X -f advisors/gr1_shard_growth_advisor.sql
psql -X -f advisors/c3_max_external_connections.sql \
       -v headroom_pct=70 -v k_reuse=0.5 -v overhead=20
```

All of them accept `psql -v` overrides for their input assumptions (RAM per
node, link Mbps, future shard count, connection-pool reuse factor, etc).
Sensible defaults are picked from your live cluster.

### M1 — Node memory minimum (OOM-safety)

**The most important advisor.** Produces a concrete minimum- and
recommended-RAM number for each node (coordinator and every worker) so you
can size or audit hardware without guesswork — and catches the classic
Citus failure mode where raising shard count or partition count pushes a
node over the edge into OOM.

Model:

```
  MIN RAM (steady) = shared_buffers
                   + max_connections * per_backend_steady
                   + autovacuum_max_workers * maintenance_work_mem
                   + wal_buffers + temp_buffers * max_conn/4
                   + citus_outbound_pool        (MX entry nodes only)

  PEAK RAM (burst) = same but per_backend peaks at
                     (baseline + citus_meta + peak_query_mult * work_mem)
                   + 2 * maintenance_work_mem   (CREATE INDEX, VACUUM burst)
                   + max_parallel_workers * work_mem

  RECOMMENDED RAM  = PEAK RAM + max(1 GB, 10% OS/kernel reserve)
```

`per_backend_steady` grows with the Citus metadata cache, which scales
with placement count. **MX entry nodes cache every placement in the
cluster** (not just local placements) — this is why memory use balloons
on MX clusters as you add shards. M1 captures this correctly by using
`cached_placements` per node role.

Pass measured RAM with `-v coord_ram_mb=N -v worker_ram_mb=N` (or via the
driver's `--coord-ram-mb` / `--worker-ram-mb` flags) for a verdict:

| verdict  | condition                                                     |
|----------|---------------------------------------------------------------|
| CRITICAL | measured < PEAK RAM — node **will OOM** under burst load      |
| WARN     | measured < 1.5 × MIN RAM — no burst headroom, flappy           |
| OK       | measured ≥ PEAK RAM                                           |
| INFO     | measured RAM not supplied — advisory-only number printed       |

Example on the current live cluster (defaults, no measured RAM):

```
MIN RAM (steady peak)       : 2028 MB  ~  1.98 GB
PEAK RAM (worst-case burst) : 2988 MB  ~  2.92 GB
OS/kernel reserve (added)   : 1024 MB
RECOMMENDED NODE RAM        : 4012 MB  ~  3.92 GB
```

### GR1 — Shard / partition growth memory model

Models the per-node memory and lock-slot budget *as a function of proposed
shard count and partition count*. Answers "can I safely bump shard count
from 32 to 256?" — and if not, by exactly how many MB of RAM and how many
lock slots each worker would fall short.

**Lock-table math** (corrected against PG `storage/lmgr/lock.c`):

PostgreSQL's shared lock hash has capacity
`max_locks_per_transaction × (MaxBackends + max_prepared_xacts)`,
where `MaxBackends = max_connections + autovacuum_max_workers
+ max_wal_senders + max_worker_processes`. The per-backend quota in the
GUC name is an *average*, not a cap — a single backend can exceed it as
long as the total hash has room. Failure (`"out of shared memory; You
might need to increase max_locks_per_transaction"`) fires only when the
hash cannot admit a new entry.

GR1 therefore sizes by *peak cluster demand*:

```
peak_single_backend = proposed_shards × (1 + avg_indexes_per_shard
                                         + (write_mode ? 2 : 0))
                      # +2 for LockShardResource + LockShardDistributionMetadata
peak_cluster_demand = N_xshard × peak_single_backend
                    + (max_conn - N_xshard) × ordinary_locks_per_backend
                    × lock_safety_factor        # default 1.5
lpt_needed = ceil(peak_cluster_demand / (MaxBackends + max_prepared_xacts) / 64) × 64
```

Tunable knobs (via `-v`):
`avg_indexes_per_shard` (auto-detected from `pg_index`),
`fraction_cross_shard` (default 0.10),
`ordinary_locks_per_backend` (default 10),
`lock_safety_factor` (default 1.5),
`write_mode` (default 1).

Flags CRITICAL when:
- `lpt_needed > 2000` (unworkable max_locks_per_transaction)
- predicted lock-table shared memory delta exceeds 512 MB
- predicted worst-case backend RSS exceeds 2 GB

Reports the single number every field engineer actually needs:
**minimum safe `max_locks_per_transaction`** (rounded up to the nearest 64).

### C3 — Max safe external connections (MX-aware)

Closed-form model of the three concurrent connection constraints in a Citus
cluster:

1. **Per-entry-node inbound cap** — bounded by entry node's `max_connections`.
2. **Per-entry-node outbound fan-out** — bounded by
   `citus.max_shared_pool_size` across all workers.
3. **Per-target aggregate inbound** — every MX entry opens backend
   connections to every other worker, so the target worker's
   `max_connections - reserved` is *shared* across all MX entry nodes.

Correctly handles **MX mode** where any worker can be the entry point
(uses `(n_mx - 𝟙[target∈MX])` as the fan-in denominator, not `n_mx`).
All inputs — headroom %, connection-reuse factor, per-backend overhead —
are `-v` overridable for sensitivity analysis.

### S3 — Data skew across shards & workers

Three sub-checks:

- **S3a** — per-colocation group: max/avg shard-bytes ratio. ≥ 5 → CRITICAL.
- **S3b** — per-table top-N hot shards, with emitted remediation SQL
  (`SELECT isolate_tenant_to_new_shard(...)`).
- **S3c** — per-worker aggregate bytes distribution.

Uses the built-in `citus_shards` view — one round trip, no placement-level
fan-out required.

### R1 — Rebalance / background-job health

Inspects `pg_dist_background_job.state`, `pg_dist_background_task.status`,
and `get_rebalance_progress()`. Flags:

- tasks in `error` state with `retry_count ≥ 10` → CRITICAL (will not
  auto-retry further; needs manual intervention)
- tasks running > 240 min → CRITICAL (likely wedged)
- cleanup record backlog
- jobs in `failing`/`failed` state

Validated against an injected failure scenario (simulated stuck job with
max-retry error task).

### N6 — Metadata-sync feasibility for `citus_add_node`

This is the "will it actually work?" advisor for adding a node to an
established cluster. Models the metadata-sync payload as 9 components
(shell CREATEs, partition attachments, `pg_dist_*` rows, FK propagation,
pg_dist_object, schema-sharded metadata, etc) and predicts:

- **Lock-slot demand** on the candidate — the most common Citus add-node
  failure is `out of shared memory; raise max_locks_per_transaction`.
  Modelled as a **single transaction on the candidate** creating every
  shell table, every partition, and their indexes — so the peak is one
  backend holding
  `(dist_tables + ref_tables + partitions) × (1 + avg_indexes_per_shard)`
  relation locks. Required capacity is compared against the candidate's
  planned
  `max_locks_per_transaction × (MaxBackends + max_prepared_xacts)`
  (see PG `lock.c` `NLOCKENTS()`). Tunable via `-v candidate_max_lpt`,
  `candidate_max_prep`, `candidate_max_conn`, `avg_indexes_per_shard`,
  `lock_safety_factor`.
- **Candidate backend peak memory** — flagged if > 25% of candidate RAM.
- **Coordinator peak memory** during command-list materialisation
  (transactional mode only).
- **Wall-time** vs `citus.node_connection_timeout`.

Emits a mode recommendation (transactional vs nontransactional) and, if
add-node would fail AS-IS, the exact `max_locks_per_transaction` value to
raise to before attempting the add.

Overridable inputs let you sanity-check an add-node against a *proposed*
candidate's RAM / lock budget / timeout — not just current cluster reality.

### A3 — 2PC backlog & orphan prepared xacts

Detects prepared transactions on workers that the coordinator's
`pg_dist_transaction` table does not know about. These are **not** picked
up by Citus's maintenance daemon and will accumulate forever, silently
consuming `max_prepared_transactions` slots and WAL until they break the
cluster.

For each orphan, emits the exact node-specific remediation line:

```
On 10.0.1.7:5433 run:  ROLLBACK PREPARED 'citus_0_1234_99_...';
```

Also flags headroom on `max_prepared_transactions` (CRITICAL at ≥ 80%
used) and classifies per-node counts into `normal_citus / orphan_citus /
foreign_xacts`.

---

## The collector (`citus_gather.sql`)

Stand-alone — runs independently of the driver:

```bash
psql -X -A -q -f citus_gather.sql > gather.out 2>&1
```

Produces 31 sections bracketed by machine-parseable markers:

```
### BEGIN: <section_id>
<CSV with header>
### END : <section_id>
```

Each section can be extracted with:

```bash
awk '/^### BEGIN: cluster_topology$/,/^### END : cluster_topology$/' gather.out
```

Sections include: cluster topology, `pg_dist_partition`/`shard`/
`placement`/`object`/`colocation`/`schema`/`transaction`/`cleanup`/
`background_job`/`background_task`, `get_rebalance_progress()`,
`citus_shards` sizes, all `citus.*` GUCs + critical PG GUCs coordinator-
side, per-node GUCs + prepared-xacts via `run_command_on_workers`,
coordinator `pg_stat_activity`, `citus_dist_stat_activity`, partition
inventory, foreign-keys on distributed tables, `pg_stat_database`,
`pg_stat_bgwriter`, replication slots, roles, extensions, and (if
installed) top `pg_stat_statements` / `citus_stat_statements`.

Optional-extension sections are guarded with existence probes so the
script never errors on clusters that don't have them.

---

## Roadmap

The current set of six advisors is v0. Planned next tranche:

- **M1** — coordinator RAM baseline (shared_buffers + work_mem × backends +
  maintenance_work_mem × autovac workers + palloc overhead).
- **W1** — worker RAM baseline, per-backend peak with colocated joins.
- **C4/C5/C6** — client-side pool sizing recommendations, PgBouncer fit,
  `citus.max_client_connections` tuning.
- **G1..G3** — GUC drift detector (coord vs worker, known bad combinations).
- **N1..N5** — node-addition preflight: extension parity, type parity,
  role parity, `pg_dist_node` staleness, replication-slot presence.
- **S1/S2** — row-count skew and foreign-key induced colocation constraints.
- **R2** — rebalance strategy picker (by_shard_count vs by_disk_size vs custom).

---

## Development & contributing

The advisors are intentionally **plain SQL scripts, not PL/pgSQL functions
and not an extension**. This keeps them:

- runnable on *any* existing cluster without installation rights,
- auditable by reading — no compiled code,
- easy to override for testing (`psql -v` on every input).

Convention that every advisor follows:

- `\pset pager off; \pset border 2; \pset format aligned`
- inputs via `\if :{?var} \else \set var default \endif`
- all per-advisor state in `CREATE TEMP TABLE _<id>` (CTEs don't persist
  across statements)
- top-level verdict emitted in a final `\pset tuples_only on` block in the
  strict form `OK : …` / `WARN : …` / `CRITICAL : …` — the driver greps
  on that form, most-severe-wins.

If you add a new advisor:

1. drop it under `advisors/`,
2. register it in `ADVISORS=(...)` in `citus_analyze.sh`,
3. make sure it emits at least one top-level `(OK|WARN|CRITICAL) :` line.

---

## License

MIT.
