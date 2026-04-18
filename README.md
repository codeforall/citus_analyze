<div align="center">

<img src="assets/logo.svg" alt="citus_analyze" width="320"/>

# citus_analyze

**Health, sizing, and capacity-planning utility for Citus distributed PostgreSQL clusters**

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12--17-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Citus](https://img.shields.io/badge/Citus-10--13-336791?logo=postgresql&logoColor=white)](https://github.com/citusdata/citus)
[![Bash](https://img.shields.io/badge/Bash-4%2B-4EAA25?logo=gnubash&logoColor=white)](https://www.gnu.org/software/bash/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![Zero install](https://img.shields.io/badge/install-zero--config-4f46e5)](#quick-start)
[![Output](https://img.shields.io/badge/output-text%20%7C%20HTML%20%7C%20PDF-059669)](#output-formats)

[Quick start](#quick-start) •
[What it diagnoses](#what-it-diagnoses) •
[HTML report](#the-html-report) •
[Advisors](#advisor-catalogue) •
[Driver flags](#driver-flags--exit-codes)

</div>

---

Run one script against the coordinator and, in under a minute, get a
complete picture of cluster health — a snapshot of every catalog that
matters, 25 quantitative advisors that compute concrete numbers and
recommended actions, a traffic-light executive summary on stdout, and
(optionally) a self-contained HTML or PDF report you can share.

```bash
./bin/citus_analyze -h coord.example.com -d citus -f html
# -> reports/<run>/report.html    (plus per-advisor .out files)
```

`citus_analyze` is a **read-only, zero-install** diagnostic tool. It
needs nothing on the server side — no extensions, no schema, no agents.
Everything is a plain SQL script or Python stdlib. Point it at a
coordinator, read the output.

---

## Contents

- [What it diagnoses](#what-it-diagnoses)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Output formats](#output-formats)
- [The HTML report](#the-html-report)
- [Privacy & redaction](#privacy--redaction)
- [Advisor catalogue](#advisor-catalogue)
- [Highlighted diagnostics](#highlighted-diagnostics)
- [Running advisors standalone](#running-advisors-standalone)
- [The collector (`sql/gather.sql`)](#the-collector-sqlgathersql)
- [Driver flags & exit codes](#driver-flags--exit-codes)
- [Repository layout](#repository-layout)
- [Contributing](#contributing)
- [License](#license)

---

## What it diagnoses

Each advisor is a closed-form model, not a flag. You get a number, a
threshold, and the exact SQL or config change to apply.

| Capability | What you learn |
|---|---|
| **Memory sizing** | The *minimum* and *recommended* RAM for the coordinator and every worker, accounting for shard count, partition count, MX caching, and peak query multipliers. Pass measured RAM for a pass/fail verdict. |
| **Growth planning** | Before you raise shard or partition count, see exactly how many MB of RAM and how many lock-table slots each worker would fall short by. Turns "will this scale?" into a number. |
| **Connection capacity** | The maximum external client sessions the cluster can safely serve — jointly modelling coordinator/worker `max_connections`, `citus.max_shared_pool_size`, per-target fan-in, and pgbouncer pool sizing. |
| **Data skew** | Hot-shard and hot-worker detection across every colocation group, with the exact `SELECT` against `citus_shards` that identifies the offending tenant or key. |
| **Partition hygiene** | Days of future partitions on every time-partitioned table, time-bomb partitions (wrong direction, missing default), and the `create_time_partitions()` call to catch up. |
| **Add-node feasibility** | Before `citus_add_node`, predicts metadata payload size, lock-slot demand on the new node, transactional vs non-transactional mode choice, and whether it will fit within `citus.node_connection_timeout`. |
| **Rebalance & background jobs** | Stuck jobs, orphan background tasks, imbalanced placements, plus a preview of what the next rebalance would move. |
| **2PC safety** | Orphan prepared transactions the coordinator has forgotten about — silent consumers of `max_prepared_transactions` slots and WAL that the Citus maintenance daemon does *not* clean. |
| **Config audit (GUC)** | Side-by-side comparison of coordinator and worker GUCs against the Citus must-match / must-warn lists, plus PG defaults that matter for correctness (`max_prepared_transactions`, `lock_timeout`, etc.). |
| **WAL & checkpoint pressure** | `checkpoints_req` ratios, `wal_buffers_full` counters, and recommended `max_wal_size` / `checkpoint_timeout` / `wal_buffers` targets based on observed workload. |
| **Index health** | Per-shard aware: invalid indexes, unused distributed indexes (aggregated across every shard), duplicate indexes. |
| **Bloat & autovacuum** | Table/index bloat estimates, autovacuum lag, stale statistics, and the exact `VACUUM (ANALYZE)` command to run. |
| **Security** | Login roles without passwords (cross-referenced with `pg_hba` auth method so peer/trust isn't false-flagged), md5 usage, overly broad hba rules, SUPERUSER role sprawl. |
| **Network & replication** | RTT between every node pair, slot-holding replication lag, sender status. |
| **Disk runway** | Days until disk fill-up at the observed growth rate on every node (auto-discovered locally via `df` when possible, or pass `-v coord_disk_mb=N`). |
| **Version & upgrade readiness** | PG and Citus version skew across nodes, pending `ALTER EXTENSION UPDATE`, pointer to the official update channel. |

Each check emits one of `OK` / `INFO` / `WARN` / `CRITICAL`. The driver
rolls the worst verdict into an exit code so `citus_analyze` drops
straight into CI, cron, or monitoring pipelines.

---

## Requirements

- **PostgreSQL 12+** with **Citus 10+**. Tested against Citus `main` on
  PG 17; works on Citus 11 / 12 / 13 / 15 with minor catalog differences.
- `psql` on `PATH` (or set `PSQL_BIN=/full/path/to/psql` — useful on
  macOS where the server build often isn't on the system `PATH`).
- A role that can read Citus catalogs and call
  `run_command_on_workers()`. For a hardened role, `pg_monitor` plus
  the `citus_monitoring` grant (when available) is the minimum; a
  superuser works everywhere.
- Bash 4+, `awk`, `sed`, `grep`, `gzip` — standard on every Linux /
  macOS system.
- Python 3.8+ for the HTML renderer. **Standard library only**; no
  `pip install` required.
- *(Optional, for PDF export)* any one of `weasyprint`, `wkhtmltopdf`,
  or Chrome / Chromium. Auto-detected.

---

## Quick start

```bash
git clone https://github.com/<you>/citus_analyze.git
cd citus_analyze

# Point at your coordinator — any libpq flag set works:
./bin/citus_analyze -h coord.example.com -p 5432 -d citus -U admin

# ...or a libpq URI:
./bin/citus_analyze --uri "postgres://admin@coord.example.com:5432/citus"

# ...or PG* env vars:
PGHOST=coord PGDATABASE=citus ./bin/citus_analyze
```

Add `--output-format` (or `-f`) to render HTML and/or PDF in the same
run:

```bash
./bin/citus_analyze -h coord -d citus -f html        # + HTML report
./bin/citus_analyze -h coord -d citus -f html,pdf    # + HTML + PDF
./bin/citus_analyze -h coord -d citus -f all         # + HTML + PDF
```

You can always render an HTML report later from any existing run:

```bash
python3 lib/render_html.py reports/<run-dir>
open    reports/<run-dir>/report.html
```

For the strongest verdicts, pass measured RAM and disk so capacity
advisors can give you a pass/fail (they degrade to `INFO` otherwise):

```bash
./bin/citus_analyze \
    -h coord -d citus \
    --coord-ram-mb 16384  --worker-ram-mb 32768 \
    --coord-disk-mb 512000 --worker-disk-mb 2048000 \
    -f all
```

---

## Output formats

`--output-format` / `-f` takes a comma-separated list from
`text | html | pdf | all`.

| format | always produced | notes |
|---|:---:|---|
| **text**  | ✅ | executive summary on stdout + per-advisor `.out` files + `summary.txt` |
| **html**  | opt-in | single-file self-contained report (CSS + JS + data inlined, no external assets) |
| **pdf**   | opt-in | implies `html`; rendered via the first of `weasyprint`, `wkhtmltopdf`, `chromium`, `chrome` found on `PATH` (also detects Chrome.app / Chromium.app on macOS). If none are available, PDF is skipped with an install hint; text + HTML still succeed. |

Each run lands in its own directory, so re-running against different
clusters (or the same cluster over time) stays tidy:

```
reports/
├── 2026-04-18_14-28-58Z_coord.prod.example.com_5432/
├── 2026-04-18_14-31-02Z_coord.stage.example.com_5432/
└── 2026-04-18_15-45-10Z_coord.prod.example.com_5432/
```

Each run directory contains:

| file          | contents                                            |
|---------------|-----------------------------------------------------|
| `gather.out`  | 31 CSV-with-header sections — the cluster snapshot  |
| `gather.err`  | psql stderr from the collector                      |
| `<ID>.out`    | Per-advisor full output (25 files: `M1.out`, …)     |
| `summary.txt` | Executive summary, same text as stdout              |
| `report.html` | Self-contained HTML report *(opt-in)*               |
| `report.pdf`  | PDF rendering of `report.html` *(opt-in)*           |
| `pdf.log`     | Converter stdout/stderr from the PDF step           |

Sample executive summary on stdout:

```text
============================================================
 CITUS_ANALYZE EXECUTIVE SUMMARY
============================================================
  SEVERITY      ID     HEADLINE
  ---------     ----   -------------------------------------------
  [i] INFO      M1     Node memory minimum (OOM-safety)             -- INFO : pass --coord-ram-mb / --worker-ram-mb for a verdict.
  [+] OK        GR1    Shard/partition growth memory model          -- OK  : target is at or below current footprint.
  [!] WARN      C3     Max safe external connections (MX-aware)     -- WARN: fan-out ceiling 6 clients; raise citus.max_shared_pool_size.
  [X] CRITICAL  S3     Data skew across shards & workers            -- CRIT: colocation 11 has max/avg = 124.78 (>= 5.0). Isolate hot tenant.
  ...

  OVERALL: CRITICAL
```

---

## The HTML report

A single self-contained `.html` file — no server, no external CSS/JS,
no database required to view it. Open it locally, attach it to a
ticket, or host it on any static site.

**What you get on first scroll:**

- **Sticky top bar** with the overall-verdict chip and a Print / PDF
  action.
- **Sticky Cluster fingerprint strip** under the top bar — a ribbon of
  facts (database, PostgreSQL version, Citus version, node count,
  cluster mode, distributed tables, colocation groups, total shards,
  total data size) that stays visible while you scroll.
- **Cluster health report (hero):**
  - overall severity bar + traffic-light legend,
  - **Top things to fix** — the top 5 critical/warn findings with
    severity-tinted one-line fixes,
  - **Recommendations needing attention** — every critical + warn
    advisor that carries a quantitative target (e.g. `Recommended:
    ≥ 7.60 GB per node`) in a glanceable list, colour-coded by
    severity.
- **Collapsible left sidebar** grouping advisors by category (Memory,
  Connections, Data, Config, Performance, Ops, Security, Availability,
  Upgrade). Each item shows its severity dot.
- **Remediation playbook** — ordered list of every CRITICAL/WARN item
  with its recommended fix, copy-to-clipboard as plain text.
- **Filterable advisor summary** — severity chips, category chips,
  free-text search, show/hide passing checks toggle.
- **Per-advisor cards** — *What it checks*, *Why it matters*, *Current
  finding*, a prominent *Recommended action* pill (the exact
  quantitative target), and a collapsible full raw output.
- **Cluster snapshot section** — every `### BEGIN:` block from
  `gather.out` rendered as a browsable table with per-section CSV
  download and a live text filter.
- **Auto dark mode** via `prefers-color-scheme`.
- **Print stylesheet** — detail panes auto-expand, chrome is hidden,
  and the fingerprint strip unpins, so the report prints (or exports
  to PDF) cleanly.

---

## Privacy & redaction

`sql/gather.sql` redacts query text at collection time. Single-quoted
string literals in `pg_stat_activity.query`,
`pg_stat_statements.query`, `citus_dist_stat_activity.query`, and
`citus_stat_statements.query` are replaced with the token `<literal>`.
The SQL shape is preserved (so advisors and humans can still read
queries), but literal user data (emails, tokens, names, IDs embedded
as strings) never reaches `gather.out`. Doubled-quote escapes (`''`)
and `E'\\X'` escape strings are handled; dollar-quoted bodies and
numeric literals pass through.

GUC sections use explicit allowlists that exclude `primary_conninfo`,
`archive_command`, `restore_command`, and `ssl_passphrase_command`, so
credentials never appear in the output.

The HTML report carries a visible privacy banner that names the
redaction state, so reports are self-identifying when shared.

---

## Advisor catalogue

| ID    | Category       | Title                                               |
|-------|----------------|-----------------------------------------------------|
| M1    | Memory         | Node memory minimum (OOM-safety)                    |
| GR1   | Memory         | Shard / partition growth memory model               |
| D1    | Memory         | Disk capacity & shard-growth runway                 |
| C3    | Connections    | Max safe external connections (MX-aware)            |
| CP1   | Connections    | pgbouncer pool sizing                               |
| MX1   | Connections    | MX mesh connection budget                           |
| S3    | Data           | Data skew across shards & workers                   |
| SC1   | Data           | Shard-count right-sizing                            |
| REF1  | Data           | Reference-table health                              |
| P1    | Data           | Partition hygiene & maintenance runway              |
| P2    | Data           | Placement stats freshness                           |
| GUC1  | Config         | Citus + PostgreSQL configuration audit              |
| V1    | Upgrade        | Version & upgrade readiness                         |
| Q1    | Performance    | Long-running queries & lock waits                   |
| I1    | Performance    | Index health (per-shard aware)                      |
| B1    | Performance    | Table bloat & autovacuum lag                        |
| STAT1 | Performance    | Statistics freshness                                |
| W1    | Performance    | WAL & checkpoint pressure                           |
| R1    | Ops            | Rebalance / background-job health                   |
| R2    | Ops            | Rebalance plan preview                              |
| N6    | Ops            | Metadata-sync feasibility for `citus_add_node`      |
| A3    | Ops            | 2PC backlog & orphan prepared xacts                 |
| SEC1  | Security       | Security & role audit                               |
| NET1  | Availability   | Node reachability & latency                         |
| REP1  | Availability   | Streaming replication & slot lag                    |

---

## Highlighted diagnostics

A few of the checks worth calling out — these are the ones that
commonly turn "the cluster feels slow" or "the upgrade just failed"
into a clear, actionable number.

### M1 — Node memory minimum

The anchor advisor. Produces a concrete **minimum** and **recommended**
RAM number for the coordinator and each worker so you can size (or
audit) hardware without guesswork. Correctly models MX entry nodes
that cache every placement in the cluster, not just local ones.

```
MIN RAM   = shared_buffers + max_conn × per_backend_steady
          + autovacuum_max_workers × maintenance_work_mem
          + wal_buffers + citus_outbound_pool (MX only)

PEAK RAM  = MIN RAM but per_backend peaks at
            (baseline + citus_meta + peak_query_mult × work_mem)
          + 2 × maintenance_work_mem (VACUUM / CREATE INDEX burst)
          + excess global parallel-worker pool (when max_parallel_workers
            exceeds what backends already reserve)

RECOMMENDED = PEAK RAM + max(1 GB, 10% OS reserve)
```

Pass observed RAM with `--coord-ram-mb` / `--worker-ram-mb` (or via
`-v coord_ram_mb=N -v worker_ram_mb=N` when running the advisor
standalone) for a hard pass/fail verdict.

### GR1 — Shard / partition growth

Answers "can I bump shard count from 32 to 256?" — and if not, by
exactly how many MB of RAM and how many lock-table slots each worker
would fall short. Uses the correct PostgreSQL lock-table capacity
formula:

```
capacity = max_locks_per_transaction × (MaxBackends + max_prepared_xacts)
```

Sizes by peak cluster demand (not per-backend quota) and reports the
minimum safe `max_locks_per_transaction`, rounded to the nearest 64.

### C3 — Max safe external connections (MX-aware)

Closed-form model of the three concurrent-connection constraints every
Citus cluster faces:

1. **Per-entry-node inbound** (`max_connections`).
2. **Per-entry-node outbound fan-out** (`citus.max_shared_pool_size`).
3. **Per-target aggregate inbound** — a worker's free connection slots
   are shared across **all** MX entry nodes that can drive traffic
   into it.

Correctly handles MX mode with `(n_mx − 𝟙[target ∈ MX])` fan-in
accounting, and filters out coordinators with `shouldhaveshards = false`
from per-target aggregate math.

### N6 — Metadata-sync feasibility for `citus_add_node`

The "will it actually work?" advisor for adding a node. Models the
metadata payload as nine components (shell table CREATEs, partition
hierarchy, `pg_dist_*` rows, FK propagation, `pg_dist_object`,
schema-sharded metadata, …) and predicts:

- lock-slot demand on the new node,
- candidate backend peak memory,
- coordinator peak memory in transactional mode,
- wall-time vs `citus.node_connection_timeout`.

Emits an explicit transactional-vs-non-transactional recommendation
and, if add-node would fail, the exact `max_locks_per_transaction`
bump to apply first.

### A3 — 2PC backlog & orphan prepared xacts

Detects prepared transactions on workers that the coordinator's
`pg_dist_transaction` has forgotten about. These are **not** cleaned by
Citus's maintenance daemon — they silently consume
`max_prepared_transactions` slots and hold WAL forever. The advisor
emits the exact per-node remediation line:

```
On 10.0.1.7:5433 run:  ROLLBACK PREPARED 'citus_0_1234_99_...';
```

---

## Running advisors standalone

Every advisor is a plain `psql` script with its inputs overridable via
`-v key=value`. Run one in isolation without the driver:

```bash
psql -X -f sql/advisors/m1_node_memory_minimum.sql \
       -v coord_ram_mb=4096 -v worker_ram_mb=8192

psql -X -f sql/advisors/gr1_shard_growth_advisor.sql \
       -v proposed_shards=256 -v proposed_partitions=52

psql -X -f sql/advisors/c3_max_external_connections.sql \
       -v headroom_pct=70 -v k_reuse=0.5
```

See the header comment of each `sql/advisors/<id>_*.sql` file for the
full list of `-v` overrides it supports and the verdict thresholds it
applies.

---

## The collector (`sql/gather.sql`)

The collector runs independently of the driver — useful if you want to
ship a snapshot from a locked-down environment and analyse offline:

```bash
psql -X -A -q -f sql/gather.sql > gather.out 2>&1
```

It produces 31 CSV sections bracketed by machine-parseable markers:

```
### BEGIN: <section_id>
<CSV with header>
### END : <section_id>
```

Extract one section with:

```bash
awk '/^### BEGIN: cluster_topology$/,/^### END : cluster_topology$/' gather.out
```

Sections cover: cluster topology; `pg_dist_partition` / `shard` /
`placement` / `object` / `colocation` / `schema` / `transaction` /
`cleanup` / `background_job` / `background_task`;
`get_rebalance_progress()`; `citus_shards` sizes; all `citus.*` GUCs
plus critical PG GUCs on the coordinator; per-node GUCs + prepared
xacts via `run_command_on_workers`; coordinator `pg_stat_activity`;
`citus_dist_stat_activity`; partition inventory; foreign keys on
distributed tables; `pg_stat_database`; `pg_stat_bgwriter`;
replication slots; roles; extensions; and, if installed, top
`pg_stat_statements` / `citus_stat_statements`.

Optional-extension sections are guarded with existence probes so the
collector never errors on clusters that don't have them.

---

## Driver flags & exit codes

```
./bin/citus_analyze [options]

  -h, --host HOST             coordinator host
  -p, --port PORT             coordinator port
  -d, --dbname DB             database
  -U, --username USER         role
  -W, --password              force a password prompt up front
                              (cached for the whole run; never re-prompted)
  -w, --no-password           never prompt; rely on PGPASSWORD or ~/.pgpass
                              (suitable for CI)
      --uri URI               full libpq URI (overrides -h/-p/-d/-U)
  -o, --out-dir DIR           output directory
                              (default: ./reports/<ts>_<host>_<port>)
      --psql PATH             path to psql binary (default: psql on PATH;
                              overrides the PSQL_BIN env var)

  -f, --output-format LIST    comma-separated subset of
                              text | html | pdf | all
                              text is always produced; pdf implies html.

      --coord-ram-mb MB       coordinator RAM (enables M1 verdict)
      --worker-ram-mb MB      per-worker RAM (enables M1 verdict)
      --coord-disk-mb MB      coordinator data-dir size MB (enables D1)
      --worker-disk-mb MB     per-worker  data-dir size MB (enables D1)
                              (D1 auto-discovers via `df` on a local
                              loopback connection — override to force
                              a value or for remote coordinators.)

      --fail-on LEVEL         threshold that makes the driver exit
                              non-zero: none | warn (default) | critical

      --gather-only           run gather.sql, skip advisors
      --advisors-only         run advisors, skip gather.sql
      --help
```

### Authentication

`citus_analyze` honours every standard libpq mechanism — `PGHOST`,
`PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSERVICE`,
`PGSSLMODE`, `~/.pgpass`, etc. — and never invents its own.

If the server requires a password and none is cached, the driver
prompts **once** at startup and propagates the result via `PGPASSWORD`
to every subsequent advisor call. You will never be re-prompted
mid-run.

| Scenario                                | What to use                         |
|-----------------------------------------|-------------------------------------|
| Interactive run, password user          | nothing — answer the prompt         |
| Interactive run, force a fresh password | `-W` / `--password`                 |
| CI / scripted run                       | `PGPASSWORD=… citus_analyze … -w`   |
| CI with stored credentials              | `~/.pgpass` + `-w`                  |
| URI with embedded password              | `--uri "postgres://u:p@host/db"`    |

```bash
# Interactive: server requires a password — prompted once.
./bin/citus_analyze -h coord -d citus -U admin

# Force a prompt up front (ignore PGPASSWORD/.pgpass).
./bin/citus_analyze -h coord -d citus -U admin -W

# CI: never prompt; fail fast if no password is available.
PGPASSWORD="$DB_PWD" ./bin/citus_analyze -h coord -d citus -U admin -w
```

### Exit codes

| code | meaning                                                        |
|-----:|----------------------------------------------------------------|
| `0`  | every advisor OK (or worst severity below `--fail-on`)         |
| `1`  | at least one advisor returned WARN (when `--fail-on warn`)     |
| `2`  | at least one advisor returned CRITICAL, or a fatal error occurred |

The default `--fail-on warn` is the right choice for CI. Use
`--fail-on critical` if you only want to block on hard failures; use
`--fail-on none` to always exit `0` (the text/HTML report still
captures everything).

---

## Repository layout

```
citus_analyze/
├── README.md
├── LICENSE
├── assets/
│   ├── icon.svg              # square brand mark (color)
│   ├── logo.svg              # icon + wordmark (color)
│   └── favicon.svg           # 32x32 favicon mark
├── bin/
│   └── citus_analyze         # driver (bash)
├── lib/
│   └── render_html.py        # HTML report generator (Python stdlib)
├── sql/
│   ├── gather.sql            # collector — 31 CSV sections
│   └── advisors/             # 25 advisor .sql files
└── reports/                  # default output directory (gitignored)
```

Each advisor is a **self-contained `psql` script**. You can run any one
of them standalone against a coordinator — no setup, no extensions to
install, no schema to create.

---

## Contributing

The advisors are deliberately **plain SQL, not PL/pgSQL functions and
not an extension**. That keeps them:

- runnable on any existing cluster without installation rights,
- auditable by reading — no compiled code,
- easy to override for testing (`psql -v` on every input).

Conventions every advisor follows:

- `\pset pager off; \pset border 2; \pset format aligned`
- Inputs via `\if :{?var} \else \set var default \endif`
- All per-advisor state in `CREATE TEMP TABLE _<id>` (CTEs don't
  persist across statements).
- Top-level verdict in a final `\pset tuples_only on` block in the
  strict form `OK : …` / `INFO : …` / `WARN : …` / `CRITICAL : …` —
  the driver greps on that form, most-severe-wins.

If you add a new advisor:

1. Drop the `.sql` file under `sql/advisors/`.
2. Register it in the `ADVISORS=(...)` array in `bin/citus_analyze`.
3. Add metadata (category, checks, matters, fix, docs URL) to the
   `ADVISOR_META` and `ADVISORS` tables in `lib/render_html.py` so it
   shows up in the HTML report.
4. Emit at least one top-level `(OK|INFO|WARN|CRITICAL) :` line so the
   driver can surface its verdict.

---

## License

MIT. See [LICENSE](LICENSE).
