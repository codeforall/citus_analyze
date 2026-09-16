# citus_analyze

One-shot diagnostics and capacity-planning scenarios for Citus/PostgreSQL.
The driver collects a coordinator snapshot, runs 25 live SQL advisors, and
produces text, structured JSON findings, and optionally self-contained HTML/PDF.

**Observations are not guarantees.** Capacity estimates use explicit assumptions;
an OK finding means an audited rule was not triggered in available evidence, not
that every aspect of the cluster is healthy. Unknown coverage is reported separately.

## Quick Start

```bash
./bin/citus_analyze -h coord.example.com -p 5432 -d postgres -U monitor -w -f html
```

Use libpq environment variables, a service, or a protected password file for
authentication. `-W` prompts once; `-w` never prompts. Avoid embedding passwords
in a URI or shell command. Both `--option value` and `--option=value` work.

```bash
./bin/citus_analyze --psql "$HOME/work/community/installed/pg-17/bin/psql" \
  -h coord.example.com -d postgres -U monitor -w --output-format=html
```

Each run uses a new output directory. Existing runs are not overwritten.
Reports are offline artifacts; rendering an old bundle does **not** rerun SQL
or correct the measurements made by older advisor implementations.

The HTML summary uses plain-language descriptions and next steps. Status labels
are **Urgent**, **Review**, **Information**, **No issue found**, and **Not fully
checked**. Exact SQL findings remain under Technical details; JSON severity values
and command-line exit policies are unchanged.

## Requirements And Compatibility

- Bash, Python 3.8+ (standard library only), `psql` with `\if` support, and standard Unix utilities.
- A Citus coordinator connection with catalog visibility and access to the
  relevant Citus inspection functions. Several advisors create temporary tables;
  the role needs database TEMP privilege. Monitoring grants vary by version/provider.
- Authentication/HBA inspection requires additional privileges. Denied checks
  remain incomplete; the tool does not grant privileges or conceal denied access.
- Optional PDF conversion needs WeasyPrint, wkhtmltopdf, or Chrome/Chromium.

There is **no global Citus-version pin**. Advisors check required functions,
relations and selected columns in [sql/capabilities.sql](sql/capabilities.sql).
An unavailable feature produces an explicit unsupported/incomplete result;
other advisors continue. PostgreSQL/Citus catalog or signature differences not
covered by a guard are captured as collection errors, never silently OK.

This is best-effort capability-based compatibility, **not a certification of
every Citus/PostgreSQL combination**. The collector also retains partial output
and records SQL errors when a catalog is unavailable. Extend capability guards
and regression fixtures when adding support for additional versions.

## Safety And Privacy

The tool does not execute remediation commands or modify application data.
It uses temporary tables, session settings, catalog/statistics reads, Citus
fan-out probes and the rebalance plan-preview API. These can consume CPU,
connections, locks and IO; large-cluster collection is not free.

The driver bounds each statement to 120 seconds and lock acquisition to 5 seconds.
Initial libpq connection timeout defaults to 10 seconds (`PGCONNECT_TIMEOUT` may
override it). Direct standalone SQL runs should set their own limits.

New collections omit query bodies and background command/message text rather
than attempting regex sanitization. GUC collection uses restricted setting lists;
archive commands are represented only as configured/empty. Password hashes are
never collected. **The entire bundle is still confidential:** identities, object
names, topology and error messages can contain sensitive information. New files
use a restrictive umask. Review all artifacts before sharing. Older bundles may
contain unredacted query bodies and are not sanitized by the new renderer.

Prepared transactions and replication slots are never automatically committed,
rolled back, or removed. Their ownership and recovery state require independent
verification. A missing recovery record alone is not a safe rollback decision.

## Results And Exit Codes

Each advisor produces `.out`, `.err`, `.status` (psql exit status), and `.json`.
JSON includes `schema_version`, `advisor`, `severity`, `headline`, `findings`,
`collection_status`, and coverage/error notes. SQL errors and failed/missing
node probes remain visible even when other nodes have known findings.

| Code | Meaning |
| --- | --- |
| 0 | No findings meeting the selected failure policy; collection completed. |
| 1 | WARN under `--fail-on warn`, or INFO under `--fail-on info`. |
| 2 | CRITICAL under the selected policy, or incomplete/failed collection, invalid input, or rendering failure. |

`--fail-on` accepts `warn` (default), `critical`, `info`, or `none`.
`none` suppresses failure **for findings only**, not execution/coverage failures.
An unsupported advisor is a coverage limitation, not a diagnosis that Citus is unhealthy.

Use `--gather-only` or `--advisors-only` to restrict collection. Text is always
written; `-f html`, `-f pdf`, and `-f all` select additional artifacts.
To rerender existing evidence:

```bash
python3 lib/render_html.py reports/<run-directory>
```

## Capacity Scenarios

`--advisor-var name=number` forwards a repeatable numeric scenario/policy input.
Inputs apply to relevant advisors only; supported names and defaults are declared
at the beginning of each SQL file. Non-numeric inputs can be passed to standalone
`psql -v` after reviewing the SQL parameter contract.

### Memory

M1 collects settings independently on each node. Its operator budget is:

```text
(active leaders + globally capped parallel workers)
  * work_mem * (sort operators + hash operators * hash_mem_multiplier)
```

Connected-backend overhead, shared buffers, effective autovacuum memory,
maintenance operations, temp-table users, outbound connections, other memory
and reserve are separate components. `max_connections` is a limit, not active
concurrency. Defaults use observed concurrency and produce INFO, not a peak
capacity verdict. Supplied RAM is user input, not measured RAM. Units are MiB/GiB.

```bash
./bin/citus_analyze -h coord.example.com -d postgres -w -f html \
  --coord-ram-mb=32000 --worker-ram-mb=32000 \
  --advisor-var=m1_peak_connected=200 --advisor-var=m1_peak_active=20 \
  --advisor-var=m1_sort_ops=1 --advisor-var=m1_hash_ops=1 \
  --advisor-var=m1_temp_sessions=2 --advisor-var=m1_outbound_connections=80 \
  --advisor-var=m1_other_memory_mb=1024
```

These example counts are **not recommended production values**. Supply measured
peak scenarios for your workload. They apply uniformly while settings remain
per-node; heterogeneous workloads need separate node-specific evaluation.
Old `plan_ops_per_query`/`peak_query_mult` sizing is superseded by explicit
`m1_sort_ops` and `m1_hash_ops`. No numeric minimum or inevitable OOM is asserted.

### Memory-Based Connection And Growth Estimates

The report also answers two separate planning questions:

1. **How many database connections fit?** M1 works backward from provided RAM
  and the estimated memory cost of each connection. It counts busy queries and
  caps parallel workers globally. The result cannot exceed `max_connections`
  minus reserved slots. It includes internal Citus sessions, not just application
  clients, and is not a limit on the number of users.
2. **How many more tables or shards fit at the same load?** M1 keeps the selected
  peak workload fixed and estimates the extra table/index tracking and data-cache
  memory for similar new shards. The server with the least room limits growth
  across the cluster. It does not sum each server's growth allowance.

Both estimates leave a **20% planning buffer** by default, in addition to the
operating-system reserve. A connection limit is a memory-only planning estimate,
not a promise that all those connections will perform well. Validate with a load
test and the connection-budget checks: CPU, disk, locks and network work can
limit capacity before RAM does. Do not add the connection and growth allowances
together; they use the same memory.

| Input | Meaning | Default |
| --- | --- | --- |
| `m1_capacity_active_pct` | Percentage of connections running queries at once when estimating the connection limit. | 100 (all busy) |
| `m1_capacity_headroom_pct` | Percentage of provided RAM left unused for this planning estimate, before the OS reserve. | 20 |
| `m1_internal_connections` | Peak database connections reserved for incoming/internal Citus work when calculating application capacity. | Unknown (-1) |
| `m1_other_client_connections` | External connections outside the application being sized, including administrative sessions. | 0 |
| `m1_growth_cache_pct` | Percentage of distributed data you expect to keep in memory. Required for table/shard growth. | Unknown |
| `m1_growth_shard_mb` | On-disk MiB per new shard, including indexes. | Average of current distributed shard copies |
| `m1_growth_shards_per_table` | Shards per new distributed table. | Current average, rounded up |
| `k_relation_cache_bytes` | Assumed memory per table/index tracking entry per connection. | 8192 bytes |

For example, this estimates capacity with 20% of connections busy at once, and
table growth with 200 connected / 40 busy sessions kept fixed:

```bash
./bin/citus_analyze -h coord.example.com -d postgres -w -f html \
  --coord-ram-mb=32000 --worker-ram-mb=32000 \
  --advisor-var=m1_peak_connected=200 --advisor-var=m1_peak_active=40 \
  --advisor-var=m1_capacity_active_pct=20 \
  --advisor-var=m1_capacity_headroom_pct=20 \
  --advisor-var=m1_growth_cache_pct=25 \
  --advisor-var=m1_growth_shard_mb=1024 \
  --advisor-var=m1_growth_shards_per_table=32
```

The example assumes new shards grow to 1 GiB and one quarter of distributed data
is cached. These are **example assumptions, not recommended values**. Provide
appropriate maintenance, temporary-table, outbound and other memory allowances
as described above. Values such as `m1_other_memory_mb` stay fixed when solving
for more connections; increase them if those costs grow with workload.

Current database size is displayed for context, not treated as a requirement to
fit the whole database in RAM. When a cache percentage is supplied, existing
distributed data's cache target above `shared_buffers` is reserved separately;
new data adds its full selected cache share for conservative planning. Reference
copies and non-distributed data are not extrapolated as distributed-table growth.
New shards follow the current copy distribution and average index count.

Missing RAM, failed nodes or invalid inputs suppress connection estimates. Table
growth additionally needs explicit peak connection counts, a cache assumption
and complete shard sizes. An empty cluster cannot provide a representative growth
estimate. Growth inside existing shards, different query complexity, future
placement changes and practical catalog limits require a separate test. These
calculations do **not** predict the exact point where the server runs out of memory.

The SQL source is [sql/memory_capacity.sql](sql/memory_capacity.sql). Its structured
results are available under `analysis` in M1's JSON output. Older report bundles
must be collected again to obtain these numbers.

M1 keeps **total database capacity** separate from **application-client capacity**.
The second figure is for regular, non-superuser clients and is calculated as:

```text
application limit = max(0, min(
  total memory/PG-slot limit - internal allowance - other-client allowance,
  effective Citus client cap - other-client allowance
))
```

When the Citus cap is disabled, only the first bound applies. The total limit
already accounts for PostgreSQL reserved slots and memory buffers. These are
total application connections, not extra connections above current usage.
Without a whole-number internal allowance or a known Citus client-cap state,
the application estimate remains unknown while the total memory estimate can
still be shown. For example, add `--advisor-var=m1_internal_connections=80`
and `--advisor-var=m1_other_client_connections=5` using your measured peak needs.
Allowances apply to each node; differing per-node workloads need separate runs
or a conservative common allowance. Internal connections can grow with client
traffic, so validate this fixed allowance against C3/MX1 and a load test.

M1 JSON includes `application_connection_limit`, `citus_client_setting`,
`citus_client_default`, `citus_client_limit`, `citus_client_limit_status`,
`internal_connection_allowance` and `other_client_allowance` for every node.
An unknown application limit does not invalidate otherwise available memory data.

### Connections And Pooling

C3, MX1 and CP1 share [sql/connection_budget.sql](sql/connection_budget.sql).
It models source sessions, connections per session/target, cached connections,
per-peer throttling, backend reservations and effective Citus client limits.
Non-MX shard targets still receive fan-in. C3 and MX1 warn when proposed/observed
external demand exceeds the resulting budget; CP1 uses it for pool sizing and
includes reserve pools. Missing client-limit data produces INCOMPLETE rather
than an unlimited budget. Routing and pool behavior remain planning scenarios,
not certified safe concurrency ceilings.

The shared resolver is [sql/client_limit.sql](sql/client_limit.sql):

| `citus.max_client_connections` | Interpretation |
| --- | --- |
| Positive integer | Per-server external-client cap, shared by all databases/applications. |
| `0` | No regular client connections allowed. **Not automatic.** |
| `-1` | Citus client cap disabled; PostgreSQL limits still apply. Also the verified upstream default. |
| Missing, unrecognized, or less than `-1` | Unknown; do not assume unlimited. |

These semantics are verified in upstream Citus 13.3's
[setting registration and authentication hook](https://github.com/citusdata/citus/blob/v13.3.0/src/backend/distributed/shared_library_init.c)
and tested against the local Citus 15.0devel build. This is different from
`citus.max_shared_pool_size=0`, which selects an automatic pool size.
Internal Citus connections do not count against the external cap. Superusers
are exempt from rejection, but external administrative sessions still contribute
to the external count. The advisors budget for ordinary application roles, not
superuser exceptions. They record the value/default seen by collection sessions;
provider or role-specific behavior still needs validation for that deployment.

CP1 needs actual `(database,user)` pool groups and pooler-instance counts. Its
budget includes reserve pools; desired demand can be lower than the maximum
budget. No ready-to-deploy PgBouncer config is generated from a snapshot.

```bash
./bin/citus_analyze -h coord.example.com -d postgres -w -f html \
  --advisor-var=connection_sessions_per_entry=20 \
  --advisor-var=connections_per_session=2 \
  --advisor-var=cached_connections_per_peer=10 \
  --advisor-var=pool_groups=4 --advisor-var=pooler_instances=2 \
  --advisor-var=cp1_peak_backend_demand=40
```

Session versus transaction pooling depends on application semantics. Protocol
prepared statements in transaction mode require appropriate PgBouncer support
and configuration, not a PostgreSQL-major threshold.

### Disk And Growth

D1 does not infer filesystem free space from database size, or physical growth
from tuple updates/deletes. Supply filesystem measurements and a measured trend:

```bash
./bin/citus_analyze -h coord.example.com -d postgres -w -f html \
  --coord-disk-mb=102400 --worker-disk-mb=102400 \
  --advisor-var=coord_disk_free_mb=40000 \
  --advisor-var=worker_disk_free_mb=50000 \
  --advisor-var=disk_growth_mb_per_day=500
```

Uniform worker inputs are unsuitable for heterogeneous disks. Unknown free
space/growth stays unknown. Rebalance throughput is an idealized scenario, not
an ETA or a filesystem-capacity check.

GR1 standalone growth inputs use these names:

```bash
psql -X -v ON_ERROR_STOP=on -f sql/advisors/gr1_shard_growth_advisor.sql \
  -v target_total_shards=256 -v target_partitions_per_parent=52
```

N6 models metadata inventory and candidate assumptions. Connection-establishment
timeout is not metadata-execution timeout. It does not mandate nontransactional
mode from a payload threshold; consult the installed version's documented
recovery procedure and atomicity tradeoffs after diagnosing an actual failure.

## Advisor Catalogue

| ID | Evidence And Scope |
| --- | --- |
| M1 | Per-node memory scenarios, not minimum-RAM certification. |
| D1 | Database size and supplied filesystem/growth scenarios. |
| C3 / CP1 / MX1 | Shared connection and pool-budget scenarios. |
| GUC1 | Per-node configuration rules and policy/semantic drift. |
| V1 | Loaded versions, installed SQL and per-worker extension presence. |
| Q1 | Sessions and lock waits; cancellation requires workload review. |
| P1 | Partition coverage including DEFAULTs and empty parents. |
| I1 | Index state/usage and possible redundancy, with dependency checks. |
| B1 | Estimated dead tuples and vacuum observations, not physical bloat. |
| STAT1 | ANALYZE history and churn; age alone does not prove stale estimates. |
| SEC1 | Scoped roles/authentication with privileged-probe coverage. |
| NET1 | Connectivity matrix and batch duration, not per-node RTT. |
| REP1 | Visible senders/slots and retention, not proof of HA topology. |
| S3 | Per-table shard sizes and physical bytes on eligible nodes. |
| R1 | Active/recent job evidence; repeated progress is needed for stalls. |
| R2 | Actual selected rebalance moves and live-size scenarios. |
| REF1 | Required reference copies and physical-size policy, not row equality. |
| GR1 / N6 | Explicit growth and metadata-sync planning assumptions. |
| A3 | Prepared-transaction age/capacity and scoped record visibility. |
| SC1 | Colocated bucket targets and physical shard-count scenarios. |
| P2 | Recorded/live placement discrepancy; not proof of a bad cost model. |

## Tests

Offline result/CLI tests:

```bash
python3 -B -m unittest discover -s tests -v
```

SQL tests create three disposable loopback-only nodes using an existing
PostgreSQL installation with Citus. They use fresh data directories and ports,
never an existing cluster, and shut down the fixture nodes afterward:

```bash
CITUS_TEST_BINDIR="$HOME/work/community/installed/pg-17/bin" \
  python3 -B -m unittest discover -s tests -v
```

Set `CITUS_TEST_ADVISORS=M1,A3` to narrow the advisor smoke loop. Behavioral and
full-driver tests still run. Set `CITUS_TEST_REPORT` to a new directory to retain
the fixture's HTML/JSON bundle. Repeat with other installed Citus/PostgreSQL
builds to extend verified compatibility. Tests include failure handling,
memory worker limits, fan-in, skew, partition defaults, age/churn, and rendering.

## Layout

- [bin/citus_analyze](bin/citus_analyze): connection handling, collection and exit policy.
- [sql/gather.sql](sql/gather.sql): marker-delimited snapshot sections.
- [sql/advisors](sql/advisors): live advisor SQL.
- [sql/capabilities.sql](sql/capabilities.sql): feature guards.
- [sql/advisor_coverage.sql](sql/advisor_coverage.sql): failed/missing probe reporting.
- [lib/advisor_result.py](lib/advisor_result.py): shared text/JSON result interpretation.
- [lib/recommendations.py](lib/recommendations.py): scoped guidance without healthy fallbacks.
- [lib/render_html.py](lib/render_html.py): offline HTML report.

## License

[MIT](LICENSE).