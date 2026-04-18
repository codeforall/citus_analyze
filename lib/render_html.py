#!/usr/bin/env python3
"""
render_html.py -- convert a citus_analyze run directory into a single
self-contained HTML report (pg_gather-style, stdlib-only).

Usage
    ./render_html.py ./citus_analyze_20260418T005000Z
    ./render_html.py ./citus_analyze_20260418T005000Z -o report.html

Reads
    <run-dir>/summary.txt
    <run-dir>/gather.out        -- CSV sections bracketed by ### BEGIN: / ### END :
    <run-dir>/{M1,GR1,C3,S3,R1,N6,A3}.out  -- advisor outputs (psql aligned format)

Writes
    <run-dir>/report.html       (or path given by -o)

No external dependencies (stdlib csv + html + re only).
"""
from __future__ import annotations
import argparse, csv, html, io, re, sys, urllib.parse
from pathlib import Path
from datetime import datetime, timezone


# --- advisor metadata & recommendation extractors --------------------------

def _strip_pipe_table(text: str) -> str:
    """Strip psql aligned-format '|' borders so regexes can match cleanly."""
    return re.sub(r'^\s*\|\s?', '', text, flags=re.M).replace('|', ' ')


def _rec_m1(text: str) -> str:
    m = re.search(r'RECOMMENDED NODE RAM\s*:\s*([\d.]+\s*MB)\s*~\s*([\d.]+\s*[KMG]B)', text)
    if m:
        return f'≥ {m.group(2).strip()} per node ({m.group(1).strip()})'
    return ''


def _rec_gr1(text: str) -> str:
    m = re.search(r'max_locks_per_transaction\s*>=\s*(\d+)\s*\(current\s+(\d+)', text)
    if m:
        lpt, cur = m.group(1), m.group(2)
        if cur == lpt:
            return f'max_locks_per_transaction ≥ {lpt} (current — OK)'
        return f'max_locks_per_transaction ≥ {lpt} (current {cur})'
    m = re.search(r'max_locks_per_transaction\s*>=\s*(\d+)', text)
    if m:
        return f'max_locks_per_transaction ≥ {m.group(1)}'
    return ''


def _rec_c3(text: str) -> str:
    m = re.search(r'MAX SAFE EXTERNAL CONCURRENCY\s*:\s*(\d+)', text)
    if m:
        return f'≤ {m.group(1)} concurrent external sessions'
    m2 = re.search(r'safe up to (\d+) concurrent', text)
    if m2:
        return f'≤ {m2.group(1)} concurrent external sessions'
    return ''


def _rec_s3(text: str) -> str:
    # Extract the worst max/avg for any colocation group, surface target
    stripped = _strip_pipe_table(text)
    vals = []
    for m in re.finditer(r'\bmax/avg\s*=\s*([\d.]+)', text):
        try:
            vals.append(float(m.group(1)))
        except ValueError:
            pass
    if vals:
        return f'shard max/avg ≤ 2.0 per colocation group (current worst {max(vals):.2f})'
    return 'shard max/avg ≤ 2.0 per colocation group'


def _rec_r1(text: str) -> str:
    if re.search(r'\bCRITICAL\b|\bWARN\b', text):
        return '0 failing jobs, 0 stuck rebalance steps'
    return '0 failing jobs (currently clean)'


def _rec_n6(text: str) -> str:
    m = re.search(r'Raise candidate max_locks_per_transaction to (\d+)', text)
    if m:
        return f'candidate max_locks_per_transaction ≥ {m.group(1)} before citus_add_node'
    m2 = re.search(r'suggested lpt=(\d+).*?planned\s+(\d+)', text)
    if m2:
        need, planned = m2.group(1), m2.group(2)
        if need == planned:
            return f'candidate max_locks_per_transaction ≥ {need} (current — OK)'
        return f'candidate max_locks_per_transaction ≥ {need} (planned {planned})'
    if 'nontransactional' in text and 'prefer' in text.lower():
        return 'citus.metadata_sync_mode = nontransactional before adding the node'
    return 'current transactional sync is safe'


def _rec_a3(text: str) -> str:
    m = re.search(r'(\d+)\s+orphan(?:ed)?\s+2PC', text)
    if m and m.group(1) != '0':
        return f'0 orphaned 2PC records (currently {m.group(1)})'
    return '0 orphaned 2PC records'


def _rec_sc1(text: str) -> str:
    m_cur = re.search(r'current_total_shards[^\d]*(\d+)', text)
    m_rec = re.search(r'recommended_total_shards[^\d]*\d+\s*\|\s*(\d+)', text)
    if m_cur and m_rec:
        return f'{m_rec.group(1)} shards (currently {m_cur.group(1)})'
    return '1-2x worker count per colocation group'


def _rec_p2(text: str) -> str:
    m = re.search(r'stale_zero_count[^\d]*(\d+)', text)
    if m and m.group(1) != '0':
        return f'0 stale placements (currently {m.group(1)})'
    return '0 stale placements'


def _rec_mx1(text: str) -> str:
    # Report worst (smallest) client_slots across MX nodes.
    slots = [int(m.group(1)) for m in re.finditer(r'\|\s*(\d+)\s*\|\s*\n?\s*\+', text)]
    crit = re.search(r'critical_nodes[^\d]*\d+\s*\|\s*(\d+)', text)
    if crit and crit.group(1) != '0':
        return f'0 MX nodes with <10 client slots (currently {crit.group(1)})'
    return '>=20 client slots free on every MX node'


def _rec_d1(text: str) -> str:
    # Extract the minimum runway observed across nodes.
    runways = [int(m.group(1))
               for m in re.finditer(r'runway at current rate\s*:\s*(\d+)\s*days', text)]
    free_pcts = [float(m.group(1))
                 for m in re.finditer(r'(-?[\d.]+)%\s*of disk', text)]
    parts = []
    if runways:
        parts.append(f'runway ≥ 90 days (current worst {min(runways)})')
    if free_pcts:
        parts.append(f'≥ 20% free (current worst {min(free_pcts):.1f}%)')
    if parts:
        return '; '.join(parts)
    return '≥ 20% free and ≥ 90 days runway per node'


def _rec_q1(text: str) -> str:
    # Summarise current session pressure from the headline-ish lines.
    # Look for "N long active + M idle-in-tx session(s); K cross-node lock wait(s)"
    m = re.search(r'(\d+)\s+long active\s*\+\s*(\d+)\s+idle-in-tx.*?(\d+)\s+cross-node lock wait',
                  text)
    if m:
        la, iit, lw = m.group(1), m.group(2), m.group(3)
        return (f'0 queries > 5 min; 0 idle-in-tx > 5 min; 0 cross-node lock waits '
                f'(currently {la} / {iit} / {lw})')
    if re.search(r'no long-running queries or idle-in-tx sessions', text):
        return '0 queries > 5 min; 0 idle-in-tx > 5 min; 0 cross-node lock waits'
    return '0 queries > 5 min; 0 idle-in-tx > 5 min; 0 cross-node lock waits'


def _rec_guc1(text: str) -> str:
    # Look for CRITICAL / WARN counts in the headline.
    m = re.search(r'(\d+)\s+rule violation\(s\);\s*(\d+)\s+drift\(s\);\s*(\d+)\s+warning',
                  text)
    if m:
        return (f'0 rule violations; 0 drift; 0 warnings '
                f'(currently {m.group(1)} / {m.group(2)} / {m.group(3)})')
    m = re.search(r'(\d+)\s+rule warning\(s\);\s*(\d+)\s+cross-node drift', text)
    if m:
        return (f'0 rule violations; 0 drift; 0 warnings '
                f'(currently 0 / {m.group(2)} / {m.group(1)})')
    if re.search(r'no drift and no rule violations', text):
        return '0 rule violations; 0 drift; 0 warnings'
    return '0 rule violations; 0 drift; 0 warnings'


def _rec_v1(text: str) -> str:
    if re.search(r'matching PG major and Citus versions, no pending upgrades', text):
        return 'all nodes matching PG major + Citus version; no pending upgrades'
    if re.search(r'Citus BINARY package version differs', text):
        return 'homogeneous Citus binary package version (currently DRIFTED)'
    if re.search(r'Citus extension SQL version differs', text):
        return 'homogeneous Citus extension version (currently DRIFTED)'
    if re.search(r"missing 'citus' from shared_preload_libraries", text):
        return "'citus' in shared_preload_libraries on every node"
    if re.search(r'PostgreSQL MAJOR version differs', text):
        return 'same PostgreSQL major on every node'
    if re.search(r'pending ALTER EXTENSION citus UPDATE', text):
        return '0 nodes with pending ALTER EXTENSION citus UPDATE'
    if re.search(r'missing on at least one worker', text):
        return 'coord extensions fully present on every worker'
    if re.search(r'could upgrade Citus to a newer available version', text):
        return 'installed Citus = latest available (informational)'
    return 'all nodes matching PG major + Citus version; no pending upgrades'


def _rec_p1(text: str) -> str:
    if re.search(r'no partition covering now\(\)', text):
        return 'every time-partitioned table covers now() or has a DEFAULT partition'
    if re.search(r'future partition gap', text):
        return '0 future partition gaps (no imminent write failure)'
    if re.search(r'partition range overlap', text):
        return 'no overlapping partition ranges (catalog clean)'
    if re.search(r'less than \S+ days of future partitions', text):
        return 'sufficient future partition runway for every time-partitioned table'
    if re.search(r'historical partition gap', text):
        return 'no historical partition gaps'
    if re.search(r'exceed partition/shard limits', text):
        return 'no partitioned table exceeds the partition/shard width limits'
    if re.search(r'further than \S+ days in the future', text):
        return 'no over-premade future partitions'
    if re.search(r'time_partitions view not available', text):
        return 'Citus time_partitions view available (requires Citus >= 10.0)'
    return 'adequate partition runway, no gaps, no width hotspots'


def _rec_i1(text: str) -> str:
    if re.search(r'INVALID or NOT-READY index', text):
        return '0 invalid/not-ready indexes (REINDEX/DROP recommended on hits)'
    if re.search(r'pg_stat counters reset only', text):
        return 'pg_stat history old enough to trust unused-index verdict'
    if re.search(r'with 0 scans across all shards', text):
        return '0 unused distributed-table indexes (every index is read at least once)'
    if re.search(r'duplicate index pair', text):
        return '0 duplicate indexes'
    if re.search(r'unreachable; index health', text):
        return 'all nodes reachable for index health check'
    return 'no invalid indexes, no unused distributed indexes, no duplicates'


def _rec_b1(text: str) -> str:
    m = re.search(r'(\d+)\s+relation\(s\)\s*>=\s*(\d+)%\s+dead tuples', text)
    if m:
        return f'< {m.group(2)}% dead tuples per relation (currently {m.group(1)} over)'
    if re.search(r'past their autovacuum trigger', text):
        return 'no relations past their autovacuum trigger'
    if re.search(r'autovacuum_max_workers fully busy', text):
        return 'autovacuum_max_workers headroom on every node'
    if re.search(r'stale ANALYZE', text):
        return 'fresh ANALYZE on every distributed shard'
    if re.search(r'bloat estimate is unreliable', text):
        return 'pg_stat history old enough to trust bloat estimate'
    if re.search(r'unreachable; bloat', text):
        return 'all nodes reachable for bloat check'
    return 'bloat under control, autovacuum keeping up, ANALYZE fresh'


def _rec_cp1(text: str) -> str:
    m = re.search(r'pool_size\s+range\s+(\d+)\.\.(\d+)', text)
    if m:
        return f'pgbouncer pool_size {m.group(1)}–{m.group(2)} per entry node (transaction mode)'
    if re.search(r'ALREADY over the safe external cap', text):
        return 'reduce client traffic or scale pool DOWN before adopting recs'
    if re.search(r'80% of safe cap', text):
        return 'adopt CP1a recommendations before next traffic peak'
    if re.search(r'per-db pool sums above the safe cap', text):
        return 'partition the per-db pool budget; sum must respect safe cap'
    if re.search(r'PG <14.*prepared statements is unsupported', text):
        return 'disable prepared statements in app, or upgrade PG to 14+'
    return 'pool_size sized within safe cap; transaction mode in pgbouncer'



def _rec_w1(text: str) -> str:
    if re.search(r'fsync or full_page_writes is OFF', text):
        return 'set fsync=on and full_page_writes=on immediately'
    if re.search(r'archive_mode is on but archiving is broken', text):
        return 'fix archive_command / archive_library destination'
    m = re.search(r'(\d+) node\(s\) have >= \d+% requested checkpoints', text)
    if m:
        return f'raise max_wal_size on {m.group(1)} node(s) to shift checkpoints to timed'
    m = re.search(r'(\d+) node\(s\) have wal_buffers < (\d+) MB', text)
    if m:
        return f'raise wal_buffers to >= {m.group(2)} MB on {m.group(1)} node(s)'
    if re.search(r'archiver has a recent failure', text):
        return 'investigate archiver destination (W1d)'
    return 'WAL path healthy'


def _rec_stat1(text: str) -> str:
    m = re.search(r'(\d+) distributed table\(s\) have NO analyzed shards', text)
    if m:
        return f'run ANALYZE on {m.group(1)} distributed table(s)'
    m = re.search(r'(\d+) distributed table\(s\) have shard analyze older than (\d+) days', text)
    if m:
        return f're-ANALYZE {m.group(1)} table(s) older than {m.group(2)}d'
    m = re.search(r'(\d+) distributed table\(s\) have some un-analyzed shards', text)
    if m:
        return f'ANALYZE {m.group(1)} table(s): plans differ across workers'
    m = re.search(r'(\d+) distributed table\(s\) have shards older than (\d+) days', text)
    if m:
        return f'schedule ANALYZE for {m.group(1)} table(s) stale >{m.group(2)}d'
    m = re.search(r'(\d+) distributed table\(s\) have >= (\d+)% churn', text)
    if m:
        return f'{m.group(1)} table(s) with >={m.group(2)}% churn: ANALYZE'
    if re.search(r'default_statistics_target too low', text):
        return 'raise default_statistics_target on flagged node(s)'
    return 'statistics fresh'


def _rec_sec1(text: str) -> str:
    m = re.search(r'(\d+) role\(s\) missing on some cluster nodes', text)
    if m:
        return f'pre-create {m.group(1)} role(s) on missing nodes or enable role propagation'
    m = re.search(r'(\d+) extension\(s\) at different versions', text)
    if m:
        return f'ALTER EXTENSION UPDATE for {m.group(1)} extension(s)'
    m = re.search(r'(\d+) superuser role\(s\) \(> (\d+) allowed\)', text)
    if m:
        return f'reduce superusers from {m.group(1)} (> {m.group(2)} allowed)'
    if re.search(r'password_encryption is not scram-sha-256', text):
        return 'set password_encryption=scram-sha-256 cluster-wide'
    if re.search(r'login role\(s\) with md5 / missing password', text):
        return 'reset passwords to scram-sha-256, set passwords on all login roles'
    if re.search(r'pg_hba rules use trust or plain password', text):
        return 'replace trust/password rules with scram-sha-256 in pg_hba'
    if re.search(r'inter-node traffic may be unencrypted', text):
        return 'enable ssl and set sslmode=require in citus.node_conninfo'
    return 'auth posture healthy'


def _rec_ref1(text: str) -> str:
    if re.search(r'no reference tables defined', text):
        return 'no reference tables'
    if re.search(r'missing placements', text):
        return 'run SELECT replicate_reference_tables() to restore placements'
    m = re.search(r'(\d+) reference table\(s\) exceed (\d+) MB per copy', text)
    if m:
        return f'{m.group(1)} ref(s) over {m.group(2)} MB/copy: consider distributing instead'
    if re.search(r'are oversized', text):
        return 'oversized reference tables waste worker RAM per copy'
    if re.search(r'size drift across workers', text):
        return 'investigate replication: copies diverge across workers'
    if re.search(r'extra placements', text):
        return 'run citus_cleanup_orphaned_resources() to remove extra placements'
    return 'reference tables healthy'


def _rec_net1(text: str) -> str:
    m = re.search(r'(\d+) connectivity edge\(s\) failed', text)
    if m:
        return f'fix {m.group(1)} broken edge(s) in cluster mesh (pg_hba/firewall/citus.node_conninfo)'
    if re.search(r'at least one node unreachable', text):
        return 'one or more nodes dropped during sampling -- investigate'
    m = re.search(r'at least one node has avg RTT > (\d+) ms', text)
    if m:
        return f'investigate network path (RTT > {m.group(1)} ms)'
    return 'mesh healthy'


def _rec_rep1(text: str) -> str:
    m = re.search(r'replay_lag exceeds (\d+) s', text)
    if m:
        return f'investigate standby: replay_lag >= {m.group(1)}s'
    m = re.search(r'inactive replication slot\(s\) retain >= (\d+) MB', text)
    if m:
        return f'drop inactive slots or revive subscribers ({m.group(1)} MB retained)'
    if re.search(r'synchronous_standby_names set without matching', text):
        return 'verify sync standby is connected or clear synchronous_standby_names'
    if re.search(r'orphaned Citus rebalancer replication slot', text):
        return 'investigate pg_dist_cleanup and drop orphan slots'
    if re.search(r'no replication senders or slots configured', text):
        return 'no HA configured (informational)'
    return 'replication healthy'


def _rec_r2(text: str) -> str:
    if re.search(r'cluster is balanced', text):
        return 'no rebalance needed under default strategy'
    m = re.search(r'(\d+)\s+moves,\s+([^;]+);.*est wall time\s+(\d+)\s*s', text)
    if m:
        return f'plan: {m.group(1)} moves, {m.group(2).strip()}, ~{m.group(3)}s'
    if re.search(r'target\(s\) would >2x', text):
        return 'verify free disk on target nodes before citus_rebalance_start()'
    if re.search(r'by_shard_count but shards vary', text):
        return 'switch default rebalance_strategy to by_disk_size'
    return 'review R2a-R2f before invoking citus_rebalance_start()'


# (id, title, short blurb, extractor)
ADVISORS = [
    ("M1",  "Node memory minimum (OOM-safety)",
     "Bottom-up per-node RAM model (shared_buffers + backends + AV + WAL + MX pool + OS).",
     _rec_m1),
    ("D1",  "Disk capacity & shard-growth runway",
     "Per-node used/free space, historical growth rate, and days-until-full projection.",
     _rec_d1),
    ("Q1",  "Long-running queries & lock waits",
     "Snapshots long active queries, idle-in-tx, citus_lock_waits, and ungranted locks.",
     _rec_q1),
    ("GUC1", "Citus + PostgreSQL configuration audit",
     "Cross-node GUC drift + rule-based checks on 30+ availability-sensitive settings.",
     _rec_guc1),
    ("V1",  "Version & upgrade readiness",
     "Per-node PG/Citus version match, pending ALTER EXTENSION UPDATE, extension drift.",
     _rec_v1),
    ("P1",  "Partition hygiene & maintenance runway",
     "Future-partition runway, gap detection, partition/shard width limits, pg_partman reality check.",
     _rec_p1),
    ("I1",  "Index health (per-shard aware)",
     "Invalid indexes, unused distributed-table indexes (0 scans across all shards), duplicates, missing FK indexes.",
     _rec_i1),
    ("B1",  "Table bloat & autovacuum lag",
     "Per-shard estimated dead-tuple ratio, past-due autovacuum triggers, suggested per-table scale_factor, AV worker saturation, stale ANALYZE.",
     _rec_b1),
    ("CP1", "pgbouncer pool sizing",
     "Per-entry-node pool_size, max_client_conn, reserve/min pool, multi-db budget, copy-paste pgbouncer.ini.",
     _rec_cp1),
    ("R2",  "Rebalance plan preview",
     "Dry-run of get_rebalance_table_shards_plan(); bytes moved, per-worker impact, disk amplification, strategy sanity, wall-time estimate.",
     _rec_r2),
    ("REF1", "Reference-table health",
     "Per-reference-table inventory, oversize warnings, placement count mismatches, size drift across workers.",
     _rec_ref1),
    ("W1",   "WAL & checkpoint pressure",
     "Per-node wal_buffers, max_wal_size, checkpoint trigger mix (timed vs requested), archiver health, replication slots, durability GUCs (fsync, full_page_writes).",
     _rec_w1),
    ("STAT1","Statistics freshness",
     "Per-shard last_analyze/last_autoanalyze drift, tables with significant churn since ANALYZE, never-analyzed tables, extended-stats suggestions, default_statistics_target sanity.",
     _rec_stat1),
    ("SEC1", "Security & role audit",
     "Superuser inventory, password hash methods (scram vs md5 vs none), role drift across nodes (MX blocker), per-node security GUCs, PUBLIC grants on distributed tables, extension version drift, weak pg_hba rules.",
     _rec_sec1),
    ("NET1", "Node reachability & latency",
     "Full NxN connectivity matrix from citus_check_cluster_node_health(), asymmetric-edge detection (A->B ok, B->A fails), coord->node RTT sampling with per-node verdict vs cluster median.",
     _rec_net1),
    ("REP1", "Streaming replication & slot lag",
     "Per-node pg_stat_replication senders (state / sync_state / replay_lag), replication slots with WAL retention bytes, orphan Citus rebalancer slots, recovery state, synchronous_commit vs synchronous_standby_names sanity.",
     _rec_rep1),
    ("GR1", "Shard / partition growth memory model",
     "Per-backend cache growth + PG lock-hash capacity (lpt × (MaxBackends + mpx)).",
     _rec_gr1),
    ("C3",  "Max safe external connections (MX-aware)",
     "Min-bottleneck of inbound limits, outbound pool fan-out, and internal quota.",
     _rec_c3),
    ("S3",  "Data skew across shards & workers",
     "Per-colocation-group shard size skew + per-worker bytes balance.",
     _rec_s3),
    ("R1",  "Rebalance / background-job health",
     "Citus background_job / background_task queue hygiene, stuck steps, cleanup backlog.",
     _rec_r1),
    ("N6",  "Metadata-sync feasibility for add-node",
     "Predicts add-node success: payload, candidate lock-hash sizing, wall-time, memory.",
     _rec_n6),
    ("A3",  "2PC backlog & orphan prepared xacts",
     "Finds prepared xacts on workers that pg_dist_transaction doesn't know about.",
     _rec_a3),
    ("SC1", "Shard-count right-sizing",
     "Per-colocation shard-count recommendation based on size and worker count.",
     _rec_sc1),
    ("P2",  "Placement stats freshness",
     "Detects pg_dist_placement.shardlength drift vs on-disk size (stale rebalancer cost model).",
     _rec_p2),
    ("MX1", "MX mesh connection budget",
     "Per-node client-slot headroom after subtracting MX mesh fan-in + system reserve.",
     _rec_mx1),
]


# --- advisor metadata -------------------------------------------------------
# Plain-language wrapper around each advisor: category (for filtering +
# color), what it checks (1 sentence, no jargon), why it matters (risk to
# availability/performance), and how to fix (a concrete action hint).

CATEGORIES = {
    "memory":    ("Memory",       "#6b4fbb"),
    "conn":      ("Connections",  "#1565c0"),
    "data":      ("Data layout",  "#00897b"),
    "config":    ("Configuration","#455a64"),
    "perf":      ("Performance",  "#f57c00"),
    "ops":       ("Operations",   "#5d4037"),
    "security":  ("Security",     "#c62828"),
    "avail":     ("Availability", "#2e7d32"),
    "upgrade":   ("Upgrade",      "#0288d1"),
}

# Tiny inline SVG icons (24x24, currentColor stroke). Kept simple on purpose.
CAT_ICON = {
    "memory":   '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="14" height="14" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3"/></svg>',
    "conn":     '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M9 7V3M15 7V3M9 21v-4M15 21v-4M6 7h12v4a6 6 0 0 1-12 0V7z"/></svg>',
    "data":     '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>',
    "config":   '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/></svg>',
    "perf":     '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>',
    "ops":      '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a4 4 0 0 0-5.4 5.4L3 18l3 3 6.3-6.3a4 4 0 0 0 5.4-5.4l-2.5 2.5-2.5-2.5 2.5-2.5z"/></svg>',
    "security": '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L4 5v7c0 5 3.5 8.5 8 10 4.5-1.5 8-5 8-10V5l-8-3z"/><path d="M9 12l2 2 4-4"/></svg>',
    "avail":    '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4l3-8 4 16 3-8h4"/></svg>',
    "upgrade":  '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 19V5M5 12l7-7 7 7"/></svg>',
}

# Severity icons (conveyed in addition to color for a11y).
SEV_ICON = {
    "ok":   '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12l5 5 9-11"/></svg>',
    "warn": '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3L2 21h20L12 3z"/><path d="M12 10v5M12 18v.01"/></svg>',
    "crit": '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M15 9l-6 6M9 9l6 6"/></svg>',
    "info": '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v.01M11 12h1v5h1"/></svg>',
    "unk":  '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M9.5 9a2.5 2.5 0 1 1 4 2c-1 .7-1.5 1.3-1.5 2.5M12 17v.01"/></svg>',
}

# Plain-language metadata per advisor. Keys: cat, checks, matters, fix, docs.
# Wording is deliberately jargon-light so a non-DBA can make sense of it.
ADVISOR_META = {
    "M1": dict(
        cat="memory",
        checks="The minimum RAM each node needs so PostgreSQL can run the current workload without hitting the kernel's OOM killer.",
        matters="If the machine runs out of memory, the kernel kills PostgreSQL processes at random and the node goes down hard. Adding shards or partitions makes the peak-memory footprint grow; a cluster that fits today can OOM next week.",
        fix="Compare the RAM reported against the advisor's recommended minimum. If below, resize the instance or lower work_mem / max_connections. See the advisor output for the exact shortfall and the formula.",
        docs="https://www.postgresql.org/docs/current/runtime-config-resource.html",
    ),
    "D1": dict(
        cat="ops",
        checks="Free disk on every node, the recent growth rate, and how many days until the disk fills.",
        matters="Postgres stops accepting writes the moment a data directory fills up, and a disk-full node blocks the whole distributed cluster. Runway under a week is an outage waiting to happen.",
        fix="For tight runways: expand the volume, drop stale partitions with DROP TABLE, or trigger citus_rebalance_start() to move shards to a less-full node.",
        docs="https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html",
    ),
    "Q1": dict(
        cat="perf",
        checks="Currently running queries that have been active a long time, idle-in-transaction sessions, and lock waits across the cluster.",
        matters="One long query or a stuck transaction can hold locks that block shard moves, DDL, or every writer to a table. Idle-in-tx also holds xmin horizon open, causing bloat.",
        fix="Identify the culprit via pg_stat_activity / citus_lock_waits in the advisor output, then SELECT pg_terminate_backend(<pid>) after confirming with the app owner.",
        docs="https://www.postgresql.org/docs/current/monitoring-stats.html",
    ),
    "GUC1": dict(
        cat="config",
        checks="PostgreSQL and Citus configuration settings on each node, compared against each other and against safety rules.",
        matters="Citus assumes every worker has the same settings for several criticals (max_worker_processes, shared_preload_libraries, timezone). Drift breaks parallel execution, 2PC, and MX routing. Unsafe values (fsync=off, trust auth) risk data loss or compromise.",
        fix="For drift: align the setting on every node and reload. For rule violations: adopt the recommended value per the advisor's per-setting note.",
        docs="https://docs.citusdata.com/en/stable/develop/api_guc.html",
    ),
    "V1": dict(
        cat="upgrade",
        checks="Whether every node is on the same PostgreSQL and Citus version, and whether pending extension upgrades exist.",
        matters="Mixed versions block metadata sync, break add_node, and can corrupt shard placements. A pending ALTER EXTENSION UPDATE means the cluster is half-upgraded.",
        fix="Upgrade lagging nodes first, then run ALTER EXTENSION citus UPDATE on every database. Follow Citus's documented major-version upgrade path.",
        docs="https://docs.citusdata.com/en/stable/admin_guide/upgrading_citus.html",
    ),
    "P1": dict(
        cat="data",
        checks="Time-partitioned tables: how many future partitions exist, missing month/day gaps, widely-varying partition sizes.",
        matters="If future partitions run out, inserts fail with 'no partition of relation found'. A gap in the partition chain silently drops rows or causes routing errors.",
        fix="Run SELECT create_time_partitions(...) to extend runway; reconcile gaps with explicit CREATE TABLE; consider pg_partman for auto-maintenance.",
        docs="https://docs.citusdata.com/en/stable/use_cases/timeseries.html",
    ),
    "I1": dict(
        cat="perf",
        checks="Invalid indexes, unused indexes on distributed tables, and duplicate indexes.",
        matters="Invalid indexes waste space and are silently ignored by the planner. Unused indexes still slow down every INSERT/UPDATE on the shard. Duplicates double the write cost for nothing.",
        fix="DROP INDEX CONCURRENTLY the flagged invalids. For unused ones, confirm with the app team and DROP. For duplicates, keep the broader index and drop the narrower redundant one.",
        docs="https://www.postgresql.org/docs/current/sql-dropindex.html",
    ),
    "B1": dict(
        cat="perf",
        checks="Estimated table bloat, autovacuum lag, and tables whose ANALYZE is stale.",
        matters="Bloated tables bloat their indexes, slow sequential scans, waste disk, and can even trigger xid wraparound emergencies. Stale stats cause bad plans across every shard.",
        fix="For heavy bloat: VACUUM (FULL) during maintenance window, or pg_repack for online reclaim. For autovacuum lag: lower per-table autovacuum_vacuum_scale_factor or raise autovacuum_max_workers.",
        docs="https://www.postgresql.org/docs/current/routine-vacuuming.html",
    ),
    "CP1": dict(
        cat="conn",
        checks="The safe pgbouncer pool_size and max_client_conn for this cluster, given max_connections and Citus internal reservations.",
        matters="If pgbouncer is over-sized, a connection spike cascades into PostgreSQL and hits max_connections limits on every worker. Under-sized, it throttles the app.",
        fix="Copy the pgbouncer.ini snippet from the advisor output into your pgbouncer config and reload. Reverify after any change to max_connections or citus.max_adaptive_executor_pool_size.",
        docs="https://www.pgbouncer.org/config.html",
    ),
    "R2": dict(
        cat="ops",
        checks="A preview of what a rebalance would do: bytes moved, per-worker impact, disk amplification, and estimated wall time.",
        matters="An uninformed rebalance can saturate the network, double a worker's disk usage mid-move, or block for hours during peak traffic.",
        fix="Review the plan before running citus_rebalance_start(). If disk amp > 1.5x or runtime too long, raise citus.max_background_task_executors_per_node or split the job by colocation group.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-rebalance-start",
    ),
    "REF1": dict(
        cat="data",
        checks="Reference tables: size per copy, number of placements, and drift between workers.",
        matters="Reference tables are fully replicated to every worker. A 1 GB reference table costs 1 GB × n_workers of disk and slows down every write by 2PC across the cluster.",
        fix="If oversized: convert heavy-write reference tables to distributed. If drift: citus_copy_reference_table_placements() to repair. Aim to keep reference tables under 100 MB each.",
        docs="https://docs.citusdata.com/en/stable/develop/reference_ddl.html#reference-tables",
    ),
    "W1": dict(
        cat="ops",
        checks="WAL and checkpoint pressure: wal_buffers size, checkpoint trigger mix, archiver health, replication slot lag, durability settings.",
        matters="Too-frequent requested checkpoints cause I/O stalls. Tiny wal_buffers serialize writes. fsync=off risks corruption. Orphaned replication slots retain WAL forever and fill the disk.",
        fix="Raise wal_buffers to at least 16 MB. Tune max_wal_size so checkpoints are mostly timed, not requested. Drop unused replication slots. Verify fsync=on and full_page_writes=on.",
        docs="https://www.postgresql.org/docs/current/wal-configuration.html",
    ),
    "STAT1": dict(
        cat="perf",
        checks="Last_analyze / last_autoanalyze per shard; tables with significant change since the last ANALYZE; never-analyzed tables.",
        matters="Stale stats cause the planner to pick wrong join orders, seq-scan instead of index-scan, or badly underestimate row counts on distributed queries. Every worker gets the same bad plan.",
        fix="Run ANALYZE on flagged tables. For chronically stale ones, lower autovacuum_analyze_scale_factor per-table. Consider CREATE STATISTICS for correlated join-key columns.",
        docs="https://www.postgresql.org/docs/current/planner-stats.html",
    ),
    "SEC1": dict(
        cat="security",
        checks="Superuser inventory, password hash methods, PUBLIC grants on distributed tables, extension version drift, weak pg_hba rules.",
        matters="md5 and no-password accounts are phishing-and-sniff vulnerable. PUBLIC grants on distributed tables let any login read them. Extension drift blocks upgrade or causes per-node behavior differences.",
        fix="Migrate md5 → scram-sha-256: ALTER USER ... PASSWORD to re-hash. Revoke PUBLIC on distributed tables. Align extensions to the same version on every node.",
        docs="https://www.postgresql.org/docs/current/auth-password.html",
    ),
    "NET1": dict(
        cat="avail",
        checks="Node-to-node connectivity matrix and coord→node latency.",
        matters="Citus assumes any node can reach any other node. Asymmetric connectivity (A→B ok but B→A fails) silently breaks MX queries and 2PC commits mid-flight.",
        fix="For unreachable pairs: fix firewall, routing, or pg_hba.conf. For high RTT: move the offending node into the same availability zone as the coordinator.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-check-connection-to-node",
    ),
    "REP1": dict(
        cat="avail",
        checks="Streaming replication senders, replica replay lag, replication slots with WAL retention, recovery state.",
        matters="High replay lag means your HA standby is not actually protecting you. An orphan slot retains WAL forever and will fill the disk. Wrong synchronous_commit + synchronous_standby_names combinations can freeze writes entirely.",
        fix="For lag: check replica I/O and reduce write pressure. For orphan slots: pg_drop_replication_slot() after confirming no standby needs them. Review HA topology before changing sync settings.",
        docs="https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION",
    ),
    "GR1": dict(
        cat="memory",
        checks="How much extra memory per backend is needed as the cluster grows more shards or partitions (relation cache + plan cache + lock-hash).",
        matters="Per-backend memory scales with shard count. Growing from 32 to 256 shards can 10x the resident set of every backend. This is the #1 reported cause of OOM complaints after aggressive sharding.",
        fix="Before adding shards: raise RAM per node per the advisor's projection, or lower max_connections so fewer backends share the budget. Consider citus.max_cached_conns_per_worker tuning.",
        docs="https://docs.citusdata.com/en/stable/develop/api_guc.html",
    ),
    "C3": dict(
        cat="conn",
        checks="The maximum external client connections the cluster can safely handle, given inbound limits, outbound MX pool fan-out, and internal quotas.",
        matters="Every client query fans out to workers. Miss the cap and workers run out of connections first — the cluster returns 'too many connections' errors and stays that way until load drops.",
        fix="Sit pgbouncer in front of the coordinator (and every MX node) at the pool size shown. If worst-case is already low, raise max_connections on workers or lower citus.max_adaptive_executor_pool_size.",
        docs="https://docs.citusdata.com/en/stable/develop/api_guc.html#citus-max-shared-pool-size",
    ),
    "S3": dict(
        cat="data",
        checks="Size skew across shards of each colocation group, and byte balance across workers.",
        matters="A single hot shard (one big tenant, one hot key) can saturate one worker's CPU/disk while others idle. Rebalancer can't fix it if the skew is inside the hash range — only redistribution or tenant isolation can.",
        fix="For tenant hot spots: move the tenant to its own table/schema (schema-based sharding) or use CITUS_ISOLATE_TENANT_TO_NEW_SHARD. For key skew: rechoose distribution column to a higher-cardinality key.",
        docs="https://docs.citusdata.com/en/stable/sharding/data_modeling.html",
    ),
    "R1": dict(
        cat="ops",
        checks="Citus background job and task queue: stuck steps, failed jobs, cleanup backlog from deferred shard drops.",
        matters="A stuck rebalance holds shard locks that block DDL and shard moves forever. A cleanup backlog means dropped shards are still consuming disk.",
        fix="For stuck jobs: inspect pg_dist_background_job / _task and citus_rebalance_stop() + diagnose. For cleanup backlog: call citus_cleanup_orphaned_resources() manually.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-rebalance-stop",
    ),
    "N6": dict(
        cat="ops",
        checks="Whether add_node will succeed given the current metadata payload size, lock-hash sizing, expected wall-time, and memory needed during sync.",
        matters="Metadata sync on a too-small new node silently times out or OOMs half-way, leaving the cluster in a mixed state that blocks every subsequent admin operation.",
        fix="Use the recommended add-node mode (transactional vs nontransactional) and pre-size max_locks_per_transaction on the new node. Prepare the node with matching extensions and types before calling citus_add_node.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-add-node",
    ),
    "A3": dict(
        cat="avail",
        checks="Prepared 2PC transactions on workers that the coordinator doesn't know about (orphans).",
        matters="Orphaned prepared transactions hold locks forever, block VACUUM, and prevent shard moves. Eventually they exhaust xid space and trigger cluster-wide reads-only shutdown.",
        fix="Run SELECT recover_prepared_transactions() on the coordinator. If that doesn't clear them, ROLLBACK PREPARED each GID manually after confirming no replay is expected.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#recover-prepared-transactions",
    ),
    "SC1": dict(
        cat="data",
        checks="Per-colocation-group shard count against the amount of data actually stored.",
        matters="Too few shards bottlenecks parallelism to a small number of workers. Too many shards bloats every backend's metadata cache and planning cost, and slows every distributed query.",
        fix="Use alter_distributed_table or undistribute_table + create_distributed_table to rebuild at the recommended count. A common rule of thumb is 1-2x the worker count per colocation group.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#alter-distributed-table",
    ),
    "P2": dict(
        cat="data",
        checks="Whether pg_dist_placement.shardlength matches the real on-disk size of each shard.",
        matters="The rebalancer picks moves from shardlength. Stale (often 0) values make it pick the wrong moves or declare the cluster balanced when it isn't.",
        fix="Run SELECT citus_update_table_statistics('<table>') for each affected distributed table, then re-check the rebalance plan before starting a rebalance.",
        docs="https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-update-table-statistics",
    ),
    "MX1": dict(
        cat="conn",
        checks="On each MX-metadata-synced node: how many client connection slots remain after subtracting inbound mesh fan-in and system reservations.",
        matters="In MX mode every other node can open pool connections to this node. If mesh + system reserve already eats most of max_connections, a spike in client traffic overflows immediately.",
        fix="Raise max_connections on every MX node, or lower citus.max_adaptive_executor_pool_size to cap the per-peer fan-in.",
        docs="https://docs.citusdata.com/en/stable/develop/api_guc.html#citus-max-adaptive-executor-pool-size",
    ),
}


# --- severity ---------------------------------------------------------------

SEV_ORDER    = ["CRITICAL", "WARN", "INFO", "OK"]
SEV_TO_CLASS = {"CRITICAL": "crit", "WARN": "warn", "INFO": "info", "OK": "ok"}
SEV_LABEL    = {"crit": "CRITICAL", "warn": "WARN", "info": "INFO", "ok": "OK", "unk": "—"}
# overall-escalation rank (higher = worse); INFO never escalates.
ESCALATE = {"crit": 3, "warn": 2, "ok": 1, "info": 0, "unk": 0}


def worst_verdict(text: str) -> tuple[str, str]:
    """Return (severity-class, headline). Most severe match wins."""
    for sev in SEV_ORDER:
        m = re.search(rf'^\s*\|?\s*{sev}\s*:[^\n|]*', text, re.M)
        if m:
            line = m.group(0).lstrip().lstrip("|").strip().rstrip("|").strip()
            return SEV_TO_CLASS[sev], line
    return "unk", "(no verdict headline)"


# --- gather.out section parsing --------------------------------------------

def parse_sections(gather_path: Path) -> list[tuple[str, str]]:
    if not gather_path.exists():
        return []
    text = gather_path.read_text(errors="replace")
    rx = re.compile(r'^### BEGIN: (\S+)\s*\n(.*?)^### END\s*: \1\s*$', re.M | re.S)
    return [(m.group(1), m.group(2).rstrip("\n")) for m in rx.finditer(text)]


def csv_to_html(body: str, max_rows: int = 200) -> tuple[str, int]:
    body = body.strip()
    if not body:
        return '<p class="muted"><em>(empty)</em></p>', 0
    if body.startswith("--"):
        return f'<p class="muted"><em>{html.escape(body)}</em></p>', 0
    rdr = csv.reader(io.StringIO(body))
    try:
        header = next(rdr)
    except StopIteration:
        return '<p class="muted"><em>(empty)</em></p>', 0
    rows = list(rdr)
    total = len(rows)
    shown = rows[:max_rows]
    out = ['<div class="scroll"><table class="csv"><thead><tr>']
    out += [f'<th>{html.escape(h)}</th>' for h in header]
    out.append('</tr></thead><tbody>')
    for r in shown:
        out.append('<tr>' + ''.join(f'<td>{html.escape(c)}</td>' for c in r) + '</tr>')
    out.append('</tbody></table></div>')
    meta = f'<p class="muted">{total} row(s)'
    if total > max_rows:
        meta += f' — showing first {max_rows}'
    out.append(meta + '</p>')
    return "\n".join(out), total


# --- snapshot section classification ----------------------------------------

# Order matters -- first regex match wins.
SECTION_CATEGORY_RULES: list[tuple[str, str]] = [
    (r'(cluster_topology|node_health|pg_dist_node|citus_check)',      'topology'),
    (r'(shard|placement|colocation)',                                  'shards'),
    (r'(partition)',                                                   'partitions'),
    (r'(transaction|prepared_xact|background_job|background_task|'
     r'rebalance|cleanup|recover_prepared)',                           'ops'),
    (r'(stat_activity|locks|lock_waits|dist_stat_activity)',           'activity'),
    (r'(gucs?|settings|show_)',                                        'settings'),
    (r'(stat_|pg_statio|stat_user|stat_all|stat_bgwriter|'
     r'stat_database|stat_replication|replication_slot)',              'stats'),
    (r'(role|auth|security|pg_hba)',                                   'security'),
    (r'(extension|object|schema|dist_object|dist_schema)',             'metadata'),
    (r'(meta|end_of_gather)',                                          'meta'),
]

SECTION_CATEGORY_LABELS = {
    "topology":   ("Topology",   "#1565c0"),
    "shards":     ("Shards",     "#6b4fbb"),
    "partitions": ("Partitions", "#2e7d32"),
    "ops":        ("Ops",        "#5d4037"),
    "activity":   ("Activity",   "#ed6c02"),
    "settings":   ("Settings",   "#455a64"),
    "stats":      ("Stats",      "#00897b"),
    "security":   ("Security",   "#c62828"),
    "metadata":   ("Metadata",   "#78909c"),
    "meta":       ("Meta",       "#78909c"),
    "other":      ("Other",      "#78909c"),
}


def classify_section(name: str) -> str:
    n = name.lower()
    for rx, cat in SECTION_CATEGORY_RULES:
        if re.search(rx, n):
            return cat
    return "other"


# --- cluster fingerprint ----------------------------------------------------
# Best-effort extraction of cluster facts from gather.out, to render a tile
# row at the top of the report. Missing sections just hide tiles; we never
# raise on malformed data.

def _section_rows(sections: list[tuple[str, str]], name: str) -> list[dict]:
    """Parse a CSV section by name into a list of dicts. Returns [] if empty
    or missing; also returns [] when the section was skipped (starts with '--')."""
    for sid, body in sections:
        if sid != name:
            continue
        body = body.strip()
        if not body or body.startswith("--"):
            return []
        try:
            rdr = csv.DictReader(io.StringIO(body))
            return list(rdr)
        except Exception:
            return []
    return []


def _short_version(s: str, prefix: str) -> str:
    """Extract e.g. '17.9' from 'PostgreSQL 17.9 on aarch64-...' or '15.0devel'
    from 'Citus 15.0devel on arm-...'."""
    if not s:
        return ""
    m = re.search(rf'{prefix}\s+([\w.+-]+)', s, re.I)
    return m.group(1) if m else ""


def _fmt_bytes(n: int) -> str:
    x = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024 or unit == "TB":
            return f"{x:.1f} {unit}" if unit != "B" else f"{int(x)} B"
        x /= 1024
    return f"{x:.1f} TB"


def build_fingerprint(sections: list[tuple[str, str]]) -> list[tuple[str, str, str]]:
    """Return a list of (label, value, sublabel) tiles. Best-effort; any
    section that is missing or malformed just drops its tile. Tiles are
    rendered in the hero dashboard."""
    tiles: list[tuple[str, str, str]] = []

    meta = _section_rows(sections, "meta")
    pg_ver = citus_ver = db_name = ""
    if meta:
        row = meta[0]
        pg_ver = _short_version(row.get("pg_version", ""), "PostgreSQL")
        citus_ver = _short_version(row.get("citus_version", ""), "Citus")
        db_name = (row.get("db") or "").strip()
    if db_name:
        tiles.append(("Database", db_name, ""))
    if pg_ver:
        tiles.append(("PostgreSQL", pg_ver, ""))
    if citus_ver:
        tiles.append(("Citus", citus_ver, ""))

    topo = _section_rows(sections, "cluster_topology")
    if topo:
        active = [r for r in topo if r.get("isactive") in ("t", "true", "1")]
        coord  = [r for r in active if r.get("groupid") in ("0", 0)]
        workers = [r for r in active if r.get("groupid") not in ("0", 0)]
        mx_count = sum(1 for r in active if r.get("hasmetadata") in ("t", "true", "1"))
        mode = "MX" if mx_count > 1 else "classic"
        tiles.append(("Nodes", f"{len(active)}",
                      f"{len(coord)} coord + {len(workers)} worker(s)"))
        tiles.append(("Cluster mode", mode,
                      f"{mx_count} metadata-synced" if mx_count else "no MX"))

    parts = _section_rows(sections, "pg_dist_partition")
    if parts:
        dist = [r for r in parts if r.get("partmethod") == "h"]
        ref  = [r for r in parts if r.get("partmethod") == "n"]
        tiles.append(("Distributed tables", str(len(dist)),
                      f"{len(ref)} reference" if ref else ""))

    coloc = _section_rows(sections, "pg_dist_colocation")
    if coloc:
        hash_groups = [r for r in coloc if r.get("dist_type") not in ("-", "", None)]
        tiles.append(("Colocation groups", str(len(hash_groups)), ""))

    shard_summary = _section_rows(sections, "pg_dist_shard_summary")
    if shard_summary:
        try:
            total = sum(int(r.get("shards", 0) or 0) for r in shard_summary)
        except Exception:
            total = 0
        if total:
            tiles.append(("Total shards", str(total), ""))

    sizes = _section_rows(sections, "citus_shards_sizes")
    if sizes:
        try:
            tb = sum(int(r.get("shard_size", 0) or 0) for r in sizes)
        except Exception:
            tb = 0
        if tb:
            tiles.append(("Total data size", _fmt_bytes(tb), ""))

    return tiles


# --- build top-N fix list ---------------------------------------------------

def build_top_fixes(results: list[tuple], limit: int = 5) -> list[tuple]:
    """Pick the most urgent advisors for the hero 'top things to fix' list.
    Sort: critical first, then warn, by advisor order within each tier."""
    rank = {"crit": 0, "warn": 1, "info": 2, "ok": 3, "unk": 4}
    filt = [r for r in results if r[3] in ("crit", "warn")]
    filt.sort(key=lambda r: rank[r[3]])
    return filt[:limit]


# --- CSS --------------------------------------------------------------------

CSS = r"""
:root {
  /* ---- brand & semantic palette ------------------------------------
     A warmer, more refined scheme than the previous flat primaries.
     Primary accent is indigo (--info/--accent); emerald/amber/rose
     carry the three severity states. All text-on-tint pairs verified
     for WCAG AA contrast at 15px+ body size. */
  --ok:    #059669; --ok-fg:   #047857;
  --warn:  #d97706; --warn-fg: #b45309;
  --crit:  #dc2626; --crit-fg: #b91c1c;
  --info:  #4f46e5; --info-fg: #4338ca;   /* indigo = brand accent */
  --accent:#4f46e5; --accent-soft: #6366f1;
  --unk:   #64748b;

  /* Severity surfaces: soft tints that sit on --card without clashing.
     Kept intentionally low-saturation for an "editorial" feel. */
  --ok-bg:   #ecfdf5;
  --warn-bg: #fef3c7;
  --crit-bg: #fee2e2;
  --info-bg: #eef2ff;

  /* Neutral text/UI ramp — slate family */
  --fg:  #0f172a;   /* slate-900 */
  --mut: #475569;   /* slate-600 */
  --sub: #64748b;   /* slate-500 */

  /* Canvas: a hair warmer than neutral slate-50 for a premium feel */
  --bg:     #f7f8fb;
  --card:   #ffffff;
  --border: #e2e8f0;
  --hover:  #f1f5f9;

  /* Shadows: layered, subtle, with a touch of indigo tint so cards feel
     rooted in the accent system. */
  --shadow:    0 1px 2px rgba(15,23,42,.04), 0 1px 3px rgba(15,23,42,.06);
  --shadow-md: 0 2px 4px rgba(15,23,42,.04), 0 6px 16px rgba(15,23,42,.07);
  --shadow-lg: 0 10px 15px -3px rgba(15,23,42,.08), 0 4px 6px -4px rgba(15,23,42,.06);
  --shadow-accent: 0 1px 3px rgba(79,70,229,.10), 0 8px 22px rgba(79,70,229,.12);

  --mono: "SFMono-Regular","JetBrains Mono","Fira Code",Consolas,Menlo,monospace;
  --sans: -apple-system,BlinkMacSystemFont,"Inter","Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
  --radius:    12px;
  --radius-lg: 16px;
  --radius-sm: 8px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --fg:  #f1f5f9;
    --mut: #cbd5e1;
    --sub: #94a3b8;

    --bg:     #0b1120;   /* deep slate, not pure black */
    --card:   #131a2b;
    --border: #1f2a44;
    --hover:  #1a2238;

    --ok-bg:   #052e1e;
    --warn-bg: #3b2508;
    --crit-bg: #3b1414;
    --info-bg: #1e1b4b;

    --ok:    #34d399;
    --warn:  #fbbf24;
    --crit:  #f87171;
    --info:  #a5b4fc;
    --accent:#a5b4fc; --accent-soft: #c7d2fe;

    --shadow:        0 1px 2px rgba(0,0,0,.4), 0 1px 3px rgba(0,0,0,.35);
    --shadow-md:     0 4px 10px rgba(0,0,0,.45);
    --shadow-lg:     0 12px 28px rgba(0,0,0,.6);
    --shadow-accent: 0 1px 3px rgba(165,180,252,.18), 0 8px 22px rgba(165,180,252,.12);
  }
}

* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  font: 15px/1.6 var(--sans);
  color: var(--fg);
  background: var(--bg);
  background-image:
    radial-gradient(1200px 500px at 10% -10%, rgba(79,70,229,.05), transparent 60%),
    radial-gradient(900px  420px at 90% -5%,  rgba(16,185,129,.04), transparent 55%);
  background-attachment: fixed;
  margin: 0;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
}

::selection { background: rgba(79,70,229,.18); color: var(--fg); }

/* Layout: sidebar on wide screens, stacked on narrow */
.wrap {
  max-width: 1440px; margin: 0 auto; padding: 1.75rem 2rem 5rem;
  display: grid; grid-template-columns: 260px minmax(0,1fr); gap: 2.25rem;
}
@media (max-width: 1100px) { .wrap { grid-template-columns: 1fr; padding: 1rem; } }

/* Sticky top bar */
.topbar {
  position: sticky; top: 0; z-index: 20;
  background: rgba(247,248,251,.78);
  border-bottom: 1px solid var(--border);
  padding: .85rem 2rem; display: flex; align-items: center; gap: 1rem;
  backdrop-filter: saturate(160%) blur(14px);
  -webkit-backdrop-filter: saturate(160%) blur(14px);
}
@media (prefers-color-scheme: dark) {
  .topbar { background: rgba(11,17,32,.78); }
}
.topbar h1 {
  font-size: 16px; font-weight: 650; margin: 0; letter-spacing: -.1px;
  display: flex; align-items: center; gap: .65rem;
}
.topbar .logo {
  width: 30px; height: 30px; color: var(--accent);
  filter: drop-shadow(0 2px 6px rgba(79,70,229,.28));
  flex-shrink: 0;
}
.topbar .spacer { flex: 1; }
.topbar a {
  color: var(--mut); text-decoration: none; font-size: 13.5px;
  padding: 5px 11px; border-radius: 7px; transition: color .12s, background .12s;
}
.topbar a:hover { color: var(--fg); background: var(--hover); }
.topbar button.print-btn {
  font: 500 13.5px var(--sans); cursor: pointer; background: transparent; color: var(--mut);
  border: 1px solid var(--border); border-radius: 7px; padding: 5px 12px;
  transition: color .12s, border-color .12s, background .12s;
}
.topbar button.print-btn:hover {
  color: var(--accent); border-color: var(--accent); background: var(--info-bg);
}

/* Sticky cluster-fingerprint strip — horizontal ribbon of facts that
   rides under the topbar. Uses a subtle accent wash + accent-tinted
   border to stand out from the topbar above and content below. */
.fingerstrip {
  position: sticky; top: 55px; z-index: 19;
  background:
    linear-gradient(90deg, rgba(79,70,229,.06) 0%, rgba(16,185,129,.045) 100%),
    rgba(247,248,251,.92);
  border-top: 1px solid rgba(79,70,229,.18);
  border-bottom: 1px solid rgba(79,70,229,.22);
  box-shadow: 0 2px 8px rgba(15,23,42,.06);
  backdrop-filter: saturate(160%) blur(14px);
  -webkit-backdrop-filter: saturate(160%) blur(14px);
  padding: 11px 2rem;
  display: flex; gap: 0;
  overflow-x: auto; overflow-y: hidden;
  white-space: nowrap;
  scrollbar-width: thin;
}
@media (prefers-color-scheme: dark) {
  .fingerstrip {
    background:
      linear-gradient(90deg, rgba(165,180,252,.09) 0%, rgba(52,211,153,.06) 100%),
      rgba(11,17,32,.90);
    border-top-color: rgba(165,180,252,.22);
    border-bottom-color: rgba(165,180,252,.28);
    box-shadow: 0 2px 10px rgba(0,0,0,.35);
  }
}
.fingerstrip::before {
  content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: linear-gradient(90deg, var(--accent) 0%, var(--ok) 50%, var(--accent-soft) 100%);
  opacity: .55;
}
.fingerstrip .fs-item {
  display: inline-flex; flex-direction: column; gap: 2px;
  padding: 2px 18px; border-right: 1px solid rgba(79,70,229,.18);
  flex: 0 0 auto; min-width: 0;
}
.fingerstrip .fs-item:first-child { padding-left: 0; }
.fingerstrip .fs-item:last-child { border-right: none; padding-right: 0; }
.fingerstrip .fs-item .k {
  font-size: 12px; font-weight: 700; letter-spacing: 1px;
  text-transform: uppercase; color: var(--sub); line-height: 1.2;
}
.fingerstrip .fs-item .v {
  font-size: 15px; font-weight: 700; color: var(--fg); line-height: 1.25;
  letter-spacing: -.15px;
}
.fingerstrip .fs-item .s {
  font-size: 12.5px; color: var(--mut); line-height: 1.2;
}
.fingerstrip .fs-item.brand .v { color: var(--accent); }
@media (prefers-color-scheme: dark) {
  .fingerstrip .fs-item { border-right-color: rgba(165,180,252,.18); }
}
@media (max-width: 800px) {
  .fingerstrip { padding: 9px 1rem; }
  .fingerstrip .fs-item { padding: 2px 12px; }
}

/* Sidebar */
.sidebar {
  position: sticky; top: 118px; align-self: start;
  max-height: calc(100vh - 134px); overflow: auto;
  padding-right: .4rem;
  font-size: 14.5px;
}
.sidebar h4 {
  margin: 1.25rem 0 .4rem; font-size: 12px; letter-spacing: 1.2px;
  text-transform: uppercase; color: var(--sub); font-weight: 700;
}
.sidebar h4:first-child { margin-top: 0; }

/* Collapsible nav groups (native <details>) */
.sidebar details.nav-group { margin: 14px 0 6px; }
.sidebar details.nav-group:first-of-type { margin-top: 0; }
.sidebar details.nav-group > summary {
  list-style: none; cursor: pointer;
  display: flex; align-items: center; gap: 6px;
  padding: 6px 4px 6px 4px;
  font-size: 12px; font-weight: 700;
  letter-spacing: 1.2px; text-transform: uppercase;
  color: var(--sub);
  border-radius: 4px;
  user-select: none;
}
.sidebar details.nav-group > summary:hover { color: var(--fg); }
.sidebar details.nav-group > summary::-webkit-details-marker { display: none; }
.sidebar details.nav-group > summary::before {
  content: "▸"; display: inline-block;
  width: 10px; font-size: 12px; color: var(--mut);
  transition: transform .15s ease;
}
.sidebar details.nav-group[open] > summary::before { transform: rotate(90deg); }
.sidebar details.nav-group > .group-body { padding: 2px 0 4px; }

/* Per-category collapsible block */
.sidebar details.cat-group { margin: 6px 0 2px; }
.sidebar details.cat-group > summary {
  list-style: none; cursor: pointer;
  display: flex; align-items: center; gap: 6px;
  padding: 6px 10px; border-radius: 6px;
  font-size: 13px; font-weight: 700; letter-spacing: .5px;
  text-transform: uppercase; line-height: 1.25;
  user-select: none;
}
.sidebar details.cat-group > summary:hover { background: var(--hover); }
.sidebar details.cat-group > summary::-webkit-details-marker { display: none; }
.sidebar details.cat-group > summary::before {
  content: "▸"; display: inline-block;
  width: 9px; font-size: 12px; color: currentColor; opacity: .7;
  transition: transform .15s ease;
}
.sidebar details.cat-group[open] > summary::before { transform: rotate(90deg); }
.sidebar details.cat-group > summary .cat-lbl { flex: 1; min-width: 0; }
.sidebar details.cat-group > summary .cnt {
  font-size: 12px; color: var(--mut); font-weight: 600;
  padding: 1px 8px; background: var(--hover); border-radius: 99px;
  letter-spacing: 0; text-transform: none;
}

.sidebar a {
  display: flex; align-items: center; gap: 8px;
  color: var(--mut); text-decoration: none; padding: 6px 10px; border-radius: 6px;
  line-height: 1.4;
  font-size: 14px;
  transition: color .12s, background .12s;
}
.sidebar a:hover { color: var(--fg); background: var(--hover); }
.sidebar a.active {
  color: var(--accent); background: var(--info-bg); font-weight: 600;
}
.sidebar a code { font: 12.5px var(--mono); color: var(--sub); min-width: 34px; }
.sidebar a span { font-size: 14px; }
.sidebar svg { width: 14px; height: 14px; flex: 0 0 14px; }
.sidebar .mini-verdict {
  padding: .75rem .9rem; border-radius: 10px; border: 1px solid var(--border);
  background: var(--card); display: flex; flex-direction: column; gap: 3px;
  box-shadow: var(--shadow);
}
.sidebar .mini-verdict .big { font-weight: 700; font-size: 16px; }
.sidebar .mini-verdict .big.ok   { color: var(--ok); }
.sidebar .mini-verdict .big.warn { color: var(--warn); }
.sidebar .mini-verdict .big.crit { color: var(--crit); }
.sidebar .mini-verdict .big.info { color: var(--info); }
.sidebar .mini-verdict .meta { font-size: 12.5px; color: var(--sub); }
@media (max-width: 1100px) { .sidebar { position: static; max-height: none; } }

/* Hero */
.hero {
  position: relative; overflow: hidden;
  background: linear-gradient(135deg, var(--card) 0%, var(--hover) 100%);
  border: 1px solid var(--border); border-radius: var(--radius-lg);
  padding: 1.75rem 2rem; margin-bottom: 2rem; box-shadow: var(--shadow-md);
  display: grid; grid-template-columns: 1fr 260px; gap: 2rem;
}
.hero::before {
  content: ""; position: absolute; top: 0; left: 0; right: 0; height: 3px;
  background: linear-gradient(90deg, var(--accent) 0%, var(--ok) 50%, var(--warn) 100%);
  border-radius: var(--radius-lg) var(--radius-lg) 0 0;
}
@media (max-width: 800px) { .hero { grid-template-columns: 1fr; } }
.hero h2 {
  font-size: 30px; margin: 0 0 .35rem; letter-spacing: -.6px;
  font-weight: 700; line-height: 1.15;
}
.hero .sub { color: var(--mut); font-size: 14px; margin-bottom: 1rem; line-height: 1.55; }
.hero .sub code {
  font: 13px var(--mono); padding: 2px 7px;
  background: var(--card); border: 1px solid var(--border); border-radius: 5px;
}

/* Severity bar */
.sev-bar {
  display: flex; height: 14px; border-radius: 99px; overflow: hidden;
  background: var(--border); margin: .6rem 0 .7rem;
  box-shadow: inset 0 1px 2px rgba(15,23,42,.06);
}
.sev-bar .seg { height: 100%; transition: width .3s; }
.sev-bar .seg.crit { background: var(--crit); }
.sev-bar .seg.warn { background: var(--warn); }
.sev-bar .seg.info { background: var(--info); }
.sev-bar .seg.ok   { background: var(--ok); }
.sev-legend {
  display: flex; flex-wrap: wrap; gap: 1rem; font-size: 13px; color: var(--mut);
}
.sev-legend span { display: inline-flex; align-items: center; gap: 6px; }
.sev-legend .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; }
.dot.ok { background: var(--ok); } .dot.warn { background: var(--warn); }
.dot.crit { background: var(--crit); } .dot.info { background: var(--info); }
.dot.unk { background: var(--unk); }

/* Big verdict */
.verdict {
  background: var(--card); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 1.1rem 1.25rem;
  display: flex; flex-direction: column; gap: .4rem; align-self: start;
  box-shadow: var(--shadow);
}
.verdict .lbl {
  font-size: 12px; font-weight: 700; letter-spacing: 1.2px;
  text-transform: uppercase; color: var(--sub);
}
.verdict .big {
  font-size: 26px; font-weight: 750; line-height: 1.1;
  display: flex; align-items: center; gap: 8px;
}
.verdict .big svg { width: 24px; height: 24px; }
.verdict.ok   .big { color: var(--ok); }
.verdict.warn .big { color: var(--warn); }
.verdict.crit .big { color: var(--crit); }
.verdict.info .big { color: var(--info); }
.verdict .counts {
  display: flex; gap: 1rem; margin-top: .4rem; font-size: 13px; color: var(--mut);
}
.verdict .counts span { display: inline-flex; align-items: center; gap: 5px; }

/* Fingerprint tiles */
.fingerprint {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: .85rem; margin-bottom: 2rem;
}
.tile {
  position: relative; overflow: hidden;
  background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
  padding: .9rem 1.15rem; min-height: 82px;
  box-shadow: var(--shadow);
  transition: transform .15s, box-shadow .15s, border-color .15s;
}
.tile:hover {
  transform: translateY(-1px); box-shadow: var(--shadow-md);
  border-color: rgba(79,70,229,.25);
}
.tile::after {
  content: ""; position: absolute; top: 0; left: 0; height: 2px; width: 28px;
  background: var(--accent); opacity: .7;
}
.tile .k {
  font-size: 12px; font-weight: 700; letter-spacing: 1px; color: var(--sub);
  text-transform: uppercase;
}
.tile .v {
  font-size: 22px; font-weight: 700; color: var(--fg); line-height: 1.15; margin-top: 6px;
  letter-spacing: -.3px;
}
.tile .s { font-size: 12.5px; color: var(--mut); margin-top: 3px; }

/* "Top fixes" list in hero */
.topfix { margin-top: 1.25rem; }
.topfix h3 {
  font-size: 16px; letter-spacing: -.1px; text-transform: none;
  color: var(--fg); margin: 0 0 .7rem; font-weight: 800;
  display: flex; align-items: center; gap: 9px;
  padding-left: 11px; border-left: 4px solid var(--crit);
  line-height: 1.2;
}
.topfix h3 .cnt {
  font: 700 13px var(--mono); color: var(--crit);
  background: var(--crit-bg); padding: 2px 10px; border-radius: 99px;
  letter-spacing: 0; text-transform: none;
  border: 1px solid rgba(220,38,38,.30);
}
.topfix ol { margin: 0; padding: 0; list-style: none; }
.topfix li {
  display: flex; align-items: flex-start; gap: .7rem; padding: 10px 0;
  border-top: 1px solid var(--border); font-size: 14px;
}
.topfix li:first-child { border-top: none; padding-top: 6px; }
.topfix .num {
  flex: 0 0 26px; height: 26px; border-radius: 50%; font-size: 12.5px;
  display: inline-flex; align-items: center; justify-content: center; font-weight: 700;
  background: var(--hover); color: var(--mut);
}
.topfix .body { flex: 1; min-width: 0; }
.topfix .body a { color: inherit; text-decoration: none; }
.topfix .body a:hover { color: var(--accent); }
.topfix .title-line {
  font-weight: 600; display: flex; align-items: center; gap: .55rem;
  flex-wrap: wrap; font-size: 14px;
}
.topfix .headline { color: var(--mut); font-size: 13.5px; margin-top: 3px; line-height: 1.5; }
.topfix .fix {
  margin-top: 6px; padding: 8px 12px;
  background: var(--info-bg); border-left: 3px solid var(--info);
  border-radius: 0 6px 6px 0;
  font-size: 13.5px; line-height: 1.5; color: var(--fg);
  display: flex; gap: 7px; align-items: flex-start;
}
.topfix .fix svg { width: 14px; height: 14px; margin-top: 2px; flex: 0 0 14px; color: var(--info); }
.topfix .fix b {
  color: var(--info); font-weight: 700; letter-spacing: .4px; text-transform: uppercase;
  font-size: 12px; margin-right: 5px;
}
.topfix li.sev-warn .fix { background: var(--warn-bg); border-left-color: var(--warn); }
.topfix li.sev-warn .fix svg, .topfix li.sev-warn .fix b { color: var(--warn); }
.topfix li.sev-crit .fix { background: var(--crit-bg); border-left-color: var(--crit); }
.topfix li.sev-crit .fix svg, .topfix li.sev-crit .fix b { color: var(--crit); }
.topfix .empty { color: var(--mut); font-size: 13.5px; padding: .5rem 0; }

/* "Recommendations needing attention" — compact list inside the hero,
   second subsection under Top fixes. Shows only warn + crit advisors
   that carry a quantitative recommendation (e.g. "Recommended ≥ X"),
   so operators see the exact actionable numbers without scrolling. */
.attn-recs { margin-top: 1.5rem; }
.attn-recs h3 {
  font-size: 16px; letter-spacing: -.1px; text-transform: none;
  color: var(--fg); margin: 0 0 .7rem; font-weight: 800;
  display: flex; align-items: center; gap: 9px;
  padding-left: 11px; border-left: 4px solid var(--accent);
  line-height: 1.2;
}
.attn-recs h3 .cnt {
  font: 700 13px var(--mono); color: var(--accent);
  background: var(--info-bg); padding: 2px 10px; border-radius: 99px;
  letter-spacing: 0; text-transform: none;
  border: 1px solid rgba(79,70,229,.30);
}
.attn-recs ul { list-style: none; padding: 0; margin: 0;
                display: flex; flex-direction: column; gap: 8px; }
.attn-recs li {
  border: 1px solid var(--border); border-radius: 9px;
  background: var(--card); overflow: hidden;
  transition: transform .12s, box-shadow .12s;
}
.attn-recs li:hover { transform: translateY(-1px); box-shadow: var(--shadow-md); }
.attn-recs li a {
  display: block; padding: 10px 14px;
  color: inherit; text-decoration: none;
}
.attn-recs .rec-hd {
  display: flex; align-items: center; gap: 7px; flex-wrap: wrap;
  font-size: 13.5px;
}
.attn-recs .rec-ttl { font-weight: 700; color: var(--fg); font-size: 13.5px; }
.attn-recs li:hover .rec-ttl { color: var(--accent); }
.attn-recs .rec-val {
  margin-top: 7px;
  font: 600 13.5px var(--mono);
  color: var(--fg); line-height: 1.55;
  word-break: break-word;
  padding: 7px 11px;
  background: rgba(127,127,127,.08);
  border-radius: 6px;
  border-left: 3px solid var(--border);
}
.attn-recs li.sev-crit {
  border-left: 4px solid var(--crit);
  background: linear-gradient(90deg, var(--crit-bg) 0%, var(--card) 55%);
}
.attn-recs li.sev-crit .rec-val { border-left-color: var(--crit); }
.attn-recs li.sev-warn {
  border-left: 4px solid var(--warn);
  background: linear-gradient(90deg, var(--warn-bg) 0%, var(--card) 55%);
}
.attn-recs li.sev-warn .rec-val { border-left-color: var(--warn); }

/* Severity & category badges */
.badge {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 3px 10px; border-radius: 99px;
  font-size: 12px; font-weight: 700; letter-spacing: .4px; color: #fff;
  line-height: 1.6; white-space: nowrap;
}
.badge.ok   { background: var(--ok); }
.badge.warn { background: var(--warn); }
.badge.crit { background: var(--crit); }
.badge.info { background: var(--info); }
.badge.unk  { background: var(--unk); }
.badge svg { width: 13px; height: 13px; }

.cat {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 3px 10px; border-radius: 99px;
  font-size: 12px; font-weight: 600;
  border: 1px solid currentColor;
  background: transparent;
  line-height: 1.6; white-space: nowrap;
}
.cat svg { width: 13px; height: 13px; }

/* Section heading */
h2.section {
  font-size: 22px; margin: 2.5rem 0 1.1rem; letter-spacing: -.3px; font-weight: 700;
  padding-bottom: .6rem; border-bottom: 2px solid var(--border);
  display: flex; align-items: baseline; gap: .9rem;
  position: relative; scroll-margin-top: 128px;
}
h2.section::before {
  content: ""; position: absolute; bottom: -2px; left: 0; width: 60px; height: 2px;
  background: var(--accent); border-radius: 2px;
}
h2.section .count { color: var(--sub); font-weight: 500; font-size: 14px; }
h2.section .descr { color: var(--mut); font-weight: 400; font-size: 14px; margin-left: auto; }

/* Playbook */
.playbook {
  background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
  box-shadow: var(--shadow); padding: 1rem 1.25rem; margin-bottom: 1.5rem;
}
.playbook header {
  display: flex; align-items: center; gap: .75rem; margin-bottom: .75rem;
}
.playbook header h3 { margin: 0; font-size: 15px; font-weight: 700; }
.playbook header .count { color: var(--sub); font-size: 13px; }
.playbook header .copy-btn {
  margin-left: auto; font: inherit; cursor: pointer; border: 1px solid var(--border);
  background: var(--hover); color: var(--fg); border-radius: 7px; padding: 5px 12px;
  font-size: 13px;
}
.playbook header .copy-btn:hover { border-color: var(--accent); color: var(--accent); }
.playbook header .copy-btn.copied { background: var(--ok); color: #fff; border-color: var(--ok); }
.playbook ol { padding: 0; margin: 0; list-style: none; counter-reset: p; }
.playbook li {
  counter-increment: p; padding: .8rem 0 .9rem; border-top: 1px solid var(--border);
  display: grid; grid-template-columns: 40px 1fr; gap: .7rem;
}
.playbook li:first-child { border-top: none; padding-top: .25rem; }
.playbook li::before {
  content: counter(p); font: 700 13px var(--mono); color: var(--sub);
  background: var(--hover); border-radius: 50%; width: 30px; height: 30px;
  display: inline-flex; align-items: center; justify-content: center; margin-top: 2px;
}
.playbook li .hd { display: flex; align-items: center; gap: .55rem; flex-wrap: wrap; margin-bottom: 4px; }
.playbook li .hd .ttl { font-weight: 700; font-size: 15px; }
.playbook li .hd .ttl a { color: inherit; text-decoration: none; }
.playbook li .hd .ttl a:hover { color: var(--accent); }
.playbook li .headline { color: var(--fg); font-size: 14px; margin: 3px 0; line-height: 1.55; }
.playbook li .fix {
  margin-top: 7px; padding: 8px 12px;
  background: var(--info-bg); border-left: 3px solid var(--info);
  border-radius: 0 6px 6px 0;
  color: var(--fg); font-size: 14px; line-height: 1.55;
}
.playbook li .fix b { color: var(--info); font-weight: 700; letter-spacing: .4px;
                      text-transform: uppercase; font-size: 12px; margin-right: 7px; }
.playbook li.sev-warn .fix { background: var(--warn-bg); border-left-color: var(--warn); }
.playbook li.sev-warn .fix b { color: var(--warn); }
.playbook li.sev-crit .fix { background: var(--crit-bg); border-left-color: var(--crit); }
.playbook li.sev-crit .fix b { color: var(--crit); }
.playbook .empty { padding: 1rem 0; color: var(--mut); font-size: 14px; }

/* Summary: filter bar */
.filter-bar {
  display: flex; flex-wrap: wrap; align-items: center; gap: .55rem;
  background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
  padding: .6rem .75rem; margin-bottom: .85rem;
  box-shadow: var(--shadow);
}
.filter-bar input[type=search] {
  flex: 1 1 200px; min-width: 140px; border: 1px solid var(--border);
  border-radius: 7px; padding: 7px 12px; background: var(--bg); color: var(--fg);
  font: inherit; outline: none; font-size: 14px;
  transition: border-color .12s, box-shadow .12s;
}
.filter-bar input[type=search]:focus { border-color: var(--info); box-shadow: 0 0 0 3px rgba(79,70,229,.18); }
.chip {
  display: inline-flex; align-items: center; gap: 6px;
  font: 13px var(--sans); padding: 5px 12px; border-radius: 99px;
  border: 1px solid var(--border); background: var(--hover); color: var(--mut);
  cursor: pointer; user-select: none;
  transition: color .12s, background .12s, border-color .12s;
}
.chip:hover { color: var(--fg); border-color: var(--mut); }
.chip.active { color: #fff; background: var(--info); border-color: var(--info); }
.chip.active.sev-crit { background: var(--crit); border-color: var(--crit); }
.chip.active.sev-warn { background: var(--warn); border-color: var(--warn); }
.chip.active.sev-ok   { background: var(--ok);   border-color: var(--ok); }
.chip.active.sev-info { background: var(--info); border-color: var(--info); }
.chip svg { width: 13px; height: 13px; }
.filter-bar .sep { color: var(--sub); font-size: 12px; padding: 0 5px; }

/* Summary table */
.card {
  background: var(--card); border: 1px solid var(--border);
  border-radius: var(--radius); box-shadow: var(--shadow); overflow: hidden;
}
table.summary { width: 100%; border-collapse: collapse; }
table.summary th, table.summary td {
  padding: 12px 16px; text-align: left; vertical-align: top;
  border-bottom: 1px solid var(--border); font-size: 14px;
}
table.summary th {
  font-size: 12px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;
  color: var(--sub); background: var(--hover);
}
table.summary tr:last-child td { border-bottom: none; }
table.summary tr:hover td { background: var(--hover); }
table.summary td code.mono { font: 13px var(--mono);
                             padding: 2px 7px; background: var(--hover);
                             border: 1px solid var(--border); border-radius: 5px; color: var(--fg); }
table.summary .col-sev  { width: 115px; }
table.summary .col-cat  { width: 135px; }
table.summary .col-id   { width: 56px; }
table.summary .col-name { width: 270px; }
table.summary .col-rec  { width: 310px; }
table.summary tr.is-passing { opacity: 0.7; }
table.summary tr[hidden-row] { display: none; }
.headline { font-size: 14px; color: var(--fg); line-height: 1.55; }
.rec { font: 600 14px var(--mono); color: var(--info); }

/* Passing toggle */
.pass-toggle {
  margin: .55rem 0 1.1rem; display: flex; justify-content: flex-end;
}
.pass-toggle button {
  font: inherit; cursor: pointer; background: transparent; color: var(--mut);
  border: 1px dashed var(--border); border-radius: 7px; padding: 5px 14px; font-size: 13px;
}
.pass-toggle button:hover { color: var(--fg); border-color: var(--mut); }
.summary-empty { text-align: center; padding: 2rem; color: var(--mut); font-size: 14px; display: none; }

/* Per-advisor card */
.adv {
  background: var(--card); border: 1px solid var(--border);
  border-radius: var(--radius); box-shadow: var(--shadow);
  padding: 1.25rem 1.5rem; margin-bottom: 1.1rem;
  scroll-margin-top: 128px;
  transition: box-shadow .15s, border-color .15s;
}
.adv:hover { box-shadow: var(--shadow-md); }
.adv .head { display: flex; align-items: center; gap: .75rem; flex-wrap: wrap; margin-bottom: .5rem; }
.adv .head code { font: 12px var(--mono); color: var(--sub); padding: 2px 7px;
                  background: var(--hover); border: 1px solid var(--border); border-radius: 5px; }
.adv .head .title { font-size: 17px; font-weight: 700; margin: 0; letter-spacing: -.15px; }
.adv .head .docs { margin-left: auto; font-size: 13px; color: var(--mut); text-decoration: none; }
.adv .head .docs:hover { color: var(--accent); text-decoration: underline; }

.adv .panes {
  display: grid; grid-template-columns: repeat(2, 1fr); gap: .8rem;
  margin: .9rem 0;
}
@media (max-width: 740px) { .adv .panes { grid-template-columns: 1fr; } }
.pane {
  padding: .8rem 1rem; border-radius: 10px; border: 1px solid var(--border);
  background: var(--bg);
}
.pane .lbl {
  font-size: 12px; font-weight: 700; letter-spacing: 1.1px;
  text-transform: uppercase; color: var(--sub); margin-bottom: 5px;
  display: inline-flex; align-items: center; gap: 5px;
}
.pane .lbl svg { width: 13px; height: 13px; }
.pane .val { font-size: 14px; line-height: 1.6; color: var(--fg); }
.pane.finding { background: var(--info-bg); border-color: rgba(79,70,229,.30); }
.pane.finding.ok   { background: var(--ok-bg);   border-color: rgba(5,150,105,.30); }
.pane.finding.warn { background: var(--warn-bg); border-color: rgba(217,119,6,.30); }
.pane.finding.crit { background: var(--crit-bg); border-color: rgba(220,38,38,.30); }
.pane.fix {
  background: linear-gradient(180deg, var(--info-bg) 0%, var(--card) 70%);
  border-left: 3px solid var(--info);
  grid-column: 1 / -1;
  padding: .85rem 1rem;
}
.pane.fix .lbl { color: var(--info); font-size: 12px; letter-spacing: 1.1px; }
.pane.fix .lbl svg { width: 14px; height: 14px; }
.pane.fix .val { font-size: 14.5px; line-height: 1.6; color: var(--fg); font-weight: 500; }

/* Severity-tinted variants */
.pane.fix.warn { border-left-color: var(--warn);
                 background: linear-gradient(180deg, var(--warn-bg) 0%, var(--card) 70%); }
.pane.fix.warn .lbl { color: var(--warn); }
.pane.fix.crit { border-left-color: var(--crit);
                 background: linear-gradient(180deg, var(--crit-bg) 0%, var(--card) 70%); }
.pane.fix.crit .lbl { color: var(--crit); }
.pane.fix.ok   { border-left-color: var(--ok);
                 background: linear-gradient(180deg, var(--ok-bg) 0%, var(--card) 70%); }
.pane.fix.ok   .lbl { color: var(--ok); }

/* Quantitative recommendation chip on each advisor card — make this
   unmistakable: bigger, brighter, with a soft glow. */
.adv .rec-pill {
  display: flex; align-items: center; gap: 12px;
  margin: 1rem 0 .25rem; padding: 14px 18px;
  background: linear-gradient(135deg, rgba(79,70,229,.20) 0%, rgba(79,70,229,.06) 100%),
              var(--card);
  border: 1px solid rgba(79,70,229,.40);
  border-left: 5px solid var(--info);
  border-radius: var(--radius);
  font: 600 15.5px var(--mono);
  color: var(--fg);
  box-shadow: var(--shadow-accent);
  position: relative;
}
.adv .rec-pill::before {
  content: "✨";
  font-size: 18px; line-height: 1;
  flex: 0 0 auto;
  filter: drop-shadow(0 0 6px rgba(79,70,229,.35));
}
.adv .rec-pill .rec-lbl {
  font: 700 12px var(--sans);
  letter-spacing: 1.3px; text-transform: uppercase;
  color: var(--info);
  padding: 4px 10px; border-radius: 99px;
  background: var(--card);
  border: 1px solid rgba(79,70,229,.35);
  flex: 0 0 auto;
}
.adv .rec-pill .rec-val {
  flex: 1; min-width: 0; font: 600 15.5px var(--mono); color: var(--fg);
  word-break: break-word; line-height: 1.5;
}
/* Severity-tinted variants */
.adv .rec-pill.warn {
  background: linear-gradient(135deg, rgba(217,119,6,.22) 0%, rgba(217,119,6,.06) 100%), var(--card);
  border-color: rgba(217,119,6,.45); border-left-color: var(--warn);
  box-shadow: 0 1px 3px rgba(217,119,6,.14), 0 8px 22px rgba(217,119,6,.14);
}
.adv .rec-pill.warn .rec-lbl { color: var(--warn); border-color: rgba(217,119,6,.35); }
.adv .rec-pill.crit {
  background: linear-gradient(135deg, rgba(220,38,38,.22) 0%, rgba(220,38,38,.06) 100%), var(--card);
  border-color: rgba(220,38,38,.50); border-left-color: var(--crit);
  box-shadow: 0 1px 3px rgba(220,38,38,.18), 0 10px 26px rgba(220,38,38,.16);
}
.adv .rec-pill.crit .rec-lbl { color: var(--crit); border-color: rgba(220,38,38,.40); }
.adv .rec-pill.ok {
  background: linear-gradient(135deg, rgba(5,150,105,.18) 0%, rgba(5,150,105,.05) 100%), var(--card);
  border-color: rgba(5,150,105,.40); border-left-color: var(--ok);
  box-shadow: 0 1px 3px rgba(5,150,105,.12), 0 8px 22px rgba(5,150,105,.10);
}
.adv .rec-pill.ok .rec-lbl { color: var(--ok); border-color: rgba(5,150,105,.30); }

.adv details { margin-top: .85rem; }
.adv summary { cursor: pointer; font-weight: 600; font-size: 13.5px;
               padding: 8px 14px; background: var(--hover);
               border: 1px solid var(--border); border-radius: 7px;
               list-style: none; color: var(--mut); }
.adv summary:hover { color: var(--fg); }
.adv summary::-webkit-details-marker { display: none; }
.adv summary::before { content: "▸"; margin-right: 7px; color: var(--sub);
                       display: inline-block; transition: transform .15s; }
.adv details[open] summary::before { transform: rotate(90deg); }

pre.raw { background: #0b1021; color: #d7dbe6; font-family: var(--mono);
          font-size: 13px; padding: 16px 18px; border-radius: 8px;
          overflow: auto; max-height: 600px; margin: .6rem 0 0; white-space: pre;
          line-height: 1.55; }

/* Snapshot */
.snap-filter { display: flex; align-items: center; gap: .7rem; margin: 0 0 .85rem; }
.snap-filter input {
  flex: 1; padding: 9px 14px; font-size: 14px; font-family: inherit;
  color: var(--fg); background: var(--card);
  border: 1px solid var(--border); border-radius: 7px; outline: none;
  transition: border-color .12s, box-shadow .12s;
}
.snap-filter input:focus { border-color: var(--info); box-shadow: 0 0 0 3px rgba(79,70,229,.18); }
.snap-filter .muted { font-size: 13px; color: var(--mut); }

.sect { margin-bottom: .5rem; background: var(--card);
        border: 1px solid var(--border); border-radius: 9px; }
.sect summary {
  cursor: pointer; padding: 10px 14px; list-style: none;
  display: flex; align-items: center; gap: .75rem; font-size: 14px;
}
.sect summary::-webkit-details-marker { display: none; }
.sect summary::before { content: "▸"; color: var(--sub); transition: transform .15s; }
.sect[open] summary::before { transform: rotate(90deg); }
.sect summary .sid { font: 13px var(--mono); color: var(--fg); }
.sect summary .rowcount { color: var(--sub); font-size: 13px; margin-left: auto; }
.sect summary .csv-btn {
  font: 12px var(--sans); cursor: pointer; background: transparent; color: var(--mut);
  border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px;
}
.sect summary .csv-btn:hover { color: var(--fg); border-color: var(--mut); }
.sect summary .csv-btn[disabled] { opacity: .4; cursor: not-allowed; }
.sect .body { padding: 0 14px 12px; }

.scroll { max-height: 460px; overflow: auto;
          border: 1px solid var(--border); border-radius: 7px; }
table.csv { border-collapse: collapse; width: 100%; font: 13px var(--mono); }
table.csv th, table.csv td {
  padding: 7px 12px; border: 1px solid var(--border);
  text-align: left; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; max-width: 420px;
}
table.csv th { background: var(--hover); position: sticky; top: 0;
               font-weight: 700; color: var(--fg); }
table.csv tr:nth-child(even) td { background: rgba(127,127,127,.04); }

.muted { color: var(--mut); font-size: 13.5px; }

/* Privacy banner */
.privacy {
  margin: 0 0 20px;
  padding: 12px 16px;
  border: 1px solid var(--border);
  border-left: 4px solid #d1a14a;
  border-radius: 9px;
  background: var(--card);
  color: var(--mut);
  font-size: 13.5px;
  line-height: 1.6;
  display: flex; gap: 11px; align-items: flex-start;
  box-shadow: var(--shadow);
}
.privacy .pico { flex: 0 0 auto; color: #d1a14a; font-weight: 700; }
.privacy strong { color: var(--fg); }
.privacy code { font: 12.5px var(--mono); padding: 1px 6px;
                background: rgba(127,127,127,.12); border-radius: 4px; }

/* Print */
@media print {
  @page { margin: 16mm 14mm; }
  html, body { background: #fff !important; color: #000 !important; }
  .topbar, .sidebar, .filter-bar, .pass-toggle, .snap-filter, .csv-btn, .copy-btn, .print-btn { display: none !important; }
  /* Print-adapted fingerprint strip: unpin, simplify */
  .fingerstrip {
    position: static !important; background: #fff !important;
    border: 1px solid #ccc !important; border-radius: 4px;
    padding: 8mm 6mm !important; margin-bottom: 4mm;
    overflow: visible !important; flex-wrap: wrap;
  }
  .fingerstrip::before { display: none; }
  .fingerstrip .fs-item { border-right: 1px solid #ccc !important; padding: 2px 10mm !important; }
  .wrap { grid-template-columns: 1fr; max-width: none; padding: 0; }
  .hero, .card, .adv, .playbook, .tile, .sect {
    box-shadow: none !important; border-color: #999 !important; page-break-inside: avoid;
  }
  .adv { break-inside: avoid; }
  .hero { page-break-after: avoid; }
  h2.section { page-break-before: auto; page-break-after: avoid; }
  details[open] { border: none; }
  details summary { list-style: none; }
  pre.raw { background: #f6f7f9 !important; color: #222 !important;
            max-height: none; overflow: visible; white-space: pre-wrap; font-size: 10px; }
  .sect[hidden-by-filter] { display: block !important; }
  a[href^="http"]::after { content: " (" attr(href) ")"; font-size: 9px; color: #555; }
}
"""

# --- JS ---------------------------------------------------------------------

JS = r"""
(function () {
  'use strict';

  // ---------- snapshot section filter + CSV download ----------
  var snapIn = document.getElementById('snapshot-filter');
  var snapCount = document.getElementById('snapshot-count');
  var sects = document.querySelectorAll('.sect');
  if (snapIn) {
    var total = sects.length;
    snapIn.addEventListener('input', function () {
      var q = snapIn.value.toLowerCase().trim();
      var shown = 0;
      sects.forEach(function (s) {
        var id = (s.getAttribute('data-sid') || '').toLowerCase();
        var cat = (s.getAttribute('data-cat') || '').toLowerCase();
        var ok = !q || id.indexOf(q) !== -1 || cat.indexOf(q) !== -1;
        s.style.display = ok ? '' : 'none';
        if (ok) shown++;
      });
      if (snapCount) snapCount.textContent = shown + ' / ' + total + ' sections';
    });
  }

  function tsvToCsv(tsv) {
    var lines = tsv.split(/\r?\n/);
    var out = [];
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      if (i === lines.length - 1 && line === '') break;
      var fields = line.split('\t');
      var quoted = fields.map(function (f) {
        if (/[",\r\n]/.test(f)) return '"' + f.replace(/"/g, '""') + '"';
        return f;
      });
      out.push(quoted.join(','));
    }
    return out.join('\r\n') + '\r\n';
  }

  document.querySelectorAll('.csv-btn[data-src]').forEach(function (btn) {
    btn.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      var srcId = btn.getAttribute('data-src');
      var fname = btn.getAttribute('data-fname') || 'section.csv';
      var node = document.getElementById(srcId);
      if (!node) return;
      // For TSV-embedded data, innerText contains raw tab-separated content.
      // Decode common HTML entities that textContent leaves intact.
      var raw = node.textContent || '';
      var isTsv = (node.getAttribute('data-fmt') || 'tsv') === 'tsv';
      var csv = isTsv ? tsvToCsv(raw) : raw;
      var blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      var a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = fname;
      document.body.appendChild(a);
      a.click();
      setTimeout(function () {
        URL.revokeObjectURL(a.href);
        document.body.removeChild(a);
      }, 50);
    });
  });

  // ---------- summary table filter ----------
  var tbl = document.querySelector('table.summary');
  if (tbl) {
    var rows = tbl.querySelectorAll('tbody tr');
    var sevChips = document.querySelectorAll('.chip[data-sev]');
    var catChips = document.querySelectorAll('.chip[data-cat]');
    var searchIn = document.getElementById('summary-search');
    var passBtn = document.getElementById('pass-toggle-btn');
    var emptyEl = document.getElementById('summary-empty');
    var activeSev = new Set();
    var activeCat = new Set();
    var query = '';
    var showPassing = false;

    function applyFilter() {
      var shown = 0;
      rows.forEach(function (tr) {
        var sev = tr.getAttribute('data-sev');
        var cat = tr.getAttribute('data-cat');
        var txt = tr.textContent.toLowerCase();
        var passes = sev === 'ok' || sev === 'info';
        var sevOk = !activeSev.size || activeSev.has(sev);
        var catOk = !activeCat.size || activeCat.has(cat);
        var qOk = !query || txt.indexOf(query) !== -1;
        var passOk = !passes || showPassing || (activeSev.size && activeSev.has(sev));
        var ok = sevOk && catOk && qOk && passOk;
        if (ok) { tr.removeAttribute('hidden-row'); shown++; }
        else { tr.setAttribute('hidden-row', ''); }
      });
      if (emptyEl) emptyEl.style.display = shown === 0 ? 'block' : 'none';
      if (passBtn) {
        var passCount = 0;
        rows.forEach(function (tr) {
          var sev = tr.getAttribute('data-sev');
          if (sev === 'ok' || sev === 'info') passCount++;
        });
        passBtn.textContent = showPassing
          ? 'Hide ' + passCount + ' passing checks'
          : 'Show ' + passCount + ' passing checks';
      }
    }

    sevChips.forEach(function (c) {
      c.addEventListener('click', function () {
        var v = c.getAttribute('data-sev');
        var pressed = activeSev.has(v);
        if (pressed) { activeSev.delete(v); c.classList.remove('active'); c.setAttribute('aria-pressed','false'); }
        else { activeSev.add(v); c.classList.add('active'); c.setAttribute('aria-pressed','true'); }
        applyFilter();
      });
    });
    catChips.forEach(function (c) {
      c.addEventListener('click', function () {
        var v = c.getAttribute('data-cat');
        var pressed = activeCat.has(v);
        if (pressed) { activeCat.delete(v); c.classList.remove('active'); c.setAttribute('aria-pressed','false'); }
        else { activeCat.add(v); c.classList.add('active'); c.setAttribute('aria-pressed','true'); }
        applyFilter();
      });
    });
    if (searchIn) {
      searchIn.addEventListener('input', function () {
        query = searchIn.value.toLowerCase().trim();
        applyFilter();
      });
    }
    if (passBtn) {
      passBtn.addEventListener('click', function () {
        showPassing = !showPassing;
        applyFilter();
      });
    }
    applyFilter();
  }

  // ---------- playbook copy-to-clipboard ----------
  var copyBtn = document.getElementById('playbook-copy');
  if (copyBtn) {
    copyBtn.addEventListener('click', function () {
      var src = document.getElementById('playbook-plaintext');
      var text = src ? (src.textContent || '') : '';
      if (!text) return;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function () {
          copyBtn.textContent = 'Copied ✓';
          copyBtn.classList.add('copied');
          setTimeout(function () {
            copyBtn.textContent = 'Copy playbook';
            copyBtn.classList.remove('copied');
          }, 1500);
        });
      } else {
        var ta = document.createElement('textarea');
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand('copy'); } catch (e) {}
        document.body.removeChild(ta);
        copyBtn.textContent = 'Copied ✓';
        copyBtn.classList.add('copied');
        setTimeout(function () {
          copyBtn.textContent = 'Copy playbook';
          copyBtn.classList.remove('copied');
        }, 1500);
      }
    });
  }

  // ---------- print button ----------
  var printBtn = document.getElementById('print-report');
  if (printBtn) {
    printBtn.addEventListener('click', function () { window.print(); });
  }
})();
"""

# --- rendering --------------------------------------------------------------

LOGO_SVG = (
    '<svg viewBox="0 0 64 64" aria-hidden="true" class="logo" '
    'xmlns="http://www.w3.org/2000/svg">'
    # connecting mesh from hub to 6 outer nodes (Citus distribution)
    '<g stroke="currentColor" stroke-width="1.4" stroke-linecap="round" '
    'stroke-opacity="0.55" fill="none">'
    '<line x1="32" y1="32" x2="32"   y2="13"/>'
    '<line x1="32" y1="32" x2="48.5" y2="22.5"/>'
    '<line x1="32" y1="32" x2="48.5" y2="41.5"/>'
    '<line x1="32" y1="32" x2="32"   y2="51"/>'
    '<line x1="32" y1="32" x2="15.5" y2="41.5"/>'
    '<line x1="32" y1="32" x2="15.5" y2="22.5"/>'
    '</g>'
    # 6 outer nodes
    '<g fill="currentColor">'
    '<circle cx="32"   cy="13"   r="3"/>'
    '<circle cx="48.5" cy="22.5" r="3"/>'
    '<circle cx="48.5" cy="41.5" r="3"/>'
    '<circle cx="32"   cy="51"   r="3"/>'
    '<circle cx="15.5" cy="41.5" r="3"/>'
    '<circle cx="15.5" cy="22.5" r="3"/>'
    '</g>'
    # central hub (PostgreSQL nod)
    '<circle cx="32" cy="32" r="6.5" fill="currentColor" '
    'fill-opacity="0.92"/>'
    # heartbeat / EKG line crossing the hub (monitoring)
    '<path d="M4 32 H22 l2 -7 3 14 3 -10 2 6 H60" '
    'fill="none" stroke="#10b981" stroke-width="2" '
    'stroke-linecap="round" stroke-linejoin="round"/>'
    # alert spark on the top-right node (alerting)
    '<circle cx="48.5" cy="22.5" r="3" fill="#fbbf24"/>'
    '<circle cx="48.5" cy="22.5" r="5.5" fill="none" '
    'stroke="#f59e0b" stroke-width="0.9" stroke-opacity="0.55"/>'
    '</svg>'
)


# Favicon: tiny self-contained mark, embedded as a data URI in <head>.
# Same icon as the topbar logo, but drawn with explicit colors so it
# renders independently of the page foreground.
FAVICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
    '<rect x="1" y="1" width="30" height="30" rx="7" ry="7" fill="#0b1120"/>'
    '<g stroke="#6366f1" stroke-width="1.1" stroke-linecap="round" '
    'stroke-opacity="0.85" fill="none">'
    '<line x1="16" y1="16" x2="16"   y2="6.5"/>'
    '<line x1="16" y1="16" x2="24.2" y2="11.2"/>'
    '<line x1="16" y1="16" x2="24.2" y2="20.8"/>'
    '<line x1="16" y1="16" x2="16"   y2="25.5"/>'
    '<line x1="16" y1="16" x2="7.8"  y2="20.8"/>'
    '<line x1="16" y1="16" x2="7.8"  y2="11.2"/>'
    '</g>'
    '<g fill="#818cf8">'
    '<circle cx="16"   cy="6.5"  r="1.9"/>'
    '<circle cx="24.2" cy="11.2" r="1.9"/>'
    '<circle cx="24.2" cy="20.8" r="1.9"/>'
    '<circle cx="16"   cy="25.5" r="1.9"/>'
    '<circle cx="7.8"  cy="20.8" r="1.9"/>'
    '<circle cx="7.8"  cy="11.2" r="1.9"/>'
    '</g>'
    '<circle cx="16" cy="16" r="3.8" fill="#5b8fc4"/>'
    '<path d="M3 16 H11 l1 -3.5 1.5 7 1.5 -5 1 3 H29" fill="none" '
    'stroke="#10b981" stroke-width="1.2" stroke-linecap="round" '
    'stroke-linejoin="round"/>'
    '<circle cx="24.2" cy="11.2" r="1.5" fill="#fbbf24"/>'
    '</svg>'
)


def _cat_pill(cat_key: str) -> str:
    """Render a category pill with icon + label, colored by category."""
    label, color = CATEGORIES.get(cat_key, ("Other", "#78909c"))
    icon = CAT_ICON.get(cat_key, "")
    return (
        f'<span class="cat" style="color:{color}">'
        f'{icon}<span>{html.escape(label)}</span></span>'
    )


def _sev_badge(sev: str) -> str:
    """Severity badge: icon + text."""
    lbl = SEV_LABEL.get(sev, "—")
    icon = SEV_ICON.get(sev, "")
    return f'<span class="badge {sev}">{icon}<span>{html.escape(lbl)}</span></span>'


def _section_cat_pill(cat_key: str) -> str:
    label, color = SECTION_CATEGORY_LABELS.get(cat_key, SECTION_CATEGORY_LABELS["other"])
    return f'<span class="cat" style="color:{color}">{html.escape(label)}</span>'


def _playbook_plaintext(playbook: list) -> str:
    """A copy-pasteable text version of the playbook for the clipboard."""
    lines = ["citus_analyze — remediation playbook", "=" * 40, ""]
    for i, item in enumerate(playbook, 1):
        aid, title, sev, hl, fix = item
        lines.append(f"{i}. [{SEV_LABEL[sev]}] {aid} — {title}")
        if hl:
            lines.append(f"   Finding: {hl}")
        if fix:
            lines.append(f"   Fix:     {fix}")
        lines.append("")
    return "\n".join(lines)


def render(run_dir: Path, out_path: Path) -> None:
    # 1. Gather inputs -------------------------------------------------------
    results = []   # (aid, title, blurb, sev, headline, rec, text)
    counts = {"ok": 0, "warn": 0, "crit": 0, "info": 0, "unk": 0}
    overall = "ok"

    for aid, title, blurb, extract_rec in ADVISORS:
        p = run_dir / f"{aid}.out"
        if p.exists():
            text = p.read_text(errors="replace")
            sev, hl = worst_verdict(text)
            rec = extract_rec(text) or ''
        else:
            text, sev, hl, rec = "", "unk", f"(missing: {aid}.out)", ""
        results.append((aid, title, blurb, sev, hl, rec, text))
        counts[sev] += 1
        if ESCALATE[sev] > ESCALATE[overall]:
            overall = sev

    sections    = parse_sections(run_dir / "gather.out")
    fingerprint = build_fingerprint(sections)
    summary_txt = (run_dir / "summary.txt").read_text(errors="replace") \
                  if (run_dir / "summary.txt").exists() else ""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ov_label = SEV_LABEL[overall]
    total = sum(counts.values())

    # Collection-time redaction state (from meta section, written by
    # citus_gather.sql). Absent column means older gather without redaction.
    meta_rows = _section_rows(sections, "meta")
    redaction_on = bool(meta_rows and
                        str(meta_rows[0].get("query_redaction", "")).lower() == "on")

    # 2. Pre-compute derived views ------------------------------------------
    top_fixes = [r for r in results if r[3] in ("crit", "warn")]
    sev_rank = {"crit": 0, "warn": 1, "info": 2, "ok": 3, "unk": 4}
    top_fixes.sort(key=lambda r: sev_rank[r[3]])

    # Playbook: every advisor needing action (crit or warn), sorted by sev.
    playbook = []
    for aid, title, _blurb, sev, hl, _rec, _text in top_fixes:
        meta = ADVISOR_META.get(aid, {})
        playbook.append((aid, title, sev, hl, meta.get("fix", "")))

    # Sorted summary rows: crit -> warn -> info -> ok (stable).
    sorted_results = sorted(
        results, key=lambda r: (sev_rank[r[3]], r[0]))

    # Summary sidebar: group advisors by category.
    cat_order = list(CATEGORIES.keys())
    adv_by_cat: dict[str, list] = {k: [] for k in cat_order}
    for aid, title, _b, sev, _hl, _rec, _t in results:
        meta = ADVISOR_META.get(aid, {})
        cat = meta.get("cat", "config")
        adv_by_cat.setdefault(cat, []).append((aid, title, sev))

    # Distinct categories actually represented (for filter chips).
    present_cats = [k for k in cat_order if adv_by_cat.get(k)]

    # 3. Build HTML ----------------------------------------------------------
    H: list[str] = []
    a = H.append

    a('<!doctype html><html lang="en"><head><meta charset="utf-8">')
    a(f'<title>citus_analyze report — {html.escape(run_dir.name)}</title>')
    a('<meta name="viewport" content="width=device-width,initial-scale=1">')
    a('<link rel="icon" type="image/svg+xml" '
      'href="data:image/svg+xml;utf8,'
      + urllib.parse.quote(FAVICON_SVG, safe="")
      + '">')
    a(f'<style>{CSS}</style></head><body>')

    # --- Top bar ---
    a('<div class="topbar">')
    a(f'<h1>{LOGO_SVG}citus_analyze {_sev_badge(overall)}</h1>')
    a('<div class="spacer"></div>')
    a('<a href="#summary">Summary</a>')
    a('<a href="#playbook">Playbook</a>')
    a('<a href="#advisors">Advisors</a>')
    a('<a href="#snapshot">Snapshot</a>')
    a('<button class="print-btn" id="print-report" type="button">Print / PDF</button>')
    a('</div>')

    # --- Sticky cluster fingerprint strip ---
    # Always-visible ribbon of cluster facts (Database, PostgreSQL, Citus,
    # nodes, shards, size, ...) that stays pinned under the topbar while
    # the user scrolls through advisors.
    if fingerprint:
        a('<div class="fingerstrip" role="region" aria-label="Cluster fingerprint">')
        brand_keys = {"Database", "PostgreSQL", "Citus"}
        for k, v, s in fingerprint:
            cls = "fs-item brand" if k in brand_keys else "fs-item"
            a(f'<div class="{cls}">')
            a(f'<div class="k">{html.escape(k)}</div>')
            a(f'<div class="v">{html.escape(v)}</div>')
            if s:
                a(f'<div class="s">{html.escape(s)}</div>')
            a('</div>')
        a('</div>')

    a('<div class="wrap">')

    # --- Sidebar ---
    a('<aside class="sidebar" aria-label="Navigation">')
    a('<div class="mini-verdict">')
    a(f'<span class="big {overall}">{html.escape(ov_label)}</span>')
    a(f'<span class="meta">{counts["crit"]} critical &middot; {counts["warn"]} warn &middot; '
      f'{counts["ok"]} ok &middot; {counts["info"]} info</span>')
    a('</div>')
    a('<details class="nav-group" open><summary>Sections</summary>')
    a('<div class="group-body">')
    a('<a href="#summary"><span>Executive summary</span></a>')
    if playbook:
        a('<a href="#playbook"><span>Remediation playbook</span></a>')
    a('<a href="#snapshot"><span>Cluster snapshot</span></a>')
    a('</div></details>')

    a('<details class="nav-group" open><summary>Advisors by category</summary>')
    a('<div class="group-body">')
    for k in present_cats:
        lbl, color = CATEGORIES[k]
        entries = adv_by_cat[k]
        a(f'<details class="cat-group" open style="color: {color};"><summary>'
          f'{CAT_ICON.get(k,"")}'
          f'<span class="cat-lbl">{html.escape(lbl)}</span>'
          f'<span class="cnt">{len(entries)}</span>'
          '</summary>')
        for aid, title, sev in entries:
            badge_dot = f'<span class="dot {sev}" style="margin-right:6px"></span>'
            a(f'<a href="#adv-{aid}">{badge_dot}<code>{aid}</code>'
              f'<span>{html.escape(title)}</span></a>')
        a('</details>')
    a('</div></details>')
    a('</aside>')

    a('<main>')

    # --- Privacy / handling banner ---
    if redaction_on:
        a('<div class="privacy" role="note" aria-label="Privacy notice">'
          '<span class="pico" aria-hidden="true">🛡</span>'
          '<div><strong>Privacy:</strong> query text in this report has been '
          'redacted at collection time &mdash; single-quoted string literals '
          'are replaced with <code>&lt;literal&gt;</code>. SQL shape is '
          'preserved for analysis; user data in <code>WHERE</code> clauses, '
          '<code>VALUES</code>, and parameters is removed. Dollar-quoted '
          'bodies and numeric literals pass through. This is still a '
          'diagnostic report &mdash; treat it as internal and share only '
          'with people who should see cluster topology, placements, and '
          'query shapes.</div></div>')
    else:
        a('<div class="privacy" role="note" aria-label="Privacy notice">'
          '<span class="pico" aria-hidden="true">⚠</span>'
          '<div><strong>Privacy:</strong> this report was produced by an '
          'older <code>citus_gather.sql</code> without collection-time '
          'redaction, so the embedded <code>query</code> columns may '
          'contain literal user data (emails, IDs, tokens). Treat the '
          'report as <strong>confidential</strong>. Re-run with the '
          'current <code>citus_gather.sql</code> to produce a redacted '
          'version.</div></div>')

    # --- Hero ---
    a('<section class="hero">')
    a('<div>')
    a('<h2>Cluster health report</h2>')
    a(f'<div class="sub">run: <code>{html.escape(run_dir.name)}</code> &middot; '
      f'rendered {now} &middot; {total} advisors, {len(sections)} snapshot sections</div>')
    # Severity bar
    a('<div class="sev-bar" aria-label="Severity distribution">')
    for sev_key in ("crit", "warn", "info", "ok"):
        c = counts.get(sev_key, 0)
        pct = (c / total * 100) if total else 0
        if c:
            a(f'<div class="seg {sev_key}" style="width:{pct:.2f}%" '
              f'title="{c} {SEV_LABEL[sev_key]}"></div>')
    a('</div>')
    a('<div class="sev-legend">'
      f'<span><span class="dot crit"></span>{counts["crit"]} critical</span>'
      f'<span><span class="dot warn"></span>{counts["warn"]} warn</span>'
      f'<span><span class="dot info"></span>{counts["info"]} info</span>'
      f'<span><span class="dot ok"></span>{counts["ok"]} ok</span>'
      '</div>')
    # Top fixes
    a('<div class="topfix">')
    a(f'<h3>Top things to fix <span class="cnt">{len(top_fixes)}</span></h3>')
    if not top_fixes:
        a('<div class="empty">✓ Nothing urgent. Review INFO items below when you have time.</div>')
    else:
        a('<ol>')
        for i, (aid, title, _b, sev, hl, _rec, _t) in enumerate(top_fixes[:5], 1):
            meta = ADVISOR_META.get(aid, {})
            cat_pill = _cat_pill(meta.get("cat", "config"))
            fix = meta.get("fix", "")
            fix_html = ''
            if fix:
                fix_html = (
                    '<div class="fix">'
                    '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" '
                    'stroke="currentColor" stroke-width="1.9" stroke-linecap="round" '
                    'stroke-linejoin="round">'
                    '<path d="M9 18h6M10 22h4M12 2a7 7 0 0 0-4 12.7V17h8v-2.3A7 7 0 0 0 12 2z"/>'
                    '</svg>'
                    f'<div><b>Fix</b>{html.escape(fix)}</div></div>'
                )
            a(f'<li class="sev-{sev}"><span class="num">{i}</span><div class="body">'
              f'<div class="title-line">{_sev_badge(sev)}'
              f'<a href="#adv-{aid}"><code style="font-size:12px;color:var(--sub)">{aid}</code> '
              f'<strong>{html.escape(title)}</strong></a>{cat_pill}</div>'
              f'<div class="headline">{html.escape(hl)}</div>'
              f'{fix_html}</div></li>')
        a('</ol>')
    a('</div>')

    # --- Attention recommendations (only critical + warn advisors that
    # carry a quantitative recommendation) — surfaced inside the hero so
    # operators see the exact numbers they need to act on without
    # scrolling. ---
    attn_recs = [(aid, title, sev, rec)
                 for (aid, title, _b, sev, _hl, rec, _t) in results
                 if rec and sev in ("crit", "warn")]
    if attn_recs:
        sev_rk = {"crit": 0, "warn": 1}
        attn_recs.sort(key=lambda r: (sev_rk.get(r[2], 9), r[0]))
        a('<div class="attn-recs">')
        a(f'<h3>Recommendations needing attention '
          f'<span class="cnt">{len(attn_recs)}</span></h3>')
        a('<ul>')
        for aid, title, sev, rec in attn_recs:
            a(f'<li class="sev-{sev}">'
              f'<a href="#adv-{aid}">'
              f'<div class="rec-hd">{_sev_badge(sev)}'
              f'<code style="font-size:12px;color:var(--sub)">{aid}</code>'
              f'<span class="rec-ttl">{html.escape(title)}</span></div>'
              f'<div class="rec-val">{html.escape(rec)}</div>'
              f'</a></li>')
        a('</ul>')
        a('</div>')
    a('</div>')
    a(f'<div class="verdict {overall}">'
      f'<span class="lbl">Overall verdict</span>'
      f'<span class="big">{SEV_ICON.get(overall,"")}{html.escape(ov_label)}</span>'
      '<div class="counts">'
      f'<span><span class="dot crit"></span>{counts["crit"]}</span>'
      f'<span><span class="dot warn"></span>{counts["warn"]}</span>'
      f'<span><span class="dot info"></span>{counts["info"]}</span>'
      f'<span><span class="dot ok"></span>{counts["ok"]}</span>'
      '</div></div>')
    a('</section>')

    # --- "All recommendations" section removed: critical + warn
    # recommendations are surfaced directly inside the hero
    # (Recommendations needing attention), and per-advisor recommendation
    # pills (.rec-pill) already display the value on every advisor card
    # in the advisors section below.

    # --- Cluster fingerprint ---
    # NOTE: the fingerprint tiles now live in the sticky strip rendered
    # directly under the topbar (see .fingerstrip). The in-body section
    # used to duplicate that information, so it has been removed.

    # --- Playbook ---
    if playbook:
        a(f'<h2 class="section" id="playbook">Remediation playbook'
          f'<span class="count">{len(playbook)} actions</span>'
          '<span class="descr">prioritised by severity</span></h2>')
        a('<div class="playbook">')
        a('<header>'
          f'<h3>Do these in order</h3>'
          f'<span class="count">critical first, then warn</span>'
          '<button class="copy-btn" id="playbook-copy" type="button">Copy playbook</button>'
          '</header>')
        a('<ol>')
        for aid, title, sev, hl, fix in playbook:
            meta = ADVISOR_META.get(aid, {})
            cat_pill = _cat_pill(meta.get("cat", "config"))
            a(f'<li class="sev-{sev}"><div>'
              f'<div class="hd">{_sev_badge(sev)}'
              f'<span class="ttl"><a href="#adv-{aid}">'
              f'<code style="font-size:12px;color:var(--sub)">{aid}</code> '
              f'{html.escape(title)}</a></span>'
              f'{cat_pill}</div>'
              f'<div class="headline">{html.escape(hl)}</div>'
              f'<div class="fix"><b>Recommended</b>{html.escape(fix)}</div>'
              '</div></li>')
        a('</ol>')
        # Hidden plain-text for clipboard
        a(f'<script id="playbook-plaintext" type="text/plain">'
          f'{html.escape(_playbook_plaintext(playbook))}</script>')
        a('</div>')

    # --- Executive summary ---
    a(f'<h2 class="section" id="summary">Executive summary'
      f'<span class="count">{total} advisors</span>'
      '<span class="descr">filter, search, and drill in</span></h2>')

    # Filter bar
    a('<div class="filter-bar">')
    a('<input type="search" id="summary-search" placeholder="Search advisors…" '
      'autocomplete="off" spellcheck="false">')
    a('<span class="sep">severity</span>')
    for sev_key in ("crit", "warn", "info", "ok"):
        lbl = SEV_LABEL[sev_key]
        c = counts.get(sev_key, 0)
        a(f'<button class="chip sev-{sev_key}" data-sev="{sev_key}" type="button" aria-pressed="false">'
          f'{SEV_ICON.get(sev_key,"")}<span>{html.escape(lbl)} ({c})</span></button>')
    a('<span class="sep">category</span>')
    for k in present_cats:
        lbl, color = CATEGORIES[k]
        n = len(adv_by_cat[k])
        a(f'<button class="chip" data-cat="{k}" type="button" aria-pressed="false" '
          f'style="color:{color}">{CAT_ICON.get(k,"")}'
          f'<span>{html.escape(lbl)} ({n})</span></button>')
    a('</div>')

    a('<div class="card"><table class="summary">')
    a('<thead><tr>'
      '<th class="col-sev">Severity</th>'
      '<th class="col-cat">Category</th>'
      '<th class="col-id">ID</th>'
      '<th class="col-name">Advisor</th>'
      '<th>Current finding</th>'
      '<th class="col-rec">Recommended</th>'
      '</tr></thead><tbody>')
    for aid, title, _blurb, sev, hl, rec, _text in sorted_results:
        meta = ADVISOR_META.get(aid, {})
        cat_key = meta.get("cat", "config")
        passing_class = ' class="is-passing"' if sev in ("ok", "info") else ''
        rec_cell = (f'<span class="rec">{html.escape(rec)}</span>' if rec
                    else '<span class="muted">—</span>')
        a(f'<tr{passing_class} data-sev="{sev}" data-cat="{cat_key}">'
          f'<td>{_sev_badge(sev)}</td>'
          f'<td>{_cat_pill(cat_key)}</td>'
          f'<td><code class="mono">{aid}</code></td>'
          f'<td><a href="#adv-{aid}" style="color:inherit;text-decoration:none">'
          f'{html.escape(title)}</a></td>'
          f'<td class="headline">{html.escape(hl)}</td>'
          f'<td>{rec_cell}</td>'
          '</tr>')
    a('</tbody></table>')
    a('<div class="summary-empty" id="summary-empty">No advisors match the current filters.</div>')
    a('</div>')
    a('<div class="pass-toggle"><button id="pass-toggle-btn" type="button">Show passing checks</button></div>')

    # --- Per-advisor cards ---
    a(f'<h2 class="section" id="advisors">Advisor details'
      f'<span class="count">{total} advisors</span></h2>')
    for aid, title, blurb, sev, hl, rec, text in sorted_results:
        meta = ADVISOR_META.get(aid, {})
        cat_key = meta.get("cat", "config")
        docs = meta.get("docs", "")
        checks = meta.get("checks", blurb)
        matters = meta.get("matters", "")
        fix = meta.get("fix", "")

        a(f'<div class="adv" id="adv-{aid}">')
        a('<div class="head">'
          f'{_sev_badge(sev)}{_cat_pill(cat_key)}'
          f'<code>{aid}</code>'
          f'<span class="title">{html.escape(title)}</span>')
        if docs:
            a(f'<a class="docs" href="{html.escape(docs)}" target="_blank" rel="noopener">Docs ↗</a>')
        a('</div>')
        a('<div class="panes">')
        a('<div class="pane"><div class="lbl">What it checks</div>'
          f'<div class="val">{html.escape(checks)}</div></div>')
        a('<div class="pane"><div class="lbl">Why it matters</div>'
          f'<div class="val">{html.escape(matters) if matters else "—"}</div></div>')
        a(f'<div class="pane finding {sev}"><div class="lbl">Current finding</div>'
          f'<div class="val">{html.escape(hl)}</div></div>')
        fix_cls = sev if sev in ("crit", "warn", "ok") else ""
        a(f'<div class="pane fix {fix_cls}"><div class="lbl">'
          '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" '
          'stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
          '<path d="M9 18h6M10 22h4M12 2a7 7 0 0 0-4 12.7V17h8v-2.3A7 7 0 0 0 12 2z"/>'
          '</svg>Recommended action</div>'
          f'<div class="val">{html.escape(fix) if fix else "—"}</div></div>')
        a('</div>')
        if rec:
            a(f'<div class="rec-pill {sev}" role="note" aria-label="Recommendation">'
              '<span class="rec-lbl">Recommended</span>'
              f'<span class="rec-val">{html.escape(rec)}</span></div>')
        if text:
            n_lines = len(text.splitlines())
            a(f'<details><summary>Technical details ({aid}.out &middot; {n_lines} lines)</summary>'
              f'<pre class="raw">{html.escape(text)}</pre></details>')
        a('</div>')

    # --- Snapshot ---
    a(f'<h2 class="section" id="snapshot">Cluster snapshot'
      f'<span class="count">{len(sections)} sections</span>'
      '<span class="descr">raw data from citus_gather</span></h2>')
    if not sections:
        a('<p class="muted"><em>No gather.out found or no sections parsed.</em></p>')
    else:
        a('<div class="snap-filter">'
          '<input id="snapshot-filter" type="search" '
          'placeholder="Filter sections by name or category (e.g. shards, stats)…" autocomplete="off">'
          f'<span class="muted" id="snapshot-count">{len(sections)} / {len(sections)} sections</span>'
          '</div>')
        for sid, body in sections:
            table_html, nrows = csv_to_html(body)
            cat_key = classify_section(sid)
            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', sid)
            raw_id = f"raw-{safe_id}"
            fname = re.sub(r'[^a-zA-Z0-9_-]', '_', sid) + ".csv"
            can_download = bool(body.strip() and not body.strip().startswith("--")
                                and nrows > 0)
            a(f'<details class="sect" data-sid="{html.escape(sid)}" data-cat="{cat_key}">')
            a('<summary>')
            a(_section_cat_pill(cat_key))
            a(f'<span class="sid">{html.escape(sid)}</span>')
            a(f'<span class="rowcount">{nrows} row(s)</span>')
            if can_download:
                a(f'<button class="csv-btn" type="button" data-src="{raw_id}" '
                  f'data-fname="{html.escape(fname)}" data-fmt="csv" '
                  'title="Download this section as CSV">⬇ CSV</button>')
            else:
                a('<button class="csv-btn" type="button" disabled aria-disabled="true">⬇ CSV</button>')
            a('</summary>')
            a('<div class="body">')
            a(table_html)
            a('</div>')
            # Embed the raw CSV body (escaped) for the download button to read.
            if can_download:
                a(f'<script id="{raw_id}" type="text/plain" data-fmt="csv">'
                  f'{html.escape(body)}</script>')
            a('</details>')

    if summary_txt:
        a('<h2 class="section" id="txt">Driver summary.txt'
          '<span class="descr">stdout from citus_analyze.sh</span></h2>')
        a(f'<pre class="raw">{html.escape(summary_txt)}</pre>')

    a('</main>')
    a('</div>')  # .wrap
    a(f'<script>{JS}</script>')
    a('</body></html>')

    out_path.write_text("\n".join(H))
    print(f"wrote {out_path} ({out_path.stat().st_size // 1024} KB)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="citus_analyze output directory")
    ap.add_argument("-o", "--output",
                    help="output HTML path (default: <run-dir>/report.html)")
    args = ap.parse_args()
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        print(f"not a directory: {run_dir}", file=sys.stderr)
        sys.exit(2)
    out = Path(args.output) if args.output else run_dir / "report.html"
    render(run_dir, out)


if __name__ == "__main__":
    main()
