#!/usr/bin/env bash
# =====================================================================
# citus_analyze.sh
# ---------------------------------------------------------------------
# One-shot driver that (1) runs citus_gather.sql to produce a full
# cluster snapshot bundle, and (2) runs every advisor sketch against
# the live coordinator, collecting traffic-light headlines and assembling
# an executive summary.
#
# Usage
#   ./citus_analyze.sh                             # uses PG* env vars
#   ./citus_analyze.sh -h 10.0.0.1 -p 5432 -d citus -U admin
#   ./citus_analyze.sh --uri "postgres://admin@coord:5432/citus"
#   ./citus_analyze.sh --psql /opt/pg17/bin/psql -h coord ...
#   ./citus_analyze.sh -o /tmp/citus_report_2025
#
# Exit codes
#   0  all advisors OK
#   1  at least one WARN
#   2  at least one CRITICAL (or any error)
#
# Requires: psql (libpq), bash 4+, awk, sed. Read-only role is enough
# for most checks (pg_monitor + membership in citus_monitoring role
# recommended). Some sections need superuser; they degrade to NOTICE.
# =====================================================================
set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PSQL_BIN="${PSQL_BIN:-psql}"

# -------- defaults / args ---------
OUT_DIR=""
URI=""
HOSTARG=""
PORTARG=""
DBARG=""
USERARG=""
COORD_RAM_MB=""
WORKER_RAM_MB=""
ADVISORS_ONLY=0
GATHER_ONLY=0

usage() {
    sed -n '3,22p' "$0"
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--host)      HOSTARG="$2"; shift 2;;
        -p|--port)      PORTARG="$2"; shift 2;;
        -d|--dbname)    DBARG="$2"; shift 2;;
        -U|--username)  USERARG="$2"; shift 2;;
        --uri)          URI="$2"; shift 2;;
        -o|--out-dir)   OUT_DIR="$2"; shift 2;;
        --psql)         PSQL_BIN="$2"; shift 2;;
        --coord-ram-mb)  COORD_RAM_MB="$2"; shift 2;;
        --worker-ram-mb) WORKER_RAM_MB="$2"; shift 2;;
        --coord-disk-mb)  COORD_DISK_MB="$2"; shift 2;;
        --worker-disk-mb) WORKER_DISK_MB="$2"; shift 2;;
        --advisors-only) ADVISORS_ONLY=1; shift;;
        --gather-only)  GATHER_ONLY=1; shift;;
        --help)         usage;;
        *)              echo "unknown arg: $1" >&2; usage;;
    esac
done

# -------- build psql argv ---------
# preflight: check psql is actually reachable (after arg parsing so --psql wins)
if ! command -v "$PSQL_BIN" >/dev/null 2>&1; then
    echo "ERROR: psql not found: '$PSQL_BIN'" >&2
    echo "  Pass an absolute path with --psql, e.g." >&2
    echo "    ./citus_analyze.sh --psql /usr/lib/postgresql/17/bin/psql ..." >&2
    echo "  or set PSQL_BIN in the environment, or add psql to your PATH." >&2
    exit 2
fi

PSQL_ARGS=(-X -A -q -v ON_ERROR_STOP=off)
# Make Citus shard relations visible to our pg_class-based queries.
# (citus.override_table_visibility=on hides tenantA_102234 etc. from pg_class
#  so tools don't drown in shard rows. We need to see them to audit them.)
# application_name is also set for the same reason via
# citus.show_shards_for_app_name_prefixes in case a deployment enforces it.
export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c citus.override_table_visibility=off -c application_name=citus_analyze"
if [[ -n "$URI" ]]; then
    PSQL_ARGS+=("$URI")
else
    [[ -n "$HOSTARG" ]] && PSQL_ARGS+=(-h "$HOSTARG")
    [[ -n "$PORTARG" ]] && PSQL_ARGS+=(-p "$PORTARG")
    [[ -n "$DBARG"   ]] && PSQL_ARGS+=(-d "$DBARG")
    [[ -n "$USERARG" ]] && PSQL_ARGS+=(-U "$USERARG")
fi

# -------- output directory ---------
TS="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="./citus_analyze_${TS}"
fi
mkdir -p "$OUT_DIR"

GATHER_OUT="$OUT_DIR/gather.out"
GATHER_ERR="$OUT_DIR/gather.err"
SUMMARY="$OUT_DIR/summary.txt"

echo "citus_analyze run at ${TS} (UTC)" > "$SUMMARY"
echo "output directory: $OUT_DIR"      | tee -a "$SUMMARY"

# -------- connectivity probe --------
if ! "$PSQL_BIN" "${PSQL_ARGS[@]}" -c "SELECT citus_version();" > "$OUT_DIR/preflight.out" 2> "$OUT_DIR/preflight.err"; then
    echo "ERROR: cannot connect or Citus not installed. See preflight.err." | tee -a "$SUMMARY"
    cat "$OUT_DIR/preflight.err" | tee -a "$SUMMARY" >&2
    exit 2
fi
CITUS_VER="$(tr -d ' ' < "$OUT_DIR/preflight.out" | sed -n '2p')"
echo "connected OK. citus_version: ${CITUS_VER}" | tee -a "$SUMMARY"

# =====================================================================
# Step 1: collector
# =====================================================================
if [[ "$ADVISORS_ONLY" -eq 0 ]]; then
    echo "running citus_gather.sql ..." | tee -a "$SUMMARY"
    if ! "$PSQL_BIN" "${PSQL_ARGS[@]}" -f "$SCRIPT_DIR/citus_gather.sql" > "$GATHER_OUT" 2> "$GATHER_ERR"; then
        echo "WARN : citus_gather.sql returned non-zero; partial output may still be usable" | tee -a "$SUMMARY"
    fi
    SECTIONS=$(grep -c '^### BEGIN:' "$GATHER_OUT" || echo 0)
    GZ_SIZE=$(gzip -c "$GATHER_OUT" | wc -c)
    echo "  gather OK: $SECTIONS sections, $(wc -l < "$GATHER_OUT") lines, ~$((GZ_SIZE/1024)) KB gzipped" | tee -a "$SUMMARY"
fi

if [[ "$GATHER_ONLY" -eq 1 ]]; then
    exit 0
fi

# =====================================================================
# Step 2: advisors
# =====================================================================
# each entry: "id:title:file"
ADVISORS=(
  "M1:Node memory minimum (OOM-safety):advisors/m1_node_memory_minimum.sql"
  "D1:Disk capacity & shard-growth runway:advisors/d1_disk_capacity_runway.sql"
  "Q1:Long-running queries & lock waits:advisors/q1_long_running_queries.sql"
  "GUC1:Citus + PostgreSQL configuration audit:advisors/guc1_config_audit.sql"
  "V1:Version & upgrade readiness:advisors/v1_version_readiness.sql"
  "P1:Partition hygiene & maintenance runway:advisors/p1_partition_hygiene.sql"
  "I1:Index health (per-shard aware):advisors/i1_index_health.sql"
  "B1:Table bloat & autovacuum lag:advisors/b1_bloat_autovacuum.sql"
  "CP1:pgbouncer pool sizing:advisors/cp1_pgbouncer_pool.sql"
  "R2:Rebalance plan preview:advisors/r2_rebalance_preview.sql"
  "REF1:Reference-table health:advisors/ref1_reference_table_health.sql"
  "W1:WAL & checkpoint pressure:advisors/w1_wal_checkpoint.sql"
  "STAT1:Statistics freshness:advisors/stat1_stats_freshness.sql"
  "SEC1:Security & role audit:advisors/sec1_security_audit.sql"
  "NET1:Node reachability & latency:advisors/net1_node_reachability.sql"
  "REP1:Streaming replication & slot lag:advisors/rep1_replication_lag.sql"
  "GR1:Shard/partition growth memory model:advisors/gr1_shard_growth_advisor.sql"
  "C3:Max safe external connections (MX-aware):advisors/c3_max_external_connections.sql"
  "S3:Data skew across shards & workers:advisors/s3_data_skew_advisor.sql"
  "R1:Rebalance / background-job health:advisors/r1_rebalance_health.sql"
  "N6:Metadata-sync feasibility for add-node:advisors/n6_metadata_sync_feasibility.sql"
  "A3:2PC backlog & orphan prepared xacts:advisors/a3_2pc_backlog_advisor.sql"
)

WORST=0   # 0=OK 1=WARN 2=CRITICAL

declare -a HEADLINES

run_advisor() {
    local id="$1" title="$2" file="$3"
    local path="$SCRIPT_DIR/$file"
    local out="$OUT_DIR/${id}.out"
    local err="$OUT_DIR/${id}.err"
    if [[ ! -r "$path" ]]; then
        HEADLINES+=("? ${id}  ${title}  -- sketch file missing: $file")
        return
    fi
    "$PSQL_BIN" "${PSQL_ARGS[@]}" \
        ${COORD_RAM_MB:+-v coord_ram_mb=$COORD_RAM_MB} \
        ${WORKER_RAM_MB:+-v worker_ram_mb=$WORKER_RAM_MB} \
        ${COORD_DISK_MB:+-v coord_disk_mb=$COORD_DISK_MB} \
        ${WORKER_DISK_MB:+-v worker_disk_mb=$WORKER_DISK_MB} \
        -f "$path" > "$out" 2> "$err" || true

    # Pick the most severe verdict line anywhere in the output (advisors
    # may emit several: one per sub-check). Order CRITICAL > WARN > INFO > OK.
    local line=""
    local sev_token=""
    if grep -qE '^[[:space:]]*\|?[[:space:]]*CRITICAL[[:space:]]*:' "$out"; then
        sev_token="CRITICAL"
        line="$(grep -E '^[[:space:]]*\|?[[:space:]]*CRITICAL[[:space:]]*:' "$out" | head -n1 \
                 | sed -E 's/^[[:space:]]*\|?[[:space:]]*//; s/[[:space:]]*\|[[:space:]]*$//')"
    elif grep -qE '^[[:space:]]*\|?[[:space:]]*WARN[[:space:]]*:' "$out"; then
        sev_token="WARN"
        line="$(grep -E '^[[:space:]]*\|?[[:space:]]*WARN[[:space:]]*:' "$out" | head -n1 \
                 | sed -E 's/^[[:space:]]*\|?[[:space:]]*//; s/[[:space:]]*\|[[:space:]]*$//')"
    elif grep -qE '^[[:space:]]*\|?[[:space:]]*OK[[:space:]]*:' "$out"; then
        sev_token="OK"
        line="$(grep -E '^[[:space:]]*\|?[[:space:]]*OK[[:space:]]*:' "$out" | head -n1 \
                 | sed -E 's/^[[:space:]]*\|?[[:space:]]*//; s/[[:space:]]*\|[[:space:]]*$//')"
    elif grep -qE '^[[:space:]]*\|?[[:space:]]*INFO[[:space:]]*:' "$out"; then
        sev_token="INFO"
        line="$(grep -E '^[[:space:]]*\|?[[:space:]]*INFO[[:space:]]*:' "$out" | head -n1 \
                 | sed -E 's/^[[:space:]]*\|?[[:space:]]*//; s/[[:space:]]*\|[[:space:]]*$//')"
    fi
    [[ -z "$line" ]] && line="(no verdict headline; see ${id}.out)"

    case "$sev_token" in
        CRITICAL) (( WORST < 2 )) && WORST=2;;
        WARN)     (( WORST < 1 )) && WORST=1;;
    esac
    HEADLINES+=("${sev_token:-?} ${id}  ${title}  -- ${line}")
}

echo "running advisors ..." | tee -a "$SUMMARY"
for entry in "${ADVISORS[@]}"; do
    IFS=":" read -r aid atitle afile <<< "$entry"
    run_advisor "$aid" "$atitle" "$afile"
done

# =====================================================================
# Step 3: executive summary
# =====================================================================
badge() {
    case "$1" in
        CRITICAL) echo "[X]";;
        WARN)     echo "[!]";;
        OK)       echo "[+]";;
        INFO)     echo "[i]";;
        *)        echo "[?]";;
    esac
}

{
    echo
    echo "============================================================"
    echo " CITUS_ANALYZE EXECUTIVE SUMMARY"
    echo "============================================================"
    printf "  %-9s  %-4s  %s\n" "SEVERITY" "ID" "HEADLINE"
    echo "  ---------  ----  -------------------------------------------"
    for h in "${HEADLINES[@]}"; do
        sev="$(echo "$h" | awk '{print $1}')"
        rest="$(echo "$h" | cut -d' ' -f2-)"
        printf "  %s %-6s  %s\n" "$(badge "$sev")" "$sev" "$rest"
    done
    echo
    case "$WORST" in
        0) echo "  OVERALL: OK   - no issues flagged.";;
        1) echo "  OVERALL: WARN - at least one advisor returned WARN.";;
        2) echo "  OVERALL: CRITICAL - at least one advisor returned CRITICAL.";;
    esac
    echo "  Full per-advisor output: $OUT_DIR/{M1,D1,Q1,GUC1,V1,P1,I1,B1,CP1,R2,REF1,W1,STAT1,SEC1,NET1,REP1,GR1,C3,S3,R1,N6,A3}.out"
    echo "  Raw cluster snapshot:    $GATHER_OUT"
    echo
} | tee -a "$SUMMARY"

exit "$WORST"
