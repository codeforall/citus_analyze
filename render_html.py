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
import argparse, csv, html, io, re, sys
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


# (id, title, short blurb, extractor)
ADVISORS = [
    ("M1",  "Node memory minimum (OOM-safety)",
     "Bottom-up per-node RAM model (shared_buffers + backends + AV + WAL + MX pool + OS).",
     _rec_m1),
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
]


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


# --- CSS --------------------------------------------------------------------

CSS = r"""
:root {
  --ok:#2e7d32; --warn:#ed6c02; --crit:#c62828; --info:#1565c0; --unk:#78909c;
  --fg:#1a1f27; --mut:#5a6b7a; --sub:#8a95a2;
  --bg:#fafbfc; --card:#ffffff; --border:#e4e7eb; --hover:#f0f4f8;
  --shadow:0 1px 3px rgba(16,24,40,.06), 0 1px 2px rgba(16,24,40,.04);
  --shadow-lg:0 4px 12px rgba(16,24,40,.08), 0 2px 4px rgba(16,24,40,.04);
  --mono:"SFMono-Regular","JetBrains Mono",Consolas,Menlo,monospace;
  --radius:10px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --fg:#e6ebf2; --mut:#9aa4b2; --sub:#6b7684;
    --bg:#0f1419; --card:#171c23; --border:#252c36; --hover:#1e252d;
    --shadow:0 1px 3px rgba(0,0,0,.4);
    --shadow-lg:0 6px 16px rgba(0,0,0,.5);
  }
}

* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  font: 14px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  color: var(--fg); background: var(--bg); margin: 0;
}
.wrap { max-width: 1280px; margin: 0 auto; padding: 2rem 2.5rem 4rem; }

/* Sticky top bar */
.topbar {
  position: sticky; top: 0; z-index: 10;
  background: var(--bg); border-bottom: 1px solid var(--border);
  padding: .75rem 2.5rem; display: flex; align-items: center; gap: 1rem;
  backdrop-filter: saturate(120%) blur(8px);
}
.topbar h1 { font-size: 15px; margin: 0; letter-spacing: .2px; }
.topbar .spacer { flex: 1; }
.topbar a { color: var(--mut); text-decoration: none; font-size: 13px; padding: 4px 10px;
            border-radius: 6px; }
.topbar a:hover { color: var(--fg); background: var(--hover); }

/* Hero */
.hero { margin: 0 0 2rem; display: flex; align-items: flex-start; gap: 2rem; flex-wrap: wrap; }
.hero h2 { font-size: 28px; margin: 0 0 .35rem; letter-spacing: -.3px; }
.hero .sub { color: var(--mut); font-size: 13px; }
.hero .sub code { font-family: var(--mono); padding: 1px 6px;
                  background: var(--card); border: 1px solid var(--border); border-radius: 4px; }

/* Verdict chip (big) */
.verdict {
  min-width: 220px; padding: 1rem 1.25rem; border-radius: var(--radius);
  background: var(--card); border: 1px solid var(--border); box-shadow: var(--shadow);
  display: flex; flex-direction: column; gap: .35rem;
}
.verdict .lbl { font-size: 11px; font-weight: 700; letter-spacing: 1px;
                text-transform: uppercase; color: var(--sub); }
.verdict .big { font-size: 22px; font-weight: 700; line-height: 1.1; }
.verdict.ok   .big { color: var(--ok); }
.verdict.warn .big { color: var(--warn); }
.verdict.crit .big { color: var(--crit); }
.verdict.info .big { color: var(--info); }
.verdict .counts { display: flex; gap: .8rem; margin-top: .35rem; font-size: 12px; color: var(--mut); }
.verdict .counts span { display: inline-flex; align-items: center; gap: 4px; }
.verdict .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; }
.dot.ok   { background: var(--ok); }
.dot.warn { background: var(--warn); }
.dot.crit { background: var(--crit); }
.dot.info { background: var(--info); }
.dot.unk  { background: var(--unk); }

/* Badges */
.badge {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 2px 9px; border-radius: 99px;
  font-size: 11px; font-weight: 700; letter-spacing: .3px; color: #fff;
  line-height: 1.6; white-space: nowrap;
}
.badge.ok   { background: var(--ok); }
.badge.warn { background: var(--warn); }
.badge.crit { background: var(--crit); }
.badge.info { background: var(--info); }
.badge.unk  { background: var(--unk); color: #fff; }

/* Section heading */
h2.section { font-size: 18px; margin: 2.5rem 0 1rem; letter-spacing: -.1px;
             padding-bottom: .4rem; border-bottom: 1px solid var(--border); }
h2.section .count { color: var(--sub); font-weight: 400; font-size: 13px; margin-left: .5rem; }

/* Summary table */
.card {
  background: var(--card); border: 1px solid var(--border);
  border-radius: var(--radius); box-shadow: var(--shadow); overflow: hidden;
}
table.summary { width: 100%; border-collapse: collapse; }
table.summary th, table.summary td {
  padding: 11px 14px; text-align: left; vertical-align: top;
  border-bottom: 1px solid var(--border); font-size: 13px;
}
table.summary th {
  font-size: 11px; font-weight: 700; letter-spacing: .5px; text-transform: uppercase;
  color: var(--sub); background: var(--hover);
}
table.summary tr:last-child td { border-bottom: none; }
table.summary tr:hover td { background: var(--hover); }
table.summary td code { font-family: var(--mono); font-size: 12px;
                        padding: 1px 6px; background: var(--hover);
                        border: 1px solid var(--border); border-radius: 4px; }
table.summary .col-sev  { width: 108px; }
table.summary .col-id   { width: 60px; }
table.summary .col-name { width: 250px; }
table.summary .col-rec  { width: 340px; }
.headline { font-family: var(--mono); font-size: 12.5px; color: var(--fg); }
.rec      { font-family: var(--mono); font-size: 12.5px; color: var(--info);
            font-weight: 600; }

/* Per-advisor card */
.adv {
  background: var(--card); border: 1px solid var(--border);
  border-radius: var(--radius); box-shadow: var(--shadow);
  padding: 1.1rem 1.25rem; margin-bottom: 1rem;
}
.adv .head { display: flex; align-items: center; gap: .75rem; flex-wrap: wrap; }
.adv .title { font-size: 15.5px; font-weight: 600; margin: 0; }
.adv .blurb { color: var(--mut); font-size: 13px; margin: .35rem 0 .9rem; }
.adv .kv { display: grid; grid-template-columns: 140px 1fr; gap: 6px 14px;
           font-size: 13px; margin: .5rem 0 .6rem; }
.adv .kv .k { color: var(--sub); font-size: 11px; font-weight: 700;
              text-transform: uppercase; letter-spacing: .5px; padding-top: 2px; }
.adv .kv .v { font-family: var(--mono); font-size: 12.5px; }
.adv details { margin-top: .75rem; }
.adv summary { cursor: pointer; font-weight: 600; font-size: 13px;
               padding: 8px 12px; background: var(--hover);
               border: 1px solid var(--border); border-radius: 6px;
               list-style: none; }
.adv summary::-webkit-details-marker { display: none; }
.adv summary::before { content: "▸"; margin-right: 6px; color: var(--sub);
                       display: inline-block; transition: transform .15s; }
.adv details[open] summary::before { transform: rotate(90deg); }

/* Generic scrollable block */
.scroll { max-height: 440px; overflow: auto;
          border: 1px solid var(--border); border-radius: 6px; }
pre.raw { background: #0b1021; color: #d7dbe6; font-family: var(--mono);
          font-size: 12px; padding: 14px 16px; border-radius: 6px;
          overflow: auto; max-height: 560px; margin: .5rem 0 0; }

/* CSV tables */
table.csv { border-collapse: collapse; width: 100%; font-family: var(--mono); font-size: 12px; }
table.csv th, table.csv td { padding: 6px 10px; border: 1px solid var(--border);
                             text-align: left; white-space: nowrap;
                             overflow: hidden; text-overflow: ellipsis; max-width: 420px; }
table.csv th { background: var(--hover); position: sticky; top: 0;
               font-weight: 700; color: var(--fg); }
table.csv tr:nth-child(even) td { background: rgba(127,127,127,.04); }

/* Snapshot grid */
.filter {
  display: flex; align-items: center; gap: .6rem; margin: 0 0 .75rem;
}
.filter input {
  flex: 1; padding: 8px 12px; font-size: 13px; font-family: inherit;
  color: var(--fg); background: var(--card);
  border: 1px solid var(--border); border-radius: 6px; outline: none;
}
.filter input:focus { border-color: var(--info); box-shadow: 0 0 0 3px rgba(21,101,192,.15); }
.filter .muted { font-size: 12px; }
.sect { margin-bottom: .55rem; }
.sect summary .rowcount { color: var(--sub); font-weight: 400; font-size: 12px; margin-left: 6px; }
.sect[hidden-by-filter] { display: none; }

/* Contents/TOC */
.toc { columns: 2; column-gap: 2rem; margin: 0; padding: 1rem 1.25rem; }
.toc div { margin: 2px 0; break-inside: avoid; font-size: 13px; }
.toc a { color: var(--info); text-decoration: none; }
.toc a:hover { text-decoration: underline; }

.muted { color: var(--mut); font-size: 12.5px; }
"""

# --- JS (snapshot filter) ---------------------------------------------------

JS = r"""
(function () {
  var inp = document.getElementById('snapshot-filter');
  if (!inp) return;
  var sects = document.querySelectorAll('.sect');
  var countEl = document.getElementById('snapshot-count');
  var total = sects.length;
  inp.addEventListener('input', function () {
    var q = inp.value.toLowerCase().trim();
    var shown = 0;
    sects.forEach(function (s) {
      var id = s.getAttribute('data-sid') || '';
      var ok = !q || id.toLowerCase().indexOf(q) !== -1;
      s.style.display = ok ? '' : 'none';
      if (ok) shown++;
    });
    if (countEl) countEl.textContent = shown + ' / ' + total + ' sections';
  });
})();
"""


# --- rendering --------------------------------------------------------------

def render(run_dir: Path, out_path: Path) -> None:
    results = []
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
    summary_txt = (run_dir / "summary.txt").read_text(errors="replace") \
                  if (run_dir / "summary.txt").exists() else ""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ov_label = SEV_LABEL[overall]

    H = []
    a = H.append

    a('<!doctype html><html lang="en"><head><meta charset="utf-8">')
    a(f'<title>citus_analyze report — {html.escape(run_dir.name)}</title>')
    a('<meta name="viewport" content="width=device-width,initial-scale=1">')
    a(f'<style>{CSS}</style></head><body>')

    # Top bar
    a('<div class="topbar">')
    a(f'<h1>citus_analyze <span class="badge {overall}">{ov_label}</span></h1>')
    a('<div class="spacer"></div>')
    a('<a href="#summary">Summary</a>')
    a('<a href="#advisors">Advisors</a>')
    a('<a href="#snapshot">Snapshot</a>')
    if summary_txt:
        a('<a href="#txt">summary.txt</a>')
    a('</div>')

    a('<div class="wrap">')

    # Hero
    a('<div class="hero">')
    a('<div style="flex:1;min-width:320px">')
    a('<h2>Cluster health report</h2>')
    a(f'<div class="sub">run dir: <code>{html.escape(str(run_dir))}</code> &middot; '
      f'rendered {now} &middot; {len(ADVISORS)} advisors, {len(sections)} snapshot sections</div>')
    a('</div>')
    # Verdict chip
    a(f'<div class="verdict {overall}"><span class="lbl">Overall verdict</span>'
      f'<span class="big">{ov_label}</span>'
      '<div class="counts">'
      f'<span><span class="dot crit"></span>{counts["crit"]} critical</span>'
      f'<span><span class="dot warn"></span>{counts["warn"]} warn</span>'
      f'<span><span class="dot ok"></span>{counts["ok"]} ok</span>'
      f'<span><span class="dot info"></span>{counts["info"]} info</span>'
      '</div></div>')
    a('</div>')

    # Executive summary
    a('<h2 class="section" id="summary">Executive summary'
      f'<span class="count">{len(results)} advisors</span></h2>')
    a('<div class="card"><table class="summary">')
    a('<thead><tr>'
      '<th class="col-sev">Severity</th>'
      '<th class="col-id">ID</th>'
      '<th class="col-name">Advisor</th>'
      '<th>Current finding</th>'
      '<th class="col-rec">Minimum recommended</th>'
      '</tr></thead><tbody>')
    for aid, title, _blurb, sev, hl, rec, _text in results:
        lbl = SEV_LABEL[sev]
        rec_cell = f'<span class="rec">{html.escape(rec)}</span>' if rec else '<span class="muted">—</span>'
        a(f'<tr>'
          f'<td><span class="badge {sev}">{lbl}</span></td>'
          f'<td><code>{aid}</code></td>'
          f'<td><a href="#adv-{aid}" style="color:inherit;text-decoration:none">{html.escape(title)}</a></td>'
          f'<td class="headline">{html.escape(hl)}</td>'
          f'<td>{rec_cell}</td>'
          f'</tr>')
    a('</tbody></table></div>')

    # Contents card
    a('<h2 class="section">Contents</h2>')
    a('<div class="card"><div class="toc">')
    for aid, title, *_ in ADVISORS:
        a(f'<div><a href="#adv-{aid}"><code>{aid}</code> — {html.escape(title)}</a></div>')
    a(f'<div><a href="#snapshot">Cluster snapshot ({len(sections)} sections)</a></div>')
    if summary_txt:
        a('<div><a href="#txt">Driver summary.txt</a></div>')
    a('</div></div>')

    # Advisors
    a('<h2 class="section" id="advisors">Advisors'
      f'<span class="count">{len(results)} total</span></h2>')
    for aid, title, blurb, sev, hl, rec, text in results:
        lbl = SEV_LABEL[sev]
        a(f'<div class="adv" id="adv-{aid}">')
        a('<div class="head">'
          f'<span class="badge {sev}">{lbl}</span>'
          f'<code style="font-size:12px;color:var(--mut)">{aid}</code>'
          f'<div class="title">{html.escape(title)}</div>'
          '</div>')
        a(f'<div class="blurb">{html.escape(blurb)}</div>')
        a('<div class="kv">')
        a(f'<div class="k">Finding</div><div class="v">{html.escape(hl)}</div>')
        if rec:
            a(f'<div class="k">Recommended</div><div class="v" style="color:var(--info)">{html.escape(rec)}</div>')
        a('</div>')
        if text:
            a(f'<details><summary>Full output ({aid}.out) &middot; {len(text.splitlines())} lines</summary>'
              f'<pre class="raw">{html.escape(text)}</pre></details>')
        a('</div>')

    # Snapshot
    a(f'<h2 class="section" id="snapshot">Cluster snapshot'
      f'<span class="count">{len(sections)} sections</span></h2>')
    if not sections:
        a('<p class="muted"><em>No gather.out found or no sections parsed.</em></p>')
    else:
        a('<div class="filter">'
          '<input id="snapshot-filter" type="search" placeholder="Filter sections by name (e.g. shard, dist, stat)…" autocomplete="off">'
          f'<span class="muted" id="snapshot-count">{len(sections)} / {len(sections)} sections</span>'
          '</div>')
        for sid, body in sections:
            table_html, nrows = csv_to_html(body)
            a(f'<details class="sect" data-sid="{html.escape(sid)}">'
              f'<summary><code>{html.escape(sid)}</code>'
              f'<span class="rowcount">{nrows} row(s)</span></summary>')
            a(table_html)
            a('</details>')

    if summary_txt:
        a('<h2 class="section" id="txt">Driver <code>summary.txt</code></h2>')
        a(f'<pre class="raw">{html.escape(summary_txt)}</pre>')

    a('</div>')  # .wrap
    a(f'<script>{JS}</script>')
    a('</body></html>')

    out_path.write_text("\n".join(H))
    print(f"wrote {out_path} ({out_path.stat().st_size // 1024} KB)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="citus_analyze output directory")
    ap.add_argument("-o", "--output", help="output HTML path (default: <run-dir>/report.html)")
    args = ap.parse_args()
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        print(f"not a directory: {run_dir}", file=sys.stderr)
        sys.exit(2)
    out = Path(args.output) if args.output else run_dir / "report.html"
    render(run_dir, out)


if __name__ == "__main__":
    main()
