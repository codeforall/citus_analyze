#!/usr/bin/env python3
"""
render_html.py -- convert a citus_analyze run directory into a single
self-contained HTML report (pg_gather-style).

Usage:
    ./render_html.py ./citus_analyze_20260418T005000Z
    ./render_html.py ./citus_analyze_20260418T005000Z -o report.html

Reads:
    <run-dir>/summary.txt
    <run-dir>/gather.out        -- CSV sections bracketed by ### BEGIN: / ### END :
    <run-dir>/{GR1,C3,S3,R1,N6,A3}.out  -- advisor outputs (psql aligned format)

Writes:
    <run-dir>/report.html       (or path given by -o)

Zero external dependencies: stdlib csv + html only.
"""
from __future__ import annotations
import argparse, csv, html, io, os, re, sys
from pathlib import Path
from datetime import datetime, timezone

ADVISORS = [
    ("M1",  "Node memory minimum (OOM-safety)"),
    ("GR1", "Shard / partition growth memory model"),
    ("C3",  "Max safe external connections (MX-aware)"),
    ("S3",  "Data skew across shards & workers"),
    ("R1",  "Rebalance / background-job health"),
    ("N6",  "Metadata-sync feasibility for add-node"),
    ("A3",  "2PC backlog & orphan prepared xacts"),
]

SEV_RX = re.compile(r'^\s*\|?\s*(CRITICAL|WARN|INFO|OK)\s*:', re.M)

CSS = """
:root { --ok:#2e7d32; --warn:#ed6c02; --crit:#c62828; --fg:#222; --mut:#666;
        --bg:#fff; --panel:#f7f7f9; --border:#e2e2e6; --mono:"SFMono-Regular",Consolas,monospace; }
* { box-sizing: border-box; }
body { font: 14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif; color: var(--fg);
       background: var(--bg); margin: 0; padding: 2rem 3rem; max-width: 1400px; }
h1 { margin: 0 0 .25rem 0; }
h2 { margin-top: 2.5rem; border-bottom: 1px solid var(--border); padding-bottom: .4rem; }
h3 { margin-top: 1.5rem; color: var(--mut); font-weight: 600; }
.muted { color: var(--mut); font-size: 13px; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 10px; font-weight: 600;
         font-size: 12px; color: #fff; }
.badge.ok { background: var(--ok); }
.badge.warn { background: var(--warn); }
.badge.crit { background: var(--crit); }
.badge.info { background: #1565c0; }
.badge.unk { background: #777; }
.summary { margin: 1.2rem 0 1.6rem; padding: 1rem 1.2rem;
           background: var(--panel); border: 1px solid var(--border); border-radius: 8px; }
.summary table { width: 100%; border-collapse: collapse; }
.summary th, .summary td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }
.summary th { font-size: 12px; text-transform: uppercase; color: var(--mut); font-weight: 600; }
.summary tr:last-child td { border-bottom: none; }
.overall.ok { color: var(--ok); }
.overall.warn { color: var(--warn); }
.overall.crit { color: var(--crit); }
table.csv { border-collapse: collapse; width: 100%; margin: .5rem 0 1rem;
            font-family: var(--mono); font-size: 12px; }
table.csv th, table.csv td { padding: 4px 8px; border: 1px solid var(--border); text-align: left;
                             white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 400px; }
table.csv th { background: var(--panel); position: sticky; top: 0; }
table.csv tr:nth-child(even) td { background: #fafafa; }
details { margin: .5rem 0; }
summary { cursor: pointer; font-weight: 600; padding: 6px 10px; background: var(--panel);
          border: 1px solid var(--border); border-radius: 6px; user-select: none; }
summary:hover { background: #eef; }
.section-scroll { max-height: 400px; overflow: auto; border: 1px solid var(--border); border-radius: 6px; }
pre.raw { background: #0b1021; color: #d7dbe6; font-family: var(--mono); font-size: 12px;
          padding: 12px 14px; border-radius: 6px; overflow: auto; max-height: 600px; }
.headline { font-family: var(--mono); font-size: 13px; }
.tag { display: inline-block; font-family: var(--mono); font-size: 11px; padding: 1px 6px;
       border: 1px solid var(--border); border-radius: 4px; color: var(--mut); margin-right: 6px; }
.toc { columns: 2; column-gap: 2rem; margin: 1rem 0; }
.toc a { color: #06c; text-decoration: none; }
.toc a:hover { text-decoration: underline; }
"""

def classify(line: str) -> str:
    m = SEV_RX.search(line or "")
    if not m: return "unk"
    return {"OK": "ok", "WARN": "warn", "CRITICAL": "crit"}[m.group(1)]

def worst_verdict(text: str) -> tuple[str, str]:
    """Return (severity, line) — most severe match wins."""
    order = ["CRITICAL", "WARN", "OK", "INFO"]
    sev_map = {"CRITICAL":"crit","WARN":"warn","OK":"ok","INFO":"info"}
    for sev in order:
        m = re.search(rf'^\s*\|?\s*{sev}\s*:[^\n|]*', text, re.M)
        if m:
            line = m.group(0).lstrip().lstrip("|").strip().rstrip("|").strip()
            return (sev_map[sev], line)
    return ("unk", "(no verdict headline)")

def parse_sections(gather_path: Path) -> list[tuple[str, str]]:
    """Yield (section_id, csv_body) pairs from a gather.out file."""
    if not gather_path.exists(): return []
    text = gather_path.read_text(errors="replace")
    sections = []
    rx = re.compile(r'^### BEGIN: (\S+)\s*\n(.*?)^### END\s*: \1\s*$', re.M | re.S)
    for m in rx.finditer(text):
        sid = m.group(1)
        body = m.group(2).rstrip("\n")
        sections.append((sid, body))
    return sections

def csv_to_html(body: str, max_rows: int = 200) -> str:
    """Render a CSV body (first line = header) as an HTML table."""
    body = body.strip()
    if not body:
        return '<p class="muted"><em>(empty)</em></p>'
    # Skip pure `\echo` placeholder lines ("-- skipped: ...")
    if body.startswith("--"):
        return f'<p class="muted"><em>{html.escape(body)}</em></p>'
    rdr = csv.reader(io.StringIO(body))
    try:
        header = next(rdr)
    except StopIteration:
        return '<p class="muted"><em>(empty)</em></p>'
    rows = list(rdr)
    total = len(rows)
    truncated = total > max_rows
    rows = rows[:max_rows]
    out = ['<div class="section-scroll"><table class="csv"><thead><tr>']
    out += [f'<th>{html.escape(h)}</th>' for h in header]
    out.append('</tr></thead><tbody>')
    for r in rows:
        out.append('<tr>' + ''.join(f'<td>{html.escape(c)}</td>' for c in r) + '</tr>')
    out.append('</tbody></table></div>')
    meta = f'<p class="muted">{total} row(s)'
    if truncated: meta += f' -- showing first {max_rows}'
    out.append(meta + '</p>')
    return "\n".join(out)

def render(run_dir: Path, out_path: Path) -> None:
    advisor_results = []
    overall = "ok"
    for aid, title in ADVISORS:
        p = run_dir / f"{aid}.out"
        if p.exists():
            text = p.read_text(errors="replace")
            sev, line = worst_verdict(text)
        else:
            text, sev, line = "", "unk", f"(missing file: {aid}.out)"
        advisor_results.append((aid, title, sev, line, text))
        if sev == "crit" and overall != "crit": overall = "crit"
        elif sev == "warn" and overall not in ("crit",): overall = "warn"
        # INFO and OK do not escalate overall

    sections = parse_sections(run_dir / "gather.out")
    summary_txt = (run_dir / "summary.txt").read_text(errors="replace") if (run_dir / "summary.txt").exists() else ""

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    ov_label = {"ok": "OK", "warn": "WARN", "crit": "CRITICAL", "unk": "UNKNOWN"}[overall]

    H = []
    H.append(f'<!doctype html><html><head><meta charset="utf-8">')
    H.append(f'<title>citus_analyze report -- {html.escape(run_dir.name)}</title>')
    H.append(f'<style>{CSS}</style></head><body>')
    H.append(f'<h1>citus_analyze report</h1>')
    H.append(f'<p class="muted">run dir: <code>{html.escape(str(run_dir))}</code> &middot; rendered {now}</p>')

    # Executive summary
    H.append('<div class="summary">')
    H.append(f'<h2 style="margin-top:0;border:none">Executive summary &nbsp;'
             f'<span class="badge {overall}">{ov_label}</span></h2>')
    H.append('<table><thead><tr><th>Severity</th><th>ID</th><th>Advisor</th><th>Headline</th></tr></thead><tbody>')
    for aid, title, sev, line, _ in advisor_results:
        lbl = {"ok":"OK","warn":"WARN","crit":"CRITICAL","info":"INFO","unk":"?"}[sev]
        H.append(f'<tr><td><span class="badge {sev}">{lbl}</span></td>'
                 f'<td><code>{aid}</code></td>'
                 f'<td>{html.escape(title)}</td>'
                 f'<td class="headline">{html.escape(line)}</td></tr>')
    H.append('</tbody></table>')
    H.append('</div>')

    # TOC
    H.append('<h2>Contents</h2><div class="toc">')
    for aid, title, _, _, _ in advisor_results:
        H.append(f'<div><a href="#adv-{aid}">Advisor {aid}: {html.escape(title)}</a></div>')
    H.append(f'<div><a href="#snapshot">Cluster snapshot ({len(sections)} sections)</a></div>')
    if summary_txt:
        H.append('<div><a href="#summary-txt">Driver summary.txt</a></div>')
    H.append('</div>')

    # Advisors
    H.append('<h2>Advisors</h2>')
    for aid, title, sev, line, text in advisor_results:
        lbl = {"ok":"OK","warn":"WARN","crit":"CRITICAL","info":"INFO","unk":"?"}[sev]
        H.append(f'<h3 id="adv-{aid}"><span class="badge {sev}">{lbl}</span> '
                 f'<span class="tag">{aid}</span> {html.escape(title)}</h3>')
        H.append(f'<p class="headline">{html.escape(line)}</p>')
        if text:
            H.append(f'<details><summary>Full output ({aid}.out)</summary>'
                     f'<pre class="raw">{html.escape(text)}</pre></details>')

    # Snapshot sections
    H.append(f'<h2 id="snapshot">Cluster snapshot</h2>')
    if not sections:
        H.append('<p class="muted"><em>No gather.out found or no sections parsed.</em></p>')
    for sid, body in sections:
        H.append(f'<details><summary>{html.escape(sid)}</summary>')
        H.append(csv_to_html(body))
        H.append('</details>')

    # Raw summary.txt
    if summary_txt:
        H.append('<h2 id="summary-txt">Driver summary.txt</h2>')
        H.append(f'<pre class="raw">{html.escape(summary_txt)}</pre>')

    H.append('</body></html>')
    out_path.write_text("\n".join(H))
    print(f"wrote {out_path} ({out_path.stat().st_size // 1024} KB)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="citus_analyze output directory")
    ap.add_argument("-o", "--output", help="output HTML path (default: <run-dir>/report.html)")
    args = ap.parse_args()
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        print(f"not a directory: {run_dir}", file=sys.stderr); sys.exit(2)
    out = Path(args.output) if args.output else run_dir / "report.html"
    render(run_dir, out)

if __name__ == "__main__":
    main()
