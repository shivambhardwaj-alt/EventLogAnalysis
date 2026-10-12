"""Write report.json, a self-contained report.html and CSV exports."""
from __future__ import annotations

import csv
import html
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import metrics
from .config import Config

EXPORTS = {
    "funnel_metrics": "SELECT * FROM funnel_metrics ORDER BY step_index",
    "daily_funnel": "SELECT * FROM daily_funnel ORDER BY session_date, step_index",
    "funnel_anomalies": "SELECT * FROM funnel_anomalies ORDER BY session_date, step_index",
    "alerts": "SELECT * FROM alerts ORDER BY created_at",
    "schema_drift": "SELECT * FROM schema_drift ORDER BY rows DESC",
    "task_runs": "SELECT * FROM task_runs ORDER BY run_id, seq",
    "funnel_segments": "SELECT * FROM funnel_segments ORDER BY dimension, segment_value, step_index",
    "sessions": "SELECT * FROM sessions ORDER BY user_id, session_seq",
    "quarantine_summary": "SELECT reason, COUNT(*) AS rows FROM quarantine_events GROUP BY reason ORDER BY rows DESC",
    "pipeline_runs": "SELECT * FROM pipeline_runs ORDER BY started_at",
}


def _export_csv(con: sqlite3.Connection, out: Path) -> dict[str, str]:
    out.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, sql in EXPORTS.items():
        cur = con.execute(sql)
        path = out / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow([d[0] for d in cur.description])
            w.writerows(cur)
        paths[name] = str(path)
    return paths


def build_payload(con: sqlite3.Connection, cfg: Config, summary: Any) -> dict[str, Any]:
    runs = [dict(r) for r in con.execute(
        "SELECT run_id, started_at, mode, status, files_ingested, raw_rows_ingested, rows_processed, rows_accepted, "
        "rows_quarantined, exact_duplicates, conflicting_duplicates, users_recomputed FROM pipeline_runs "
        "ORDER BY started_at DESC LIMIT 15")]
    exp = summary.explanation
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run": {"run_id": summary.run_id, "mode": summary.mode, "status": summary.status,
                "full_refresh_reason": summary.full_refresh_reason, "duration_seconds": summary.duration_seconds,
                "ingest": {"files_ingested": summary.ingest.files_ingested,
                           "rows_ingested": summary.ingest.rows_ingested,
                           "loaded_files": summary.ingest.loaded_files,
                           "skipped_already_loaded": summary.ingest.skipped_already_loaded,
                           "empty_files": summary.ingest.empty_files,
                           "unsupported_files": summary.ingest.unsupported_files}},
        "config": {"funnel": cfg.funnel_name, "steps": list(cfg.funnel_steps),
                   "inactivity_gap_minutes": cfg.gap_minutes,
                   "near_duplicate_window_seconds": cfg.near_duplicate_window_seconds,
                   "future_tolerance_minutes": cfg.future_tolerance_seconds / 60,
                   "min_event_date": cfg.min_event_date},
        "overview": metrics.overview(con),
        "funnel": metrics.funnel_rows(con),
        "largest_drop": summary.drop.to_dict() if summary.drop else None,
        "explanation": None if exp is None else {
            "text": exp.text, "provider": exp.provider, "model": exp.model, "status": exp.status,
            "ai_generated": exp.is_ai_generated, "warnings": exp.warnings},
        "data_quality": summary.quality,
        "run_history": runs,
        "daily_funnel": [dict(r) for r in con.execute(
            "SELECT * FROM daily_funnel ORDER BY session_date, step_index")],
        "anomalies": [dict(r) for r in con.execute("SELECT * FROM funnel_anomalies ORDER BY session_date, step_index")],
        "alerts": [dict(r) for r in con.execute(
            "SELECT severity, kind, message, created_at, delivered FROM alerts ORDER BY created_at DESC, rowid DESC LIMIT 20")],
        "schema_drift": [dict(r) for r in con.execute("SELECT * FROM schema_drift ORDER BY rows DESC")],
        "tasks": summary.tasks,
    }


def write_reports(con: sqlite3.Connection, cfg: Config, summary: Any) -> dict[str, str]:
    out = cfg.output_dir
    out.mkdir(parents=True, exist_ok=True)
    payload = build_payload(con, cfg, summary)
    (out / "report.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    (out / "report.html").write_text(render_html(payload), encoding="utf-8")
    paths = {"json": str(out / "report.json"), "html": str(out / "report.html")}
    paths.update({f"csv:{k}": v for k, v in _export_csv(con, out / "exports").items()})
    return paths


# ---------------------------------------------------------------------- HTML
def _e(x: Any) -> str:
    return html.escape("" if x is None else str(x))


def _pct(x: float | None, digits: int = 1) -> str:
    return "–" if x is None else f"{x * 100:.{digits}f}%"


def _num(x: Any) -> str:
    return "–" if x is None else f"{x:,}" if isinstance(x, int) else _e(x)


def _dur(seconds: float | None) -> str:
    if seconds is None:
        return "–"
    return f"{seconds:.0f}s" if seconds < 120 else f"{seconds / 60:.1f} min"



def _trend_svg(p: dict[str, Any]) -> str:
    """Small multiple: daily conversion into each funnel step, anomalies in red."""
    daily = p.get("daily_funnel") or []
    steps = [s for s in p["funnel"] if s["step_index"] > 1]
    if not daily or not steps:
        return "<p class='fine'>Not enough days yet.</p>"
    dates = sorted({d["session_date"] for d in daily})
    anom = {(a["session_date"], a["step_index"]) for a in p.get("anomalies", [])}
    w, h, pad_l, pad_b = 250, 120, 34, 22
    cells = []
    for s in steps:
        pts = [(d["session_date"], d["step_conversion"]) for d in daily
               if d["step_index"] == s["step_index"] and d["step_conversion"] is not None]
        if not pts:
            continue
        n = len(dates)
        bw = (w - pad_l - 6) / max(n, 1)
        bars = []
        for i, dt in enumerate(dates):
            val = next((v for d0, v in pts if d0 == dt), None)
            if val is None:
                continue
            bh = val * (h - pad_b - 8)
            x = pad_l + i * bw + 2
            y = h - pad_b - bh
            cls = "bar hot" if (dt, s["step_index"]) in anom else "bar"
            bars.append(f'<rect class="{cls}" x="{x:.1f}" y="{y:.1f}" width="{max(bw - 4, 2):.1f}" height="{bh:.1f}" rx="2">'
                        f'<title>{_e(dt)}: {val * 100:.1f}%</title></rect>')
        labels = (f'<text class="ax" x="{pad_l}" y="{h - 6}">{_e(dates[0][5:])}</text>'
                  f'<text class="ax" x="{w - 4}" y="{h - 6}" text-anchor="end">{_e(dates[-1][5:])}</text>'
                  f'<text class="ax" x="{pad_l - 4}" y="12" text-anchor="end">100%</text>'
                  f'<text class="ax" x="{pad_l - 4}" y="{h - pad_b}" text-anchor="end">0%</text>')
        cells.append(f'<div class="mini"><div class="mt">{_e(s["step_name"])}</div>'
                     f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="daily conversion into {_e(s["step_name"])}">'
                     f'<line class="axl" x1="{pad_l}" y1="{h - pad_b}" x2="{w}" y2="{h - pad_b}"/>{"".join(bars)}{labels}</svg></div>')
    return f'<div class="minis">{"".join(cells)}</div>'


def _ops_sections(p: dict[str, Any]) -> str:
    alerts = "".join(
        f"<tr><td><span class='pill {'bad' if a['severity'] == 'critical' else 'warnp' if a['severity'] == 'warning' else 'muted'}'>"
        f"{_e(a['severity'])}</span></td><td><code>{_e(a['kind'])}</code></td><td>{_e(a['message'])}</td>"
        f"<td class=fine>{_e(a['created_at'])}<br>via {_e(a['delivered'])}</td></tr>"
        for a in p.get("alerts", [])) or "<tr><td colspan=4>No alerts</td></tr>"
    drift = "".join(
        f"<tr><td><code>{_e(d['field'])}</code></td><td class=r>{_num(d['rows'])}</td><td>{_e(d['first_seen_file'])}</td>"
        f"<td><code class=sample>{_e(d['example_value'])}</code></td></tr>"
        for d in p.get("schema_drift", [])) or "<tr><td colspan=4>Every field matches the contract</td></tr>"
    tasks = p.get("tasks") or []
    total = sum(t["duration_ms"] for t in tasks) or 1
    trow_list = []
    for t in tasks:
        rows_txt = "" if t["rows_affected"] is None else f"{t['rows_affected']:,} rows"
        trow_list.append(
            f"<div class='trow'><span class='tn'>{_e(t['task'])}</span><span class='ttrack'>"
            f"<span class='tbar' style='width:{100 * t['duration_ms'] / total:.1f}%'></span></span>"
            f"<span class='tv'>{t['duration_ms']:,.0f} ms</span><span class='tv'>{rows_txt}</span></div>")
    trows = "".join(trow_list)
    return f"""
<div class="card"><h2>Alerts</h2>
<table><tr><th>Severity</th><th>Kind</th><th>Message</th><th>Raised</th></tr>{alerts}</table>
<p class="fine">Each problem has a stable key, so it is alerted once - not on every rerun. Set ALERT_WEBHOOK_URL in .env to push them to Slack / Discord / Teams.</p></div>
<div class="grid2">
<div class="card"><h2>Schema drift (data contract)</h2>
<table><tr><th>Undeclared field</th><th class=r>Rows</th><th>First seen in</th><th>Example</th></tr>{drift}</table></div>
<div class="card"><h2>Task timings - this run</h2>{trows or "<p class='fine'>-</p>"}</div>
</div>"""


def render_html(p: dict[str, Any]) -> str:
    ov, funnel, dq = p["overview"], p["funnel"], p["data_quality"]
    drop, exp = p["largest_drop"], p["explanation"]
    worst_idx = drop["step_index"] if drop else None
    top = max((s["sessions_reached"] for s in funnel), default=0) or 1

    bars = []
    for s in funnel:
        w = 100 * s["sessions_reached"] / top
        hot = s["step_index"] == worst_idx
        drop_txt = "" if s["step_index"] == 1 else (
            f'<span class="drop{" hot" if hot else ""}">−{_pct(s["drop_off_rate"])} '
            f'({_num(s["drop_off_sessions"])} lost)</span>')
        bars.append(f"""
        <div class="frow{' hotrow' if hot else ''}">
          <div class="fname"><span class="idx">{s['step_index']}</span>{_e(s['step_name'])}</div>
          <div class="ftrack"><div class="fbar{' hotbar' if hot else ''}" style="width:{w:.2f}%"></div></div>
          <div class="fnum">{_num(s['sessions_reached'])}<small>{_pct(s['overall_conversion'])} of entries</small></div>
          <div class="fdrop">{drop_txt}<small>{'median ' + _dur(s['median_seconds_from_prev']) + ' from previous' if s['step_index'] > 1 else 'funnel entry'}</small></div>
        </div>""")

    seg_rows = ""
    if drop:
        for g in drop["segments"]:
            sig = g["p_value"] < 0.05
            seg_rows += (f"<tr><td>{_e(g['dimension'])}</td><td><b>{_e(g['value'])}</b></td>"
                         f"<td class=r>{_num(g['sessions_prev'])}</td><td class=r>{_pct(g['conversion'])}</td>"
                         f"<td class=r>{_pct(g['rest_conversion'])}</td><td class=r>{_num(g['lost_sessions'])}</td>"
                         f"<td class=r><span class='pill {'bad' if sig else 'muted'}'>"
                         f"{'p<0.001' if g['p_value'] < 0.001 else 'p=' + format(g['p_value'], '.3f')}</span></td></tr>")

    if exp:
        badge = "AI-generated" if exp["ai_generated"] else "Rule-based (offline)"
        warn = "".join(f"<li>{_e(w)}</li>" for w in exp["warnings"])
        exp_html = f"""
        <div class="card explain">
          <div class="chead"><h2>Why is this the biggest drop?</h2>
            <span class="pill {'ai' if exp['ai_generated'] else 'muted'}">{badge}</span>
            <span class="pill muted">{_e(exp['provider'])} · {_e(exp['model'])} · {_e(exp['status'])}</span></div>
          <pre class="exptext">{_e(exp['text'])}</pre>
          {'<ul class="warn">' + warn + '</ul>' if warn else ''}
          <p class="fine">The model only receives aggregated numbers (no raw events, no user ids). Its answer is
          checked: every number it cites must exist in the data, otherwise the rule-based explanation is used.</p>
        </div>"""
    else:
        exp_html = '<div class="card"><h2>Explanation</h2><p>Not enough funnel data to pick a largest drop.</p></div>'

    t = dq["totals"]
    reasons = "".join(
        f"<tr><td><code>{_e(r['reason'])}</code></td><td class=r>{_num(r['rows'])}</td>"
        f"<td><code class=sample>{_e(dq['quarantine_samples'].get(r['reason'], ''))}</code></td></tr>"
        for r in dq["quarantine_by_reason"]) or "<tr><td colspan=3>No rejected rows</td></tr>"
    coerc_labels = {
        "ts_epoch_number": "Timestamp as epoch number → UTC", "ts_epoch_string": "Timestamp as epoch string → UTC",
        "ts_with_utc_offset": "Timestamp with +hh:mm offset → UTC", "event_name_normalised": "Event name normalised",
        "price_string_cast": "Price string → REAL", "price_invalid_nulled": "Invalid price → NULL",
        "user_id_number_cast": "Numeric user_id → TEXT", "device_mapped": "Device alias mapped (iOS→mobile …)",
        "device_unknown": "Missing device → 'unknown'", "country_unknown": "Missing/invalid country → 'ZZ'",
        "channel_unknown": "Missing channel → 'unknown'"}
    coerc = "".join(f"<tr><td>{_e(coerc_labels.get(k, k))}</td><td class=r>{_num(v)}</td></tr>"
                    for k, v in dq["type_coercions"].items())
    checks = "".join(
        f"<tr><td><code>{_e(c['check'])}</code></td><td class=r>{_num(c['failures'])}</td>"
        f"<td><span class='pill {'good' if c['passed'] else 'bad'}'>{'pass' if c['passed'] else 'FAIL'}</span></td></tr>"
        for c in dq["checks"])
    runs = "".join(
        f"<tr><td><code>{_e(r['run_id'])}</code></td><td>{_e(r['mode'])}</td><td>{_e(r['status'])}</td>"
        f"<td class=r>{_num(r['files_ingested'])}</td><td class=r>{_num(r['rows_processed'])}</td>"
        f"<td class=r>{_num(r['rows_accepted'])}</td><td class=r>{_num(r['rows_quarantined'])}</td>"
        f"<td class=r>{_num(r['exact_duplicates'])}</td><td class=r>{_num(r['users_recomputed'])}</td></tr>"
        for r in p["run_history"])
    files = "".join(
        f"<tr><td>{_e(f['file_name'])}</td><td>{_e(f['file_format'])}</td><td class=r>{_num(f['line_count'])}</td>"
        f"<td class=r>{_num(f['blank_lines'])}</td><td><code>{_e(f['run_id'])}</code></td></tr>" for f in dq["files"])

    run = p["run"]
    kpis = [
        ("Users", _num(ov["users"])), ("Sessions", _num(ov["sessions"])),
        ("Funnel entries", _num(ov["funnel_entries"])), ("Purchases", _num(ov["funnel_completions"])),
        ("End-to-end conversion", _pct(ov["funnel_conversion"])),
        ("Avg session", f"{ov['avg_session_minutes'] or 0:.1f} min"),
    ]
    kpi_html = "".join(f'<div class="kpi"><div class="kv">{v}</div><div class="kl">{_e(k)}</div></div>' for k, v in kpis)
    headline = (f"Biggest leak: <b>{_e(drop['from_step'])} → {_e(drop['to_step'])}</b> loses "
                f"<b>{_pct(drop['drop_off_rate'])}</b> of sessions ({_num(drop['drop_off_sessions'])})") if drop else "No funnel data yet"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Funnel Report</title>
<style>
:root{{--bg:#f6f5f1;--card:#fff;--ink:#1d1f23;--mute:#6b6f76;--line:#e4e2dc;--bar:#3d6b8f;--hot:#c4462c;
--good:#2f7d4f;--soft:#eef2f5;--ai:#5b4bb7}}
@media (prefers-color-scheme:dark){{:root{{--bg:#15171a;--card:#1d2024;--ink:#e8e6e1;--mute:#9a9ea5;--line:#2e3237;
--bar:#6c9cc2;--hot:#e2694f;--good:#5cb383;--soft:#252a30;--ai:#9d8ff0}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}}
.wrap{{max-width:1100px;margin:0 auto;padding:28px 16px 60px}}
header h1{{margin:0;font-size:26px;letter-spacing:-.01em}}header p{{margin:4px 0 0;color:var(--mute)}}
.headline{{margin:18px 0;padding:14px 18px;border-left:4px solid var(--hot);background:var(--card);border-radius:6px;font-size:17px}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:16px 0}}
.kpi{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px}}
.kv{{font-size:22px;font-weight:650;font-variant-numeric:tabular-nums}}.kl{{color:var(--mute);font-size:13px}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px 20px;margin:16px 0;overflow-x:auto}}
h2{{font-size:17px;margin:0 0 12px}}.chead{{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px}}.chead h2{{margin:0 8px 0 0}}
.frow{{display:grid;grid-template-columns:190px 1fr 120px 190px;gap:12px;align-items:center;padding:8px 0;border-bottom:1px solid var(--line)}}
.frow:last-child{{border:0}}.hotrow{{background:linear-gradient(90deg,transparent,color-mix(in srgb,var(--hot) 8%,transparent))}}
.fname{{font-weight:550}}.idx{{display:inline-block;width:20px;color:var(--mute);font-variant-numeric:tabular-nums}}
.ftrack{{background:var(--soft);border-radius:4px;height:22px}}.fbar{{background:var(--bar);height:100%;border-radius:4px}}.hotbar{{background:var(--hot)}}
.fnum{{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}}.fnum small,.fdrop small{{display:block;color:var(--mute);font-weight:400;font-size:12px}}
.drop{{color:var(--mute);font-variant-numeric:tabular-nums}}.drop.hot{{color:var(--hot);font-weight:650}}
table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:top}}
th{{color:var(--mute);font-weight:550;font-size:12px;text-transform:uppercase;letter-spacing:.04em}}td.r,th.r{{text-align:right;font-variant-numeric:tabular-nums}}
code{{font:12.5px ui-monospace,SFMono-Regular,Menlo,monospace}}code.sample{{color:var(--mute);word-break:break-all}}
.pill{{display:inline-block;padding:2px 9px;border-radius:99px;font-size:12px;font-weight:600;border:1px solid var(--line)}}
.pill.good{{color:var(--good);border-color:var(--good)}}.pill.bad{{color:var(--hot);border-color:var(--hot)}}.pill.muted{{color:var(--mute)}}
.pill.ai{{color:var(--ai);border-color:var(--ai)}}
.exptext{{white-space:pre-wrap;font:15px/1.6 inherit;font-family:inherit;margin:8px 0;background:var(--soft);padding:14px 16px;border-radius:8px}}
.warn{{color:var(--hot);font-size:13px}}.fine{{color:var(--mute);font-size:12.5px;margin:6px 0 0}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}.grid2 .card{{margin:0}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;margin-bottom:12px}}
.stat{{background:var(--soft);border-radius:6px;padding:8px 10px}}.stat b{{display:block;font-size:18px;font-variant-numeric:tabular-nums}}.stat span{{color:var(--mute);font-size:12px}}
.minis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}}.mini .mt{{font-size:13px;font-weight:550;margin-bottom:2px}}
.bar{{fill:var(--bar)}}.bar.hot{{fill:var(--hot)}}.ax{{fill:var(--mute);font-size:10px}}.axl{{stroke:var(--line)}}
.pill.warnp{{color:#b7791f;border-color:#b7791f}}
.trow{{display:grid;grid-template-columns:150px 1fr 70px 90px;gap:8px;align-items:center;font-size:13px;padding:3px 0}}
.ttrack{{background:var(--soft);height:10px;border-radius:3px}}.tbar{{display:block;height:100%;background:var(--bar);border-radius:3px}}
.tv{{text-align:right;color:var(--mute);font-variant-numeric:tabular-nums}}.tn{{font-family:ui-monospace,Menlo,monospace;font-size:12px}}
@media (max-width:760px){{.frow{{grid-template-columns:1fr 1fr}}.ftrack{{grid-column:1/-1;order:3}}.grid2{{grid-template-columns:1fr}}}}
</style></head><body><div class="wrap">
<header><h1>{_e(p['config']['funnel'].title())} funnel report</h1>
<p>Run <code>{_e(run['run_id'])}</code> · {_e(run['mode'])} · {_e(run['status'])} · generated {_e(p['generated_at'])} ·
data {_e(ov['first_event'])} → {_e(ov['last_event'])}</p></header>
<div class="headline">{headline}</div>
<div class="kpis">{kpi_html}</div>

<div class="card"><h2>Funnel (strict order, within a session)</h2>{''.join(bars)}
<p class="fine">Session = activity with no gap longer than {p['config']['inactivity_gap_minutes']:g} minutes.
A session counts for a step only if it reached the previous step first. Double clicks within
{p['config']['near_duplicate_window_seconds']:g}s are excluded.</p></div>

<div class="card"><h2>Daily conversion per step <span class="pill bad">red = anomaly</span></h2>{_trend_svg(p)}
<p class="fine">Each day is compared with the healthy days before it (two-proportion z-test). A day is flagged when the drop is both
significant and material; flagged days are kept out of the baseline so a lasting regression stays flagged.</p></div>

{exp_html}
{_ops_sections(p)}

<div class="card"><h2>Where the drop is concentrated</h2>
<table><tr><th>Dimension</th><th>Segment</th><th class=r>Sessions at step</th><th class=r>Continue rate</th>
<th class=r>All other values</th><th class=r>Lost vs others</th><th class=r>Significance</th></tr>{seg_rows or '<tr><td colspan=7>–</td></tr>'}</table>
<p class="fine">Two-proportion z-test of each segment vs. all other values of the same dimension. "Lost" = sessions that would
have continued at the other segments' rate.</p></div>

<div class="card"><h2>Data quality</h2>
<div class="stats">
<div class="stat"><b>{_num(t['raw_rows'])}</b><span>raw rows received</span></div>
<div class="stat"><b>{_num(t['accepted_rows'])}</b><span>accepted</span></div>
<div class="stat"><b>{_num(t['quarantined_rows'])}</b><span>quarantined ({_pct(t['quarantine_rate'], 2)})</span></div>
<div class="stat"><b>{_num(t['exact_duplicates'])}</b><span>exact duplicates</span></div>
<div class="stat"><b>{_num(t['conflicting_duplicates'])}</b><span>conflicting duplicates</span></div>
<div class="stat"><b>{_num(t['near_duplicates'])}</b><span>double clicks</span></div>
<div class="stat"><b>{_num(dq['late_arriving_events'])}</b><span>late arrivals (older than data already loaded)</span></div>
<div class="stat"><b>{_num(t['blank_lines_skipped'])}</b><span>blank lines skipped</span></div>
</div>
<div class="grid2">
<div class="card"><h2>Quarantined rows by reason</h2><table><tr><th>Reason</th><th class=r>Rows</th><th>First example</th></tr>{reasons}</table></div>
<div class="card"><h2>Cleaning &amp; type coercion applied</h2><table><tr><th>Fix</th><th class=r>Rows</th></tr>{coerc}</table></div>
</div>
<div class="card"><h2>Invariant checks <span class="pill {'good' if dq['all_checks_passed'] else 'bad'}">{'all passing' if dq['all_checks_passed'] else 'FAILURES'}</span></h2>
<table><tr><th>Check</th><th class=r>Violations</th><th>Result</th></tr>{checks}</table></div>
</div>

<div class="card"><h2>Pipeline runs (incremental history)</h2>
<table><tr><th>Run</th><th>Mode</th><th>Status</th><th class=r>Files</th><th class=r>Processed</th><th class=r>Accepted</th>
<th class=r>Quarantined</th><th class=r>Exact dups</th><th class=r>Users rebuilt</th></tr>{runs}</table></div>

<div class="card"><h2>Ingested files</h2>
<table><tr><th>File</th><th>Format</th><th class=r>Rows</th><th class=r>Blank lines</th><th>Loaded by run</th></tr>{files}</table></div>
</div></body></html>"""
