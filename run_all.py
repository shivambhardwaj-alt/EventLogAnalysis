"""ONE command that demonstrates the whole project from a clean state.

    python run_all.py            # everything, then prints how to open the dashboard
    python run_all.py --serve    # ...and starts the live dashboard at the end
    python run_all.py --quick    # smaller data set (faster)

What it does - exactly what a real daily pipeline would see:
  1. wipes previous outputs (raw input folders included)
  2. generates 7 days of messy synthetic event logs into data/landing/
     (a buggy app release ships on day 5; a producer adds undeclared fields on day 7)
  3. delivers ONE file per "day" into data/incoming/ and runs the pipeline after each,
     so you can watch incremental loading, late events, the anomaly being detected
     on the day it starts, and the schema-drift alert on day 7
  4. runs the pipeline again with nothing new (proves idempotency)
  5. runs the test suite and the failure / edge-case demonstration
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

if sys.version_info < (3, 11):
    sys.exit(f"Python 3.11 or newer is required (you have {sys.version.split()[0]}). Get it from https://www.python.org/downloads/")

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from funnel.ai import build_service  # noqa: E402
from funnel.alerts import default_notifiers  # noqa: E402
from funnel.config import Config  # noqa: E402
from funnel.env import load_dotenv, setup_logging  # noqa: E402
from funnel.generator import GeneratorSettings, generate  # noqa: E402
from funnel.pipeline import run_pipeline  # noqa: E402


def banner(text: str) -> None:
    print(f"\n{'=' * 78}\n {text}\n{'=' * 78}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quick", action="store_true", help="2,000 users instead of 5,000")
    ap.add_argument("--serve", action="store_true", help="start the dashboard at the end")
    ap.add_argument("--port", type=int, default=8050)
    ap.add_argument("--no-tests", action="store_true")
    ap.add_argument("--no-demo", action="store_true")
    args = ap.parse_args()

    setup_logging("WARNING")          # keep the console readable; the pipeline itself logs at INFO
    loaded = load_dotenv(ROOT / ".env")
    cfg = Config.load()
    landing = ROOT / "data" / "landing"
    started = time.perf_counter()

    banner("1/5  Clean start")
    for p in (cfg.warehouse_path, Path(f"{cfg.warehouse_path}-wal"), Path(f"{cfg.warehouse_path}-shm"),
              cfg.warehouse_path.with_suffix(".lock")):
        p.unlink(missing_ok=True)
    for d in (landing, cfg.incoming_dir, cfg.output_dir / "exports"):
        shutil.rmtree(d, ignore_errors=True)
    cfg.incoming_dir.mkdir(parents=True, exist_ok=True)
    print("  removed old warehouse, reports and input files")
    svc = build_service()
    print(f"  .env loaded: {'yes (' + ', '.join(sorted(loaded)) + ')' if loaded else 'no (.env not found - fine)'}")
    print(f"  AI explanations: " + (f"LLM via {svc.provider} / {svc.model}" if svc.provider != "rule_based"
                                    else "offline rule-based (add GROQ_API_KEY to .env for an LLM)"))

    banner("2/5  Generate 7 days of synthetic event logs")
    truth = generate(landing, GeneratorSettings(users=2000 if args.quick else 5000, days=7, seed=42),
                     truth_path=ROOT / "data" / "generator_truth.json")
    inj = truth["injected"]
    print(f"  {sum(1 for _ in truth['files'])} files, {inj['clean_events_simulated']:,} real events plus injected problems:")
    print(f"    {inj['exact_duplicates']:,} retries, {inj['conflicting_duplicates']} conflicting ids, "
          f"{inj['near_duplicates']} double clicks, {inj['late_arrivals']:,} late uploads, "
          f"{sum(v for k, v in inj.items() if k.startswith('quarantine:'))} invalid rows")
    print(f"  planted: release {truth['planted_anomaly']['segment']} breaks the payment step from "
          f"{truth['planted_anomaly']['release_date_utc']} (day {truth['planted_anomaly']['release_day']})")
    print(f"  planted: undeclared fields {truth['schema_drift']['fields']} in {truth['schema_drift']['file']}")

    banner("3/5  Daily incremental loads (one file arrives per day)")
    notifiers = [n for n in default_notifiers() if n.name != "log"]   # printed below instead
    day_files = sorted(f for f in truth["files"] if f.startswith("events_day_"))
    extra = [f for f in truth["files"] if not f.startswith("events_day_")]
    print(f"  {'day':<4}{'file(s)':<46}{'rows':>7}{'accepted':>10}{'quarant.':>10}{'dups':>7}{'users':>7}  largest drop")
    for i, name in enumerate(day_files, start=1):
        batch = [name] + (extra if i == 1 else [])
        for f in batch:
            shutil.move(str(landing / f), cfg.incoming_dir / f)
        s = run_pipeline(cfg, write_report=(i == len(day_files)), notifiers=notifiers)
        drop = f"{s.drop.to_step} -{s.drop.drop_off_rate:.0%}" if s.drop else "-"
        print(f"  {i:<4}{' + '.join(batch):<46}{s.rows_processed:>7,}{s.rows_accepted:>10,}{s.rows_quarantined:>10,}"
              f"{s.exact_duplicates + s.conflicting_duplicates:>7,}{s.users_recomputed:>7,}  {drop}")
        for a in s.new_alerts:
            print(f"       ALERT [{a.severity}] {a.kind}: {a.message[:150]}")
        failed = [c["check"] for c in s.quality["checks"] if not c["passed"]]
        if failed:
            print(f"       DATA-QUALITY CHECKS FAILED: {failed}")
            return 1
    print("  all 10 data-quality checks passed after every load")

    banner("4/5  Run again with no new data (must change nothing)")
    s = run_pipeline(cfg, notifiers=notifiers)
    print(f"  status={s.status}, files skipped={len(s.ingest.skipped_already_loaded)}, rows processed={s.rows_processed}, "
          f"new alerts={len(s.new_alerts)}")
    if s.explanation:
        print(f"\n  Explanation of the largest drop [{s.explanation.provider} / {s.explanation.status}]:")
        for line in s.explanation.text.splitlines():
            print("    " + line)
        for w in s.explanation.warnings:
            print(f"    ! {w}")

    banner("5/5  Tests and edge-case demonstration")
    rc = 0
    if not args.no_tests:
        r = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
                           cwd=ROOT, capture_output=True, text=True)
        summary = [ln for ln in r.stderr.splitlines() if ln.startswith(("Ran ", "OK", "FAILED"))]
        print("  tests: " + " | ".join(summary))
        if r.returncode:
            print(r.stderr[-3000:])
            rc = 1
    if not args.no_demo:
        r = subprocess.run([sys.executable, "-m", "funnel", "--log-level", "ERROR", "demo-edge-cases"],
                           cwd=ROOT, capture_output=True, text=True)
        out = r.stdout.splitlines()
        print("  edge-case demo: " + ("OK" if r.returncode == 0 else "FAILED") + f" ({len(out)} lines) - "
              "full output: python -m funnel demo-edge-cases")
        rc = rc or r.returncode

    print(f"\nDone in {time.perf_counter() - started:.0f}s.")
    print(f"  Report    : {cfg.output_dir / 'report.html'}")
    print(f"  Exports   : {cfg.output_dir / 'exports'}")
    print(f"  Warehouse : {cfg.warehouse_path}   (python -m funnel query \"SELECT * FROM funnel_metrics\")")
    if args.serve:
        from funnel.server import serve
        serve(cfg, port=args.port)
    else:
        print(f"  Dashboard : python -m funnel serve      then open http://127.0.0.1:{args.port}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
