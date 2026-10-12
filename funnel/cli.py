"""Command line interface:  python -m funnel <command>"""
from __future__ import annotations

import argparse
import logging
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import PROJECT_ROOT, Config, ConfigError
from .env import load_dotenv, setup_logging

log = logging.getLogger("funnel")


def _load_config(args: argparse.Namespace) -> Config:
    overrides = {}
    if getattr(args, "gap_minutes", None) is not None:
        overrides["inactivity_gap_seconds"] = args.gap_minutes * 60
    return Config.load(args.config, **overrides)


def cmd_generate(args: argparse.Namespace) -> int:
    from .generator import GeneratorSettings, generate
    cfg = _load_config(args)
    if args.clean and cfg.incoming_dir.exists():
        for p in cfg.incoming_dir.iterdir():
            if p.is_file():
                p.unlink()
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.start else None
    truth = generate(cfg.incoming_dir,
                     GeneratorSettings(users=args.users, days=args.days, seed=args.seed, start=start,
                                       inject_issues=not args.no_issues),
                     truth_path=PROJECT_ROOT / "data" / "generator_truth.json")
    print(f"Wrote {len(truth['files'])} files to {cfg.incoming_dir}: {', '.join(truth['files'])}")
    print("Injected issues (ground truth):")
    for k, v in truth["injected"].items():
        print(f"  {k:<45} {v:>7,}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    from .pipeline import run_pipeline
    cfg = _load_config(args)
    s = run_pipeline(cfg, full_refresh=args.full_refresh, write_report=not args.no_report)
    print()
    print(f"Run {s.run_id}  mode={s.mode}  status={s.status}  ({s.duration_seconds}s)")
    if s.full_refresh_reason:
        print(f"  full refresh: {s.full_refresh_reason}")
    i = s.ingest
    print(f"  files: {i.files_ingested} new, {len(i.skipped_already_loaded)} already loaded, "
          f"{len(i.empty_files)} empty, {len(i.unsupported_files)} unsupported")
    print(f"  rows : {s.rows_processed:,} processed -> {s.rows_accepted:,} accepted, "
          f"{s.rows_quarantined:,} quarantined, {s.exact_duplicates:,} exact dups, "
          f"{s.conflicting_duplicates:,} conflicting dups; {s.users_recomputed:,} users re-sessionized")
    failed = [c["check"] for c in s.quality.get("checks", []) if not c["passed"]]
    print(f"  data-quality checks: {'ALL PASS' if not failed else 'FAILED ' + ', '.join(failed)}")
    if s.drop:
        print(f"\nLargest drop: {s.drop.from_step} -> {s.drop.to_step}: "
              f"{s.drop.drop_off_rate * 100:.1f}% ({s.drop.drop_off_sessions:,} of {s.drop.sessions_prev:,} sessions)")
    if s.explanation:
        e = s.explanation
        print(f"\nExplanation [{e.provider} / {e.model} / {e.status}]:\n{e.text}")
        for w in e.warnings:
            print(f"  ! {w}")
    if s.new_alerts:
        print(f"\nNew alerts ({len(s.new_alerts)}):")
        for a in s.new_alerts:
            print(f"  [{a.severity.upper()}] {a.kind}: {a.message}")
    if s.report_paths:
        print(f"\nReport: {s.report_paths['html']}")
        print("Dashboard: python -m funnel serve   ->  http://127.0.0.1:8050")
    return 1 if failed else 0


def cmd_status(args: argparse.Namespace) -> int:
    cfg = _load_config(args)
    if not cfg.warehouse_path.exists():
        print("No warehouse yet - run `python -m funnel run` first.")
        return 0
    con = sqlite3.connect(f"file:{cfg.warehouse_path}?mode=ro", uri=True)
    print("Table row counts:")
    for t in ("ingest_files", "raw_events", "stg_events", "quarantine_events", "duplicate_events",
              "clean_events", "sessions", "funnel_session_steps", "funnel_metrics", "daily_funnel",
              "funnel_anomalies", "schema_drift", "alerts", "erased_users", "pipeline_runs"):
        print(f"  {t:<22} {con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:>9,}")
    print("\nLast runs:")
    for r in con.execute("SELECT run_id, mode, status, files_ingested, rows_processed FROM pipeline_runs "
                         "ORDER BY started_at DESC LIMIT 8"):
        print(f"  {r[0]}  {r[1]:<12} {r[2]:<12} files={r[3]} rows={r[4]}")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Read-only ad-hoc SQL against the warehouse."""
    cfg = _load_config(args)
    con = sqlite3.connect(f"file:{cfg.warehouse_path}?mode=ro", uri=True)
    try:
        cur = con.execute(args.sql)
    except sqlite3.Error as exc:
        print(f"SQL error: {exc}", file=sys.stderr)
        return 2
    cols = [d[0] for d in cur.description or []]
    rows = cur.fetchmany(args.limit)
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c)) for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    cfg = _load_config(args)
    if not args.yes:
        print("This deletes the warehouse and reports (raw input files are kept). Re-run with --yes.")
        return 1
    for p in (cfg.warehouse_path, Path(f"{cfg.warehouse_path}-wal"), Path(f"{cfg.warehouse_path}-shm"),
              cfg.warehouse_path.with_suffix(".lock")):
        p.unlink(missing_ok=True)
    for p in (cfg.output_dir / "report.html", cfg.output_dir / "report.json"):
        p.unlink(missing_ok=True)
    shutil.rmtree(cfg.output_dir / "exports", ignore_errors=True)
    print("Warehouse and reports removed.")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .server import serve
    cfg = _load_config(args)
    serve(cfg, args.host, args.port)
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    """Micro-batch mode: poll the incoming folder and process new files as they land."""
    import time
    from .lock import PipelineLockedError
    from .pipeline import run_pipeline
    cfg = _load_config(args)
    print(f"Watching {cfg.incoming_dir} every {args.interval}s (Ctrl+C to stop)")
    runs = 0
    try:
        while True:
            try:
                s = run_pipeline(cfg)
                if s.status == "success":
                    print(f"  {s.run_id}: {s.ingest.files_ingested} new file(s), {s.rows_accepted:,} rows accepted, "
                          f"{len(s.new_alerts)} new alert(s)")
            except PipelineLockedError as exc:
                print(f"  skipped: {exc}")
            runs += 1
            if args.max_runs and runs >= args.max_runs:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped")
        return 0


def cmd_benchmark(args: argparse.Namespace) -> int:
    """Throughput test on a throw-away warehouse with N generated users."""
    import tempfile
    import time
    from .ai import ExplanationService, RuleBasedExplainer
    from .generator import GeneratorSettings, generate
    from .pipeline import run_pipeline
    base = _load_config(args)
    with tempfile.TemporaryDirectory(prefix="funnel_bench_") as tmp:
        root = Path(tmp)
        cfg = base.replace(incoming_dir=root / "in", warehouse_path=root / "wh.db", output_dir=root / "out")
        t0 = time.perf_counter()
        generate(cfg.incoming_dir, GeneratorSettings(users=args.users, days=args.days, seed=1))
        gen_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        s = run_pipeline(cfg, service=ExplanationService(RuleBasedExplainer()), write_report=False, notifiers=[])
        run_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        run_pipeline(cfg, service=ExplanationService(RuleBasedExplainer()), write_report=False, notifiers=[])
        rerun_s = time.perf_counter() - t0
        size_mb = (root / "wh.db").stat().st_size / 1e6
    print(f"\nBenchmark: {args.users:,} users, {args.days} days")
    print(f"  generated {s.ingest.rows_ingested:,} raw rows in {gen_s:.1f}s")
    print(f"  full pipeline : {run_s:6.1f}s  -> {s.ingest.rows_ingested / run_s:,.0f} rows/s")
    print(f"  no-op rerun   : {rerun_s:6.2f}s")
    print(f"  warehouse size: {size_mb:,.1f} MB")
    print("  slowest tasks :", ", ".join(f"{t['task']} {t['duration_ms'] / 1000:.1f}s" for t in
                                        sorted(s.tasks, key=lambda t: -t["duration_ms"])[:4]))
    return 0


def cmd_forget(args: argparse.Namespace) -> int:
    from .privacy import erase_user
    cfg = _load_config(args)
    r = erase_user(cfg, args.user_id)
    print(f"Erased user (sha256 {r.user_hash[:16]}...): {r.raw_rows} raw rows, {r.clean_rows} clean events, "
          f"{r.sessions} sessions removed{' (was already erased before)' if r.already_erased else ''}.")
    print("Aggregates recomputed. Future files containing this user are filtered at ingestion.")
    print("Run `python -m funnel run` to refresh the report.")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo
    return run_demo()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="funnel", description="Event logs -> sessions -> funnel -> AI explanation")
    p.add_argument("--config", default=None, help="path to pipeline.toml")
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write synthetic event-log files into the incoming folder")
    g.add_argument("--users", type=int, default=5000)
    g.add_argument("--days", type=int, default=7, help="one file per day")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--start", help="UTC start date YYYY-MM-DD (default: N days before today)")
    g.add_argument("--no-issues", action="store_true", help="generate clean data only")
    g.add_argument("--clean", action="store_true", help="delete existing files in the incoming folder first")
    g.set_defaults(func=cmd_generate)

    r = sub.add_parser("run", help="ingest new files and refresh everything (incremental)")
    r.add_argument("--full-refresh", action="store_true", help="rebuild all derived tables from raw")
    r.add_argument("--gap-minutes", type=float, help="override the inactivity gap")
    r.add_argument("--no-report", action="store_true")
    r.set_defaults(func=cmd_run)

    sub.add_parser("status", help="row counts and run history").set_defaults(func=cmd_status)
    q = sub.add_parser("query", help="run a read-only SQL query")
    q.add_argument("sql")
    q.add_argument("--limit", type=int, default=50)
    q.set_defaults(func=cmd_query)
    rs = sub.add_parser("reset", help="delete warehouse + reports (keeps raw files)")
    rs.add_argument("--yes", action="store_true")
    rs.set_defaults(func=cmd_reset)
    sub.add_parser("demo-edge-cases", help="failure / edge-case demonstration").set_defaults(func=cmd_demo)

    sv = sub.add_parser("serve", help="live dashboard + JSON API")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8050)
    sv.set_defaults(func=cmd_serve)
    w = sub.add_parser("watch", help="poll the incoming folder and process new files automatically")
    w.add_argument("--interval", type=float, default=30)
    w.add_argument("--max-runs", type=int, default=0, help="stop after N polls (0 = forever)")
    w.set_defaults(func=cmd_watch)
    b = sub.add_parser("benchmark", help="throughput test on generated data")
    b.add_argument("--users", type=int, default=20000)
    b.add_argument("--days", type=int, default=7)
    b.set_defaults(func=cmd_benchmark)
    fg = sub.add_parser("forget-user", help="erase a user everywhere (GDPR / DPDP right to be forgotten)")
    fg.add_argument("user_id")
    fg.set_defaults(func=cmd_forget)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level)
    load_dotenv()
    from .lock import PipelineLockedError
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except PipelineLockedError as exc:
        print(f"Locked: {exc}", file=sys.stderr)
        return 3
    except OSError as exc:
        if getattr(exc, "errno", None) in (48, 98, 10048):  # address already in use (mac / linux / windows)
            print(f"Port already in use - try: python -m funnel serve --port 8051 ({exc})", file=sys.stderr)
            return 4
        raise


if __name__ == "__main__":
    raise SystemExit(main())
