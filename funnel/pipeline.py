"""Orchestrator.

    lock -> ingest -> validate -> dedupe -> near-dups -> sessionize -> funnel steps
         -> aggregates (funnel, segments, daily) -> schema drift -> quality checks
         -> anomaly detection -> largest drop + AI explanation -> alerts -> report

Incremental by default:
  * only files never seen before are ingested (content hash);
  * only raw rows not yet processed are validated (a crashed run is simply
    picked up by the next one - everything below runs in one transaction);
  * only users with new events get their sessions/funnel rebuilt;
  * cheap aggregates are rebuilt every time, so they are always consistent.
Re-running with no new files changes nothing. A config or schema change that
affects derived data triggers an automatic full refresh from the raw layer.
Every task is timed and recorded in `task_runs` (observability / lineage).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

from . import alerts as alerting
from . import metrics, quality
from .ai import ExplanationService, build_service
from .ai.base import DropContext, Explanation
from .config import SEGMENT_COLUMNS, Config
from .db import apply_schema, connect, read_sql, run_file, run_script, safe_identifier, scalar, sql_blocks, transaction
from .ingest import IngestResult, ingest_new_files
from .lock import RunLock

log = logging.getLogger(__name__)

DERIVED_TABLES = ("funnel_session_steps", "sessions", "clean_events", "duplicate_events",
                  "quarantine_events", "stg_events")


@dataclass
class RunSummary:
    run_id: str
    mode: str
    status: str
    ingest: IngestResult
    rows_processed: int = 0
    rows_accepted: int = 0
    rows_quarantined: int = 0
    exact_duplicates: int = 0
    conflicting_duplicates: int = 0
    users_recomputed: int = 0
    duration_seconds: float = 0.0
    full_refresh_reason: str | None = None
    explanation: Explanation | None = None
    drop: DropContext | None = None
    quality: dict[str, Any] = field(default_factory=dict)
    anomalies: list[dict[str, Any]] = field(default_factory=list)
    schema_drift: list[dict[str, Any]] = field(default_factory=list)
    new_alerts: list[alerting.Alert] = field(default_factory=list)
    tasks: list[dict[str, Any]] = field(default_factory=list)
    report_paths: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["ingest"] = asdict(self.ingest)
        return d


class TaskTracker:
    """Times each pipeline task; persisted to task_runs at the end of the run."""

    def __init__(self) -> None:
        self.tasks: list[dict[str, Any]] = []

    @contextmanager
    def task(self, name: str) -> Iterator[dict[str, Any]]:
        rec: dict[str, Any] = {"task": name, "status": "success", "rows_affected": None}
        t0 = time.perf_counter()
        try:
            yield rec
        except BaseException:
            rec["status"] = "failed"
            raise
        finally:
            rec["duration_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            self.tasks.append(rec)
            log.debug("task %-22s %8.1f ms rows=%s", name, rec["duration_ms"], rec["rows_affected"])

    def save(self, con: sqlite3.Connection, run_id: str) -> None:
        con.executemany(
            "INSERT OR REPLACE INTO task_runs (run_id, seq, task, status, duration_ms, rows_affected) VALUES (?, ?, ?, ?, ?, ?)",
            [(run_id, i, t["task"], t["status"], t["duration_ms"], t["rows_affected"]) for i, t in enumerate(self.tasks, 1)])


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:6]


def _sync_reference_data(con: sqlite3.Connection, cfg: Config) -> None:
    with transaction(con):
        con.execute("DELETE FROM dim_funnel_steps")
        con.executemany("INSERT INTO dim_funnel_steps (step_index, step_name) VALUES (?, ?)",
                        list(enumerate(cfg.funnel_steps, start=1)))
        con.executemany("INSERT OR IGNORE INTO dim_event_types (event_name) VALUES (?)",
                        [(e,) for e in cfg.allowed_events])
        con.execute(
            f"DELETE FROM dim_event_types WHERE event_name NOT IN ({','.join('?' * len(cfg.allowed_events))}) "
            "AND event_name NOT IN (SELECT DISTINCT event_name FROM clean_events)",
            cfg.allowed_events)


def _needs_full_refresh(con: sqlite3.Connection, cfg: Config) -> str | None:
    previous = scalar(con, "SELECT value FROM pipeline_state WHERE key = 'processing_fingerprint'")
    has_data = scalar(con, "SELECT EXISTS (SELECT 1 FROM stg_events)")
    if has_data and previous and previous != cfg.processing_fingerprint():
        return "processing config changed since the data was built"
    if not has_data and scalar(con, "SELECT EXISTS (SELECT 1 FROM raw_events)"):
        return "derived tables are empty but raw data exists (new schema version or reset)"
    return None


def _create_temp_tables(con: sqlite3.Connection) -> None:
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _batch (raw_id INTEGER PRIMARY KEY)")
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _affected_users (user_id TEXT PRIMARY KEY)")
    con.execute("DELETE FROM _batch")
    con.execute("DELETE FROM _affected_users")


def _process_batch(con: sqlite3.Connection, cfg: Config, run_id: str, tracker: TaskTracker) -> None:
    """Run every row-level transformation stage for the rows currently in _batch."""
    p = {
        "run_id": run_id,
        "min_epoch": cfg.min_epoch,
        "future_tolerance_s": cfg.future_tolerance_seconds,
        "near_dup_window_s": cfg.near_duplicate_window_seconds,
        "gap_seconds": cfg.inactivity_gap_seconds,
    }
    for name, file in (("validate", "01_validate.sql"), ("deduplicate", "02_dedupe.sql"),
                       ("near_duplicates", "03_near_duplicates.sql"), ("sessionize", "04_sessionize.sql")):
        with tracker.task(name) as t:
            t["rows_affected"] = run_file(con, file, p)

    with tracker.task("funnel_steps") as t:
        blocks = sql_blocks("05_funnel_steps.sql")
        run_script(con, blocks["reset"])
        total = 0
        for index, step in enumerate(cfg.funnel_steps, start=1):
            block = blocks["first_step"] if index == 1 else blocks["next_step"]
            total += run_script(con, block, {"step_index": index, "step_name": step})
        t["rows_affected"] = total


def rebuild_aggregates(con: sqlite3.Connection, cfg: Config, tracker: TaskTracker | None = None) -> None:
    tracker = tracker or TaskTracker()
    with tracker.task("funnel_metrics") as t:
        t["rows_affected"] = run_file(con, "06_funnel_metrics.sql")
    with tracker.task("funnel_segments") as t:
        con.execute("DELETE FROM funnel_segments")
        template = read_sql("07_funnel_segments.sql")
        total = 0
        for dim in cfg.segment_dimensions:
            col = safe_identifier(dim, set(SEGMENT_COLUMNS))
            total += run_script(con, template.replace("{dimension}", col), {"dimension": col})
        t["rows_affected"] = total
    with tracker.task("daily_funnel") as t:
        t["rows_affected"] = run_file(con, "10_daily_funnel.sql")
    with tracker.task("schema_drift") as t:
        t["rows_affected"] = run_file(con, "11_schema_drift.sql", {
            "contract_fields": json.dumps(list(cfg.contract_fields)),
            "contract_property_fields": json.dumps(list(cfg.contract_property_fields))})


def _explain(con: sqlite3.Connection, ctx: DropContext, service: ExplanationService) -> Explanation:
    """Explanations are cached per (context, provider, model): unchanged data
    => same text, zero API calls, deterministic reruns."""
    key = ctx.fingerprint()
    row = con.execute(
        "SELECT explanation, status, warnings FROM ai_explanations WHERE context_hash = ? AND provider = ? AND model = ?",
        (key, service.provider, service.model)).fetchone()
    if row and row["status"] == "ok":
        return Explanation(row["explanation"], service.provider, service.model, "cached", json.loads(row["warnings"]))
    exp = service.explain(ctx)
    if exp.status == "ok":
        with transaction(con):
            con.execute(
                "INSERT OR REPLACE INTO ai_explanations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (key, exp.provider, exp.model, ctx.to_step, exp.text, exp.status, json.dumps(exp.warnings), _utcnow()))
    return exp


def run_pipeline(cfg: Config, *, full_refresh: bool = False, service: ExplanationService | None = None,
                 write_report: bool = True, notifiers: list[alerting.Notifier] | None = None) -> RunSummary:
    with RunLock(cfg.warehouse_path.with_suffix(".lock"), cfg.lock_timeout_seconds, owner="run"):
        return _run(cfg, full_refresh=full_refresh, service=service, write_report=write_report, notifiers=notifiers)


def _run(cfg: Config, *, full_refresh: bool, service: ExplanationService | None, write_report: bool,
         notifiers: list[alerting.Notifier] | None) -> RunSummary:
    started = time.perf_counter()
    tracker = TaskTracker()
    con = connect(cfg.warehouse_path)
    try:
        with tracker.task("schema"):
            migrated = apply_schema(con)
        _sync_reference_data(con, cfg)

        auto_reason = None
        if migrated:
            auto_reason = "warehouse schema upgraded"
        elif not full_refresh:
            auto_reason = _needs_full_refresh(con, cfg)
        if auto_reason:
            log.warning("full refresh forced: %s", auto_reason)
            full_refresh = True
        mode = "full_refresh" if full_refresh else "incremental"

        run_id = _new_run_id()
        with transaction(con):
            con.execute("INSERT INTO pipeline_runs (run_id, started_at, mode, status) VALUES (?, ?, ?, 'running')",
                        (run_id, _utcnow(), mode))
        log.info("run %s mode=%s", run_id, mode)

        try:
            with tracker.task("ingest") as t:
                ingest = ingest_new_files(con, cfg.incoming_dir, run_id)
                t["rows_affected"] = ingest.rows_ingested
            summary = RunSummary(run_id=run_id, mode=mode, status="running", ingest=ingest,
                                 full_refresh_reason=auto_reason or ("requested" if full_refresh else None))

            with transaction(con):
                _create_temp_tables(con)
                if full_refresh:
                    for table in DERIVED_TABLES:
                        con.execute(f"DELETE FROM {table}")
                    con.execute(f"DELETE FROM dim_event_types WHERE event_name NOT IN "
                                f"({','.join('?' * len(cfg.allowed_events))})", cfg.allowed_events)
                    con.execute("INSERT INTO _batch (raw_id) SELECT raw_id FROM raw_events")
                else:
                    con.execute("INSERT INTO _batch (raw_id) SELECT r.raw_id FROM raw_events r "
                                "WHERE NOT EXISTS (SELECT 1 FROM stg_events s WHERE s.raw_id = r.raw_id)")
                pending = scalar(con, "SELECT COUNT(*) FROM _batch")

                if pending:
                    _process_batch(con, cfg, run_id, tracker)
                    summary.rows_processed = pending
                    summary.rows_accepted = scalar(con, "SELECT COUNT(*) FROM clean_events c JOIN _batch b USING (raw_id)")
                    summary.rows_quarantined = scalar(con, "SELECT COUNT(*) FROM quarantine_events q JOIN _batch b USING (raw_id)")
                    summary.exact_duplicates = scalar(con, "SELECT COUNT(*) FROM duplicate_events d JOIN _batch b USING (raw_id) WHERE kind='exact'")
                    summary.conflicting_duplicates = scalar(con, "SELECT COUNT(*) FROM duplicate_events d JOIN _batch b USING (raw_id) WHERE kind='conflicting'")
                    summary.users_recomputed = scalar(con, "SELECT COUNT(*) FROM _affected_users")
                    rebuild_aggregates(con, cfg, tracker)
                    summary.status = "success"
                else:
                    if not scalar(con, "SELECT EXISTS (SELECT 1 FROM funnel_metrics)"):
                        rebuild_aggregates(con, cfg, tracker)
                    summary.status = "no_new_data"
                with tracker.task("anomaly_detection") as t:
                    summary.anomalies = metrics.detect_anomalies(con, cfg)
                    t["rows_affected"] = len(summary.anomalies)
                con.execute("INSERT OR REPLACE INTO pipeline_state (key, value) VALUES ('processing_fingerprint', ?)",
                            (cfg.processing_fingerprint(),))

            log.info("processed rows=%d accepted=%d quarantined=%d dup_exact=%d dup_conflicting=%d users_recomputed=%d",
                     summary.rows_processed, summary.rows_accepted, summary.rows_quarantined,
                     summary.exact_duplicates, summary.conflicting_duplicates, summary.users_recomputed)

            with tracker.task("quality_checks") as t:
                summary.quality = quality.report(con, cfg)
                t["rows_affected"] = sum(c["failures"] for c in summary.quality["checks"])
            failed = [c["check"] for c in summary.quality["checks"] if not c["passed"]]
            if failed:
                log.error("data-quality checks FAILED: %s", failed)
            summary.schema_drift = [dict(r) for r in con.execute("SELECT * FROM schema_drift ORDER BY rows DESC")]

            with tracker.task("explain_largest_drop"):
                summary.drop = metrics.find_largest_drop(con, cfg, quality.notes_for_ai(summary.quality),
                                                         summary.anomalies)
                if summary.drop is not None:
                    summary.explanation = _explain(con, summary.drop, service or build_service())
                    log.info("largest drop %s -> %s (%.1f%%), explanation by %s [%s]",
                             summary.drop.from_step, summary.drop.to_step, summary.drop.drop_off_rate * 100,
                             summary.explanation.provider, summary.explanation.status)
                else:
                    log.warning("not enough funnel data (min_sample_size=%d) to pick a largest drop",
                                cfg.min_sample_size)

            with tracker.task("alerts") as t:
                rules = alerting.evaluate(cfg, run_id, summary, summary.anomalies, summary.schema_drift)
                with transaction(con):
                    summary.new_alerts = alerting.record_and_deliver(con, run_id, rules, notifiers)
                t["rows_affected"] = len(summary.new_alerts)

            summary.duration_seconds = round(time.perf_counter() - started, 3)
            summary.tasks = tracker.tasks
            with transaction(con):
                tracker.save(con, run_id)
                con.execute("""
                    UPDATE pipeline_runs SET finished_at=?, status=?, files_ingested=?, raw_rows_ingested=?,
                        rows_processed=?, rows_accepted=?, rows_quarantined=?, exact_duplicates=?,
                        conflicting_duplicates=?, users_recomputed=? WHERE run_id=?""",
                    (_utcnow(), summary.status, ingest.files_ingested, ingest.rows_ingested,
                     summary.rows_processed, summary.rows_accepted, summary.rows_quarantined,
                     summary.exact_duplicates, summary.conflicting_duplicates, summary.users_recomputed, run_id))

            if write_report:
                from .report import write_reports
                summary.report_paths = write_reports(con, cfg, summary)
            return summary
        except Exception as exc:
            with transaction(con):
                tracker.save(con, run_id)
                con.execute("UPDATE pipeline_runs SET finished_at=?, status='failed', error=? WHERE run_id=?",
                            (_utcnow(), f"{type(exc).__name__}: {exc}"[:2000], run_id))
            raise
    finally:
        con.close()
