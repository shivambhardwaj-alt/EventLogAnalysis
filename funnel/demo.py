
from __future__ import annotations

import json
import sqlite3
import tempfile
import textwrap
from pathlib import Path

from .ai import ExplanationService, RuleBasedExplainer
from .ai.base import DropContext
from .ai.providers import OllamaExplainer
from .config import DEFAULT_CONFIG, Config, ConfigError
from .pipeline import run_pipeline

BASE = "2026-03-02T10:00:00"


def _ev(eid, name, ts, user="u_demo", **kw):
    e = {"event_id": eid, "user_id": user, "event_name": name, "ts": ts,
         "device": kw.pop("device", "mobile"), "country": kw.pop("country", "IN"),
         "channel": kw.pop("channel", "organic"), "properties": kw.pop("properties", {"product_id": "p_1", "price": 499})}
    e.update(kw)
    return json.dumps(e)


CASES: list[tuple[str, str]] = [
    ("baseline valid event",                  _ev("e1", "view_product", BASE + "Z")),
    ("client retry: identical line again",    _ev("e1", "view_product", BASE + "Z")),
    ("same event_id, different timestamp",    _ev("e1", "view_product", "2026-03-02T10:05:00Z")),
    ("add_to_cart",                           _ev("e2", "add_to_cart", "2026-03-02T10:01:00Z")),
    ("double click 0.8s later (new id)",      _ev("e3", "add_to_cart", "2026-03-02T10:01:00.800Z")),
    ("genuine 2nd add_to_cart 6s later",      _ev("e4", "add_to_cart", "2026-03-02T10:01:06Z")),
    ("'Begin Checkout' messy name",           _ev("e5", "Begin Checkout", "2026-03-02T10:02:00Z")),
    ("epoch millis as a number",              _ev("e6", "add_payment_info", 1772445780000)),
    ("+05:30 offset (= 10:04 UTC)",           _ev("e7", "purchase", "2026-03-02T15:34:00+05:30")),
    ("price '499.00' string, device 'iOS'",   _ev("e8", "page_view", "2026-03-02T10:05:00Z", device="iOS",
                                                  properties={"product_id": "p_1", "price": "499.00"})),
    ("price 'abc' -> NULL, row kept",         _ev("e9", "page_view", "2026-03-02T10:06:00Z",
                                                  properties={"product_id": "p_1", "price": "abc"})),
    ("lower-case country, no channel",        _ev("e10", "search", "2026-03-02T10:07:00Z", country="in", channel=None)),
    ("user_id is the string 'null'",          _ev("e11", "search", BASE + "Z", user="null")),
    ("user_id missing (JSON null)",           _ev("e12", "search", BASE + "Z", user=None)),
    ("no event_id",                           json.dumps({"user_id": "u_demo", "event_name": "search", "ts": BASE + "Z"})),
    ("unknown event type",                    _ev("e13", "teleport", BASE + "Z")),
    ("timestamp 'yesterday'",                 _ev("e14", "search", "yesterday")),
    ("timestamp far in the future",           _ev("e15", "search", "2099-01-01T00:00:00Z")),
    ("timestamp in 1999",                     _ev("e16", "search", "1999-12-31T23:59:59Z")),
    ("truncated JSON",                        '{"event_id": "e17", "user_id": "u_demo", "event_na'),
    ("JSON array instead of object",          '["e18", "search"]'),
]


def _outcomes(db: Path, file_name: str) -> list[sqlite3.Row]:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT r.line_no,
               CASE
                 WHEN q.raw_id IS NOT NULL THEN 'QUARANTINED: ' || q.reason
                 WHEN d.raw_id IS NOT NULL THEN 'DUPLICATE (' || d.kind || ') of ' || d.event_id
                 WHEN c.is_near_duplicate = 1 THEN 'accepted, flagged near-duplicate (excluded)'
                 ELSE 'accepted -> ' || c.event_name || ' @ ' || c.event_ts || ' | ' || c.device || '/' || c.country
                      || '/' || c.channel || COALESCE(' | price=' || c.price, ' | price=NULL')
               END AS outcome
        FROM raw_events r
        JOIN ingest_files f ON f.file_hash = r.file_hash
        LEFT JOIN quarantine_events q ON q.raw_id = r.raw_id
        LEFT JOIN duplicate_events d ON d.raw_id = r.raw_id
        LEFT JOIN clean_events c ON c.raw_id = r.raw_id
        WHERE f.file_name = ? ORDER BY r.line_no""", (file_name,)).fetchall()
    con.close()
    return rows


def _sessions(db: Path) -> list[tuple]:
    con = sqlite3.connect(db)
    rows = con.execute("SELECT session_id, session_start, session_end, event_count FROM sessions "
                       "WHERE user_id = 'u_late' ORDER BY start_epoch").fetchall()
    con.close()
    return rows


def _h(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def run_demo() -> int:
    with tempfile.TemporaryDirectory(prefix="funnel_demo_") as tmp:
        root = Path(tmp)
        incoming = root / "incoming"
        incoming.mkdir()
        cfg = Config.load(DEFAULT_CONFIG, incoming_dir=incoming, warehouse_path=root / "wh.db",
                          output_dir=root / "out", min_sample_size=1)
        offline = ExplanationService(RuleBasedExplainer())

        
        _h("1) Every kind of bad / messy row -> what the pipeline did with it")
        (incoming / "01_edge_cases.jsonl").write_text("\n".join(l for _, l in CASES) + "\n\n", encoding="utf-8")
        s = run_pipeline(cfg, service=offline, write_report=False)
        for (label, _), row in zip(CASES, _outcomes(cfg.warehouse_path, "01_edge_cases.jsonl")):
            print(f"  line {row['line_no']:>2}  {label:<38} {row['outcome']}")
        print(f"\n  -> {s.rows_processed} rows: {s.rows_accepted} accepted, {s.rows_quarantined} quarantined, "
              f"{s.exact_duplicates} exact + {s.conflicting_duplicates} conflicting duplicates. "
              f"Blank line skipped. Nothing crashed, nothing silently dropped.")

        
        _h("2) Run again with no new files (idempotent)")
        s = run_pipeline(cfg, service=offline, write_report=False)
        print(f"  status={s.status}; skipped already-loaded files: {s.ingest.skipped_already_loaded}; "
              f"rows processed={s.rows_processed}")
        (incoming / "01_edge_cases_COPY.jsonl").write_bytes((incoming / "01_edge_cases.jsonl").read_bytes())
        s = run_pipeline(cfg, service=offline, write_report=False)
        print(f"  renamed copy of the same file -> skipped by content hash: {s.ingest.skipped_already_loaded}")

        
        _h("3) Incremental load: a late event merges two sessions")
        (incoming / "02_day1.jsonl").write_text("\n".join([
            _ev("L1", "app_open", "2026-03-03T09:00:00Z", user="u_late"),
            _ev("L2", "view_product", "2026-03-03T09:20:00Z", user="u_late"),
            _ev("L4", "add_to_cart", "2026-03-03T10:10:00Z", user="u_late"),
        ]), encoding="utf-8")
        run_pipeline(cfg, service=offline, write_report=False)
        print("  after day-1 file (09:20 -> 10:10 is a 50 min gap > 30 min):")
        for r in _sessions(cfg.warehouse_path):
            print(f"    {r[0]:<28} {r[1]} -> {r[2]}  events={r[3]}")
        (incoming / "03_day2_late.jsonl").write_text(
            _ev("L3", "search", "2026-03-03T09:45:00Z", user="u_late"), encoding="utf-8")
        s = run_pipeline(cfg, service=offline, write_report=False)
        print(f"  a late 09:45 event arrives in the next file (only {s.users_recomputed} user re-sessionized):")
        for r in _sessions(cfg.warehouse_path):
            print(f"    {r[0]:<28} {r[1]} -> {r[2]}  events={r[3]}")

        
        _h("4) Empty file, broken CSV header, unsupported file type")
        (incoming / "04_empty.jsonl").write_text("", encoding="utf-8")
        (incoming / "05_bad_header.csv").write_text("id,who,when\n1,2,3\n", encoding="utf-8")
        (incoming / "06_notes.txt").write_text("not data", encoding="utf-8")
        s = run_pipeline(cfg, service=offline, write_report=False)
        print(f"  empty files recorded (never re-read): {s.ingest.empty_files}")
        print(f"  rejected / unsupported (left in place, retried after a fix): {s.ingest.unsupported_files}")
        print(f"  pipeline status: {s.status}")

        _h("5) AI provider unreachable -> automatic fallback, pipeline still succeeds")
        dead = ExplanationService(OllamaExplainer(base_url="http://127.0.0.1:9", model="llama3.2", timeout=2))
        s = run_pipeline(cfg, service=dead, write_report=False)
        e = s.explanation
        print(f"  explanation provider={e.provider} status={e.status}")
        for w in e.warnings:
            print(f"  warning: {w}")

      
        _h("6) AI invents a number -> rejected by guardrails")

        class Hallucinating:
            name, model = "fake_llm", "liar-1"

            def explain(self, ctx: DropContext) -> str:
                return (f"Drop at {ctx.to_step}: 73.4% of 12,345 sessions abandoned because "
                        f"the price rose by 19%.")

        s = run_pipeline(cfg, service=ExplanationService(Hallucinating()), write_report=False)
        print(f"  final provider={s.explanation.provider} status={s.explanation.status}")
        for w in s.explanation.warnings:
            print("  " + textwrap.shorten(w, 150))

       
        _h("7) Invalid configuration is rejected before touching any data")
        try:
            cfg.replace(inactivity_gap_seconds=0, funnel_steps=("view_product", "teleport"))
        except ConfigError as exc:
            print("  " + str(exc).replace("\n", "\n  "))

        _h("8) Changing the session gap (30 -> 15 min) triggers an automatic full refresh")
        s = run_pipeline(cfg.replace(inactivity_gap_seconds=15 * 60), service=offline, write_report=False)
        print(f"  mode={s.mode} reason='{s.full_refresh_reason}' - u_late now splits into 4 sessions:")
        for r in _sessions(cfg.warehouse_path):
            print(f"    {r[0]:<28} {r[1]} -> {r[2]}  events={r[3]}")
        print("\nDemo finished: every failure above was handled, explained and logged.")
    return 0
