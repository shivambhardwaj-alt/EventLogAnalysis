
from __future__ import annotations

import sqlite3
from typing import Any

from .config import Config
from .db import read_sql, scalar, split_statements, sql_blocks

def run_checks(con: sqlite3.Connection, cfg: Config) -> list[dict[str, Any]]:
    params = {"gap_seconds": cfg.inactivity_gap_seconds, "min_epoch": cfg.min_epoch}
    results = []
    for name, sql in sql_blocks("08_quality_checks.sql").items():
        used = {k: v for k, v in params.items() if f":{k}" in sql}
        failures = int(scalar(con, sql, used) or 0)
        results.append({"check": name, "failures": failures, "passed": failures == 0})
    return results


def profile(con: sqlite3.Connection) -> dict[str, int]:
    sql = read_sql("09_quality_profile.sql")
    row = con.execute(sql).fetchone()
    return {k: int(row[k] or 0) for k in row.keys()}


def report(con: sqlite3.Connection, cfg: Config) -> dict[str, Any]:
    totals = dict(con.execute("""
        SELECT
            (SELECT COUNT(*) FROM ingest_files)                                  AS files,
            (SELECT COUNT(*) FROM ingest_files WHERE line_count = 0)             AS empty_files,
            (SELECT COALESCE(SUM(blank_lines), 0) FROM ingest_files)             AS blank_lines_skipped,
            (SELECT COUNT(*) FROM raw_events)                                    AS raw_rows,
            (SELECT COUNT(*) FROM clean_events)                                  AS accepted_rows,
            (SELECT COUNT(*) FROM quarantine_events)                             AS quarantined_rows,
            (SELECT COUNT(*) FROM duplicate_events WHERE kind = 'exact')         AS exact_duplicates,
            (SELECT COUNT(*) FROM duplicate_events WHERE kind = 'conflicting')   AS conflicting_duplicates,
            (SELECT COUNT(*) FROM clean_events WHERE is_near_duplicate = 1)      AS near_duplicates,
            (SELECT COUNT(*) FROM clean_events WHERE is_near_duplicate = 0)      AS analysed_events
    """).fetchone())
    raw = totals["raw_rows"] or 0
    totals["quarantine_rate"] = round(totals["quarantined_rows"] / raw, 4) if raw else 0.0
    totals["duplicate_rate"] = round((totals["exact_duplicates"] + totals["conflicting_duplicates"]) / raw, 4) if raw else 0.0

    reasons = [dict(r) for r in con.execute(
        "SELECT reason, COUNT(*) AS rows FROM quarantine_events GROUP BY reason ORDER BY rows DESC, reason")]
    samples = {}
    for r in reasons:
        row = con.execute("SELECT raw_payload FROM quarantine_events WHERE reason = ? ORDER BY raw_id LIMIT 1",
                          (r["reason"],)).fetchone()
        samples[r["reason"]] = row["raw_payload"][:160]
    files = [dict(r) for r in con.execute(
        "SELECT file_name, file_format, line_count, blank_lines, run_id, ingested_at FROM ingest_files ORDER BY ingested_at, file_name")]
   
    late = scalar(con, """
        WITH per_file AS (
            SELECT r.file_hash, MIN(r.raw_id) AS first_raw, MAX(c.ts_epoch) AS max_ts
            FROM raw_events r JOIN clean_events c ON c.raw_id = r.raw_id
            GROUP BY r.file_hash
        ),
        watermark AS (
            SELECT file_hash,
                   MAX(max_ts) OVER (ORDER BY first_raw ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
            FROM per_file
        )
        SELECT COUNT(*) FROM clean_events c
        JOIN raw_events r ON r.raw_id = c.raw_id
        JOIN watermark w ON w.file_hash = r.file_hash
        WHERE c.ts_epoch < w.prev_max""")
    checks = run_checks(con, cfg)
    return {
        "totals": totals,
        "quarantine_by_reason": reasons,
        "quarantine_samples": samples,
        "type_coercions": profile(con),
        "late_arriving_events": int(late or 0),
        "files": files,
        "checks": checks,
        "all_checks_passed": all(c["passed"] for c in checks),
    }


def notes_for_ai(q: dict[str, Any]) -> tuple[str, ...]:
    t = q["totals"]
    notes = [
        f"{t['quarantined_rows']:,} of {t['raw_rows']:,} raw rows were quarantined as invalid",
        f"{t['exact_duplicates'] + t['conflicting_duplicates']:,} duplicate deliveries and "
        f"{t['near_duplicates']:,} double-clicks were excluded",
    ]
    if not q["all_checks_passed"]:
        notes.append("some data-quality checks failed; treat numbers with caution")
    return tuple(notes)
