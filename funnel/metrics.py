"""Read gold tables and find the largest funnel drop (+ where it is concentrated)."""
from __future__ import annotations

import math
import sqlite3
from typing import Any

from .ai.base import DailyPoint, DropContext, FunnelStepSummary, SegmentFinding
from .config import Config


def funnel_rows(con: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(r) for r in con.execute("SELECT * FROM funnel_metrics ORDER BY step_index")]


def overview(con: sqlite3.Connection) -> dict[str, Any]:
    row = con.execute("""
        SELECT
            (SELECT COUNT(DISTINCT user_id) FROM sessions)                         AS users,
            (SELECT COUNT(*) FROM sessions)                                        AS sessions,
            (SELECT COUNT(*) FROM clean_events WHERE is_near_duplicate = 0)        AS analysed_events,
            (SELECT ROUND(AVG(duration_seconds) / 60.0, 2) FROM sessions)          AS avg_session_minutes,
            (SELECT ROUND(AVG(event_count), 2) FROM sessions)                      AS avg_events_per_session,
            (SELECT ROUND(AVG(event_count = 1), 4) FROM sessions)                  AS bounce_rate,
            (SELECT MIN(session_start) FROM sessions)                              AS first_event,
            (SELECT MAX(session_end) FROM sessions)                                AS last_event
    """).fetchone()
    data = dict(row)
    steps = funnel_rows(con)
    data["funnel_entries"] = steps[0]["sessions_reached"] if steps else 0
    data["funnel_completions"] = steps[-1]["sessions_reached"] if steps else 0
    data["funnel_conversion"] = steps[-1]["overall_conversion"] if steps else None
    return data


def two_proportion_p_value(x1: int, n1: int, x2: int, n2: int) -> float:
    """Two-sided p-value of H0: p1 == p2 (normal approximation)."""
    if min(n1, n2) == 0:
        return 1.0
    p = (x1 + x2) / (n1 + n2)
    se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    if se == 0:
        return 1.0
    z = (x1 / n1 - x2 / n2) / se
    return math.erfc(abs(z) / math.sqrt(2))


def segment_findings(con: sqlite3.Connection, cfg: Config, step_index: int,
                     total_prev: int, total_reached: int, limit: int = 5) -> list[SegmentFinding]:
    min_n = max(10, cfg.min_sample_size // 3)
    findings: list[SegmentFinding] = []
    rows = con.execute(
        "SELECT dimension, segment_value, sessions_reached, prev_sessions FROM funnel_segments "
        "WHERE step_index = ? AND prev_sessions >= ? ORDER BY dimension, segment_value",
        (step_index, min_n),
    ).fetchall()
    for r in rows:
        n1, x1 = r["prev_sessions"], r["sessions_reached"]
        # "rest" = same dimension, all other values
        dim_tot = con.execute(
            "SELECT SUM(prev_sessions), SUM(sessions_reached) FROM funnel_segments WHERE dimension = ? AND step_index = ?",
            (r["dimension"], step_index)).fetchone()
        n2, x2 = dim_tot[0] - n1, dim_tot[1] - x1
        if n2 < min_n:
            continue
        conv, rest = x1 / n1, x2 / n2
        lost = round(n1 * (rest - conv))
        findings.append(SegmentFinding(
            dimension=r["dimension"], value=str(r["segment_value"]), sessions_prev=n1, sessions_reached=x1,
            conversion=round(conv, 6), rest_conversion=round(rest, 6), lost_sessions=lost,
            p_value=two_proportion_p_value(x1, n1, x2, n2),
        ))
    findings.sort(key=lambda f: (-f.lost_sessions, f.p_value, f.dimension, f.value))
    return findings[:limit]


def detect_anomalies(con: sqlite3.Connection, cfg: Config) -> list[dict[str, Any]]:
    """Flag days whose step conversion is significantly AND materially below
    the healthy baseline (all earlier non-anomalous days pooled).

    Anomalous days are kept out of the baseline, so a regression that lasts
    several days is flagged on every affected day instead of slowly becoming
    the "new normal". Results are written to funnel_anomalies (rebuilt)."""
    rows = con.execute(
        "SELECT session_date, step_index, step_name, sessions_reached, prev_sessions, step_conversion "
        "FROM daily_funnel WHERE step_index > 1 ORDER BY step_index, session_date").fetchall()
    found: list[dict[str, Any]] = []
    by_step: dict[int, list[sqlite3.Row]] = {}
    for r in rows:
        by_step.setdefault(r["step_index"], []).append(r)
    for step_index, days in by_step.items():
        base_x = base_n = base_days = 0
        for d in days:
            n, x = d["prev_sessions"] or 0, d["sessions_reached"] or 0
            if n < cfg.anomaly_min_daily_sessions:
                continue
            flagged = False
            if base_days >= 2 and base_n >= cfg.anomaly_min_daily_sessions:
                conv, base = x / n, base_x / base_n
                p = two_proportion_p_value(x, n, base_x, base_n)
                rel = conv / base - 1 if base > 0 else 0.0
                if rel <= -cfg.anomaly_min_relative_decline and p <= cfg.anomaly_max_p_value:
                    flagged = True
                    found.append({
                        "session_date": d["session_date"], "step_index": step_index, "step_name": d["step_name"],
                        "day_conversion": round(conv, 6), "baseline_conversion": round(base, 6),
                        "relative_change": round(rel, 6), "day_sessions": n, "baseline_sessions": base_n,
                        "p_value": p,
                    })
            if not flagged:
                base_x, base_n, base_days = base_x + x, base_n + n, base_days + 1
    con.execute("DELETE FROM funnel_anomalies")
    con.executemany(
        "INSERT INTO funnel_anomalies VALUES (:session_date, :step_index, :step_name, :day_conversion, "
        ":baseline_conversion, :relative_change, :day_sessions, :baseline_sessions, :p_value)", found)
    return found


def daily_trend(con: sqlite3.Connection, step_index: int) -> list[DailyPoint]:
    return [DailyPoint(r["session_date"], r["prev_sessions"] or 0, r["step_conversion"])
            for r in con.execute("SELECT session_date, prev_sessions, step_conversion FROM daily_funnel "
                                 "WHERE step_index = ? ORDER BY session_date", (step_index,))]


def find_largest_drop(con: sqlite3.Connection, cfg: Config,
                      quality_notes: tuple[str, ...] = (),
                      anomalies: list[dict[str, Any]] | None = None) -> DropContext | None:
    steps = funnel_rows(con)
    candidates = [s for s in steps if s["step_index"] > 1 and (s["prev_sessions"] or 0) >= cfg.min_sample_size]
    if not candidates:
        return None
    # largest relative drop; ties -> larger absolute loss, then earlier step
    worst = max(candidates, key=lambda s: (s["drop_off_rate"], s["drop_off_sessions"], -s["step_index"]))
    prev = next(s for s in steps if s["step_index"] == worst["step_index"] - 1)
    step_anoms = sorted((a for a in (anomalies or []) if a["step_index"] == worst["step_index"]),
                        key=lambda a: a["session_date"])
    started = step_anoms[0]["session_date"] if step_anoms else None
    since = None
    if started:
        row = con.execute("SELECT SUM(sessions_reached), SUM(prev_sessions) FROM daily_funnel "
                          "WHERE step_index = ? AND session_date >= ?", (worst["step_index"], started)).fetchone()
        since = round(row[0] / row[1], 6) if row[1] else None
    return DropContext(
        funnel_name=cfg.funnel_name,
        from_step=prev["step_name"],
        to_step=worst["step_name"],
        step_index=worst["step_index"],
        sessions_prev=worst["prev_sessions"],
        sessions_reached=worst["sessions_reached"],
        drop_off_sessions=worst["drop_off_sessions"],
        drop_off_rate=worst["drop_off_rate"],
        overall_conversion=steps[-1]["overall_conversion"] or 0.0,
        median_seconds_from_prev=worst["median_seconds_from_prev"],
        session_gap_minutes=cfg.gap_minutes,
        funnel=tuple(FunnelStepSummary(s["step_name"], s["sessions_reached"], s["step_conversion"]) for s in steps),
        segments=tuple(segment_findings(con, cfg, worst["step_index"], worst["prev_sessions"], worst["sessions_reached"])),
        data_quality_notes=quality_notes,
        trend=tuple(daily_trend(con, worst["step_index"])),
        anomaly_started_on=started,
        baseline_conversion=step_anoms[0]["baseline_conversion"] if step_anoms else None,
        anomaly_conversion=since,
    )
