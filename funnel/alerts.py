
from __future__ import annotations

import json
import logging
import os
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol

from .config import Config

log = logging.getLogger(__name__)
SEVERITY_ICON = {"critical": "[CRITICAL]", "warning": "[WARNING]", "info": "[INFO]"}


@dataclass(frozen=True)
class Alert:
    key: str
    severity: str
    kind: str
    message: str


class Notifier(Protocol):
    name: str

    def send(self, alert: Alert) -> None: ...


class LogNotifier:
    name = "log"

    def send(self, alert: Alert) -> None:
        level = logging.ERROR if alert.severity == "critical" else logging.WARNING
        log.log(level, "ALERT %s %s: %s", SEVERITY_ICON[alert.severity], alert.kind, alert.message)


class WebhookNotifier:
    name = "webhook"

    def __init__(self, url: str, timeout: float = 10) -> None:
        self._url = url
        self.timeout = timeout

    def __repr__(self) -> str: 
        return "WebhookNotifier(url=<hidden>)"

    def send(self, alert: Alert) -> None:
        text = f"{SEVERITY_ICON[alert.severity]} funnel pipeline - {alert.kind}\n{alert.message}"
        body = json.dumps({"text": text, "content": text}).encode()
        req = urllib.request.Request(self._url, data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()


def default_notifiers(env: dict[str, str] | None = None) -> list[Notifier]:
    env = dict(os.environ) if env is None else env
    notifiers: list[Notifier] = [LogNotifier()]
    if env.get("ALERT_WEBHOOK_URL"):
        notifiers.append(WebhookNotifier(env["ALERT_WEBHOOK_URL"]))
    return notifiers


def evaluate(cfg: Config, run_id: str, summary: Any, anomalies: list[dict],
             drift: list[dict]) -> list[Alert]:
    """Rules -> alerts. Pure function, easy to test."""
    out: list[Alert] = []
    for c in summary.quality.get("checks", []):
        if not c["passed"]:
            out.append(Alert(f"check:{c['check']}:{run_id}", "critical", "data_quality_check_failed",
                             f"Invariant '{c['check']}' has {c['failures']} violating rows."))
    if summary.rows_processed:
        rate = summary.rows_quarantined / summary.rows_processed
        if rate > cfg.alert_max_quarantine_rate:
            out.append(Alert(f"quarantine_rate:{run_id}", "warning", "high_quarantine_rate",
                             f"{rate:.1%} of {summary.rows_processed:,} new rows were rejected "
                             f"(threshold {cfg.alert_max_quarantine_rate:.0%})."))
    if summary.conflicting_duplicates > cfg.alert_max_conflicting_duplicates:
        out.append(Alert(f"conflicting:{run_id}", "warning", "conflicting_duplicates",
                         f"{summary.conflicting_duplicates:,} events reused an event_id with different content."))
    for inc in incidents(anomalies):
        out.append(Alert(
            f"anomaly:{inc['step_name']}:{inc['started_on']}", "critical", "funnel_conversion_drop",
            f"Conversion into '{inc['step_name']}' fell from {inc['baseline_conversion']:.1%} to "
            f"{inc['latest_conversion']:.1%} (on {inc['latest_date']}); it started {inc['started_on']} "
            f"({inc['days']} day(s) affected, p={inc['min_p_value']:.1e})."))
    for d in drift:
        out.append(Alert(f"drift:{d['field']}", "warning", "schema_drift",
                         f"Undeclared field '{d['field']}' appeared in {d['first_seen_file']} "
                         f"({d['rows']:,} rows, e.g. {d['example_value']!r}). Update the contract or the producer."))
    if summary.explanation is not None and summary.explanation.status == "fallback":
        out.append(Alert(f"ai_fallback:{run_id}", "info", "ai_fallback",
                         "AI provider unavailable or answer rejected; rule-based explanation used. "
                         + "; ".join(summary.explanation.warnings)[:300]))
    return out


def incidents(anomalies: Iterable[dict]) -> list[dict]:
    """Group daily anomalies per step into one incident (first day = start)."""
    by_step: dict[str, list[dict]] = {}
    for a in sorted(anomalies, key=lambda a: (a["step_index"], a["session_date"])):
        by_step.setdefault(a["step_name"], []).append(a)
    out = []
    for step, rows in by_step.items():
        out.append({
            "step_name": step,
            "step_index": rows[0]["step_index"],
            "started_on": rows[0]["session_date"],
            "days": len(rows),
            "baseline_conversion": rows[0]["baseline_conversion"],
            "latest_conversion": rows[-1]["day_conversion"],
            "latest_date": rows[-1]["session_date"],
            "min_p_value": min(r["p_value"] for r in rows),
        })
    return out


def record_and_deliver(con: sqlite3.Connection, run_id: str, alerts: list[Alert],
                       notifiers: list[Notifier] | None = None) -> list[Alert]:
    """Insert new alerts and deliver them. Returns only the NEW alerts."""
    notifiers = default_notifiers() if notifiers is None else notifiers
    new: list[Alert] = []
    for a in alerts:
        if con.execute("SELECT 1 FROM alerts WHERE alert_key = ?", (a.key,)).fetchone():
            continue
        delivered = []
        for n in notifiers:
            try:
                n.send(a)
                delivered.append(n.name)
            except (urllib.error.URLError, OSError, ValueError) as exc:
                log.warning("alert delivery via %s failed: %s", n.name, getattr(exc, "reason", exc))
        con.execute("INSERT INTO alerts (alert_key, run_id, severity, kind, message, created_at, delivered) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (a.key, run_id, a.severity, a.kind, a.message,
                     datetime.now(timezone.utc).isoformat(timespec="seconds"), ",".join(delivered) or "none"))
        new.append(a)
    return new
