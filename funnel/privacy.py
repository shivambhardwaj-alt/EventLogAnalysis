"""Right to be forgotten (GDPR art. 17 / India DPDP Act sec. 12).

`erase_user` removes every trace of a user from every layer - raw payloads
included - recomputes the aggregates, and stores only a SHA-256 of the id so
that the user is also dropped if their events show up again in a late file.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from .config import Config
from .db import apply_schema, connect, transaction, user_hash
from .lock import RunLock

log = logging.getLogger(__name__)


@dataclass
class ErasureResult:
    user_hash: str
    raw_rows: int
    clean_rows: int
    sessions: int
    already_erased: bool


def erase_user(cfg: Config, user_id: str) -> ErasureResult:
    from .metrics import detect_anomalies
    from .pipeline import TaskTracker, rebuild_aggregates  # local import: avoid cycle

    uid = user_id.strip()
    if not uid:
        raise ValueError("user_id must not be empty")
    h = user_hash(uid)
    with RunLock(cfg.warehouse_path.with_suffix(".lock"), cfg.lock_timeout_seconds, owner="erase"):
        con = connect(cfg.warehouse_path)
        try:
            apply_schema(con)
            already = con.execute("SELECT 1 FROM erased_users WHERE user_hash = ?", (h,)).fetchone() is not None
            with transaction(con):
                con.execute("CREATE TEMP TABLE IF NOT EXISTS _erase (raw_id INTEGER PRIMARY KEY)")
                con.execute("DELETE FROM _erase")
                # raw payloads (including quarantined / duplicate rows, which never reached clean_events)
                con.execute("""
                    INSERT OR IGNORE INTO _erase
                    SELECT raw_id FROM raw_events
                    WHERE TRIM(CAST(CASE WHEN json_valid(raw_payload)
                                         THEN json_extract(raw_payload, '$.user_id') END AS TEXT)) = ?""", (uid,))
                con.execute("INSERT OR IGNORE INTO _erase SELECT raw_id FROM stg_events WHERE user_id = ?", (uid,))
                raw_n = con.execute("SELECT COUNT(*) FROM _erase").fetchone()[0]
                clean_n = con.execute("SELECT COUNT(*) FROM clean_events WHERE user_id = ?", (uid,)).fetchone()[0]
                sess_n = con.execute("SELECT COUNT(*) FROM sessions WHERE user_id = ?", (uid,)).fetchone()[0]
                for table in ("clean_events", "duplicate_events", "quarantine_events", "stg_events", "raw_events"):
                    con.execute(f"DELETE FROM {table} WHERE raw_id IN (SELECT raw_id FROM _erase)")
                con.execute("DELETE FROM funnel_session_steps WHERE user_id = ?", (uid,))
                con.execute("DELETE FROM sessions WHERE user_id = ?", (uid,))
                con.execute("DELETE FROM ai_explanations")  # contexts were computed with this user's data
                rebuild_aggregates(con, cfg, TaskTracker())
                detect_anomalies(con, cfg)
                con.execute(
                    "INSERT INTO erased_users (user_hash, erased_at, rows_removed) VALUES (?, ?, ?) "
                    "ON CONFLICT(user_hash) DO UPDATE SET rows_removed = rows_removed + excluded.rows_removed",
                    (h, datetime.now(timezone.utc).isoformat(timespec="seconds"), raw_n))
            log.info("erased user hash=%s raw_rows=%d sessions=%d", h[:12], raw_n, sess_n)
            return ErasureResult(h, raw_n, clean_n, sess_n, already)
        finally:
            con.close()
