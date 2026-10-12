"""Shared test helpers: an isolated workspace per test + tiny event builders."""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from funnel.ai import ExplanationService, RuleBasedExplainer
from funnel.config import DEFAULT_CONFIG, Config
from funnel.pipeline import RunSummary, run_pipeline

T0 = datetime(2026, 3, 2, 10, 0, 0, tzinfo=timezone.utc)


def ts(seconds: float = 0) -> str:
    """ISO timestamp T0 + seconds."""
    return (T0 + timedelta(seconds=seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ev(event_id: str, name: str, at: float | str | int, user: str = "u1", **kw: Any) -> dict:
    return {
        "event_id": event_id, "user_id": user, "event_name": name,
        "ts": ts(at) if isinstance(at, (int, float)) and not isinstance(at, bool) and abs(at) < 1e8 else at,
        "device": kw.pop("device", "desktop"), "country": kw.pop("country", "IN"),
        "channel": kw.pop("channel", "organic"),
        "properties": kw.pop("properties", {"product_id": "p1", "price": 100}),
        **kw,
    }


class PipelineTestCase(unittest.TestCase):
    """Each test gets its own folder, warehouse and offline explainer."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="funnel_test_"))
        self.incoming = self.tmp / "incoming"
        self.incoming.mkdir()
        self.cfg = Config.load(DEFAULT_CONFIG, incoming_dir=self.incoming, warehouse_path=self.tmp / "wh.db",
                               output_dir=self.tmp / "out", min_sample_size=1)
        self.service = ExplanationService(RuleBasedExplainer())
        self._n = 0

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- input
    def write(self, events: list[dict | str], name: str | None = None) -> Path:
        self._n += 1
        path = self.incoming / (name or f"batch_{self._n:03d}.jsonl")
        path.write_text("\n".join(e if isinstance(e, str) else json.dumps(e) for e in events) + "\n", encoding="utf-8")
        return path

    def run_pipeline(self, cfg: Config | None = None, **kw: Any) -> RunSummary:
        kw.setdefault("service", self.service)
        kw.setdefault("write_report", False)
        kw.setdefault("notifiers", [])
        return run_pipeline(cfg or self.cfg, **kw)

    # -- output
    def q(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        con = sqlite3.connect(self.cfg.warehouse_path)
        con.row_factory = sqlite3.Row
        try:
            return con.execute(sql, params).fetchall()
        finally:
            con.close()

    def one(self, sql: str, params: tuple = ()) -> Any:
        return self.q(sql, params)[0][0]
