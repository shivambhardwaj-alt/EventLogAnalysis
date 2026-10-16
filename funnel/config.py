"""Typed, validated pipeline configuration (TOML file + explicit overrides)."""
from __future__ import annotations

import dataclasses
import hashlib
import json
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.toml"
SEGMENT_COLUMNS = frozenset({"device", "country", "channel", "app_version"})


class ConfigError(ValueError):
    """Invalid configuration - raised before any data is touched."""


@dataclass(frozen=True)
class Config:
    incoming_dir: Path
    warehouse_path: Path
    output_dir: Path
    inactivity_gap_seconds: float = 1800
    near_duplicate_window_seconds: float = 2.0
    min_event_date: str = "2020-01-01"
    future_tolerance_seconds: float = 600
    allowed_events: tuple[str, ...] = ()
    funnel_name: str = "checkout"
    funnel_steps: tuple[str, ...] = ()
    min_sample_size: int = 30
    segment_dimensions: tuple[str, ...] = ("device", "channel", "country", "app_version")
    contract_fields: tuple[str, ...] = ("event_id", "user_id", "event_name", "ts", "device", "country",
                                        "channel", "app_version", "properties")
    contract_property_fields: tuple[str, ...] = ("product_id", "price")
    anomaly_min_daily_sessions: int = 50
    anomaly_max_p_value: float = 0.001
    anomaly_min_relative_decline: float = 0.15
    alert_max_quarantine_rate: float = 0.05
    alert_max_conflicting_duplicates: int = 100
    lock_timeout_seconds: float = 7200
    extra: dict[str, Any] = field(default_factory=dict, compare=False)

    
    @classmethod
    def load(cls, path: str | Path | None = None, root: Path | None = None, **overrides: Any) -> "Config":
        path = Path(path) if path else DEFAULT_CONFIG
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"Config file {path} is not valid TOML: {exc}") from exc
        root = root or PROJECT_ROOT

        def section(name: str) -> dict[str, Any]:
            return raw.get(name, {})

        paths, sess, dedup = section("paths"), section("sessionization"), section("dedup")
        val, fun = section("validation"), section("funnel")
        con, ano, ale, rt = section("contract"), section("anomaly"), section("alerts"), section("runtime")
        cfg = cls(
            incoming_dir=root / paths.get("incoming_dir", "data/incoming"),
            warehouse_path=root / paths.get("warehouse", "output/warehouse.db"),
            output_dir=root / paths.get("output_dir", "output"),
            inactivity_gap_seconds=float(sess.get("inactivity_gap_minutes", 30)) * 60,
            near_duplicate_window_seconds=float(dedup.get("near_duplicate_window_seconds", 2.0)),
            min_event_date=str(val.get("min_event_date", "2020-01-01")),
            future_tolerance_seconds=float(val.get("future_tolerance_minutes", 10)) * 60,
            allowed_events=tuple(val.get("allowed_events", ())),
            funnel_name=str(fun.get("name", "funnel")),
            funnel_steps=tuple(fun.get("steps", ())),
            min_sample_size=int(fun.get("min_sample_size", 30)),
            segment_dimensions=tuple(fun.get("segment_dimensions", ("device", "channel", "country", "app_version"))),
            contract_fields=tuple(con.get("fields", cls.contract_fields)),
            contract_property_fields=tuple(con.get("property_fields", cls.contract_property_fields)),
            anomaly_min_daily_sessions=int(ano.get("min_daily_sessions", 50)),
            anomaly_max_p_value=float(ano.get("max_p_value", 0.001)),
            anomaly_min_relative_decline=float(ano.get("min_relative_decline", 0.15)),
            alert_max_quarantine_rate=float(ale.get("max_quarantine_rate", 0.05)),
            alert_max_conflicting_duplicates=int(ale.get("max_conflicting_duplicates", 100)),
            lock_timeout_seconds=float(rt.get("lock_timeout_minutes", 120)) * 60,
        )
        return cfg.replace(**overrides) if overrides else cfg.validated()

    def replace(self, **changes: Any) -> "Config":
        return dataclasses.replace(self, **changes).validated()

    
    def validated(self) -> "Config":
        errors: list[str] = []
        if self.inactivity_gap_seconds <= 0:
            errors.append("inactivity gap must be > 0 minutes")
        if self.near_duplicate_window_seconds < 0:
            errors.append("near-duplicate window must be >= 0 seconds")
        if self.near_duplicate_window_seconds >= self.inactivity_gap_seconds:
            errors.append("near-duplicate window must be smaller than the inactivity gap")
        if self.future_tolerance_seconds < 0:
            errors.append("future tolerance must be >= 0")
        try:
            datetime.strptime(self.min_event_date, "%Y-%m-%d")
        except ValueError:
            errors.append(f"min_event_date {self.min_event_date!r} is not YYYY-MM-DD")
        if not self.allowed_events:
            errors.append("allowed_events must not be empty")
        bad_names = [e for e in self.allowed_events if e != e.strip().lower()]
        if bad_names:
            errors.append(f"allowed_events must be lower-case: {bad_names}")
        if len(self.funnel_steps) < 2:
            errors.append("a funnel needs at least 2 steps")
        if len(set(self.funnel_steps)) != len(self.funnel_steps):
            errors.append("funnel steps must be unique")
        unknown = [s for s in self.funnel_steps if s not in self.allowed_events]
        if unknown:
            errors.append(f"funnel steps not in allowed_events: {unknown}")
        bad_dims = [d for d in self.segment_dimensions if d not in SEGMENT_COLUMNS]
        if bad_dims:
            errors.append(f"segment_dimensions must be from {sorted(SEGMENT_COLUMNS)}: {bad_dims}")
        missing_core = {"event_id", "user_id", "event_name", "ts"} - set(self.contract_fields)
        if missing_core:
            errors.append(f"contract.fields must include {sorted(missing_core)}")
        if not 0 < self.anomaly_max_p_value < 1:
            errors.append("anomaly.max_p_value must be between 0 and 1")
        if not 0 <= self.anomaly_min_relative_decline < 1:
            errors.append("anomaly.min_relative_decline must be in [0, 1)")
        if not 0 < self.alert_max_quarantine_rate <= 1:
            errors.append("alerts.max_quarantine_rate must be in (0, 1]")
        if self.min_sample_size < 1:
            errors.append("min_sample_size must be >= 1")
        if errors:
            raise ConfigError("Invalid configuration:\n  - " + "\n  - ".join(errors))
        return self

  
    @property
    def min_epoch(self) -> float:
        dt = datetime.strptime(self.min_event_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt.timestamp()

    @property
    def gap_minutes(self) -> float:
        return self.inactivity_gap_seconds / 60

    def processing_fingerprint(self) -> str:
        """Hash of every setting that changes how stored data is derived.
        If it changes between runs, derived tables are rebuilt from raw."""
        relevant = {
            "gap": self.inactivity_gap_seconds,
            "near_dup": self.near_duplicate_window_seconds,
            "min_date": self.min_event_date,
            "future_tol": self.future_tolerance_seconds,
            "allowed": sorted(self.allowed_events),
            "steps": list(self.funnel_steps),
            "dims": list(self.segment_dimensions),
        }
        return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()[:16]
