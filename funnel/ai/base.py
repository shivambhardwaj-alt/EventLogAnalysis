"""The AI boundary.

Everything the rest of the pipeline knows about AI is in this file:
  * DropContext  - the ONLY data a model ever sees (aggregates, no raw events,
                   no user ids => no PII leaves the machine)
  * Explainer    - the interface every provider implements
  * Explanation  - what comes back, including provenance and guardrail result
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Protocol, runtime_checkable


class ExplainerError(RuntimeError):
    """A provider failed (network, auth, timeout, bad response)."""


@dataclass(frozen=True)
class SegmentFinding:
    dimension: str
    value: str
    sessions_prev: int
    sessions_reached: int
    conversion: float          # this segment's step conversion (0-1)
    rest_conversion: float     # conversion of every other segment of the same dimension
    lost_sessions: int         # sessions lost vs. performing like the rest
    p_value: float             # two-proportion z-test, segment vs. rest


@dataclass(frozen=True)
class FunnelStepSummary:
    step: str
    sessions: int
    step_conversion: float | None


@dataclass(frozen=True)
class DailyPoint:
    date: str
    sessions_prev: int
    conversion: float | None


@dataclass(frozen=True)
class DropContext:
    funnel_name: str
    from_step: str
    to_step: str
    step_index: int
    sessions_prev: int
    sessions_reached: int
    drop_off_sessions: int
    drop_off_rate: float
    overall_conversion: float
    median_seconds_from_prev: float | None
    session_gap_minutes: float
    funnel: tuple[FunnelStepSummary, ...]
    segments: tuple[SegmentFinding, ...]
    data_quality_notes: tuple[str, ...] = ()
    trend: tuple[DailyPoint, ...] = ()            # daily conversion into to_step
    anomaly_started_on: str | None = None         # first day flagged by anomaly detection
    baseline_conversion: float | None = None      # conversion before the anomaly
    anomaly_conversion: float | None = None       # conversion since the anomaly started

    def to_dict(self) -> dict:
        return asdict(self)

    def to_prompt_json(self) -> str:
        """Compact, rounded view for the model (percentages, not fractions)."""
        def pct(x: float | None) -> float | None:
            return None if x is None else round(x * 100, 1)

        payload = {
            "funnel": self.funnel_name,
            "largest_drop": {
                "from_step": self.from_step,
                "to_step": self.to_step,
                "sessions_at_from_step": self.sessions_prev,
                "sessions_reaching_to_step": self.sessions_reached,
                "sessions_lost": self.drop_off_sessions,
                "drop_off_pct": pct(self.drop_off_rate),
                "median_seconds_between_steps": self.median_seconds_from_prev,
            },
            "full_funnel": [
                {"step": s.step, "sessions": s.sessions, "step_conversion_pct": pct(s.step_conversion)}
                for s in self.funnel
            ],
            "overall_conversion_pct": pct(self.overall_conversion),
            "segments_ranked_by_impact": [
                {
                    "dimension": s.dimension,
                    "value": s.value,
                    "sessions_at_from_step": s.sessions_prev,
                    "conversion_pct": pct(s.conversion),
                    "conversion_pct_all_other_values": pct(s.rest_conversion),
                    "gap_pct_points": round((s.rest_conversion - s.conversion) * 100, 1),
                    "sessions_lost_vs_others": s.lost_sessions,
                    "p_value": round(s.p_value, 4),
                }
                for s in self.segments
            ],
            "daily_trend_into_to_step": [
                {"date": t.date, "sessions_at_from_step": t.sessions_prev, "conversion_pct": pct(t.conversion)}
                for t in self.trend
            ],
            "anomaly": None if not self.anomaly_started_on else {
                "started_on": self.anomaly_started_on,
                "conversion_pct_before": pct(self.baseline_conversion),
                "conversion_pct_since": pct(self.anomaly_conversion),
            },
            "session_definition": f"new session after more than {self.session_gap_minutes:g} minutes of inactivity",
            "data_quality_notes": list(self.data_quality_notes),
        }
        return json.dumps(payload, indent=2)

    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, default=str).encode()).hexdigest()[:24]


@dataclass
class Explanation:
    text: str
    provider: str
    model: str
    status: str                      # ok | fallback | cached
    warnings: list[str] = field(default_factory=list)

    @property
    def is_ai_generated(self) -> bool:
        return self.provider != "rule_based"


@runtime_checkable
class Explainer(Protocol):
    """Implement this to plug in any model. Must raise ExplainerError on failure."""

    name: str
    model: str

    def explain(self, ctx: DropContext) -> str: ...


SYSTEM_PROMPT = """You are a senior product analyst. You explain funnel drop-offs to a product team.
Rules:
- Use ONLY numbers that appear in the JSON you are given. Never invent numbers, dates, prices or percentages.
- Round percentages to one decimal, exactly as given.
- Clearly label causes as hypotheses; the data shows WHERE users drop, not WHY.
- Be concise: at most 180 words, plain text, no markdown tables."""

USER_PROMPT_TEMPLATE = """Here is the funnel analysis as JSON:

{context_json}

Write three short sections with these exact headings:
What happened: (one or two sentences naming the step "{from_step}" -> "{to_step}" and its drop-off; if an anomaly start date is given, say when it began)
Where it is concentrated: (the segments with the biggest impact, with their numbers)
Likely causes and next checks: (2-3 hypotheses tied to the data, then the concrete checks to confirm them)"""


def build_messages(ctx: DropContext) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT_TEMPLATE.format(
            context_json=ctx.to_prompt_json(), from_step=ctx.from_step, to_step=ctx.to_step)},
    ]
