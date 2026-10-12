"""Output guardrails: an explanation may only cite numbers that exist in the
context it was given. This catches the most damaging LLM failure in analytics -
a confident, plausible, made-up number - before it reaches a report."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .base import DropContext

MAX_CHARS = 2500
# 1,234 | 1234 | 12.5 | 12.5% | 0.0001
_NUMBER_RE = re.compile(r"(?<![\w.])(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?\s*(%|percent\b)?", re.IGNORECASE)
_STEP_NUMBER_RE = re.compile(r"(?i)\bstep\s+\d+\b")
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
SMALL_INT_FREE = 10  # list markers, "two segments", "step 3" ... are always fine


@dataclass
class GuardrailResult:
    ok: bool
    problems: list[str] = field(default_factory=list)
    unverified_numbers: list[str] = field(default_factory=list)


def _allowed_values(ctx: DropContext) -> tuple[list[float], list[float], list[float]]:
    """(counts, percentages, small decimals like p-values)."""
    counts: list[float] = [ctx.sessions_prev, ctx.sessions_reached, ctx.drop_off_sessions,
                           ctx.session_gap_minutes, ctx.step_index]
    pcts: list[float] = [ctx.drop_off_rate * 100, ctx.overall_conversion * 100,
                         (1 - ctx.drop_off_rate) * 100]
    decimals: list[float] = [0.05, 0.01, 0.001, 0.0001]
    if ctx.median_seconds_from_prev is not None:
        counts += [ctx.median_seconds_from_prev, ctx.median_seconds_from_prev / 60]
    for s in ctx.funnel:
        counts.append(s.sessions)
        if s.step_conversion is not None:
            pcts += [s.step_conversion * 100, 100 - s.step_conversion * 100]
    for g in ctx.segments:
        counts += [g.sessions_prev, g.sessions_reached, g.lost_sessions,
                   g.sessions_prev - g.sessions_reached]
        gap = (g.rest_conversion - g.conversion) * 100
        pcts += [g.conversion * 100, g.rest_conversion * 100, gap,
                 100 - g.conversion * 100, 100 - g.rest_conversion * 100]
        if g.conversion > 0:
            counts.append(round(g.rest_conversion / g.conversion, 1))  # "3.2x lower"
        decimals.append(g.p_value)
    for t in ctx.trend:
        counts.append(t.sessions_prev)
        if t.conversion is not None:
            pcts += [t.conversion * 100, 100 - t.conversion * 100]
    for v in (ctx.baseline_conversion, ctx.anomaly_conversion):
        if v is not None:
            pcts += [v * 100, 100 - v * 100]
    if ctx.baseline_conversion and ctx.anomaly_conversion is not None:
        pcts.append((1 - ctx.anomaly_conversion / ctx.baseline_conversion) * 100)
        pcts.append((ctx.baseline_conversion - ctx.anomaly_conversion) * 100)
    for note in ctx.data_quality_notes:  # numbers quoted in the caveats are context too
        for m in _NUMBER_RE.finditer(note):
            v = float(m.group(1).replace(",", "") + (m.group(2) or ""))
            (pcts if m.group(3) else counts).append(v)
    return counts, pcts, decimals


def _matches(value: float, candidates: list[float], *, abs_tol: float, rel_tol: float) -> bool:
    return any(abs(value - c) <= max(abs_tol, rel_tol * abs(c)) for c in candidates)


def validate(text: str, ctx: DropContext) -> GuardrailResult:
    problems: list[str] = []
    if not text or not text.strip():
        return GuardrailResult(False, ["empty response"])
    if len(text) > MAX_CHARS:
        problems.append(f"response too long ({len(text)} chars > {MAX_CHARS})")

    lowered = text.lower()
    step_variants = {ctx.to_step.lower(), ctx.to_step.lower().replace("_", " ")}
    if not any(v in lowered for v in step_variants):
        problems.append(f"does not mention the dropping step '{ctx.to_step}'")

    counts, pcts, decimals = _allowed_values(ctx)
    scrubbed = _STEP_NUMBER_RE.sub(" ", text)
    # dates and segment labels (e.g. app_version "2.4.0") are identifiers, not measurements:
    # a date is fine if it occurs in the context, otherwise it is an invented date.
    known_dates = {t.date for t in ctx.trend} | ({ctx.anomaly_started_on} if ctx.anomaly_started_on else set())
    for d in _DATE_RE.findall(scrubbed):
        if d not in known_dates:
            problems.append(f"cites a date not present in the data: {d}")
    scrubbed = _DATE_RE.sub(" ", scrubbed)
    for label in sorted({g.value for g in ctx.segments if any(ch.isdigit() for ch in g.value)}, key=len, reverse=True):
        scrubbed = scrubbed.replace(label, " ")
    unverified: list[str] = []
    for m in _NUMBER_RE.finditer(scrubbed):
        whole, frac, pct_sign = m.group(1), m.group(2) or "", m.group(3)
        value = float(whole.replace(",", "") + frac)
        token = m.group(0).strip()
        if pct_sign:
            ok = _matches(value, pcts, abs_tol=0.51, rel_tol=0)
        elif value < 1 and frac:
            ok = _matches(value, decimals, abs_tol=1e-4, rel_tol=0.05)
        elif value <= SMALL_INT_FREE and not frac:
            ok = True
        else:
            ok = (_matches(value, counts, abs_tol=0.51, rel_tol=0.01)
                  or _matches(value, pcts, abs_tol=0.51, rel_tol=0))
        if not ok:
            unverified.append(token)
    if unverified:
        problems.append(f"cites numbers not present in the data: {unverified}")
    return GuardrailResult(not problems, problems, unverified)
