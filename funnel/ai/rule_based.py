"""Deterministic, offline explainer.

It is the default when no model is configured and the safety net when a model
fails or its answer is rejected by the guardrails - so a report always has an
explanation and the pipeline never depends on a network call."""
from __future__ import annotations

from .base import DropContext

# Hypotheses keyed on words in the step that users fail to reach.
_HYPOTHESES: list[tuple[tuple[str, ...], list[str]]] = [
    (("payment", "pay"), [
        "the payment form fails or is hard to complete on this segment (validation errors, layout, keyboard issues)",
        "a preferred payment method is missing or a payment SDK/3-D Secure step is not loading",
    ]),
    (("checkout",), [
        "unexpected costs (shipping, taxes) appear when checkout starts",
        "forced sign-in or account creation interrupts the flow",
    ]),
    (("cart",), [
        "price or delivery information on the product page is not convincing enough",
        "the add-to-cart control is hard to find or slow to respond",
    ]),
    (("purchase", "order", "confirm"), [
        "the final submit fails (payment declines, timeouts) after details are entered",
        "order confirmation is slow, so users abandon or retry",
    ]),
]
_GENERIC = ["a UX or performance problem on this step", "a mismatch between user intent and what this step asks for"]


def _hypotheses(step: str) -> list[str]:
    s = step.lower()
    for keys, hyps in _HYPOTHESES:
        if any(k in s for k in keys):
            return hyps
    return _GENERIC


def _p(p: float) -> str:
    return "p < 0.001" if p < 0.001 else f"p = {p:.3f}"


class RuleBasedExplainer:
    name = "rule_based"
    model = "templates-v1"

    def explain(self, ctx: DropContext) -> str:
        lines = [
            "What happened:",
            f"The largest drop in the {ctx.funnel_name} funnel is {ctx.from_step} -> {ctx.to_step}: "
            f"{ctx.sessions_prev:,} sessions reached {ctx.from_step} but only {ctx.sessions_reached:,} reached "
            f"{ctx.to_step}, a {ctx.drop_off_rate * 100:.1f}% drop-off ({ctx.drop_off_sessions:,} sessions lost).",
        ]
        if ctx.anomaly_started_on and ctx.baseline_conversion is not None and ctx.anomaly_conversion is not None:
            lines.append(
                f"This is a new problem, not a normal leak: conversion into {ctx.to_step} was "
                f"{ctx.baseline_conversion * 100:.1f}% before {ctx.anomaly_started_on} and "
                f"{ctx.anomaly_conversion * 100:.1f}% since.")
        lines += [
            "",
            "Where it is concentrated:",
        ]
        significant = [s for s in ctx.segments if s.p_value < 0.05 and s.lost_sessions > 0]
        if significant:
            for s in significant[:3]:
                lines.append(
                    f"- {s.dimension} = {s.value}: {s.conversion * 100:.1f}% continue vs "
                    f"{s.rest_conversion * 100:.1f}% for other {s.dimension} values "
                    f"(about {s.lost_sessions:,} sessions lost, {_p(s.p_value)})."
                )
        else:
            lines.append("- No segment differs significantly from the rest; the drop looks product-wide.")

        lines += ["", "Likely causes and next checks (hypotheses - the data shows where, not why):"]
        top = significant[0] if significant else None
        release = next((s for s in significant if s.dimension == "app_version"), None)
        if release is not None:
            when = f" around {ctx.anomaly_started_on}" if ctx.anomaly_started_on else ""
            lines.append(f"- a regression shipped in app release {release.value}{when}: sessions on that version "
                         f"continue at {release.conversion * 100:.1f}% vs {release.rest_conversion * 100:.1f}% on others.")
        for h in _hypotheses(ctx.to_step):
            target = f" for {top.dimension} = {top.value}" if top else ""
            lines.append(f"- {h}{target}.")
        checks = (f"the release notes and crash/error logs of app version {release.value}" if release
                  else f"client error logs and recent releases touching {ctx.to_step}")
        if top and not release:
            checks += f" on {top.value}"
        lines.append(f"- Check {checks}, then watch session replays of users who stopped at {ctx.from_step}.")
        if ctx.data_quality_notes:
            lines += ["", "Data caveats: " + "; ".join(ctx.data_quality_notes)]
        return "\n".join(lines)
