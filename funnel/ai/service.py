"""ExplanationService: primary provider -> guardrails -> fallback.

The pipeline only ever calls `ExplanationService.explain(ctx)`. Whatever goes
wrong inside (no key, timeout, 500, hallucinated numbers) the result is a valid,
clearly-labelled Explanation and the pipeline carries on."""
from __future__ import annotations

import logging
import os
from typing import Mapping

from . import guardrails
from .base import DropContext, Explainer, ExplainerError, Explanation
from .providers import OllamaExplainer, OpenAICompatibleExplainer
from .rule_based import RuleBasedExplainer

log = logging.getLogger(__name__)

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_DEFAULT_MODEL = "llama-3.3-70b-versatile"


class ExplanationService:
    def __init__(self, primary: Explainer, fallback: Explainer | None = None,
                 setup_warnings: list[str] | None = None) -> None:
        self.primary = primary
        self.fallback = fallback or RuleBasedExplainer()
        self.setup_warnings = list(setup_warnings or [])

    @property
    def provider(self) -> str:
        return self.primary.name

    @property
    def model(self) -> str:
        return self.primary.model

    def explain(self, ctx: DropContext) -> Explanation:
        warnings = list(self.setup_warnings)
        try:
            text = self.primary.explain(ctx)
            check = guardrails.validate(text, ctx)
            if check.ok:
                return Explanation(text, self.primary.name, self.primary.model, "ok", warnings)
            warnings.append(f"{self.primary.name} answer rejected by guardrails: " + "; ".join(check.problems))
            log.warning("AI answer rejected: %s", check.problems)
        except ExplainerError as exc:
            warnings.append(f"{self.primary.name} failed: {exc}")
            log.warning("AI provider %s failed: %s - using fallback", self.primary.name, exc)
        except Exception as exc:  # a provider bug must never break the pipeline
            warnings.append(f"{self.primary.name} crashed: {type(exc).__name__}: {exc}")
            log.exception("AI provider crashed - using fallback")

        text = self.fallback.explain(ctx)
        return Explanation(text, self.fallback.name, self.fallback.model, "fallback", warnings)


def build_service(env: Mapping[str, str] | None = None) -> ExplanationService:
    """Pick a provider from environment variables (see .env.example).

    AI_PROVIDER = auto (default) | groq | ollama | openai_compatible | rule_based
      auto -> groq if GROQ_API_KEY is set, otherwise rule_based (offline).
    """
    env = os.environ if env is None else env
    provider = (env.get("AI_PROVIDER") or "auto").strip().lower()
    timeout = float(env.get("AI_TIMEOUT_SECONDS") or 30)

    if provider == "auto":
        provider = "groq" if env.get("GROQ_API_KEY") else "rule_based"

    if provider == "rule_based":
        return ExplanationService(RuleBasedExplainer())

    if provider == "groq":
        key = env.get("GROQ_API_KEY")
        if not key:
            return ExplanationService(RuleBasedExplainer(), setup_warnings=[
                "AI_PROVIDER=groq but GROQ_API_KEY is not set - using rule_based"])
        return ExplanationService(OpenAICompatibleExplainer(
            name="groq", base_url=env.get("GROQ_BASE_URL") or GROQ_BASE_URL,
            model=env.get("GROQ_MODEL") or GROQ_DEFAULT_MODEL, api_key=key, timeout=timeout))

    if provider == "openai_compatible":
        base, model = env.get("LLM_BASE_URL"), env.get("LLM_MODEL")
        if not base or not model:
            return ExplanationService(RuleBasedExplainer(), setup_warnings=[
                "AI_PROVIDER=openai_compatible needs LLM_BASE_URL and LLM_MODEL - using rule_based"])
        return ExplanationService(OpenAICompatibleExplainer(
            name="openai_compatible", base_url=base, model=model,
            api_key=env.get("LLM_API_KEY") or None, timeout=timeout))

    if provider == "ollama":
        return ExplanationService(OllamaExplainer(
            base_url=env.get("OLLAMA_BASE_URL") or "http://localhost:11434",
            model=env.get("OLLAMA_MODEL") or "llama3.2", timeout=max(timeout, 60)))

    return ExplanationService(RuleBasedExplainer(), setup_warnings=[
        f"unknown AI_PROVIDER={provider!r} - using rule_based"])
