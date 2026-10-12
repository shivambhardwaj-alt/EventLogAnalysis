"""AI layer. The pipeline imports only from here."""
from .base import DailyPoint, DropContext, Explainer, ExplainerError, Explanation, FunnelStepSummary, SegmentFinding
from .rule_based import RuleBasedExplainer
from .service import ExplanationService, build_service

__all__ = [
    "DailyPoint", "DropContext", "Explainer", "ExplainerError", "Explanation", "FunnelStepSummary", "SegmentFinding",
    "RuleBasedExplainer", "ExplanationService", "build_service",
]
