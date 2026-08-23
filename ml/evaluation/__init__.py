"""Leakage-aware retrieval/localization metrics and report writers."""

from .augmentations import AugmentedQuery, generate_robustness_variants
from .metrics import (
    EvaluationBundle,
    LatencyMetrics,
    LatencySummary,
    LocalizationMetrics,
    LocalizationObservation,
    QueryGroundTruth,
    RetrievalMetrics,
    evaluate_localization,
    evaluate_retrieval,
    summarize_latencies,
)
from .report import write_evaluation_reports

__all__ = [
    "AugmentedQuery",
    "EvaluationBundle",
    "LatencyMetrics",
    "LatencySummary",
    "LocalizationMetrics",
    "LocalizationObservation",
    "QueryGroundTruth",
    "RetrievalMetrics",
    "evaluate_localization",
    "evaluate_retrieval",
    "generate_robustness_variants",
    "summarize_latencies",
    "write_evaluation_reports",
]
