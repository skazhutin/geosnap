"""Production wrapper for the frozen v3 aggregation and calibrated confidence model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .confidence_model import ConfidenceModel, MultinomialRiskModel
from .models import LocalizationResult, LocalizationStatus, LocationHypothesis
from .product_policy import (
    AggregationStrategy,
    ProductAggregationConfig,
    aggregate_geographic_modes,
)


@dataclass(frozen=True, slots=True)
class ProductRuntimePolicy:
    aggregation: AggregationStrategy
    confidence_threshold: float
    catastrophic_risk_threshold: float | None = None
    cluster_radius_m: float = 100.0
    max_cluster_diameter_m: float = 150.0
    score_temperature: float = 0.08
    rank_decay_exponent: float = 0.5
    out_of_coverage_similarity: float | None = None
    minimum_cluster_mass: float | None = None
    minimum_cluster_mass_margin: float | None = None
    minimum_cluster_candidates: int | None = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence_threshold <= 1.0:
            raise ValueError("confidence_threshold must be in [0, 1]")
        if (
            self.catastrophic_risk_threshold is not None
            and not 0.0 <= self.catastrophic_risk_threshold <= 1.0
        ):
            raise ValueError("catastrophic_risk_threshold must be in [0, 1]")
        if self.out_of_coverage_similarity is not None and not -1.0 <= self.out_of_coverage_similarity <= 1.0:
            raise ValueError("out_of_coverage_similarity must be in [-1, 1]")
        if self.minimum_cluster_mass is not None and not 0.0 <= self.minimum_cluster_mass <= 1.0:
            raise ValueError("minimum_cluster_mass must be in [0, 1]")
        if self.minimum_cluster_mass_margin is not None and not 0.0 <= self.minimum_cluster_mass_margin <= 1.0:
            raise ValueError("minimum_cluster_mass_margin must be in [0, 1]")
        if self.minimum_cluster_candidates is not None and self.minimum_cluster_candidates < 1:
            raise ValueError("minimum_cluster_candidates must be positive")

    @property
    def aggregation_config(self) -> ProductAggregationConfig:
        return ProductAggregationConfig(
            strategy=self.aggregation,
            cluster_radius_m=self.cluster_radius_m,
            max_cluster_diameter_m=self.max_cluster_diameter_m,
            score_temperature=self.score_temperature,
            rank_decay_exponent=self.rank_decay_exponent,
        )


class ProductSpatialLocalizer:
    """Apply the frozen aggregation, soft evidence model, and audited safety gates."""

    def __init__(
        self,
        policy: ProductRuntimePolicy,
        confidence_model: ConfidenceModel | MultinomialRiskModel,
        catastrophic_risk_model: ConfidenceModel | None = None,
    ) -> None:
        self.policy = policy
        self.confidence_model = confidence_model
        self.catastrophic_risk_model = catastrophic_risk_model

    def localize(
        self,
        retrieval_results: Sequence[Any],
        *,
        query_quality: float | None = None,
        max_hypotheses: int = 3,
    ) -> LocalizationResult:
        if max_hypotheses < 1:
            raise ValueError("max_hypotheses must be positive")
        if not retrieval_results:
            return LocalizationResult(
                status=LocalizationStatus.OUT_OF_COVERAGE,
                lat=None,
                lon=None,
                confidence=0.0,
                uncertainty_radius_m=None,
                hypotheses=(),
                matches=(),
                diagnostics={"confidence_method": self.confidence_model.method},
                reasons=("no_retrieval_candidates",),
            )
        localized = aggregate_geographic_modes(
            retrieval_results,
            config=self.policy.aggregation_config,
            query_quality=1.0 if query_quality is None else query_quality,
        )
        prediction = self.confidence_model.predict_proba(localized.features)
        if isinstance(prediction, dict):
            confidence = float(prediction[0])
            catastrophic_risk = float(prediction[2])
        else:
            confidence = float(prediction)
            catastrophic_risk = (
                None
                if self.catastrophic_risk_model is None
                else float(self.catastrophic_risk_model.predict_proba(localized.features))
            )
        winner = localized.modes[0]
        reasons: list[str] = []
        top_similarity = float(localized.features["top1_similarity"])
        if (
            self.policy.out_of_coverage_similarity is not None
            and top_similarity < self.policy.out_of_coverage_similarity
        ):
            return LocalizationResult(
                status=LocalizationStatus.OUT_OF_COVERAGE,
                lat=None,
                lon=None,
                confidence=confidence,
                uncertainty_radius_m=None,
                hypotheses=(),
                matches=tuple(candidate for mode in localized.modes for candidate in mode.candidates),
                diagnostics=dict(localized.diagnostics)
                | {
                    "confidence_method": self.confidence_model.method,
                    "confidence_features": dict(localized.features),
                },
                reasons=("retrieval_evidence_below_out_of_coverage_threshold",),
            )
        if (
            self.policy.minimum_cluster_mass is not None
            and winner.mass_fraction < self.policy.minimum_cluster_mass
        ):
            reasons.append("winning_geographic_mode_has_low_evidence_mass")
        if (
            self.policy.minimum_cluster_mass_margin is not None
            and float(localized.features["geographic_mode_margin"])
            < self.policy.minimum_cluster_mass_margin
        ):
            reasons.append("winning_geographic_mode_is_not_dominant")
        if (
            self.policy.minimum_cluster_candidates is not None
            and len(winner.candidates) < self.policy.minimum_cluster_candidates
        ):
            reasons.append("winning_geographic_mode_has_insufficient_support")
        if confidence < self.policy.confidence_threshold:
            reasons.append("confidence_below_threshold")
        if (
            self.policy.catastrophic_risk_threshold is not None
            and catastrophic_risk is not None
            and catastrophic_risk > self.policy.catastrophic_risk_threshold
        ):
            reasons.append("catastrophic_risk_above_threshold")
        status = LocalizationStatus.OK if not reasons else LocalizationStatus.LOW_CONFIDENCE
        hypotheses = tuple(
            LocationHypothesis(
                rank=mode.rank,
                lat=mode.lat,
                lon=mode.lon,
                score=mode.raw_score,
                mass_fraction=mode.mass_fraction,
                compactness_m=mode.p90_spread_m,
                reference_ids=tuple(candidate.reference_id for candidate in mode.candidates),
                city_id=mode.candidates[0].city_id,
                index_id=mode.candidates[0].index_id,
            )
            for mode in localized.modes[:max_hypotheses]
        )
        matches = tuple(
            sorted(
                (candidate for mode in localized.modes for candidate in mode.candidates),
                key=lambda candidate: (candidate.rank, candidate.reference_id),
            )
        )
        return LocalizationResult(
            status=status,
            lat=localized.lat,
            lon=localized.lon,
            confidence=confidence,
            uncertainty_radius_m=None,
            hypotheses=hypotheses,
            matches=matches,
            diagnostics=dict(localized.diagnostics)
            | {
                "confidence_method": self.confidence_model.method,
                "confidence_features": dict(localized.features),
                "catastrophic_risk_score": catastrophic_risk,
            },
            reasons=tuple(reasons),
        )


def product_policy_from_frozen(localization: Mapping[str, Any]) -> ProductRuntimePolicy:
    safety = dict(localization.get("safety_gates", {}))
    return ProductRuntimePolicy(
        aggregation=AggregationStrategy(str(localization["aggregation"])),
        confidence_threshold=float(localization["confidence_threshold"]),
        catastrophic_risk_threshold=(
            float(localization["catastrophic_risk_threshold"])
            if localization.get("catastrophic_risk_threshold") is not None
            else None
        ),
        cluster_radius_m=float(localization.get("cluster_radius_m", 100.0)),
        max_cluster_diameter_m=float(localization.get("max_cluster_diameter_m", 150.0)),
        score_temperature=float(localization.get("score_temperature", 0.08)),
        rank_decay_exponent=float(localization.get("rank_decay_exponent", 0.5)),
        out_of_coverage_similarity=(
            float(safety["out_of_coverage_similarity"])
            if safety.get("out_of_coverage_similarity") is not None
            else None
        ),
        minimum_cluster_mass=(
            float(safety["minimum_cluster_mass"])
            if safety.get("minimum_cluster_mass") is not None
            else None
        ),
        minimum_cluster_mass_margin=(
            float(safety["minimum_cluster_mass_margin"])
            if safety.get("minimum_cluster_mass_margin") is not None
            else None
        ),
        minimum_cluster_candidates=(
            int(safety["minimum_cluster_candidates"])
            if safety.get("minimum_cluster_candidates") is not None
            else None
        ),
    )
