"""Retrieval results -> coherent geographic hypothesis -> honest status."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .clustering import CandidateCluster, group_compact_candidates
from .estimators import CoordinateEstimator, estimate_coordinate
from .geo import haversine_m, percentile
from .models import Candidate, LocalizationResult, LocalizationStatus, LocationHypothesis
from .uncertainty import UncertaintyCalibration

# Selected on the real Moscow calibration split using the safety-first policy:
# first eliminate false-confident answers beyond 100 m, then maximize useful
# all-query accuracy.  It is an operating threshold, not a calibrated
# probability or uncertainty-radius model.
DEFAULT_CONFIDENCE_THRESHOLD = 0.5548002022369389


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True, slots=True)
class LocalizerConfig:
    cluster_radius_m: float = 100.0
    max_cluster_diameter_m: float = 150.0
    estimator: CoordinateEstimator = CoordinateEstimator.WEIGHTED_MEDOID
    score_temperature: float = 0.08
    out_of_coverage_similarity: float = 0.15
    confident_similarity: float = 0.65
    good_geographic_margin: float = 0.08
    minimum_cluster_mass: float = 0.45
    minimum_cluster_mass_margin: float = 0.10
    minimum_cluster_candidates: int = 2
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD
    good_hypothesis_separation_m: float = 500.0

    def __post_init__(self) -> None:
        if self.cluster_radius_m <= 0 or self.max_cluster_diameter_m <= 0:
            raise ValueError("cluster distances must be positive")
        if self.max_cluster_diameter_m < self.cluster_radius_m:
            raise ValueError("max_cluster_diameter_m cannot be smaller than cluster_radius_m")
        if self.score_temperature <= 0:
            raise ValueError("score_temperature must be positive")
        if not -1 <= self.out_of_coverage_similarity < self.confident_similarity <= 1:
            raise ValueError("similarity thresholds must satisfy -1 <= OOC < confident <= 1")
        if self.good_geographic_margin <= 0:
            raise ValueError("good_geographic_margin must be positive")
        if not 0 <= self.minimum_cluster_mass <= 1:
            raise ValueError("minimum_cluster_mass must be in [0, 1]")
        if not 0 <= self.minimum_cluster_mass_margin <= 1:
            raise ValueError("minimum_cluster_mass_margin must be in [0, 1]")
        if self.minimum_cluster_candidates < 1:
            raise ValueError("minimum_cluster_candidates must be >= 1")
        if not 0 <= self.confidence_threshold <= 1:
            raise ValueError("confidence_threshold must be in [0, 1]")
        if self.good_hypothesis_separation_m <= 0:
            raise ValueError("good_hypothesis_separation_m must be positive")


class SpatialLocalizer:
    """Select and estimate one compact geographic mode from top retrievals."""

    def __init__(
        self,
        config: LocalizerConfig | None = None,
        *,
        uncertainty_calibration: UncertaintyCalibration | None = None,
    ) -> None:
        self.config = config or LocalizerConfig()
        self.uncertainty_calibration = uncertainty_calibration

    def _coerce_candidates(self, values: Sequence[Any]) -> list[Candidate]:
        candidates: list[Candidate] = []
        for value in values:
            candidates.append(value if isinstance(value, Candidate) else Candidate.from_retrieval_result(value))
        if len({candidate.reference_id for candidate in candidates}) != len(candidates):
            raise ValueError("retrieval results contain duplicate reference IDs")
        return candidates

    def _evidence(self, candidates: Sequence[Candidate]) -> dict[str, float]:
        scores = {
            candidate.reference_id: (
                candidate.retrieval_score
                if candidate.localization_score is None
                else candidate.localization_score
            )
            for candidate in candidates
        }
        maximum = max(scores.values())
        values: dict[str, float] = {}
        for candidate in candidates:
            values[candidate.reference_id] = math.exp(
                (scores[candidate.reference_id] - maximum) / self.config.score_temperature
            )
        return values

    def _cluster_center_and_spread(
        self,
        cluster: CandidateCluster,
        evidence: dict[str, float],
    ) -> tuple[float, float, float]:
        lat, lon = estimate_coordinate(cluster.candidates, evidence, self.config.estimator)
        distances = [
            haversine_m(lat, lon, candidate.lat, candidate.lon)
            for candidate in cluster.candidates
        ]
        return lat, lon, percentile(distances, 0.9)

    def localize(
        self,
        retrieval_results: Sequence[Any],
        *,
        query_quality: float | None = None,
        max_hypotheses: int = 3,
    ) -> LocalizationResult:
        if max_hypotheses < 1:
            raise ValueError("max_hypotheses must be >= 1")
        if query_quality is not None and not 0 <= query_quality <= 1:
            raise ValueError("query_quality must be in [0, 1]")
        if not retrieval_results:
            return LocalizationResult(
                status=LocalizationStatus.OUT_OF_COVERAGE,
                lat=None,
                lon=None,
                confidence=0.0,
                uncertainty_radius_m=None,
                hypotheses=(),
                matches=(),
                diagnostics={"confidence_method": "interpretable-v1-uncalibrated"},
                reasons=("no_retrieval_candidates",),
            )

        candidates = self._coerce_candidates(retrieval_results)
        candidates.sort(key=lambda item: (item.rank, -item.retrieval_score))
        top_score = max(candidate.retrieval_score for candidate in candidates)
        if top_score < self.config.out_of_coverage_similarity:
            return LocalizationResult(
                status=LocalizationStatus.OUT_OF_COVERAGE,
                lat=None,
                lon=None,
                confidence=0.0,
                uncertainty_radius_m=None,
                hypotheses=(),
                matches=tuple(candidates),
                diagnostics={
                    "top1_similarity": top_score,
                    "out_of_coverage_similarity": self.config.out_of_coverage_similarity,
                    "confidence_method": "interpretable-v1-uncalibrated",
                },
                reasons=("retrieval_evidence_below_out_of_coverage_threshold",),
            )

        evidence = self._evidence(candidates)
        clusters = group_compact_candidates(
            candidates,
            evidence,
            cluster_radius_m=self.config.cluster_radius_m,
            max_cluster_diameter_m=self.config.max_cluster_diameter_m,
        )
        cluster_values: list[tuple[CandidateCluster, float, float, float]] = []
        hypotheses: list[LocationHypothesis] = []
        for rank, cluster in enumerate(clusters[:max_hypotheses], start=1):
            lat, lon, spread = self._cluster_center_and_spread(cluster, evidence)
            cluster_values.append((cluster, lat, lon, spread))
            hypotheses.append(
                LocationHypothesis(
                    rank=rank,
                    lat=lat,
                    lon=lon,
                    score=cluster.evidence_mass,
                    mass_fraction=cluster.mass_fraction,
                    compactness_m=spread,
                    reference_ids=tuple(item.reference_id for item in cluster.candidates),
                    city_id=cluster.anchor.city_id,
                    index_id=cluster.anchor.index_id,
                )
            )

        winning, lat, lon, spread = cluster_values[0]
        second_mass = clusters[1].mass_fraction if len(clusters) >= 2 else None
        mode_mass_margin = (
            winning.mass_fraction - second_mass if second_mass is not None else None
        )
        outside = [candidate for candidate in candidates if candidate not in winning.candidates]
        winning_top = max(candidate.retrieval_score for candidate in winning.candidates)
        outside_top = max((candidate.retrieval_score for candidate in outside), default=None)
        geographic_margin = (
            winning_top - outside_top if outside_top is not None else None
        )
        if len(cluster_values) >= 2:
            _, second_lat, second_lon, _ = cluster_values[1]
            hypothesis_separation = haversine_m(lat, lon, second_lat, second_lon)
        else:
            hypothesis_separation = None

        similarity_signal = _clip(
            (top_score - self.config.out_of_coverage_similarity)
            / (self.config.confident_similarity - self.config.out_of_coverage_similarity)
        )
        margin_signal = (
            _clip(geographic_margin / self.config.good_geographic_margin)
            if geographic_margin is not None
            else 0.0
        )
        mass_signal = _clip(winning.mass_fraction)
        compactness_signal = math.exp(-spread / self.config.cluster_radius_m)
        dominance_signal = _clip((mode_mass_margin or 0.0) / 0.25)
        separation_signal = (
            _clip(hypothesis_separation / self.config.good_hypothesis_separation_m)
            * dominance_signal
            if hypothesis_separation is not None
            else 0.0
        )
        quality_signal = 1.0 if query_quality is None else query_quality
        confidence = _clip(
            0.30 * similarity_signal
            + 0.20 * margin_signal
            + 0.25 * mass_signal
            + 0.10 * compactness_signal
            + 0.10 * separation_signal
            + 0.05 * quality_signal
        )

        reasons: list[str] = []
        if winning.mass_fraction < self.config.minimum_cluster_mass:
            reasons.append("winning_geographic_mode_has_low_evidence_mass")
        if (
            mode_mass_margin is not None
            and mode_mass_margin < self.config.minimum_cluster_mass_margin
        ):
            reasons.append("winning_geographic_mode_is_not_dominant")
        if len(winning.candidates) < self.config.minimum_cluster_candidates:
            reasons.append("winning_geographic_mode_has_insufficient_support")
        if confidence < self.config.confidence_threshold:
            reasons.append("confidence_below_threshold")
        status = LocalizationStatus.OK if not reasons else LocalizationStatus.LOW_CONFIDENCE

        uncertainty = None
        calibration_dataset = None
        if self.uncertainty_calibration is not None:
            uncertainty = self.uncertainty_calibration.estimate(
                confidence, observed_cluster_spread_m=spread
            )
            calibration_dataset = self.uncertainty_calibration.dataset_id

        return LocalizationResult(
            status=status,
            lat=lat,
            lon=lon,
            confidence=confidence,
            uncertainty_radius_m=uncertainty,
            hypotheses=tuple(hypotheses),
            matches=tuple(candidates),
            diagnostics={
                "estimator": self.config.estimator.value,
                "candidate_count": len(candidates),
                "spatial_mode_count": len(clusters),
                "top1_similarity": top_score,
                "geographic_margin": geographic_margin,
                "winning_cluster_mass": winning.mass_fraction,
                "winning_cluster_mass_margin": mode_mass_margin,
                "winning_cluster_candidate_count": len(winning.candidates),
                "winning_cluster_p90_spread_m": spread,
                "second_hypothesis_distance_m": hypothesis_separation,
                "confidence_components": {
                    "similarity": similarity_signal,
                    "geographic_margin": margin_signal,
                    "cluster_mass": mass_signal,
                    "compactness": compactness_signal,
                    "hypothesis_separation": separation_signal,
                    "geographic_mode_dominance": dominance_signal,
                    "query_quality": quality_signal,
                },
                "confidence_method": "interpretable-v1-uncalibrated",
                "uncertainty_calibration_dataset": calibration_dataset,
            },
            reasons=tuple(reasons),
        )
