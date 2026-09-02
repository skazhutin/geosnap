"""Multimodal geographic aggregation and confidence features for v3 policy selection."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .clustering import CandidateCluster, group_compact_candidates
from .estimators import CoordinateEstimator, estimate_coordinate
from .geo import haversine_m, percentile
from .models import Candidate


class AggregationStrategy(StrEnum):
    LEGACY_WEIGHTED_MEDOID = "legacy_weighted_medoid"
    RANK_WEIGHTED_VOTE = "rank_weighted_vote"
    SEQUENCE_DEDUPLICATED_VOTE = "sequence_deduplicated_vote"
    DENSITY_AWARE_MODE_VOTE = "density_aware_mode_vote"


@dataclass(frozen=True, slots=True)
class ProductAggregationConfig:
    strategy: AggregationStrategy = AggregationStrategy.LEGACY_WEIGHTED_MEDOID
    cluster_radius_m: float = 100.0
    max_cluster_diameter_m: float = 150.0
    score_temperature: float = 0.08
    rank_decay_exponent: float = 0.5

    def __post_init__(self) -> None:
        if self.cluster_radius_m <= 0:
            raise ValueError("cluster_radius_m must be positive")
        if self.max_cluster_diameter_m < self.cluster_radius_m:
            raise ValueError("max_cluster_diameter_m cannot be smaller than cluster_radius_m")
        if self.score_temperature <= 0:
            raise ValueError("score_temperature must be positive")
        if self.rank_decay_exponent < 0:
            raise ValueError("rank_decay_exponent cannot be negative")


@dataclass(frozen=True, slots=True)
class GeographicMode:
    rank: int
    lat: float
    lon: float
    raw_score: float
    mass_fraction: float
    p90_spread_m: float
    candidates: tuple[Candidate, ...]
    independent_sequences: int
    provider_count: int
    viewpoint_bucket_count: int
    local_gallery_density_100m: float


@dataclass(frozen=True, slots=True)
class ProductLocalization:
    lat: float
    lon: float
    modes: tuple[GeographicMode, ...]
    features: Mapping[str, float]
    diagnostics: Mapping[str, Any]


def _candidate_sequence_key(candidate: Candidate) -> str:
    source = str(candidate.source or candidate.metadata.get("source") or "unknown").lower()
    sequence = candidate.metadata.get("sequence_id")
    if sequence is None or not str(sequence).strip():
        return f"{source}::reference::{candidate.reference_id}"
    return f"{source}::sequence::{str(sequence).strip()}"


def _candidate_provider(candidate: Candidate) -> str:
    return str(candidate.source or candidate.metadata.get("source") or "unknown").lower()


def _viewpoint_bucket(candidate: Candidate) -> str:
    try:
        heading = float(candidate.metadata.get("heading"))
    except (TypeError, ValueError):
        return "missing"
    if not math.isfinite(heading):
        return "missing"
    return str(int((heading % 360.0) // 45.0))


def _candidate_density(candidate: Candidate) -> float:
    try:
        value = float(candidate.metadata.get("local_gallery_density_100m", 1.0))
    except (TypeError, ValueError):
        return 1.0
    return value if math.isfinite(value) and value >= 1.0 else 1.0


def _similarity_evidence(
    candidates: Sequence[Candidate], config: ProductAggregationConfig
) -> dict[str, float]:
    maximum = max(candidate.retrieval_score for candidate in candidates)
    rank_weighted = config.strategy is not AggregationStrategy.LEGACY_WEIGHTED_MEDOID
    return {
        candidate.reference_id: math.exp(
            (candidate.retrieval_score - maximum) / config.score_temperature
        )
        * (
            candidate.rank ** (-config.rank_decay_exponent)
            if rank_weighted
            else 1.0
        )
        for candidate in candidates
    }


def _sequence_deduplicated_score(
    cluster: CandidateCluster, evidence: Mapping[str, float]
) -> float:
    by_sequence: dict[str, float] = {}
    for candidate in cluster.candidates:
        key = _candidate_sequence_key(candidate)
        by_sequence[key] = max(by_sequence.get(key, 0.0), evidence[candidate.reference_id])
    return sum(by_sequence.values())


def _coordinate_evidence(
    cluster: CandidateCluster,
    evidence: Mapping[str, float],
    *,
    sequence_deduplicated: bool,
) -> dict[str, float]:
    if not sequence_deduplicated:
        return {candidate.reference_id: evidence[candidate.reference_id] for candidate in cluster.candidates}
    sequence_counts: dict[str, int] = defaultdict(int)
    for candidate in cluster.candidates:
        sequence_counts[_candidate_sequence_key(candidate)] += 1
    return {
        candidate.reference_id: evidence[candidate.reference_id]
        / sequence_counts[_candidate_sequence_key(candidate)]
        for candidate in cluster.candidates
    }


def _cluster_spread(
    cluster: CandidateCluster, lat: float, lon: float
) -> float:
    return percentile(
        [haversine_m(lat, lon, candidate.lat, candidate.lon) for candidate in cluster.candidates],
        0.9,
    )


def _cluster_score(
    cluster: CandidateCluster,
    evidence: Mapping[str, float],
    *,
    strategy: AggregationStrategy,
    p90_spread_m: float,
) -> float:
    if strategy in {
        AggregationStrategy.LEGACY_WEIGHTED_MEDOID,
        AggregationStrategy.RANK_WEIGHTED_VOTE,
    }:
        return sum(evidence[candidate.reference_id] for candidate in cluster.candidates)
    score = _sequence_deduplicated_score(cluster, evidence)
    if strategy is AggregationStrategy.SEQUENCE_DEDUPLICATED_VOTE:
        return score
    sequence_count = len({_candidate_sequence_key(candidate) for candidate in cluster.candidates})
    provider_count = len({_candidate_provider(candidate) for candidate in cluster.candidates})
    density = percentile([_candidate_density(candidate) for candidate in cluster.candidates], 0.5)
    diversity = 1.0 + 0.10 * math.log1p(max(sequence_count - 1, 0)) + 0.10 * max(
        provider_count - 1, 0
    )
    compactness = math.exp(-p90_spread_m / 500.0)
    return score * diversity * compactness / math.sqrt(max(density, 1.0))


def _coerce_candidates(values: Sequence[Any]) -> list[Candidate]:
    candidates = [
        value if isinstance(value, Candidate) else Candidate.from_retrieval_result(value)
        for value in values
    ]
    if not candidates:
        raise ValueError("at least one retrieval candidate is required")
    if len({candidate.reference_id for candidate in candidates}) != len(candidates):
        raise ValueError("retrieval candidates contain duplicate reference IDs")
    return sorted(candidates, key=lambda item: (item.rank, -item.retrieval_score, item.reference_id))


def aggregate_geographic_modes(
    retrieval_results: Sequence[Any],
    *,
    config: ProductAggregationConfig | None = None,
    query_quality: float = 1.0,
    true_coordinate: tuple[float, float] | None = None,
) -> ProductLocalization:
    """Aggregate retrievals and expose interpretable, pixel-free confidence features."""

    active = config or ProductAggregationConfig()
    if not 0.0 <= query_quality <= 1.0:
        raise ValueError("query_quality must be in [0, 1]")
    candidates = _coerce_candidates(retrieval_results)
    evidence = _similarity_evidence(candidates, active)
    clusters = group_compact_candidates(
        candidates,
        evidence,
        cluster_radius_m=active.cluster_radius_m,
        max_cluster_diameter_m=active.max_cluster_diameter_m,
    )
    sequence_deduplicated = active.strategy in {
        AggregationStrategy.SEQUENCE_DEDUPLICATED_VOTE,
        AggregationStrategy.DENSITY_AWARE_MODE_VOTE,
    }
    intermediate: list[tuple[CandidateCluster, float, float, float, float]] = []
    for cluster in clusters:
        coordinate_evidence = _coordinate_evidence(
            cluster,
            evidence,
            sequence_deduplicated=sequence_deduplicated,
        )
        lat, lon = estimate_coordinate(
            cluster.candidates,
            coordinate_evidence,
            CoordinateEstimator.WEIGHTED_MEDOID,
        )
        spread = _cluster_spread(cluster, lat, lon)
        score = _cluster_score(
            cluster,
            evidence,
            strategy=active.strategy,
            p90_spread_m=spread,
        )
        intermediate.append((cluster, lat, lon, spread, score))
    intermediate.sort(
        key=lambda value: (
            -value[4],
            min(candidate.rank for candidate in value[0].candidates),
            value[0].anchor.reference_id,
        )
    )
    total_score = sum(value[4] for value in intermediate)
    modes: list[GeographicMode] = []
    for mode_rank, (cluster, lat, lon, spread, score) in enumerate(intermediate, start=1):
        modes.append(
            GeographicMode(
                rank=mode_rank,
                lat=lat,
                lon=lon,
                raw_score=score,
                mass_fraction=score / total_score if total_score > 0 else 0.0,
                p90_spread_m=spread,
                candidates=cluster.candidates,
                independent_sequences=len(
                    {_candidate_sequence_key(candidate) for candidate in cluster.candidates}
                ),
                provider_count=len({_candidate_provider(candidate) for candidate in cluster.candidates}),
                viewpoint_bucket_count=len({_viewpoint_bucket(candidate) for candidate in cluster.candidates}),
                local_gallery_density_100m=percentile(
                    [_candidate_density(candidate) for candidate in cluster.candidates], 0.5
                ),
            )
        )

    winner = modes[0]
    second = modes[1] if len(modes) > 1 else None
    top1 = candidates[0]
    top2_similarity = candidates[1].retrieval_score if len(candidates) > 1 else top1.retrieval_score
    winner_ids = {candidate.reference_id for candidate in winner.candidates}
    winner_top_similarity = max(
        candidate.retrieval_score for candidate in winner.candidates
    )
    outside_top_similarity = max(
        (
            candidate.retrieval_score
            for candidate in candidates
            if candidate.reference_id not in winner_ids
        ),
        default=None,
    )
    geographic_similarity_margin = (
        winner_top_similarity - outside_top_similarity
        if outside_top_similarity is not None
        else None
    )
    winner_providers = {_candidate_provider(candidate) for candidate in winner.candidates}
    separation = (
        haversine_m(winner.lat, winner.lon, second.lat, second.lon)
        if second is not None
        else 0.0
    )
    second_score = second.raw_score if second is not None else 0.0
    dominance_signal = max(
        0.0,
        min(
            1.0,
            (
                winner.mass_fraction
                - (second.mass_fraction if second is not None else 0.0)
            )
            / 0.25,
        ),
    )
    heuristic_confidence = max(
        0.0,
        min(
            1.0,
            0.30 * max(0.0, min(1.0, (top1.retrieval_score - 0.15) / 0.50))
            + 0.20
            * (
                max(0.0, min(1.0, geographic_similarity_margin / 0.08))
                if geographic_similarity_margin is not None
                else 0.0
            )
            + 0.25 * max(0.0, min(1.0, winner.mass_fraction))
            + 0.10 * math.exp(-winner.p90_spread_m / active.cluster_radius_m)
            + 0.10 * max(0.0, min(1.0, separation / 500.0)) * dominance_signal
            + 0.05 * query_quality,
        ),
    )
    features = {
        "top1_similarity": top1.retrieval_score,
        "top1_top2_similarity_margin": top1.retrieval_score - top2_similarity,
        "winning_cluster_score": winner.raw_score,
        "second_cluster_score": second_score,
        "geographic_mode_margin": winner.mass_fraction
        - (second.mass_fraction if second is not None else 0.0),
        "independent_sequence_count": float(winner.independent_sequences),
        "provider_diversity": float(winner.provider_count),
        "winning_candidate_count": float(len(winner.candidates)),
        "winning_cluster_p90_spread_m": winner.p90_spread_m,
        "best_second_mode_separation_m": separation,
        "local_gallery_density_100m": winner.local_gallery_density_100m,
        "best_winner_rank": float(min(candidate.rank for candidate in winner.candidates)),
        "top1_agrees_with_winner": float(top1.reference_id in winner_ids),
        "cross_provider_winner_evidence": float(
            len(winner_providers) >= 2
        ),
    }

    correct_mode_ranks: list[int] = []
    correct_cluster_retrieved_but_lost = False
    if true_coordinate is not None:
        true_lat, true_lon = true_coordinate
        correct_mode_ranks = [
            mode.rank
            for mode in modes
            if any(
                haversine_m(true_lat, true_lon, candidate.lat, candidate.lon) <= 100.0
                for candidate in mode.candidates
            )
        ]
        correct_cluster_retrieved_but_lost = bool(correct_mode_ranks and correct_mode_ranks[0] > 1)
    winner_is_correct = (
        true_coordinate is not None
        and haversine_m(true_coordinate[0], true_coordinate[1], winner.lat, winner.lon) <= 100.0
    )
    diagnostics: dict[str, Any] = {
        "aggregation_strategy": active.strategy.value,
        "geographic_cluster_count": len(modes),
        "winning_cluster_mass": winner.mass_fraction,
        "correct_cluster_rank": correct_mode_ranks[0] if correct_mode_ranks else None,
        "competing_cluster_separation_m": separation if second is not None else None,
        "correct_cluster_retrieved_but_lost": correct_cluster_retrieved_but_lost,
        "singleton_false_cluster": bool(len(winner.candidates) == 1 and not winner_is_correct),
        "multimodal_case": len(modes) >= 2 and modes[1].mass_fraction >= 0.20,
        "independent_sequence_count": winner.independent_sequences,
        "provider_count": winner.provider_count,
        "viewpoint_bucket_count": winner.viewpoint_bucket_count,
        "legacy_handwritten_confidence": heuristic_confidence,
        "geographic_similarity_margin": geographic_similarity_margin,
        "mode_summaries": [
            {
                "rank": mode.rank,
                "score": mode.raw_score,
                "mass_fraction": mode.mass_fraction,
                "candidate_count": len(mode.candidates),
                "independent_sequence_count": mode.independent_sequences,
                "provider_count": mode.provider_count,
                "p90_spread_m": mode.p90_spread_m,
                "best_candidate_rank": min(candidate.rank for candidate in mode.candidates),
            }
            for mode in modes
        ],
    }
    return ProductLocalization(
        lat=winner.lat,
        lon=winner.lon,
        modes=tuple(modes),
        features=features,
        diagnostics=diagnostics,
    )
