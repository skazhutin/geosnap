"""Multimodal geographic aggregation and confidence features for v3 policy selection."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
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
    radius_m: float
    diameter_m: float
    median_pairwise_distance_m: float
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


def _candidate_density(candidate: Candidate, radius_m: int = 100) -> float:
    try:
        value = float(candidate.metadata.get(f"local_gallery_density_{radius_m}m", 1.0))
    except (TypeError, ValueError):
        return 1.0
    return value if math.isfinite(value) and value >= 1.0 else 1.0


def _pairwise_distances(candidates: Sequence[Candidate]) -> list[float]:
    return [
        haversine_m(left.lat, left.lon, right.lat, right.lon)
        for position, left in enumerate(candidates)
        for right in candidates[position + 1 :]
    ]


def _reference_age_days(candidate: Candidate) -> float | None:
    value = candidate.metadata.get("captured_at")
    if value is None or not str(value).strip():
        return None
    try:
        captured = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if captured.tzinfo is None:
        captured = captured.replace(tzinfo=UTC)
    # A fixed experiment anchor keeps this feature deterministic in production.
    return max(0.0, (datetime(2026, 9, 3, tzinfo=UTC) - captured.astimezone(UTC)).total_seconds() / 86400.0)


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
        pairwise = _pairwise_distances(cluster.candidates)
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
                radius_m=max(
                    haversine_m(lat, lon, candidate.lat, candidate.lon)
                    for candidate in cluster.candidates
                ),
                diameter_m=max(pairwise, default=0.0),
                median_pairwise_distance_m=percentile(pairwise, 0.5) if pairwise else 0.0,
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
    retrieval_evidence_total = sum(evidence.values())
    winner_retrieval_evidence = sum(evidence[candidate.reference_id] for candidate in winner.candidates)
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
    winner_sequences: dict[str, float] = defaultdict(float)
    winner_provider_evidence: dict[str, float] = defaultdict(float)
    for candidate in winner.candidates:
        winner_sequences[_candidate_sequence_key(candidate)] += evidence[candidate.reference_id]
        winner_provider_evidence[_candidate_provider(candidate)] += evidence[candidate.reference_id]
    winner_evidence_total = sum(winner_sequences.values())
    sequence_shares = [value / winner_evidence_total for value in winner_sequences.values()]
    effective_support = (
        1.0 / sum(value * value for value in sequence_shares) if sequence_shares else 0.0
    )
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
    scores = [candidate.retrieval_score for candidate in candidates]
    top5 = scores[: min(5, len(scores))]
    top10 = scores[: min(10, len(scores))]
    winner_scores = [candidate.retrieval_score for candidate in winner.candidates]
    outside_scores = [
        candidate.retrieval_score
        for candidate in candidates
        if candidate.reference_id not in winner_ids
    ]
    reference_ages = [
        value
        for candidate in winner.candidates
        if (value := _reference_age_days(candidate)) is not None
    ]
    top1_to_selected = haversine_m(top1.lat, top1.lon, winner.lat, winner.lon)
    features = {
        "top1_similarity": top1.retrieval_score,
        "top2_similarity": top2_similarity,
        "top1_top2_similarity_margin": top1.retrieval_score - top2_similarity,
        "top1_top5_similarity_margin": top1.retrieval_score - top5[-1],
        "top5_similarity_mean": sum(top5) / len(top5),
        "top5_similarity_std": float(math.sqrt(sum((value - sum(top5) / len(top5)) ** 2 for value in top5) / len(top5))),
        "top5_similarity_min": min(top5),
        "top5_similarity_max": max(top5),
        "top10_similarity_mean": sum(top10) / len(top10),
        "top10_similarity_std": float(math.sqrt(sum((value - sum(top10) / len(top10)) ** 2 for value in top10) / len(top10))),
        "top10_similarity_min": min(top10),
        "top10_similarity_max": max(top10),
        "winning_cluster_score": winner.raw_score,
        "second_cluster_score": second_score,
        "geographic_mode_margin": winner.mass_fraction
        - (second.mass_fraction if second is not None else 0.0),
        "independent_sequence_count": float(winner.independent_sequences),
        "provider_diversity": float(winner.provider_count),
        "winning_candidate_count": float(len(winner.candidates)),
        "winning_cluster_p90_spread_m": winner.p90_spread_m,
        "winning_cluster_radius_m": winner.radius_m,
        "winning_cluster_diameter_m": winner.diameter_m,
        "winning_cluster_median_pairwise_distance_m": winner.median_pairwise_distance_m,
        "best_second_mode_separation_m": separation,
        "local_gallery_density_100m": winner.local_gallery_density_100m,
        "local_gallery_density_25m": percentile(
            [_candidate_density(candidate, 25) for candidate in winner.candidates], 0.5
        ),
        "local_gallery_density_50m": percentile(
            [_candidate_density(candidate, 50) for candidate in winner.candidates], 0.5
        ),
        "best_winner_rank": float(min(candidate.rank for candidate in winner.candidates)),
        "winner_rank_mean": sum(candidate.rank for candidate in winner.candidates) / len(winner.candidates),
        "winner_rank_max": float(max(candidate.rank for candidate in winner.candidates)),
        "top1_agrees_with_winner": float(top1.reference_id in winner_ids),
        "medoid_is_top1": float(top1_to_selected <= 0.01),
        "top1_to_selected_distance_m": top1_to_selected,
        "geographic_mode_count": float(len(modes)),
        "winning_mode_retrieval_mass_fraction": (
            winner_retrieval_evidence / retrieval_evidence_total
            if retrieval_evidence_total > 0
            else 0.0
        ),
        "winner_similarity_variance": float(
            sum((value - sum(winner_scores) / len(winner_scores)) ** 2 for value in winner_scores)
            / len(winner_scores)
        ),
        "outside_similarity_variance": float(
            sum((value - sum(outside_scores) / len(outside_scores)) ** 2 for value in outside_scores)
            / len(outside_scores)
        ) if outside_scores else 0.0,
        "dominant_mode_support_fraction": len(winner.candidates) / len(candidates),
        "heading_bin_count": float(winner.viewpoint_bucket_count),
        "maximum_single_sequence_contribution": max(sequence_shares, default=0.0),
        "maximum_provider_contribution": (
            max(winner_provider_evidence.values(), default=0.0) / winner_evidence_total
            if winner_evidence_total > 0
            else 0.0
        ),
        "effective_independent_support_count": effective_support,
        "winner_reference_age_days_median": percentile(reference_ages, 0.5) if reference_ages else 0.0,
        "winner_reference_age_days_range": (
            max(reference_ages) - min(reference_ages) if reference_ages else 0.0
        ),
        "winner_reference_age_available_fraction": len(reference_ages) / len(winner.candidates),
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
        "winner_candidate_ranks": [candidate.rank for candidate in winner.candidates],
        "winning_mode_retrieval_mass_fraction": features["winning_mode_retrieval_mass_fraction"],
        "maximum_single_sequence_contribution": features["maximum_single_sequence_contribution"],
        "maximum_provider_contribution": features["maximum_provider_contribution"],
        "effective_independent_support_count": effective_support,
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
