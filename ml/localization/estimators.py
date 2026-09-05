"""Interpretable coordinate estimators restricted to one spatial mode."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum

from .geo import haversine_m, weighted_spherical_centroid
from .models import Candidate


class CoordinateEstimator(StrEnum):
    TOP1 = "top1"
    WEIGHTED_CENTROID = "weighted_centroid"
    WEIGHTED_MEDOID = "weighted_medoid"


def estimate_coordinate(
    candidates: Sequence[Candidate],
    evidence: Mapping[str, float],
    method: CoordinateEstimator | str,
) -> tuple[float, float]:
    if not candidates:
        raise ValueError("cannot estimate a coordinate from an empty cluster")
    method = CoordinateEstimator(method)
    if method is CoordinateEstimator.TOP1:
        best = max(candidates, key=lambda item: (evidence[item.reference_id], -item.rank))
        return best.lat, best.lon
    weights = [float(evidence[candidate.reference_id]) for candidate in candidates]
    if method is CoordinateEstimator.WEIGHTED_CENTROID:
        return weighted_spherical_centroid(
            [(candidate.lat, candidate.lon) for candidate in candidates], weights
        )
    best = min(
        candidates,
        key=lambda proposed: (
            sum(
                weight
                * haversine_m(proposed.lat, proposed.lon, other.lat, other.lon)
                for other, weight in zip(candidates, weights, strict=True)
            ),
            proposed.rank,
        ),
    )
    return best.lat, best.lon
