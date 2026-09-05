"""Compact, non-chaining spatial grouping of top retrieval candidates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .geo import haversine_m
from .models import Candidate


@dataclass(frozen=True, slots=True)
class CandidateCluster:
    candidates: tuple[Candidate, ...]
    evidence_mass: float
    mass_fraction: float

    @property
    def anchor(self) -> Candidate:
        return self.candidates[0]


def group_compact_candidates(
    candidates: Sequence[Candidate],
    evidence: Mapping[str, float],
    *,
    cluster_radius_m: float,
    max_cluster_diameter_m: float,
) -> list[CandidateCluster]:
    """Greedily group around strong anchors with a hard diameter bound.

    This deliberately is not single-link clustering: a candidate may join only
    when it is close to the fixed high-evidence anchor *and* every current member.
    Consequently an A-B-C-D proximity chain cannot merge distant endpoints into
    one geographic mode.
    """

    if cluster_radius_m <= 0 or max_cluster_diameter_m <= 0:
        raise ValueError("spatial thresholds must be positive")
    ordered = sorted(
        candidates,
        key=lambda item: (-float(evidence[item.reference_id]), item.rank, item.reference_id),
    )
    groups: list[list[Candidate]] = []
    for candidate in ordered:
        compatible: list[tuple[float, int]] = []
        mode_key = (candidate.index_id, candidate.city_id)
        for index, group in enumerate(groups):
            anchor = group[0]
            if (anchor.index_id, anchor.city_id) != mode_key:
                continue
            anchor_distance = haversine_m(candidate.lat, candidate.lon, anchor.lat, anchor.lon)
            if anchor_distance > cluster_radius_m:
                continue
            if any(
                haversine_m(candidate.lat, candidate.lon, member.lat, member.lon)
                > max_cluster_diameter_m
                for member in group
            ):
                continue
            compatible.append((anchor_distance, index))
        if compatible:
            _, group_index = min(compatible)
            groups[group_index].append(candidate)
        else:
            groups.append([candidate])

    total_mass = sum(float(evidence[candidate.reference_id]) for candidate in ordered)
    clusters: list[CandidateCluster] = []
    for group in groups:
        group.sort(key=lambda item: (-float(evidence[item.reference_id]), item.rank))
        mass = sum(float(evidence[item.reference_id]) for item in group)
        clusters.append(
            CandidateCluster(
                candidates=tuple(group),
                evidence_mass=mass,
                mass_fraction=mass / total_mass if total_mass > 0 else 0.0,
            )
        )
    clusters.sort(
        key=lambda cluster: (
            -cluster.evidence_mass,
            -evidence[cluster.anchor.reference_id],
            cluster.anchor.rank,
        )
    )
    return clusters
