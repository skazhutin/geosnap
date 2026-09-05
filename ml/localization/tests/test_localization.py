from __future__ import annotations

import pytest

from ml.localization.clustering import group_compact_candidates
from ml.localization.estimators import CoordinateEstimator, estimate_coordinate
from ml.localization.geo import haversine_m
from ml.localization.models import Candidate, LocalizationStatus
from ml.localization.pipeline import LocalizerConfig, SpatialLocalizer
from ml.localization.uncertainty import CalibrationBin, UncertaintyCalibration


def _candidate(
    reference_id: str,
    north_m: float,
    score: float,
    rank: int,
    *,
    city_id: str | None = "moscow",
    localization_score: float | None = None,
) -> Candidate:
    return Candidate(
        reference_id=reference_id,
        lat=55.75 + north_m / 111_320.0,
        lon=37.61,
        retrieval_score=score,
        rank=rank,
        localization_score=localization_score,
        city_id=city_id,
    )


def test_compact_grouping_does_not_single_link_chain() -> None:
    candidates = [
        _candidate("a", 0, 0.95, 1),
        _candidate("b", 60, 0.94, 2),
        _candidate("c", 120, 0.93, 3),
        _candidate("d", 180, 0.92, 4),
    ]
    evidence = {candidate.reference_id: 1.0 for candidate in candidates}
    clusters = group_compact_candidates(
        candidates,
        evidence,
        cluster_radius_m=75,
        max_cluster_diameter_m=80,
    )
    assert len(clusters) == 2
    assert [item.reference_id for item in clusters[0].candidates] == ["a", "b"]
    assert [item.reference_id for item in clusters[1].candidates] == ["c", "d"]
    assert all(len(cluster.candidates) < 4 for cluster in clusters)


def test_grouping_never_mixes_city_or_index_modes() -> None:
    a = _candidate("moscow", 0, 0.9, 1, city_id="moscow")
    b = _candidate("prague", 0, 0.89, 2, city_id="prague")
    clusters = group_compact_candidates(
        [a, b], {"moscow": 1.0, "prague": 0.9}, cluster_radius_m=100, max_cluster_diameter_m=150
    )
    assert len(clusters) == 2


def test_estimators_are_restricted_to_supplied_winning_cluster() -> None:
    candidates = [_candidate("best", 0, 0.9, 1), _candidate("near", 20, 0.89, 2)]
    evidence = {"best": 1.0, "near": 0.8}
    assert estimate_coordinate(candidates, evidence, CoordinateEstimator.TOP1) == (
        candidates[0].lat,
        candidates[0].lon,
    )
    centroid = estimate_coordinate(candidates, evidence, CoordinateEstimator.WEIGHTED_CENTROID)
    assert haversine_m(candidates[0].lat, candidates[0].lon, *centroid) < 20
    medoid = estimate_coordinate(candidates, evidence, CoordinateEstimator.WEIGHTED_MEDOID)
    assert medoid in {(candidate.lat, candidate.lon) for candidate in candidates}


def test_localizer_selects_dominant_mode_without_cross_mode_average() -> None:
    near = [_candidate("near-1", 0, 0.92, 1), _candidate("near-2", 15, 0.90, 2)]
    far = Candidate("far", 55.84, 37.75, 0.70, 3, city_id="moscow")
    result = SpatialLocalizer().localize([*near, far])
    assert result.status is LocalizationStatus.OK
    assert result.lat is not None and result.lon is not None
    assert haversine_m(result.lat, result.lon, near[0].lat, near[0].lon) < 30
    assert haversine_m(result.lat, result.lon, far.lat, far.lon) > 5_000
    assert result.diagnostics["spatial_mode_count"] == 2
    assert set(result.hypotheses[0].reference_ids) == {"near-1", "near-2"}


def test_out_of_coverage_and_low_confidence_are_explicit() -> None:
    config = LocalizerConfig(
        out_of_coverage_similarity=0.2,
        confident_similarity=0.8,
        confidence_threshold=0.9,
        minimum_cluster_mass=0.8,
    )
    localizer = SpatialLocalizer(config)
    ooc = localizer.localize([_candidate("weak", 0, 0.1, 1)])
    assert ooc.status is LocalizationStatus.OUT_OF_COVERAGE
    assert ooc.lat is None and ooc.lon is None
    ambiguous = localizer.localize(
        [
            _candidate("one", 0, 0.6, 1),
            Candidate("other", 55.85, 37.75, 0.595, 2, city_id="moscow"),
        ]
    )
    assert ambiguous.status is LocalizationStatus.LOW_CONFIDENCE
    assert ambiguous.lat is not None
    assert ambiguous.reasons


def test_singleton_mode_never_becomes_ok_from_missing_competition() -> None:
    result = SpatialLocalizer().localize([_candidate("singleton", 0, 0.99, 1)])
    assert result.status is LocalizationStatus.LOW_CONFIDENCE
    assert "winning_geographic_mode_has_insufficient_support" in result.reasons
    assert result.diagnostics["confidence_components"]["geographic_margin"] == 0.0
    assert result.diagnostics["confidence_components"]["hypothesis_separation"] == 0.0


def test_weak_but_compact_retrieval_support_is_not_promoted_by_missing_modes() -> None:
    result = SpatialLocalizer().localize(
        [_candidate("weak-a", 0, 0.151, 1), _candidate("weak-b", 5, 0.150, 2)],
        query_quality=0.0,
    )
    assert result.status is LocalizationStatus.LOW_CONFIDENCE
    assert "confidence_below_threshold" in result.reasons
    assert result.diagnostics["confidence_components"]["geographic_margin"] == 0.0
    assert result.diagnostics["confidence_components"]["hypothesis_separation"] == 0.0


def test_symmetric_distant_modes_are_low_confidence_not_rewarded_for_distance() -> None:
    result = SpatialLocalizer().localize(
        [
            _candidate("a-1", 0, 0.95, 1),
            _candidate("a-2", 10, 0.94, 2),
            _candidate("b-1", 5_000, 0.95, 3),
            _candidate("b-2", 5_010, 0.94, 4),
        ],
        query_quality=0.0,
    )
    assert result.status is LocalizationStatus.LOW_CONFIDENCE
    assert "winning_geographic_mode_is_not_dominant" in result.reasons
    assert result.diagnostics["winning_cluster_mass_margin"] == pytest.approx(0.0)
    assert result.diagnostics["confidence_components"]["hypothesis_separation"] == 0.0


def test_configured_rerank_evidence_can_select_a_different_geographic_mode() -> None:
    result = SpatialLocalizer().localize(
        [
            _candidate("retrieval-a", 0, 0.95, 1, localization_score=0.20),
            _candidate("retrieval-b", 10, 0.94, 2, localization_score=0.19),
            _candidate("verified-a", 5_000, 0.90, 3, localization_score=0.95),
            _candidate("verified-b", 5_010, 0.89, 4, localization_score=0.94),
        ]
    )
    assert set(result.hypotheses[0].reference_ids) == {"verified-a", "verified-b"}
    assert result.lat is not None
    assert haversine_m(result.lat, result.lon, 55.75 + 5_000 / 111_320.0, 37.61) < 30


def test_uncertainty_is_omitted_without_defensible_calibration() -> None:
    candidates = [_candidate("a", 0, 0.95, 1), _candidate("b", 10, 0.94, 2)]
    no_calibration = SpatialLocalizer().localize(candidates)
    assert no_calibration.uncertainty_radius_m is None

    too_small = UncertaintyCalibration(
        [CalibrationBin(0.0, 1.0, p90_error_m=80.0, sample_count=5)],
        dataset_id="held-out-fixture",
        minimum_bin_samples=30,
    )
    assert SpatialLocalizer(uncertainty_calibration=too_small).localize(candidates).uncertainty_radius_m is None

    calibrated = UncertaintyCalibration(
        [CalibrationBin(0.0, 1.0, p90_error_m=80.0, sample_count=50)],
        dataset_id="moscow-held-out-v1",
        minimum_bin_samples=30,
    )
    result = SpatialLocalizer(uncertainty_calibration=calibrated).localize(candidates)
    assert result.uncertainty_radius_m is not None
    assert result.uncertainty_radius_m >= 80.0
    assert result.diagnostics["uncertainty_calibration_dataset"] == "moscow-held-out-v1"


def test_localizer_configuration_validation() -> None:
    with pytest.raises(ValueError, match="smaller"):
        LocalizerConfig(cluster_radius_m=100, max_cluster_diameter_m=50)
    with pytest.raises(ValueError, match="similarity"):
        LocalizerConfig(out_of_coverage_similarity=0.8, confident_similarity=0.5)
