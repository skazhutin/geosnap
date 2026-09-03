from __future__ import annotations

import pytest

from ml.localization.confidence_model import FEATURE_NAMES
from ml.localization.models import Candidate
from ml.localization.product_policy import (
    AggregationStrategy,
    ProductAggregationConfig,
    aggregate_geographic_modes,
)


def _candidate(
    reference_id: str,
    *,
    lat: float,
    lon: float,
    score: float,
    rank: int,
    sequence: str,
    source: str = "mapillary",
) -> Candidate:
    return Candidate(
        reference_id=reference_id,
        lat=lat,
        lon=lon,
        retrieval_score=score,
        rank=rank,
        source=source,
        metadata={
            "source": source,
            "sequence_id": sequence,
            "heading": rank * 20.0,
            "local_gallery_density_25m": 1,
            "local_gallery_density_50m": 2,
            "local_gallery_density_100m": 4,
            "captured_at": "2024-01-01T00:00:00Z",
        },
    )


def test_aggregation_is_deterministic_under_input_order() -> None:
    candidates = [
        _candidate("a", lat=55.70, lon=37.60, score=0.8, rank=1, sequence="one"),
        _candidate("b", lat=55.7001, lon=37.6001, score=0.79, rank=2, sequence="two"),
        _candidate("c", lat=55.75, lon=37.65, score=0.78, rank=3, sequence="three"),
    ]
    config = ProductAggregationConfig(strategy=AggregationStrategy.DENSITY_AWARE_MODE_VOTE)

    forward = aggregate_geographic_modes(candidates, config=config)
    reverse = aggregate_geographic_modes(list(reversed(candidates)), config=config)

    assert forward == reverse


def test_sequence_deduplication_prevents_one_burst_from_dominating() -> None:
    burst = [
        _candidate(
            f"burst-{index}",
            lat=55.7000 + index * 0.00001,
            lon=37.6000,
            score=0.90 - index * 0.005,
            rank=index + 1,
            sequence="burst",
        )
        for index in range(5)
    ]
    independent = [
        _candidate(
            "independent-1",
            lat=55.7500,
            lon=37.6500,
            score=0.90,
            rank=6,
            sequence="independent-1",
        ),
        _candidate(
            "independent-2",
            lat=55.7501,
            lon=37.6501,
            score=0.90,
            rank=7,
            sequence="independent-2",
            source="kartaview",
        ),
        _candidate(
            "independent-3",
            lat=55.75005,
            lon=37.65005,
            score=0.90,
            rank=8,
            sequence="independent-3",
        ),
    ]

    legacy = aggregate_geographic_modes(
        burst + independent,
        config=ProductAggregationConfig(strategy=AggregationStrategy.LEGACY_WEIGHTED_MEDOID),
    )
    deduplicated = aggregate_geographic_modes(
        burst + independent,
        config=ProductAggregationConfig(
            strategy=AggregationStrategy.SEQUENCE_DEDUPLICATED_VOTE
        ),
    )

    assert legacy.modes[0].independent_sequences == 1
    assert deduplicated.modes[0].independent_sequences == 3
    assert deduplicated.modes[0].provider_count == 2


def test_feature_construction_is_pixel_free_and_complete() -> None:
    result = aggregate_geographic_modes(
        [
            _candidate("a", lat=55.70, lon=37.60, score=0.8, rank=1, sequence="one"),
            _candidate("b", lat=55.7001, lon=37.6001, score=0.79, rank=2, sequence="two"),
        ],
    )

    assert set(result.features) == set(FEATURE_NAMES)
    assert result.features["independent_sequence_count"] == 2
    assert result.features["effective_independent_support_count"] == pytest.approx(2, abs=0.01)
    assert result.features["winning_cluster_diameter_m"] > 0
