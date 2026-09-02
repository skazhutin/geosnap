from __future__ import annotations

import math

from ml.localization.confidence_model import FEATURE_NAMES, ConfidenceModel
from ml.localization.models import Candidate, LocalizationStatus
from ml.localization.product_policy import AggregationStrategy
from ml.localization.product_runtime import ProductRuntimePolicy, ProductSpatialLocalizer


def _model(probability: float) -> ConfidenceModel:
    logit = math.log(probability / (1.0 - probability))
    return ConfidenceModel(
        feature_names=FEATURE_NAMES,
        means=(0.0,) * len(FEATURE_NAMES),
        scales=(1.0,) * len(FEATURE_NAMES),
        coefficients=(0.0,) * len(FEATURE_NAMES),
        intercept=logit,
    )


def _matches() -> list[Candidate]:
    return [
        Candidate(
            reference_id="a",
            lat=55.7,
            lon=37.6,
            retrieval_score=0.8,
            rank=1,
            source="mapillary",
            metadata={"sequence_id": "one", "local_gallery_density_100m": 2},
        ),
        Candidate(
            reference_id="b",
            lat=55.7001,
            lon=37.6001,
            retrieval_score=0.79,
            rank=2,
            source="kartaview",
            metadata={"sequence_id": "two", "local_gallery_density_100m": 2},
        ),
    ]


def test_product_status_threshold_is_frozen_policy() -> None:
    accepted = ProductSpatialLocalizer(
        ProductRuntimePolicy(
            aggregation=AggregationStrategy.SEQUENCE_DEDUPLICATED_VOTE,
            confidence_threshold=0.80,
        ),
        _model(0.90),
    ).localize(_matches())
    rejected = ProductSpatialLocalizer(
        ProductRuntimePolicy(
            aggregation=AggregationStrategy.SEQUENCE_DEDUPLICATED_VOTE,
            confidence_threshold=0.95,
        ),
        _model(0.90),
    ).localize(_matches())

    assert accepted.status is LocalizationStatus.OK
    assert rejected.status is LocalizationStatus.LOW_CONFIDENCE
    assert rejected.reasons == ("confidence_below_threshold",)


def test_product_sequence_support_gate_uses_winning_mode() -> None:
    result = ProductSpatialLocalizer(
        ProductRuntimePolicy(
            aggregation=AggregationStrategy.SEQUENCE_DEDUPLICATED_VOTE,
            confidence_threshold=0.50,
            minimum_cluster_candidates=3,
        ),
        _model(0.90),
    ).localize(_matches())

    assert result.status is LocalizationStatus.LOW_CONFIDENCE
    assert "winning_geographic_mode_has_insufficient_support" in result.reasons
