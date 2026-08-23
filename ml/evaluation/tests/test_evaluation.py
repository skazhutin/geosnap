from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from ml.evaluation import (
    EvaluationBundle,
    LocalizationObservation,
    QueryGroundTruth,
    evaluate_localization,
    evaluate_retrieval,
    generate_robustness_variants,
    summarize_latencies,
    write_evaluation_reports,
)


def test_recall_at_1_5_10_uses_explicit_geographic_positive() -> None:
    truths = [
        QueryGroundTruth("q1", 55.75, 37.61),
        QueryGroundTruth("q2", 55.76, 37.62),
    ]
    predictions = {
        "q1": [
            {"lat": 55.80, "lon": 37.70},
            {"lat": 55.7501, "lon": 37.6101},
        ],
        "q2": [{"lat": 55.76005, "lon": 37.62005}],
    }
    metrics = evaluate_retrieval(
        truths,
        predictions,
        positive_distance_threshold_m=25,
        ks=(1, 5, 10),
    )
    assert metrics.recall_at == {1: 0.5, 5: 1.0, 10: 1.0}
    assert metrics.positive_distance_threshold_m == 25


def test_localization_metrics_count_only_ok_status_as_answered() -> None:
    observations = [
        LocalizationObservation("exact", 55.75, 37.61, 55.75, 37.61, "ok", 0.9),
        LocalizationObservation("near", 55.75, 37.61, 55.7504, 37.61, "low_confidence", 0.4),
        LocalizationObservation("none", 55.75, 37.61, None, None, "out_of_coverage", 0.0),
    ]
    metrics = evaluate_localization(observations)
    assert metrics.query_count == 3
    assert metrics.answered_count == 1
    assert metrics.answer_rate == pytest.approx(1 / 3)
    assert metrics.accuracy_within_m[100] == pytest.approx(1 / 3)
    assert metrics.conditional_accuracy_within_m[100] == 1.0
    assert metrics.median_error_m is not None
    assert metrics.p90_error_m is not None


def test_low_confidence_coordinates_are_abstentions_not_error_samples() -> None:
    observations = [
        LocalizationObservation("ok", 55.75, 37.61, 55.75, 37.61, "ok", 0.9),
        LocalizationObservation(
            "rejected-far",
            55.75,
            37.61,
            56.75,
            38.61,
            "low_confidence",
            0.3,
        ),
    ]
    metrics = evaluate_localization(observations)
    assert metrics.answered_count == 1
    assert metrics.accuracy_within_m[100] == 0.5
    assert metrics.median_error_m == 0.0
    assert metrics.p90_error_m == 0.0
    assert metrics.to_dict()["answered_definition"] == "status_ok_with_complete_coordinates"


def test_latency_structures_and_json_markdown_writer(tmp_path: Path) -> None:
    retrieval = evaluate_retrieval(
        [QueryGroundTruth("q", 55.75, 37.61)],
        {"q": [{"lat": 55.75, "lon": 37.61}]},
        positive_distance_threshold_m=25,
    )
    localization = evaluate_localization([LocalizationObservation("q", 55.75, 37.61, 55.75, 37.61, "ok", 0.8)])
    latency = summarize_latencies(
        {"query_embedding": [10.0, 20.0], "faiss": [1.0, 3.0]},
        gallery_embedding_images=20,
        gallery_embedding_seconds=4,
    )
    bundle = EvaluationBundle(
        dataset_id="fixture-not-moscow-metrics",
        split_name="cross-sequence",
        retrieval=retrieval,
        localization=localization,
        latency=latency,
        model={"name": "fixture"},
    )
    json_path, markdown_path = write_evaluation_reports(bundle, tmp_path)
    payload = json.loads(json_path.read_text())
    assert payload["retrieval"]["recall_at"]["1"] == 1.0
    assert payload["localization"]["accuracy_denominator"].startswith("all_queries")
    markdown = markdown_path.read_text()
    assert "Recall@1" in markdown
    assert "No benchmark or Moscow result is inserted as a default" in markdown
    assert latency.stages["query_embedding"].p90_ms == pytest.approx(19.0)
    assert latency.gallery_embedding_images_per_second == 5.0


def test_real_query_augmentation_helpers_preserve_ground_truth_and_are_deterministic(
    tmp_path: Path,
) -> None:
    source = tmp_path / "real-held-out-query.jpg"
    image = Image.new("RGB", (160, 100), (120, 140, 160))
    # Add stable spatial structure; helpers do not create evaluation ground truth.
    for x in range(20, 140):
        image.putpixel((x, 50), (250, x % 255, 30))
    image.save(source)
    first = generate_robustness_variants(
        source,
        query_id="moscow/query-1",
        lat=55.75,
        lon=37.61,
        output_dir=tmp_path / "variants-a",
        seed=42,
    )
    second = generate_robustness_variants(
        source,
        query_id="moscow/query-1",
        lat=55.75,
        lon=37.61,
        output_dir=tmp_path / "variants-b",
        seed=42,
    )
    assert {item.variant for item in first} == {
        "resize_half",
        "random_crop",
        "jpeg_q45",
        "gaussian_blur",
        "motion_blur",
        "brightness_low",
        "warm_color",
        "perspective",
        "partial_occlusion",
    }
    assert all(item.lat == 55.75 and item.lon == 37.61 for item in first)
    assert all(item.output_path and item.output_path.is_file() for item in first)
    crop_a = next(item for item in first if item.variant == "random_crop")
    crop_b = next(item for item in second if item.variant == "random_crop")
    assert crop_a.parameters == crop_b.parameters
    assert crop_a.image.tobytes() == crop_b.image.tobytes()
