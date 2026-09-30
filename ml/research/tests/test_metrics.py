import numpy as np
import pytest

from ml.research.metrics import (
    paired_group_bootstrap,
    product_metrics,
    raw_metrics,
    retrieval_metrics,
    select_threshold,
)


def test_raw_never_improves_by_abstention_or_missing_prediction():
    metrics = raw_metrics([10, 75, 600, None])
    assert metrics["accuracy_25m"] == 0.25
    assert metrics["accuracy_100m"] == 0.5
    assert metrics["catastrophic_gt500m_rate"] == 0.5
    assert metrics["median_error_m"] == 337.5
    assert metrics["p90_error_m"] is None
    assert metrics["missing_prediction_count"] == 1
    accepted = product_metrics([10, 75, 600, None], [True, True, False, False])
    assert accepted["conditional_accuracy_100m"] == 1
    assert accepted["answer_rate"] == 0.5
    with pytest.raises(ValueError):
        product_metrics([None], [True])


def test_retrieval_no_positive_stays_in_denominator_and_histogram():
    metrics = retrieval_metrics([1, 100, None, 999], gallery_size=1000)
    assert metrics["recall_at"]["1"] == 0.25
    assert metrics["recall_at"]["100"] == 0.5
    assert metrics["no_positive_count"] == 1
    assert sum(metrics["positive_rank_histogram"].values()) == 3
    assert metrics["positive_rank_quantiles_all_query"]["0.9"] is None


def test_retrieval_failure_is_not_mistaken_for_missing_gallery_coverage():
    metrics = retrieval_metrics([1, None, None], gallery_size=1000, positive_counts=[2, 3, 0])
    assert metrics["no_positive_count"] == 1
    assert metrics["retrieval_failure_with_spatial_positive_count"] == 1
    assert metrics["recall_at"]["100"] == 1 / 3
    assert metrics["positive_rank_quantiles_covered_only"]["0.5"] is None
    with pytest.raises(ValueError):
        retrieval_metrics([1], gallery_size=10, positive_counts=[0])


def test_equal_scores_are_inseparable_and_no_answers_is_infeasible():
    metrics = select_threshold([5, 600], [0.8, 0.8])
    assert metrics["threshold"] is None
    assert not metrics["feasible"]
    assert metrics["conditional_accuracy_100m"] is None


def test_threshold_is_maximum_coverage_not_first_feasible_prefix():
    metrics = select_threshold([5] * 9 + [600] + [10] * 10, list(np.linspace(1, 0, 20)))
    assert metrics["accepted_count"] == 20
    assert metrics["conditional_accuracy_100m"] == 0.95
    assert metrics["threshold"] == 0


def test_missing_predictions_cannot_be_accepted_even_with_high_confidence():
    metrics = select_threshold([None, 50], [1.0, 0.1])
    assert metrics["answer_rate"] == 0.5
    assert metrics["conditional_accuracy_100m"] == 1


def test_bootstrap_is_paired_and_groups_are_sampling_units():
    metrics = paired_group_bootstrap([200] * 10, [0] * 10, ["single-group"] * 10, resamples=100)
    assert metrics["groups"] == 1
    assert metrics["gain_pp"] == 100
    assert metrics["ci95_pp"] == [100, 100]


@pytest.mark.parametrize("errors", [[], [-1], [float("nan")]])
def test_invalid_errors_fail(errors):
    with pytest.raises(ValueError):
        raw_metrics(errors)
