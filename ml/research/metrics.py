"""Raw-first, full-denominator metrics for research and frozen evaluation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np


def errors_array(errors: Sequence[float | None]) -> np.ndarray:
    values = np.asarray([np.inf if e is None else e for e in errors], dtype=float)
    if values.ndim != 1 or not len(values) or np.isnan(values).any() or (values < 0).any():
        raise ValueError("need a nonempty vector of nonnegative errors; missing predictions use None")
    return values


def quantile(values: np.ndarray, q: float) -> float | None:
    if not len(values):
        return None
    # Explicit interpolation avoids inf-inf NaNs when inference fails.
    position = (len(values) - 1) * q
    ordered = np.sort(values)
    low, high = int(np.floor(position)), int(np.ceil(position))
    if not np.isfinite(ordered[high]):
        return None  # JSON-safe infinity, with failure count reported separately.
    return float(ordered[low] + (ordered[high] - ordered[low]) * (position - low))


def raw_metrics(errors: Sequence[float | None]) -> dict[str, Any]:
    values = errors_array(errors)
    return {
        "query_count": len(values),
        **{f"accuracy_{r}m": float(np.mean(values <= r)) for r in (25, 50, 100)},
        "median_error_m": quantile(values, 0.5),
        "p90_error_m": quantile(values, 0.9),
        "catastrophic_gt500m_rate": float(np.mean(values > 500)),
        "missing_prediction_count": int(np.isinf(values).sum()),
        "null_quantile_means": "infinite error due to missing predictions",
    }


def retrieval_metrics(
    ranks: Sequence[int | None],
    *,
    gallery_size: int,
    positive_counts: Sequence[int] | None = None,
) -> dict[str, Any]:
    if not ranks or gallery_size < 1:
        raise ValueError("nonempty queries and gallery required")
    if any(r is not None and (isinstance(r, bool) or int(r) != r or not 1 <= r <= gallery_size) for r in ranks):
        raise ValueError("ranks must be one-based complete-gallery ranks or None for no positive")
    values = np.asarray([np.inf if r is None else r for r in ranks], dtype=float)
    absent = np.isinf(values)
    failed = np.zeros(len(values), dtype=bool)
    if positive_counts is not None:
        counts = np.asarray(positive_counts, dtype=float)
        if (
            counts.shape != values.shape
            or not np.isfinite(counts).all()
            or (counts < 0).any()
            or (counts > gallery_size).any()
            or (counts != np.floor(counts)).any()
        ):
            raise ValueError("spatial-positive counts must align with every query")
        if ((counts == 0) & np.isfinite(values)).any():
            raise ValueError("finite positive rank requires a spatial positive")
        absent = counts == 0
        failed = (counts > 0) & np.isinf(values)
    cuts = (0, 1, 5, 10, 20, 50, 100, 500, 1000, gallery_size)
    cuts = sorted(set(min(c, gallery_size) for c in cuts))
    return {
        "query_count": len(values),
        "gallery_size": gallery_size,
        "recall_at": {str(k): float(np.mean(values <= k)) for k in (1, 5, 10, 20, 50, 100)},
        "no_positive_count": int(absent.sum()),
        "no_positive_rate": float(absent.mean()),
        "retrieval_failure_with_spatial_positive_count": int(failed.sum()),
        "positive_rank_histogram": {
            f"{a + 1}-{b}": int(((values > a) & (values <= b)).sum()) for a, b in zip(cuts, cuts[1:], strict=False)
        },
        "positive_rank_quantiles_covered_only": {
            str(q): quantile(values[~absent], q) for q in (0.25, 0.5, 0.75, 0.9, 0.95)
        },
        "positive_rank_quantiles_all_query": {str(q): quantile(values, q) for q in (0.25, 0.5, 0.75, 0.9, 0.95)},
        "null_rank_quantile_means": "infinite rank from no spatial positive or retrieval failure, or empty covered set",
    }


def product_metrics(errors: Sequence[float | None], accepted: Sequence[bool]) -> dict[str, Any]:
    values = errors_array(errors)
    mask = np.asarray(accepted, dtype=bool)
    if mask.shape != values.shape or (mask & ~np.isfinite(values)).any():
        raise ValueError("acceptance must align with queries and cannot accept a missing prediction")
    selected = values[mask]
    return {
        "query_count": len(values),
        "accepted_count": len(selected),
        "answer_rate": float(mask.mean()),
        "conditional_accuracy_100m": float(np.mean(selected <= 100)) if len(selected) else None,
        "accepted_gt500m_count": int((selected > 500).sum()),
        "accepted_gt500m_rate": float(np.mean(selected > 500)) if len(selected) else None,
        "accepted_median_error_m": quantile(selected, 0.5),
        "accepted_p90_error_m": quantile(selected, 0.9),
    }


def select_threshold(
    errors: Sequence[float | None], scores: Sequence[float], *, precision: float = 0.9
) -> dict[str, Any]:
    """Calibration-only maximum coverage; all equal-score queries move together."""
    values = errors_array(errors)
    confidence = np.asarray(scores, dtype=float)
    if confidence.shape != values.shape or not np.isfinite(confidence).all() or not 0 < precision <= 1:
        raise ValueError("invalid scores or precision")
    # Missing predictions never enter the product, but stay in the raw denominator.
    order = np.flatnonzero(np.isfinite(values))
    order = order[np.argsort(-confidence[order], kind="stable")]
    correct = np.cumsum(values[order] <= 100)
    endpoints = np.flatnonzero(np.r_[confidence[order][1:] != confidence[order][:-1], True]) if len(order) else []
    feasible = [int(i) for i in endpoints if correct[i] / (i + 1) >= precision]
    threshold = float(confidence[order[feasible[-1]]]) if feasible else None
    accepted = np.isfinite(values) & (confidence >= threshold) if threshold is not None else np.zeros(len(values), bool)
    return {"threshold": threshold, "feasible": threshold is not None, **product_metrics(errors, accepted)}


def paired_group_bootstrap(
    baseline_errors: Sequence[float],
    candidate_errors: Sequence[float],
    groups: Sequence[str],
    *,
    resamples: int = 5000,
    seed: int = 20260905,
) -> dict[str, Any]:
    left, right = errors_array(baseline_errors), errors_array(candidate_errors)
    if left.shape != right.shape or len(groups) != len(left) or resamples < 1:
        raise ValueError("paired arrays and groups must align")
    _, inverse = np.unique(groups, return_inverse=True)
    counts = np.bincount(inverse)
    differences = np.bincount(inverse, weights=(right <= 100).astype(float) - (left <= 100))
    rng = np.random.default_rng(seed)
    samples = np.empty(resamples)
    for i in range(resamples):
        draw = rng.integers(len(counts), size=len(counts))
        samples[i] = differences[draw].sum() / counts[draw].sum()
    return {
        "gain_pp": float(100 * differences.sum() / len(left)),
        "ci95_pp": (100 * np.quantile(samples, [0.025, 0.975])).tolist(),
        "groups": len(counts),
        "resamples": resamples,
    }


def geographic_intervals(errors, accepted, groups, *, resamples=5000, seed=20260905):
    """Cluster-bootstrap rate intervals for a fixed policy; zero-answer draws explicit."""
    values = errors_array(errors)
    mask = np.asarray(accepted, dtype=bool)
    if len(groups) != len(values) or mask.shape != values.shape or (mask & ~np.isfinite(values)).any():
        raise ValueError("aligned geographic groups and valid acceptance required")
    _, inverse = np.unique(groups, return_inverse=True)
    names = [
        "accuracy_25m",
        "accuracy_50m",
        "accuracy_100m",
        "catastrophic_gt500m_rate",
        "answer_rate",
        "conditional_accuracy_100m",
        "accepted_gt500m_rate",
    ]
    columns = [
        np.ones(len(values)),
        values <= 25,
        values <= 50,
        values <= 100,
        values > 500,
        mask,
        mask & (values <= 100),
        mask & (values > 500),
    ]
    table = np.stack([np.bincount(inverse, weights=v) for v in columns], axis=1)
    rng = np.random.default_rng(seed)
    draw = rng.integers(len(table), size=(resamples, len(table)))
    sums = table[draw].sum(axis=1)
    rates = [sums[:, i] / sums[:, 0] for i in range(1, 6)]
    nonempty = sums[:, 5] > 0
    rates += [np.divide(sums[:, i], sums[:, 5], out=np.full(resamples, np.nan), where=nonempty) for i in (6, 7)]
    return {
        "method": "geographic cluster bootstrap, fixed policy",
        "geographic_groups": len(table),
        "resamples": resamples,
        "zero_accepted_resample_fraction": float((~nonempty).mean()),
        "ci95": {
            name: np.quantile(x[np.isfinite(x)], [0.025, 0.975]).tolist() if np.isfinite(x).any() else None
            for name, x in zip(names, rates, strict=True)
        },
        "limitation": "does not account for exploratory development winner/threshold selection; fewer than two groups cannot estimate between-group uncertainty",
    }
