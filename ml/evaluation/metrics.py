"""Numerically explicit VPR and localization metrics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np

from ml.localization.geo import haversine_m


@dataclass(frozen=True, slots=True)
class QueryGroundTruth:
    query_id: str
    lat: float
    lon: float
    sequence_id: str | None = None
    captured_at: str | None = None


@dataclass(frozen=True, slots=True)
class RetrievalMetrics:
    query_count: int
    positive_distance_threshold_m: float
    recall_at: Mapping[int, float]
    missing_prediction_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_count": self.query_count,
            "positive_distance_threshold_m": self.positive_distance_threshold_m,
            "recall_at": {str(key): value for key, value in self.recall_at.items()},
            "missing_prediction_count": self.missing_prediction_count,
            "denominator": "all_queries",
        }


@dataclass(frozen=True, slots=True)
class LocalizationObservation:
    query_id: str
    true_lat: float
    true_lon: float
    predicted_lat: float | None
    predicted_lon: float | None
    status: str
    confidence: float


@dataclass(frozen=True, slots=True)
class LocalizationMetrics:
    query_count: int
    answered_count: int
    answer_rate: float
    low_confidence_rate: float
    out_of_coverage_rate: float
    accuracy_within_m: Mapping[int, float]
    conditional_accuracy_within_m: Mapping[int, float]
    median_error_m: float | None
    p90_error_m: float | None
    confidence_buckets: tuple[Mapping[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["accuracy_within_m"] = {str(key): value for key, value in self.accuracy_within_m.items()}
        payload["conditional_accuracy_within_m"] = {
            str(key): value for key, value in self.conditional_accuracy_within_m.items()
        }
        payload["accuracy_denominator"] = "all_queries_unanswered_count_as_failures"
        payload["answered_definition"] = "status_ok_with_complete_coordinates"
        payload["conditional_accuracy_denominator"] = "status_ok_queries"
        payload["error_percentile_population"] = "status_ok_queries"
        return payload


@dataclass(frozen=True, slots=True)
class LatencySummary:
    count: int
    mean_ms: float | None
    median_ms: float | None
    p90_ms: float | None
    p95_ms: float | None
    max_ms: float | None


@dataclass(frozen=True, slots=True)
class LatencyMetrics:
    stages: Mapping[str, LatencySummary]
    gallery_embedding_images_per_second: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stages": {name: asdict(summary) for name, summary in self.stages.items()},
            "gallery_embedding_images_per_second": self.gallery_embedding_images_per_second,
        }


@dataclass(frozen=True, slots=True)
class EvaluationBundle:
    dataset_id: str
    split_name: str
    retrieval: RetrievalMetrics | None = None
    localization: LocalizationMetrics | None = None
    latency: LatencyMetrics | None = None
    model: Mapping[str, Any] = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "split_name": self.split_name,
            "retrieval": None if self.retrieval is None else self.retrieval.to_dict(),
            "localization": None if self.localization is None else self.localization.to_dict(),
            "latency": None if self.latency is None else self.latency.to_dict(),
            "model": dict(self.model),
            "notes": list(self.notes),
        }


def _match_coordinates(value: Any) -> tuple[float, float]:
    if isinstance(value, Mapping):
        if value.get("lat") is not None and value.get("lon") is not None:
            return float(value["lat"]), float(value["lon"])
        metadata = value.get("metadata", {})
        return float(metadata["lat"]), float(metadata["lon"])
    lat = getattr(value, "lat", None)
    lon = getattr(value, "lon", None)
    if lat is None or lon is None:
        metadata = getattr(value, "metadata", {})
        lat, lon = metadata.get("lat"), metadata.get("lon")
    if lat is None or lon is None:
        raise ValueError("retrieval prediction lacks lat/lon")
    return float(lat), float(lon)


def evaluate_retrieval(
    ground_truth: Sequence[QueryGroundTruth],
    predictions: Mapping[str, Sequence[Any]],
    *,
    positive_distance_threshold_m: float,
    ks: Sequence[int] = (1, 5, 10),
) -> RetrievalMetrics:
    """Compute geographic Recall@K over all held-out queries."""

    if positive_distance_threshold_m <= 0:
        raise ValueError("positive_distance_threshold_m must be positive")
    if not ground_truth:
        raise ValueError("ground_truth cannot be empty")
    normalized_ks = tuple(sorted(set(int(k) for k in ks)))
    if not normalized_ks or normalized_ks[0] < 1:
        raise ValueError("ks must contain positive integers")
    query_ids = [query.query_id for query in ground_truth]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("ground truth query IDs must be unique")

    hits = {k: 0 for k in normalized_ks}
    missing = 0
    for query in ground_truth:
        matches = list(predictions.get(query.query_id, ()))
        if not matches:
            missing += 1
        positives: list[bool] = []
        for match in matches[: normalized_ks[-1]]:
            lat, lon = _match_coordinates(match)
            positives.append(haversine_m(query.lat, query.lon, lat, lon) <= positive_distance_threshold_m)
        for k in normalized_ks:
            if any(positives[:k]):
                hits[k] += 1
    count = len(ground_truth)
    return RetrievalMetrics(
        query_count=count,
        positive_distance_threshold_m=positive_distance_threshold_m,
        recall_at={k: hits[k] / count for k in normalized_ks},
        missing_prediction_count=missing,
    )


def evaluate_localization(
    observations: Sequence[LocalizationObservation],
    *,
    thresholds_m: Sequence[int] = (25, 50, 100),
    confidence_bins: Sequence[float] = (0.0, 0.25, 0.5, 0.75, 1.0),
) -> LocalizationMetrics:
    """Compute product accuracy and abstention metrics without hiding failures."""

    if not observations:
        raise ValueError("observations cannot be empty")
    query_ids = [observation.query_id for observation in observations]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("localization observation query IDs must be unique")
    thresholds = tuple(sorted(set(int(value) for value in thresholds_m)))
    if not thresholds or thresholds[0] <= 0:
        raise ValueError("thresholds_m must contain positive values")
    bins = tuple(float(value) for value in confidence_bins)
    if len(bins) < 2 or bins[0] != 0.0 or bins[-1] != 1.0:
        raise ValueError("confidence bins must begin at 0 and end at 1")
    if any(left >= right for left, right in zip(bins, bins[1:], strict=False)):
        raise ValueError("confidence bins must be strictly increasing")

    allowed_statuses = {"ok", "low_confidence", "out_of_coverage"}
    errors_by_query: dict[str, float] = {}
    answered: list[LocalizationObservation] = []
    for observation in observations:
        if observation.status not in allowed_statuses:
            raise ValueError(f"unsupported localization status {observation.status!r} for {observation.query_id}")
        if not 0 <= observation.confidence <= 1:
            raise ValueError(f"confidence outside [0,1] for {observation.query_id}")
        has_lat = observation.predicted_lat is not None
        has_lon = observation.predicted_lon is not None
        if has_lat != has_lon:
            raise ValueError(f"partial predicted coordinate for {observation.query_id}")
        if observation.status == "ok" and not (has_lat and has_lon):
            raise ValueError(f"status 'ok' requires a complete coordinate for {observation.query_id}")
        if observation.status == "out_of_coverage" and (has_lat or has_lon):
            raise ValueError(f"status 'out_of_coverage' cannot carry a coordinate for {observation.query_id}")
        # Product policy: only status=ok is an answered localization. A
        # low-confidence hypothesis may retain coordinates for diagnostics/UI,
        # but it is an abstention in accuracy and error-percentile metrics.
        if observation.status == "ok":
            error = haversine_m(
                observation.true_lat,
                observation.true_lon,
                float(observation.predicted_lat),
                float(observation.predicted_lon),
            )
            errors_by_query[observation.query_id] = error
            answered.append(observation)

    total = len(observations)
    answered_count = len(answered)
    errors = np.asarray(list(errors_by_query.values()), dtype=np.float64)
    all_accuracy = {threshold: float(np.count_nonzero(errors <= threshold) / total) for threshold in thresholds}
    conditional = {
        threshold: (float(np.count_nonzero(errors <= threshold) / answered_count) if answered_count else 0.0)
        for threshold in thresholds
    }
    bucket_rows: list[Mapping[str, Any]] = []
    for index, (lower, upper) in enumerate(zip(bins, bins[1:], strict=False)):
        members = [
            observation
            for observation in observations
            if lower <= observation.confidence < upper or (index == len(bins) - 2 and observation.confidence == upper)
        ]
        member_errors = [errors_by_query[item.query_id] for item in members if item.query_id in errors_by_query]
        bucket_rows.append(
            {
                "min_confidence": lower,
                "max_confidence": upper,
                "query_count": len(members),
                "answered_count": len(member_errors),
                "answer_rate": len(member_errors) / len(members) if members else None,
                "accuracy_within_50m_all_queries": (
                    sum(error <= 50 for error in member_errors) / len(members) if members else None
                ),
                "median_error_m_answered": (float(np.median(member_errors)) if member_errors else None),
            }
        )

    return LocalizationMetrics(
        query_count=total,
        answered_count=answered_count,
        answer_rate=answered_count / total,
        low_confidence_rate=sum(item.status == "low_confidence" for item in observations) / total,
        out_of_coverage_rate=sum(item.status == "out_of_coverage" for item in observations) / total,
        accuracy_within_m=all_accuracy,
        conditional_accuracy_within_m=conditional,
        median_error_m=float(np.median(errors)) if answered_count else None,
        p90_error_m=float(np.percentile(errors, 90)) if answered_count else None,
        confidence_buckets=tuple(bucket_rows),
    )


def _summarize(values: Sequence[float]) -> LatencySummary:
    array = np.asarray(values, dtype=np.float64)
    if array.size == 0:
        return LatencySummary(0, None, None, None, None, None)
    if not np.isfinite(array).all() or np.any(array < 0):
        raise ValueError("latency samples must be finite and non-negative")
    return LatencySummary(
        count=int(array.size),
        mean_ms=float(np.mean(array)),
        median_ms=float(np.median(array)),
        p90_ms=float(np.percentile(array, 90)),
        p95_ms=float(np.percentile(array, 95)),
        max_ms=float(np.max(array)),
    )


def summarize_latencies(
    samples_by_stage: Mapping[str, Sequence[float]],
    *,
    gallery_embedding_images: int | None = None,
    gallery_embedding_seconds: float | None = None,
) -> LatencyMetrics:
    throughput = None
    if gallery_embedding_images is not None or gallery_embedding_seconds is not None:
        if gallery_embedding_images is None or gallery_embedding_seconds is None:
            raise ValueError("both embedding image count and seconds are required")
        if gallery_embedding_images < 0 or gallery_embedding_seconds <= 0:
            raise ValueError("embedding throughput inputs must be count >= 0 and seconds > 0")
        throughput = gallery_embedding_images / gallery_embedding_seconds
    return LatencyMetrics(
        stages={name: _summarize(values) for name, values in samples_by_stage.items()},
        gallery_embedding_images_per_second=throughput,
    )
