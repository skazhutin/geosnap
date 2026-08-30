"""Bounded geometric-verification ablation on real Moscow street-view data.

This runner is deliberately separate from the frozen Commons proxy ablation.
It consumes the leakage-resistant Parquet outputs produced by
``ml.evaluation.moscow_split``, embeds each image only once, exact-searches
each selected query only once, and feeds the identical retrieval result to a
retrieval-only arm and the production geometric reranker arm.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import sys
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import imagehash
import numpy as np
from PIL import Image, ImageOps, UnidentifiedImageError

from ml.evaluation import moscow_benchmark
from ml.evaluation.commons_benchmark import ExactSearch
from ml.evaluation.metrics import (
    LocalizationObservation,
    QueryGroundTruth,
    evaluate_localization,
    evaluate_retrieval,
    summarize_latencies,
)
from ml.evaluation.verification_ablation import localization_results_from_rerank
from ml.indexing.faiss_index import RetrievalResult
from ml.localization import LocalizerConfig, SpatialLocalizer, haversine_m
from ml.localization.estimators import CoordinateEstimator
from ml.query_quality import measure_query_image_quality
from ml.retrieval import BaseRetriever, create_retriever
from ml.retrieval.base import l2_normalize
from ml.verification import (
    GeometricReranker,
    RerankResult,
    VerificationCandidate,
    build_verifier,
)
from ml.verification.reranker import MAX_VERIFY_TOP_K, VerificationConfig

REPORT_SCHEMA_VERSION = 1
ABLATION_KIND = "real_moscow_geometric_verification_ablation"
DEFAULT_MAX_QUERIES = 100
MAX_QUERY_SUBSET = 200
RECALL_KS = (1, 5, 10)
LOCALIZATION_THRESHOLDS_M = (25, 50, 100)
_PHASH_PATTERN = re.compile(r"[0-9a-f]{16}")


class MoscowVerificationAblationError(moscow_benchmark.MoscowBenchmarkError):
    """A real-data ablation precondition or runtime invariant failed."""


class BoundedReranker(Protocol):
    config: VerificationConfig

    def rerank(
        self,
        query_image: Any,
        candidates: Sequence[VerificationCandidate],
    ) -> RerankResult: ...


def _optional_text(value: Any) -> str | None:
    return moscow_benchmark._optional_text(value)


def _area_key(row: moscow_benchmark.ManifestRow) -> str:
    for column in moscow_benchmark.AREA_COLUMNS:
        if (value := _optional_text(row.values.get(column))) is not None:
            return f"{column}:{value}"
    return "unassigned"


def _select_bounded_queries(
    rows: Sequence[moscow_benchmark.ManifestRow],
    *,
    max_queries: int,
) -> tuple[moscow_benchmark.ManifestRow, ...]:
    """Select a deterministic area-round-robin subset for broad coverage."""

    if not 1 <= max_queries <= MAX_QUERY_SUBSET:
        raise ValueError(f"max_queries must be in [1, {MAX_QUERY_SUBSET}]")
    buckets: dict[str, dict[str, deque[moscow_benchmark.ManifestRow]]] = defaultdict(
        lambda: defaultdict(deque)
    )
    for row in sorted(rows, key=lambda item: (_area_key(item), item.reference_id)):
        sequence = "::".join(
            (
                _optional_text(row.values.get("source")) or "unknown",
                _optional_text(row.values.get("sequence_id")) or "missing",
            )
        )
        buckets[_area_key(row)][sequence].append(row)
    sequence_queues = {
        area: deque(sorted(sequence_buckets))
        for area, sequence_buckets in buckets.items()
    }
    selected: list[moscow_benchmark.ManifestRow] = []
    active = deque(sorted(buckets))
    while active and len(selected) < min(max_queries, len(rows)):
        area = active.popleft()
        sequence = sequence_queues[area].popleft()
        selected.append(buckets[area][sequence].popleft())
        if buckets[area][sequence]:
            sequence_queues[area].append(sequence)
        if sequence_queues[area]:
            active.append(area)
    return tuple(selected)


def _actual_phash(row: moscow_benchmark.ManifestRow) -> str:
    try:
        with Image.open(row.image_path) as image:
            return str(imagehash.phash(image.convert("RGB"), hash_size=8))
    except Exception as exc:
        raise MoscowVerificationAblationError(
            f"cannot compute perceptual_hash for {row.reference_id!r}: {row.image_path}"
        ) from exc


def _validated_phashes(
    rows: Sequence[moscow_benchmark.ManifestRow],
    *,
    split_name: str,
) -> dict[str, int]:
    values: dict[str, int] = {}
    for row in rows:
        declared = _optional_text(row.values.get("perceptual_hash"))
        if declared is None or _PHASH_PATTERN.fullmatch(declared.lower()) is None:
            raise MoscowVerificationAblationError(
                f"{split_name} row {row.reference_id!r} lacks a valid 64-bit perceptual_hash"
            )
        actual = _actual_phash(row)
        if declared.lower() != actual:
            raise MoscowVerificationAblationError(
                f"{split_name} row {row.reference_id!r} declared perceptual_hash does not match image bytes"
            )
        values[row.reference_id] = int(actual, 16)
    return values


def _nearest_gallery_distance(
    query: moscow_benchmark.ManifestRow,
    gallery: Sequence[moscow_benchmark.ManifestRow],
) -> float:
    return min(
        haversine_m(
            float(query.values["lat"]),
            float(query.values["lon"]),
            float(reference.values["lat"]),
            float(reference.values["lon"]),
        )
        for reference in gallery
    )


def _preflight_split_artifacts(
    loaded: moscow_benchmark.LoadedMoscowBenchmark,
    *,
    phash_distance_threshold: int,
    positive_distance_threshold_m: float,
) -> dict[str, Any]:
    """Fail closed on missing split evidence, near duplicates, or no positives."""

    if not 0 <= phash_distance_threshold <= 64:
        raise ValueError("phash_distance_threshold must be in [0, 64]")
    if positive_distance_threshold_m <= 0:
        raise ValueError("positive_distance_threshold_m must be positive")

    required = {"evaluation_split", "file_sha256", "perceptual_hash", "sequence_id"}
    for name, manifest in (("gallery", loaded.gallery), ("query", loaded.queries)):
        missing = sorted(required.difference(manifest.columns))
        if missing:
            raise MoscowVerificationAblationError(
                f"{name} manifest is not a Moscow split artifact; missing columns: {missing}"
            )
        for row in manifest.rows:
            for column in ("file_sha256", "sequence_id"):
                if _optional_text(row.values.get(column)) is None:
                    raise MoscowVerificationAblationError(
                        f"{name} row {row.reference_id!r} has blank {column}; leakage cannot be audited"
                    )

    gallery_labels = {
        _optional_text(row.values.get("evaluation_split")) for row in loaded.gallery.rows
    }
    query_labels = {
        _optional_text(row.values.get("evaluation_split")) for row in loaded.queries.rows
    }
    if gallery_labels != {"gallery"}:
        raise MoscowVerificationAblationError(
            f"gallery evaluation_split must be exactly 'gallery', got {sorted(str(v) for v in gallery_labels)}"
        )
    if query_labels != {"calibration"}:
        raise MoscowVerificationAblationError(
            "verification selection is calibration-only; query evaluation_split "
            "must be exactly 'calibration', got "
            f"{sorted(str(v) for v in query_labels)}"
        )

    gallery_phashes = _validated_phashes(loaded.gallery.rows, split_name="gallery")
    query_phashes = _validated_phashes(loaded.queries.rows, split_name="query")
    near_pair_count = 0
    near_examples: list[dict[str, Any]] = []
    for query_id, query_hash in query_phashes.items():
        for reference_id, reference_hash in gallery_phashes.items():
            distance = (query_hash ^ reference_hash).bit_count()
            if distance <= phash_distance_threshold:
                near_pair_count += 1
                if len(near_examples) < 20:
                    near_examples.append(
                        {
                            "query_id": query_id,
                            "reference_id": reference_id,
                            "hamming_distance": distance,
                        }
                    )
    if near_pair_count:
        raise MoscowVerificationAblationError(
            "query/gallery perceptual-hash leakage detected; refusing ablation: "
            f"{near_pair_count} pair(s) at distance <= {phash_distance_threshold}; "
            f"examples={json.dumps(near_examples, sort_keys=True)}"
        )

    nearest_distances: list[float] = []
    inconsistent_declared_distances: list[str] = []
    for query in loaded.queries.rows:
        distance = _nearest_gallery_distance(query, loaded.gallery.rows)
        nearest_distances.append(distance)
        if distance > positive_distance_threshold_m:
            raise MoscowVerificationAblationError(
                f"query {query.reference_id!r} has no gallery positive within "
                f"{positive_distance_threshold_m:g} m (nearest={distance:.3f} m)"
            )
        declared = query.values.get("nearest_gallery_distance_m")
        if declared is not None:
            try:
                declared_value = float(declared)
            except (TypeError, ValueError):
                inconsistent_declared_distances.append(query.reference_id)
            else:
                if not np.isfinite(declared_value) or abs(declared_value - distance) > 1.0:
                    inconsistent_declared_distances.append(query.reference_id)
    if inconsistent_declared_distances:
        raise MoscowVerificationAblationError(
            "query nearest_gallery_distance_m does not match manifest coordinates for: "
            f"{inconsistent_declared_distances[:20]}"
        )

    distances = np.asarray(nearest_distances, dtype=np.float64)
    return {
        "passed": True,
        "policy": (
            "canonical_images_and_declared_sha_verified; whole_provider_sequences_disjoint; "
            "declared_phash_recomputed_from_bytes; cross_split_phash_hamming_distance_exceeds_threshold; "
            "every_query_has_geographic_gallery_positive"
        ),
        "required_split_columns": sorted(required),
        "gallery_evaluation_split": sorted(str(value) for value in gallery_labels),
        "query_evaluation_split": sorted(str(value) for value in query_labels),
        "phash_distance_threshold": phash_distance_threshold,
        "phash_near_duplicate_pair_count": 0,
        "nearest_gallery_distance_m": {
            "minimum": float(distances.min()),
            "median": float(np.median(distances)),
            "maximum": float(distances.max()),
            "positive_threshold": positive_distance_threshold_m,
        },
    }


def _verification_candidates(
    matches: Sequence[RetrievalResult],
    gallery_images: Mapping[str, Path],
) -> list[VerificationCandidate]:
    candidates: list[VerificationCandidate] = []
    for match in matches:
        image_path = gallery_images.get(match.reference_id)
        if image_path is None:
            raise MoscowVerificationAblationError(
                f"exact search returned unknown gallery ID {match.reference_id!r}"
            )
        candidates.append(
            VerificationCandidate(
                reference_id=match.reference_id,
                image=image_path,
                retrieval_score=match.score,
                original_rank=match.rank,
                metadata=match.metadata,
            )
        )
    return candidates


def _verifier_runtime_metadata(verifier: Any, *, injected: bool) -> dict[str, Any]:
    """Serialize material verifier settings used by this exact run."""

    metadata: dict[str, Any] = {
        "injected": injected,
        "class": f"{type(verifier).__module__}.{type(verifier).__qualname__}",
        "backend": getattr(verifier, "backend_name", None),
        "max_pairs": getattr(verifier, "max_pairs", None),
    }
    config = getattr(verifier, "config", None)
    if config is not None:
        metadata["extractor_and_geometry_config"] = (
            asdict(config) if is_dataclass(config) else repr(config)
        )
    else:
        for name in (
            "max_keypoints",
            "max_image_edge",
            "requested_device",
            "device",
            "ransac",
        ):
            value = getattr(verifier, name, None)
            if value is not None:
                metadata[name] = asdict(value) if is_dataclass(value) else value
    backend = str(metadata.get("backend") or "")
    if backend == "opencv_sift":
        metadata["package"] = {
            "name": "opencv-python-headless",
            "version": moscow_benchmark._package_version("opencv-python-headless"),
            "weights": None,
        }
    elif backend == "lightglue_sift":
        metadata["package"] = {
            "name": "lightglue",
            "version": moscow_benchmark._package_version("lightglue"),
            "weights": "official LightGlue(features='sift') package-provided pretrained weights",
        }
        metadata["loaded_device"] = getattr(verifier, "device", None)
    return moscow_benchmark._json_safe(metadata)


def _reranker_runtime_metadata(reranker: Any) -> dict[str, Any]:
    """Best-effort metadata for an injected test/custom reranker."""

    verifier = getattr(reranker, "_verifier", None)
    if verifier is not None:
        return _verifier_runtime_metadata(verifier, injected=True)
    return {
        "injected": True,
        "class": f"{type(reranker).__module__}.{type(reranker).__qualname__}",
        "backend": getattr(getattr(reranker, "config", None), "backend", None),
        "verifier_details_available": False,
    }


def _observation(
    query: moscow_benchmark.ManifestRow,
    result: Any,
) -> LocalizationObservation:
    return LocalizationObservation(
        query_id=query.reference_id,
        true_lat=float(query.values["lat"]),
        true_lon=float(query.values["lon"]),
        predicted_lat=result.lat,
        predicted_lon=result.lon,
        status=result.status.value,
        confidence=float(result.confidence),
    )


def _localization_payload(
    query: moscow_benchmark.ManifestRow,
    result: Any,
) -> dict[str, Any]:
    error_m = (
        None
        if result.lat is None or result.lon is None
        else haversine_m(
            float(query.values["lat"]),
            float(query.values["lon"]),
            float(result.lat),
            float(result.lon),
        )
    )
    return {
        "predicted_lat": result.lat,
        "predicted_lon": result.lon,
        "error_m": error_m,
        "status": result.status.value,
        "confidence": float(result.confidence),
        "reasons": list(result.reasons),
    }


def _arm_metrics(
    *,
    truths: Sequence[QueryGroundTruth],
    predictions: Mapping[str, Sequence[RetrievalResult]],
    observations: Sequence[LocalizationObservation],
    per_query_localization: Sequence[Mapping[str, Any]],
    timings: Mapping[str, Sequence[float]],
    confidence_threshold: float,
    positive_distance_threshold_m: float,
) -> dict[str, Any]:
    localization = evaluate_localization(
        observations,
        thresholds_m=LOCALIZATION_THRESHOLDS_M,
    ).to_dict()
    return {
        "retrieval": evaluate_retrieval(
            truths,
            predictions,
            positive_distance_threshold_m=positive_distance_threshold_m,
            ks=RECALL_KS,
        ).to_dict(),
        "localization": localization,
        "confidence_buckets": localization["confidence_buckets"],
        "false_confident_errors": moscow_benchmark._false_confident_errors(
            per_query_localization,
            confidence_threshold=confidence_threshold,
        ),
        "latency": summarize_latencies(timings).to_dict(),
    }


def _numeric_delta(after: Mapping[str, Any], before: Mapping[str, Any]) -> dict[str, float]:
    if set(after) != set(before):
        raise MoscowVerificationAblationError("metric arms have different keys")
    return {key: float(after[key]) - float(before[key]) for key in after}


def _paired_latency_delta(
    after_samples: Sequence[float],
    before_samples: Sequence[float],
) -> dict[str, Any]:
    """Summarize paired per-query overhead, never a difference of percentiles."""

    after = np.asarray(after_samples, dtype=np.float64)
    before = np.asarray(before_samples, dtype=np.float64)
    if after.shape != before.shape or after.ndim != 1:
        raise MoscowVerificationAblationError(
            "paired latency arms must be one-dimensional arrays of equal length"
        )
    if after.size == 0 or not np.isfinite(after).all() or not np.isfinite(before).all():
        raise MoscowVerificationAblationError(
            "paired latency arms must contain finite per-query samples"
        )
    if np.any(after < 0) or np.any(before < 0):
        raise MoscowVerificationAblationError("latency samples cannot be negative")
    overhead = after - before
    positive_baseline = before > 0
    overhead_ratios = overhead[positive_baseline] / before[positive_baseline]
    return {
        "paired": True,
        "definition": "per_query_verified_pipeline_ms_minus_same_query_retrieval_only_pipeline_ms",
        "count": int(overhead.size),
        "mean_ms": float(np.mean(overhead)),
        "median_ms": float(np.median(overhead)),
        "p90_ms": float(np.percentile(overhead, 90)),
        "p95_ms": float(np.percentile(overhead, 95)),
        "min_ms": float(np.min(overhead)),
        "max_ms": float(np.max(overhead)),
        "median_overhead_ratio": (
            None if overhead_ratios.size == 0 else float(np.median(overhead_ratios))
        ),
    }


def _selection_profile(rows: Sequence[moscow_benchmark.ManifestRow]) -> dict[str, Any]:
    profile = {
        "selected_count": len(rows),
        "area_counts": dict(sorted(Counter(_area_key(row) for row in rows).items())),
        "source_counts": dict(
            sorted(Counter(str(row.values.get("source")) for row in rows).items())
        ),
        "sequence_count": len(
            {
                (
                    _optional_text(row.values.get("source")),
                    _optional_text(row.values.get("sequence_id")),
                )
                for row in rows
            }
        ),
        "ordered_query_ids_sha256": hashlib.sha256(
            "\n".join(row.reference_id for row in rows).encode("utf-8")
        ).hexdigest(),
    }
    profile["area_count"] = len(profile["area_counts"])
    return profile


def _paired_accuracy_evidence(
    per_query: Sequence[Mapping[str, Any]],
    *,
    threshold_m: float = 100.0,
    bootstrap_samples: int = 10_000,
    seed: int = 20260824,
) -> dict[str, Any]:
    """Deterministic paired bootstrap CI for verification accuracy gain."""

    if not per_query:
        raise MoscowVerificationAblationError("paired accuracy evidence requires queries")
    if bootstrap_samples < 1:
        raise ValueError("bootstrap_samples must be positive")

    differences: list[int] = []
    differences_by_sequence: dict[tuple[str, str], list[int]] = defaultdict(list)
    transitions: Counter[str] = Counter()
    for row in per_query:
        before = row["retrieval_only"]["localization"]
        after = row["retrieval_plus_verification"]["localization"]

        def correct(value: Mapping[str, Any]) -> bool:
            error = value.get("error_m")
            return (
                value.get("status") == "ok"
                and error is not None
                and float(error) <= threshold_m
            )

        before_correct = correct(before)
        after_correct = correct(after)
        difference = int(after_correct) - int(before_correct)
        differences.append(difference)
        source = _optional_text(row.get("source"))
        sequence_id = _optional_text(row.get("sequence_id"))
        if source is None or sequence_id is None:
            raise MoscowVerificationAblationError(
                "paired accuracy cluster bootstrap requires source and sequence_id for every query"
            )
        differences_by_sequence[(source.lower(), sequence_id)].append(difference)
        transitions[f"{int(before_correct)}_to_{int(after_correct)}"] += 1

    values = np.asarray(differences, dtype=np.float64)
    cluster_values = np.asarray(
        [
            np.mean(differences_by_sequence[key])
            for key in sorted(differences_by_sequence)
        ],
        dtype=np.float64,
    )
    rng = np.random.default_rng(seed)
    indices = rng.integers(
        0,
        len(cluster_values),
        size=(bootstrap_samples, len(cluster_values)),
    )
    sampled_gains = cluster_values[indices].mean(axis=1)
    return {
        "paired": True,
        "threshold_m": threshold_m,
        "query_count": len(values),
        "provider_sequence_count": len(cluster_values),
        "observed_query_weighted_gain": float(values.mean()),
        "observed_sequence_weighted_gain": float(cluster_values.mean()),
        "bootstrap": {
            "method": "paired_provider_sequence_cluster_nonparametric_percentile",
            "sampling_unit": "source_and_sequence_id",
            "samples": bootstrap_samples,
            "seed": seed,
            "confidence_level": 0.95,
            "lower": float(np.percentile(sampled_gains, 2.5)),
            "upper": float(np.percentile(sampled_gains, 97.5)),
        },
        "transitions": dict(sorted(transitions.items())),
    }


def _answered_error_not_worse(after: Any, before: Any) -> bool:
    if before is None:
        return after is None or float(after) >= 0
    return after is not None and float(after) <= float(before)


def _false_confident_error_sum(arm: Mapping[str, Any]) -> float:
    return sum(
        float(row["error_m"])
        for row in arm["false_confident_errors"]["errors"]
    )


def _decision(
    *,
    retrieval_only: Mapping[str, Any],
    verified: Mapping[str, Any],
    comparison: Mapping[str, Any],
    selection_profile: Mapping[str, Any],
    minimum_accuracy_gain: float,
    maximum_median_overhead_ms: float,
    minimum_queries_for_enablement: int,
    minimum_sequences_for_enablement: int,
    minimum_areas_for_enablement: int,
) -> dict[str, Any]:
    accuracy_gain = float(
        comparison["accuracy_within_m"]["100"]
    )
    false_confident_delta = int(
        verified["false_confident_errors"]["count"]
        - retrieval_only["false_confident_errors"]["count"]
    )
    median_overhead = comparison["offline_query_pipeline_latency_delta_ms"]["median_ms"]
    paired_accuracy = comparison["paired_accuracy_at_100m"]
    bootstrap_lower = float(paired_accuracy["bootstrap"]["lower"])
    false_confident_severity_delta = (
        _false_confident_error_sum(verified)
        - _false_confident_error_sum(retrieval_only)
    )
    criteria = {
        "query_count_sufficient": (
            int(selection_profile["selected_count"]) >= minimum_queries_for_enablement
        ),
        "provider_sequence_diversity_sufficient": (
            int(selection_profile["sequence_count"]) >= minimum_sequences_for_enablement
        ),
        "geographic_area_diversity_sufficient": (
            int(selection_profile["area_count"]) >= minimum_areas_for_enablement
        ),
        "accuracy_at_100m_gain_sufficient": accuracy_gain >= minimum_accuracy_gain,
        "paired_accuracy_gain_ci_excludes_zero": bootstrap_lower > 0.0,
        "recall_at_1_not_worse": comparison["recall_at"]["1"] >= 0.0,
        "recall_at_5_not_worse": comparison["recall_at"]["5"] >= 0.0,
        "recall_at_10_not_worse": comparison["recall_at"]["10"] >= 0.0,
        "accuracy_at_25m_not_worse": comparison["accuracy_within_m"]["25"] >= 0.0,
        "accuracy_at_50m_not_worse": comparison["accuracy_within_m"]["50"] >= 0.0,
        "answer_rate_not_worse": comparison["answer_rate"] >= 0.0,
        "median_answered_error_not_worse": _answered_error_not_worse(
            verified["localization"]["median_error_m"],
            retrieval_only["localization"]["median_error_m"],
        ),
        "p90_answered_error_not_worse": _answered_error_not_worse(
            verified["localization"]["p90_error_m"],
            retrieval_only["localization"]["p90_error_m"],
        ),
        "false_confident_errors_not_increased": false_confident_delta <= 0,
        "false_confident_error_severity_not_increased": false_confident_severity_delta <= 0.0,
        "median_latency_overhead_within_budget": (
            median_overhead is not None and median_overhead <= maximum_median_overhead_ms
        ),
    }
    candidate = all(criteria.values())
    evidence_limited = (
        accuracy_gain >= minimum_accuracy_gain
        and (
            not criteria["paired_accuracy_gain_ci_excludes_zero"]
            or not criteria["query_count_sufficient"]
            or not criteria["provider_sequence_diversity_sufficient"]
            or not criteria["geographic_area_diversity_sufficient"]
        )
    )
    recommendation = "keep_verification_default_off"
    if candidate:
        recommendation = "consider_enabling_only_after_product_level_confirmation"
    elif evidence_limited:
        recommendation = "insufficient_evidence_keep_verification_default_off"
    return {
        "recommendation": recommendation,
        "configuration_change_applied": False,
        "criteria": criteria,
        "thresholds": {
            "minimum_accuracy_at_100m_gain": minimum_accuracy_gain,
            "maximum_median_latency_overhead_ms": maximum_median_overhead_ms,
            "minimum_queries_for_enablement": minimum_queries_for_enablement,
            "minimum_provider_sequences_for_enablement": minimum_sequences_for_enablement,
            "minimum_geographic_areas_for_enablement": minimum_areas_for_enablement,
        },
        "observed": {
            "accuracy_at_100m_gain": accuracy_gain,
            "false_confident_error_count_delta": false_confident_delta,
            "false_confident_error_sum_m_delta": false_confident_severity_delta,
            "median_latency_overhead_ms": median_overhead,
            "paired_accuracy_at_100m": paired_accuracy,
            "query_subset": dict(selection_profile),
        },
        "interpretation": (
            "This offline ablation records evidence only. It never changes the production "
            "verification default or environment configuration."
        ),
    }


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def _meters(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f} m"


def _markdown_report(payload: Mapping[str, Any]) -> str:
    baseline = payload["arms"]["retrieval_only"]
    verified = payload["arms"]["retrieval_plus_verification"]
    comparison = payload["comparison"]["retrieval_plus_verification_minus_retrieval_only"]
    selection = payload["dataset"]["query_subset"]
    preflight = payload["leakage_audit"]["verification_ablation_preflight"]
    lines = [
        f"# Real Moscow verification ablation — {payload['model']['model_name']}",
        "",
        "> Primary metrics use only original, leakage-clean real street-view calibration queries. "
        "Both arms share each query embedding and exact-search result.",
        "",
        "## Dataset and fail-closed preflight",
        "",
        f"- Gallery: `{payload['dataset']['gallery']['manifest_path']}` "
        f"({payload['dataset']['gallery']['count']} rows)",
        f"- Queries: `{payload['dataset']['queries']['manifest_path']}` "
        f"({payload['dataset']['queries']['count']} rows; {selection['selected_count']} selected)",
        f"- Area-balanced deterministic subset: `{json.dumps(selection['area_counts'], sort_keys=True)}`",
        f"- Base leakage audit: **{'PASS' if payload['leakage_audit']['base']['passed'] else 'FAIL'}**",
        f"- pHash/positive preflight: **{'PASS' if preflight['passed'] else 'FAIL'}**; "
        f"threshold `{preflight['phash_distance_threshold']}` bits",
        f"- Dataset fingerprint: `{payload['dataset']['fingerprint_sha256']}`",
        f"- Verification backend / top-K / geometric weight: "
        f"`{payload['config']['verification']['backend']}` / "
        f"{payload['config']['verification']['verify_top_k']} / "
        f"{payload['config']['verification']['geometric_weight']}",
        "",
        "## Primary geographic metrics",
        "",
        "| Metric | Retrieval only | + verification | Delta |",
        "|---|---:|---:|---:|",
    ]
    for key in ("1", "5", "10"):
        before = baseline["retrieval"]["recall_at"][key]
        after = verified["retrieval"]["recall_at"][key]
        lines.append(f"| Recall@{key} | {_percent(before)} | {_percent(after)} | {_percent(after - before)} |")
    for key in ("25", "50", "100"):
        before = baseline["localization"]["accuracy_within_m"][key]
        after = verified["localization"]["accuracy_within_m"][key]
        lines.append(
            f"| Localization <= {key} m | {_percent(before)} | {_percent(after)} | "
            f"{_percent(after - before)} |"
        )
    lines.extend(
        [
            f"| Answer rate | {_percent(baseline['localization']['answer_rate'])} | "
            f"{_percent(verified['localization']['answer_rate'])} | "
            f"{_percent(comparison['answer_rate'])} |",
            f"| Median error (answered) | {_meters(baseline['localization']['median_error_m'])} | "
            f"{_meters(verified['localization']['median_error_m'])} | — |",
            f"| P90 error (answered) | {_meters(baseline['localization']['p90_error_m'])} | "
            f"{_meters(verified['localization']['p90_error_m'])} | — |",
            f"| False-confident errors | {baseline['false_confident_errors']['count']} | "
            f"{verified['false_confident_errors']['count']} | "
            f"{comparison['false_confident_error_count']} |",
            "",
            "## Latency",
            "",
            "| Pipeline | Median ms | P90 ms |",
            "|---|---:|---:|",
        ]
    )
    for name, arm in (("Retrieval only", baseline), ("Retrieval + verification", verified)):
        stage = arm["latency"]["stages"]["offline_query_pipeline"]
        lines.append(f"| {name} | {stage['median_ms']:.2f} | {stage['p90_ms']:.2f} |")
    latency_delta = comparison["offline_query_pipeline_latency_delta_ms"]
    lines.extend(
        [
            f"| Verification overhead | {latency_delta['median_ms']:.2f} | {latency_delta['p90_ms']:.2f} |",
            "",
            "## Decision",
            "",
            f"**{payload['decision']['recommendation']}**",
            "",
            payload["decision"]["interpretation"],
            "",
            "The production default was not modified by this runner.",
            "",
        ]
    )
    return "\n".join(lines)


def write_reports(
    payload: Mapping[str, Any],
    output_dir: str | Path,
    *,
    stem: str,
) -> tuple[Path, Path]:
    if not stem or Path(stem).name != stem:
        raise ValueError("stem must be a non-empty filename stem")
    destination = Path(output_dir)
    safe_payload = moscow_benchmark._json_safe(payload)
    json_path = destination / f"{stem}.json"
    markdown_path = destination / f"{stem}.md"
    _atomic_text(
        json_path,
        json.dumps(safe_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    _atomic_text(markdown_path, _markdown_report(safe_payload))
    return json_path, markdown_path


def run_moscow_verification_ablation(
    *,
    gallery_manifest_path: str | Path,
    query_manifest_path: str | Path,
    retriever: BaseRetriever,
    output_dir: str | Path,
    expected_model: str | None = None,
    report_stem: str | None = None,
    top_k: int = 10,
    max_queries: int = DEFAULT_MAX_QUERIES,
    positive_distance_threshold_m: float = 100.0,
    phash_distance_threshold: int = 4,
    estimator: CoordinateEstimator | str = CoordinateEstimator.WEIGHTED_MEDOID,
    confidence_threshold: float = LocalizerConfig().confidence_threshold,
    verification_config: VerificationConfig | None = None,
    minimum_accuracy_gain: float = 0.02,
    maximum_median_overhead_ms: float = 250.0,
    minimum_queries_for_enablement: int = 30,
    minimum_sequences_for_enablement: int = 10,
    minimum_areas_for_enablement: int = 4,
    search_factory: Callable[[], ExactSearch] = moscow_benchmark._real_moscow_exact_search,
    localizer: SpatialLocalizer | None = None,
    reranker: BoundedReranker | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    """Compare both arms over one shared embedding/search pass."""

    if top_k < max(RECALL_KS):
        raise ValueError(f"top_k must be at least {max(RECALL_KS)} to report Recall@10")
    if not 1 <= max_queries <= MAX_QUERY_SUBSET:
        raise ValueError(f"max_queries must be in [1, {MAX_QUERY_SUBSET}]")
    if not 0 <= confidence_threshold <= 1:
        raise ValueError("confidence_threshold must be in [0, 1]")
    if not 0 <= minimum_accuracy_gain <= 1:
        raise ValueError("minimum_accuracy_gain must be in [0, 1]")
    if maximum_median_overhead_ms < 0:
        raise ValueError("maximum_median_overhead_ms must be non-negative")
    if minimum_queries_for_enablement < 1:
        raise ValueError("minimum_queries_for_enablement must be positive")
    if minimum_sequences_for_enablement < 1:
        raise ValueError("minimum_sequences_for_enablement must be positive")
    if minimum_areas_for_enablement < 1:
        raise ValueError("minimum_areas_for_enablement must be positive")

    config = verification_config or VerificationConfig(
        enabled=True,
        backend="opencv_sift",
        verify_top_k=10,
        geometric_weight=0.35,
    )
    if not config.enabled:
        raise ValueError("verification_config must be enabled for the ablation")
    if config.verify_top_k > top_k:
        raise ValueError("verify_top_k cannot exceed top_k")
    moscow_benchmark._validate_expected_model(retriever, expected_model)

    # Every data and leakage check runs before model initialization.
    loaded = moscow_benchmark.load_moscow_benchmark(
        gallery_manifest_path,
        query_manifest_path,
    )
    if len(loaded.gallery.rows) < max(RECALL_KS):
        raise MoscowVerificationAblationError(
            "gallery must contain at least 10 references to report Recall@10"
        )
    if top_k > len(loaded.gallery.rows):
        raise MoscowVerificationAblationError("top_k cannot exceed gallery size")
    preflight = _preflight_split_artifacts(
        loaded,
        phash_distance_threshold=phash_distance_threshold,
        positive_distance_threshold_m=positive_distance_threshold_m,
    )
    selected_queries = _select_bounded_queries(loaded.queries.rows, max_queries=max_queries)
    if not selected_queries:
        raise MoscowVerificationAblationError("query manifest is empty")

    estimator_value = CoordinateEstimator(estimator)
    requested_localizer_config = LocalizerConfig(
        estimator=estimator_value,
        confidence_threshold=confidence_threshold,
    )
    active_localizer = localizer or SpatialLocalizer(requested_localizer_config)
    if (
        active_localizer.config.estimator != estimator_value
        or active_localizer.config.confidence_threshold != confidence_threshold
    ):
        raise MoscowVerificationAblationError(
            "injected localizer does not match requested estimator/confidence threshold"
        )
    if reranker is not None and reranker.config != config:
        raise MoscowVerificationAblationError(
            "injected reranker config does not match verification_config"
        )

    truths: list[QueryGroundTruth] = []
    baseline_predictions: dict[str, Sequence[RetrievalResult]] = {}
    verified_predictions: dict[str, Sequence[RetrievalResult]] = {}
    baseline_observations: list[LocalizationObservation] = []
    verified_observations: list[LocalizationObservation] = []
    baseline_localizations: list[dict[str, Any]] = []
    verified_localizations: list[dict[str, Any]] = []
    per_query: list[dict[str, Any]] = []
    shared_timings: dict[str, list[float]] = {
        "query_preprocessing": [],
        "query_embedding": [],
        "exact_faiss_search": [],
    }
    baseline_timings: dict[str, list[float]] = {
        "spatial_localization": [],
        "offline_query_pipeline": [],
    }
    verified_timings: dict[str, list[float]] = {
        "geometric_verification_and_rerank": [],
        "geometric_verification_backend": [],
        "spatial_localization": [],
        "offline_query_pipeline": [],
    }
    gallery_images = {row.reference_id: row.image_path for row in loaded.gallery.rows}

    # Start isolated FAISS before loading Torch/OpenMP in this process.
    with search_factory() as exact_search:
        verification_setup_started = time.perf_counter()
        if reranker is None:
            verifier = build_verifier(config)
            loader = getattr(verifier, "load", None)
            if loader is not None:
                loader()
            active_reranker = GeometricReranker(config, verifier=verifier)
            verifier_metadata = _verifier_runtime_metadata(verifier, injected=False)
        else:
            active_reranker = reranker
            verifier_metadata = _reranker_runtime_metadata(reranker)
        verifier_setup_ms = (time.perf_counter() - verification_setup_started) * 1000.0

        started = time.perf_counter()
        retriever.load()
        model_load_ms = (time.perf_counter() - started) * 1000.0
        model_metadata = moscow_benchmark._retriever_metadata(retriever)

        # Reuse the primary Moscow benchmark's optional lifecycle: the
        # default isolated FAISS worker ACKs each model-sized append, so this
        # process never retains (or sends) the complete gallery descriptor
        # matrix. Custom/legacy injected searches retain their one-shot
        # contract for test and extension compatibility.
        gallery_build = moscow_benchmark._build_gallery_search(
            gallery_rows=loaded.gallery.rows,
            retriever=retriever,
            exact_search=exact_search,
            model_metadata=model_metadata,
        )
        gallery_embedding_seconds = gallery_build.gallery_embedding_seconds
        gallery_embedding_ms = gallery_embedding_seconds * 1000.0
        exact_faiss_build_ms = gallery_build.exact_faiss_build_ms

        for query in selected_queries:
            query_id = query.reference_id
            truth = QueryGroundTruth(
                query_id,
                float(query.values["lat"]),
                float(query.values["lon"]),
                sequence_id=_optional_text(query.values.get("sequence_id")),
                captured_at=_optional_text(query.values.get("captured_at")),
            )
            truths.append(truth)

            started = time.perf_counter()
            try:
                with Image.open(query.image_path) as opened:
                    opened.load()
                    query_image = ImageOps.exif_transpose(opened).convert("RGB")
            except (OSError, SyntaxError, ValueError, UnidentifiedImageError) as exc:
                raise MoscowVerificationAblationError(
                    f"cannot prepare query image {query_id!r}"
                ) from exc
            query_quality = measure_query_image_quality(query_image)
            query_preprocessing_ms = (time.perf_counter() - started) * 1000.0

            started = time.perf_counter()
            descriptor = retriever.embed_query(query_image)
            descriptor = l2_normalize(descriptor)[0]
            query_embedding_ms = (time.perf_counter() - started) * 1000.0
            if descriptor.shape != (retriever.descriptor_dim,):
                raise MoscowVerificationAblationError(
                    f"query {query_id!r} descriptor shape mismatch: {descriptor.shape}"
                )

            started = time.perf_counter()
            matches = exact_search.search_one(descriptor, k=top_k)
            exact_search_ms = (time.perf_counter() - started) * 1000.0
            if len(matches) != top_k:
                raise MoscowVerificationAblationError(
                    f"exact search returned {len(matches)} results for k={top_k}"
                )
            if [match.rank for match in matches] != list(range(1, top_k + 1)):
                raise MoscowVerificationAblationError(
                    f"exact search returned non-contiguous ranks for query {query_id!r}"
                )
            if len({match.reference_id for match in matches}) != len(matches):
                raise MoscowVerificationAblationError(
                    f"exact search returned duplicate references for query {query_id!r}"
                )

            started = time.perf_counter()
            baseline_result = active_localizer.localize(
                matches,
                query_quality=query_quality.confidence_signal,
            )
            baseline_localization_ms = (time.perf_counter() - started) * 1000.0

            candidates = _verification_candidates(matches, gallery_images)
            started = time.perf_counter()
            reranked = active_reranker.rerank(query_image, candidates)
            verification_wall_ms = (time.perf_counter() - started) * 1000.0
            if not reranked.enabled or reranked.backend != config.backend:
                raise MoscowVerificationAblationError(
                    f"requested verification backend {config.backend!r} did not execute"
                )
            expected_verified_count = min(config.verify_top_k, len(matches))
            if reranked.verified_count != expected_verified_count:
                raise MoscowVerificationAblationError(
                    "reranker did not verify the configured bounded candidate count: "
                    f"expected {expected_verified_count}, got {reranked.verified_count}"
                )
            if len(reranked.candidates) != len(matches) or {
                candidate.reference_id for candidate in reranked.candidates
            } != {match.reference_id for match in matches}:
                raise MoscowVerificationAblationError(
                    "reranker did not return exactly the searched candidate set"
                )
            if [candidate.final_rank for candidate in reranked.candidates] != list(
                range(1, len(matches) + 1)
            ):
                raise MoscowVerificationAblationError(
                    f"reranker returned non-contiguous final ranks for query {query_id!r}"
                )
            verified_matches = localization_results_from_rerank(reranked)

            started = time.perf_counter()
            verified_result = active_localizer.localize(
                verified_matches,
                query_quality=query_quality.confidence_signal,
            )
            verified_localization_ms = (time.perf_counter() - started) * 1000.0

            baseline_total_ms = (
                query_preprocessing_ms
                + query_embedding_ms
                + exact_search_ms
                + baseline_localization_ms
            )
            verified_total_ms = (
                query_preprocessing_ms
                + query_embedding_ms
                + exact_search_ms
                + verification_wall_ms
                + verified_localization_ms
            )
            shared_timings["query_preprocessing"].append(query_preprocessing_ms)
            shared_timings["query_embedding"].append(query_embedding_ms)
            shared_timings["exact_faiss_search"].append(exact_search_ms)
            baseline_timings["spatial_localization"].append(baseline_localization_ms)
            baseline_timings["offline_query_pipeline"].append(baseline_total_ms)
            verified_timings["geometric_verification_and_rerank"].append(
                verification_wall_ms
            )
            verified_timings["geometric_verification_backend"].append(
                reranked.total_latency_ms
            )
            verified_timings["spatial_localization"].append(verified_localization_ms)
            verified_timings["offline_query_pipeline"].append(verified_total_ms)

            baseline_predictions[query_id] = matches
            verified_predictions[query_id] = verified_matches
            baseline_observations.append(_observation(query, baseline_result))
            verified_observations.append(_observation(query, verified_result))
            baseline_payload = _localization_payload(query, baseline_result)
            verified_payload = _localization_payload(query, verified_result)
            baseline_localizations.append({"query_id": query_id} | baseline_payload)
            verified_localizations.append({"query_id": query_id} | verified_payload)
            per_query.append(
                {
                    "query_id": query_id,
                    "true_lat": truth.lat,
                    "true_lon": truth.lon,
                    "source": query.values.get("source"),
                    "source_image_id": query.values.get("source_image_id"),
                    "sequence_id": query.values.get("sequence_id"),
                    "area": _area_key(query),
                    "query_quality": {
                        "sharpness": query_quality.sharpness,
                        "brightness": query_quality.brightness,
                        "exposure": query_quality.exposure,
                        "confidence_signal": query_quality.confidence_signal,
                        "method": "shared_production_query_quality_v1",
                    },
                    "retrieval_only": {
                        "localization": baseline_payload,
                        "matches": [match.to_dict() for match in matches],
                    },
                    "retrieval_plus_verification": {
                        "localization": verified_payload,
                        "backend": reranked.backend,
                        "verified_count": reranked.verified_count,
                        "matches": [candidate.to_dict() for candidate in reranked.candidates],
                    },
                    "latency_ms": {
                        "query_preprocessing_shared": query_preprocessing_ms,
                        "query_embedding_shared": query_embedding_ms,
                        "exact_faiss_search_shared": exact_search_ms,
                        "retrieval_only_spatial_localization": baseline_localization_ms,
                        "geometric_verification_and_rerank": verification_wall_ms,
                        "geometric_verification_backend": reranked.total_latency_ms,
                        "verified_spatial_localization": verified_localization_ms,
                        "retrieval_only_offline_query_pipeline": baseline_total_ms,
                        "verified_offline_query_pipeline": verified_total_ms,
                    },
                }
            )

    baseline_metrics = _arm_metrics(
        truths=truths,
        predictions=baseline_predictions,
        observations=baseline_observations,
        per_query_localization=baseline_localizations,
        timings=baseline_timings,
        confidence_threshold=active_localizer.config.confidence_threshold,
        positive_distance_threshold_m=positive_distance_threshold_m,
    )
    verified_metrics = _arm_metrics(
        truths=truths,
        predictions=verified_predictions,
        observations=verified_observations,
        per_query_localization=verified_localizations,
        timings=verified_timings,
        confidence_threshold=active_localizer.config.confidence_threshold,
        positive_distance_threshold_m=positive_distance_threshold_m,
    )
    shared_latency = summarize_latencies(
        shared_timings,
        gallery_embedding_images=len(loaded.gallery.rows),
        gallery_embedding_seconds=gallery_embedding_seconds,
    ).to_dict()
    comparison = {
        "recall_at": _numeric_delta(
            verified_metrics["retrieval"]["recall_at"],
            baseline_metrics["retrieval"]["recall_at"],
        ),
        "accuracy_within_m": _numeric_delta(
            verified_metrics["localization"]["accuracy_within_m"],
            baseline_metrics["localization"]["accuracy_within_m"],
        ),
        "conditional_accuracy_within_m": _numeric_delta(
            verified_metrics["localization"]["conditional_accuracy_within_m"],
            baseline_metrics["localization"]["conditional_accuracy_within_m"],
        ),
        "answer_rate": (
            verified_metrics["localization"]["answer_rate"]
            - baseline_metrics["localization"]["answer_rate"]
        ),
        "false_confident_error_count": (
            verified_metrics["false_confident_errors"]["count"]
            - baseline_metrics["false_confident_errors"]["count"]
        ),
        "offline_query_pipeline_latency_delta_ms": _paired_latency_delta(
            verified_timings["offline_query_pipeline"],
            baseline_timings["offline_query_pipeline"],
        ),
        "top1_changed_query_count": sum(
            baseline_predictions[truth.query_id][0].reference_id
            != verified_predictions[truth.query_id][0].reference_id
            for truth in truths
        ),
    }
    comparison["paired_accuracy_at_100m"] = _paired_accuracy_evidence(per_query)
    selection_profile = _selection_profile(selected_queries)
    decision = _decision(
        retrieval_only=baseline_metrics,
        verified=verified_metrics,
        comparison=comparison,
        selection_profile=selection_profile,
        minimum_accuracy_gain=minimum_accuracy_gain,
        maximum_median_overhead_ms=maximum_median_overhead_ms,
        minimum_queries_for_enablement=minimum_queries_for_enablement,
        minimum_sequences_for_enablement=minimum_sequences_for_enablement,
        minimum_areas_for_enablement=minimum_areas_for_enablement,
    )
    fingerprint_inputs = {
        "gallery_manifest_sha256": loaded.gallery.sha256,
        "query_manifest_sha256": loaded.queries.sha256,
        "selected_query_ids_sha256": selection_profile["ordered_query_ids_sha256"],
        "model_name": model_metadata["model_name"],
        "verification_backend": config.backend,
        "verify_top_k": config.verify_top_k,
        "top_k": top_k,
        "geometric_weight": config.geometric_weight,
        "retrieval_score_min": config.retrieval_score_min,
        "retrieval_score_max": config.retrieval_score_max,
        "localizer": moscow_benchmark._localizer_payload(active_localizer.config),
        "model": model_metadata,
        "verifier": verifier_metadata,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_inputs, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    descriptor_dimension = gallery_build.descriptor_dimension
    descriptor_itemsize = gallery_build.descriptor_itemsize
    payload: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "benchmark_kind": ABLATION_KIND,
        "generated_at": datetime.now(UTC).isoformat(),
        "dataset": {
            "domain": "real_moscow_street_view",
            "gallery": moscow_benchmark._manifest_profile(loaded.gallery),
            "queries": moscow_benchmark._manifest_profile(loaded.queries),
            "query_subset": {
                "protocol": (
                    "deterministic nested round-robin across the first available area/H3 "
                    "column and provider sequence, then stable reference ID"
                ),
                "maximum_requested": max_queries,
                **selection_profile,
            },
            "fingerprint_sha256": fingerprint,
            "fingerprint_inputs": fingerprint_inputs,
        },
        "leakage_audit": {
            "base": loaded.leakage_audit,
            "verification_ablation_preflight": preflight,
        },
        "model": model_metadata,
        "config": {
            "top_k": top_k,
            "positive_distance_threshold_m": positive_distance_threshold_m,
            "localizer": moscow_benchmark._localizer_payload(active_localizer.config),
            "verification": {
                "enabled_for_ablation_only": config.enabled,
                "production_default_changed": False,
                "backend": config.backend,
                "verify_top_k": config.verify_top_k,
                "hard_max_verify_top_k": MAX_VERIFY_TOP_K,
                "geometric_weight": config.geometric_weight,
                "retrieval_score_min": config.retrieval_score_min,
                "retrieval_score_max": config.retrieval_score_max,
                "score_integration": (
                    "geometric evidence is mapped into the configured raw retrieval-score "
                    "domain; the combined raw-domain rerank_score is supplied exactly once "
                    "to the spatial localizer as localization_score; weight=0 is identity"
                ),
                "runtime_verifier": verifier_metadata,
            },
        },
        "measurement_protocol": {
            "primary_population": (
                "original leakage-clean real street-view calibration queries only; "
                "no test or augmented images"
            ),
            "shared_work": (
                "Gallery embedded once. Each selected query is production-preprocessed, "
                "quality-scored, embedded, and exact-searched once; both arms consume the "
                "identical query-quality signal and raw matches."
            ),
            "setup_excluded_from_per_query_latency": [
                "model_load",
                "verification_backend_build_and_weight_load",
                "gallery_embedding",
                "exact_faiss_build",
                "manifest_and_leakage_preflight",
            ],
            "offline_pipeline_latency": (
                "sum of measured shared query preprocessing, query embedding, exact search, "
                "arm-specific verification, and spatial localization stages"
            ),
            "verification_overhead": (
                "paired per-query verified pipeline latency minus the same query's "
                "retrieval-only pipeline latency; reported percentiles summarize those "
                "paired deltas"
            ),
        },
        "runtime": {
            "model_load_ms": model_load_ms,
            "verifier_setup_ms": verifier_setup_ms,
            "gallery_embedding_ms": gallery_embedding_ms,
            "exact_faiss_build_ms": exact_faiss_build_ms,
            "descriptor_dimension": descriptor_dimension,
            "descriptor_dtype": gallery_build.descriptor_dtype,
            "gallery_descriptor_storage_bytes": gallery_build.descriptor_storage_bytes,
            "selected_query_descriptor_storage_bytes": (
                len(selected_queries) * descriptor_dimension * descriptor_itemsize
            ),
            "exact_faiss_vector_storage_bytes": gallery_build.descriptor_storage_bytes,
            "exact_search_backend": (
                "faiss.IndexFlatIP (L2-normalized descriptors, isolated process by default)"
            ),
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": sys.version.split()[0],
                "numpy": np.__version__,
                "torch": moscow_benchmark._package_version("torch"),
                "faiss_cpu": moscow_benchmark._package_version("faiss-cpu"),
                "opencv_python_headless": moscow_benchmark._package_version(
                    "opencv-python-headless"
                ),
                "lightglue": moscow_benchmark._package_version("lightglue"),
            },
        },
        "shared_latency": shared_latency,
        "arms": {
            "retrieval_only": baseline_metrics,
            "retrieval_plus_verification": verified_metrics,
        },
        "comparison": {
            "retrieval_plus_verification_minus_retrieval_only": comparison,
        },
        "decision": decision,
        "per_query": per_query,
    }
    stem = report_stem or (
        f"{model_metadata['model_name']}_{config.backend}_k{config.verify_top_k}_"
        f"w{config.geometric_weight:g}_{fingerprint[:12]}_moscow_verification_ablation"
    )
    json_path, markdown_path = write_reports(payload, output_dir, stem=stem)
    return payload, json_path, markdown_path


def _compact_result(
    payload: Mapping[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Any]:
    comparison = payload["comparison"]["retrieval_plus_verification_minus_retrieval_only"]
    return {
        "benchmark_kind": payload["benchmark_kind"],
        "model": payload["model"]["model_name"],
        "selected_query_count": payload["dataset"]["query_subset"]["selected_count"],
        "leakage_audit_passed": (
            payload["leakage_audit"]["base"]["passed"]
            and payload["leakage_audit"]["verification_ablation_preflight"]["passed"]
        ),
        "accuracy_within_m_delta": comparison["accuracy_within_m"],
        "false_confident_error_count_delta": comparison["false_confident_error_count"],
        "median_latency_overhead_ms": comparison[
            "offline_query_pipeline_latency_delta_ms"
        ]["median_ms"],
        "recommendation": payload["decision"]["recommendation"],
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare retrieval-only against bounded geometric verification on a "
            "leakage-clean real Moscow calibration split"
        )
    )
    parser.add_argument("--gallery-manifest", type=Path, required=True)
    parser.add_argument("--query-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", choices=("megaloc", "dinov2-salad"), required=True)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--max-queries", type=int, default=DEFAULT_MAX_QUERIES)
    parser.add_argument("--positive-distance-threshold-m", type=float, default=100.0)
    parser.add_argument("--phash-distance-threshold", type=int, default=4)
    parser.add_argument(
        "--estimator",
        choices=tuple(value.value for value in CoordinateEstimator),
        default=CoordinateEstimator.WEIGHTED_MEDOID.value,
    )
    parser.add_argument(
        "--confidence-threshold",
        type=float,
        default=LocalizerConfig().confidence_threshold,
    )
    parser.add_argument(
        "--verification-backend",
        choices=("opencv_sift", "lightglue_sift"),
        default="opencv_sift",
    )
    parser.add_argument("--verify-top-k", type=int, default=10)
    parser.add_argument("--geometric-weight", type=float, default=0.35)
    parser.add_argument("--minimum-accuracy-gain", type=float, default=0.02)
    parser.add_argument("--maximum-median-overhead-ms", type=float, default=250.0)
    parser.add_argument("--minimum-queries-for-enablement", type=int, default=30)
    parser.add_argument("--minimum-sequences-for-enablement", type=int, default=10)
    parser.add_argument("--minimum-areas-for-enablement", type=int, default=4)
    parser.add_argument("--report-stem")
    args = parser.parse_args()

    retriever = create_retriever(
        args.model,
        device=args.device,
        batch_size=args.batch_size,
        cache_dir=args.cache_dir,
    )
    verification_config = VerificationConfig(
        enabled=True,
        backend=args.verification_backend,
        verify_top_k=args.verify_top_k,
        geometric_weight=args.geometric_weight,
    )
    try:
        payload, json_path, markdown_path = run_moscow_verification_ablation(
            gallery_manifest_path=args.gallery_manifest,
            query_manifest_path=args.query_manifest,
            retriever=retriever,
            output_dir=args.output_dir,
            expected_model=args.model,
            report_stem=args.report_stem,
            top_k=args.top_k,
            max_queries=args.max_queries,
            positive_distance_threshold_m=args.positive_distance_threshold_m,
            phash_distance_threshold=args.phash_distance_threshold,
            estimator=args.estimator,
            confidence_threshold=args.confidence_threshold,
            verification_config=verification_config,
            minimum_accuracy_gain=args.minimum_accuracy_gain,
            maximum_median_overhead_ms=args.maximum_median_overhead_ms,
            minimum_queries_for_enablement=args.minimum_queries_for_enablement,
            minimum_sequences_for_enablement=args.minimum_sequences_for_enablement,
            minimum_areas_for_enablement=args.minimum_areas_for_enablement,
        )
    finally:
        retriever.close()
    print(
        json.dumps(
            _compact_result(payload, json_path, markdown_path),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
