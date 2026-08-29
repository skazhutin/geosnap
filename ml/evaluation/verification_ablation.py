"""Reproducible retrieval-only vs bounded OpenCV-SIFT verification ablation.

The runner deliberately reuses the frozen Commons proxy loader, official
retriever contract, isolated exact-FAISS search, production geometric
reranker, and spatial localizer.  It is an evaluation-only diagnostic: the
proxy is landmark-biased Wikimedia Commons imagery, not street-view data.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from ml.evaluation.commons import audit_split
from ml.evaluation.commons_benchmark import (
    PROXY_WARNING,
    ExactSearch,
    IsolatedFaissExactSearch,
    _package_version,
    _reference_metadata,
    _retriever_metadata,
    load_proxy_snapshot,
)
from ml.evaluation.metrics import (
    LocalizationObservation,
    QueryGroundTruth,
    evaluate_localization,
    evaluate_retrieval,
    summarize_latencies,
)
from ml.indexing.faiss_index import RetrievalResult
from ml.localization import LocalizerConfig, SpatialLocalizer, haversine_m
from ml.localization.estimators import CoordinateEstimator
from ml.retrieval import BaseRetriever, create_retriever
from ml.verification import GeometricReranker, RerankResult, VerificationCandidate
from ml.verification.reranker import MAX_VERIFY_TOP_K, VerificationConfig

EXPECTED_GALLERY_COUNT = 12
EXPECTED_QUERY_COUNT = 8
REPORT_SCHEMA_VERSION = 1
PRODUCTION_BLOCKER = (
    "This tiny non-street-view proxy cannot establish production benefit or "
    "justify enabling geometric verification by default. A leakage-resistant, "
    "representative Moscow street-view benchmark remains a production blocker."
)


class BoundedReranker(Protocol):
    config: VerificationConfig

    def rerank(
        self,
        query_image: Any,
        candidates: Sequence[VerificationCandidate],
    ) -> RerankResult: ...


def localization_results_from_rerank(result: RerankResult) -> list[RetrievalResult]:
    """Convert reranked candidates using the production single-weight contract.

    The raw retrieval similarity remains ``score`` for retrieval diagnostics
    and out-of-coverage policy.  The already weighted rerank score is supplied
    once as ``localization_score``; ``SpatialLocalizer`` must not multiply the
    geometric score again.
    """

    return [
        RetrievalResult(
            reference_id=candidate.reference_id,
            score=candidate.retrieval_score,
            rank=candidate.final_rank,
            metadata=dict(candidate.metadata)
            | {
                "verification_score": candidate.verification_score,
                "localization_score": candidate.rerank_score,
            },
        )
        for candidate in result.candidates
    ]


def _verification_candidates(
    matches: Sequence[RetrievalResult],
    gallery_images: Mapping[str, Path],
) -> list[VerificationCandidate]:
    candidates: list[VerificationCandidate] = []
    for match in matches:
        try:
            image = gallery_images[match.reference_id]
        except KeyError as exc:
            raise ValueError(f"retrieval result {match.reference_id!r} is absent from the frozen gallery") from exc
        candidates.append(
            VerificationCandidate(
                reference_id=match.reference_id,
                image=image,
                retrieval_score=match.score,
                original_rank=match.rank,
                metadata=match.metadata,
            )
        )
    return candidates


def _observation(query: Mapping[str, Any], result: Any) -> LocalizationObservation:
    return LocalizationObservation(
        query_id=str(query["record_id"]),
        true_lat=float(query["lat"]),
        true_lon=float(query["lon"]),
        predicted_lat=result.lat,
        predicted_lon=result.lon,
        status=result.status.value,
        confidence=float(result.confidence),
    )


def _localization_payload(query: Mapping[str, Any], result: Any) -> dict[str, Any]:
    error_m = (
        None
        if result.lat is None or result.lon is None
        else haversine_m(
            float(query["lat"]),
            float(query["lon"]),
            float(result.lat),
            float(result.lon),
        )
    )
    return {
        "predicted_lat": result.lat,
        "predicted_lon": result.lon,
        "error_m": error_m,
        "status": result.status.value,
        "confidence": result.confidence,
        "reasons": list(result.reasons),
    }


def _localizer_config(config: LocalizerConfig) -> dict[str, Any]:
    return {
        "cluster_radius_m": config.cluster_radius_m,
        "max_cluster_diameter_m": config.max_cluster_diameter_m,
        "estimator": config.estimator.value,
        "score_temperature": config.score_temperature,
        "out_of_coverage_similarity": config.out_of_coverage_similarity,
        "confident_similarity": config.confident_similarity,
        "good_geographic_margin": config.good_geographic_margin,
        "minimum_cluster_mass": config.minimum_cluster_mass,
        "minimum_cluster_mass_margin": config.minimum_cluster_mass_margin,
        "minimum_cluster_candidates": config.minimum_cluster_candidates,
        "confidence_threshold": config.confidence_threshold,
        "good_hypothesis_separation_m": config.good_hypothesis_separation_m,
    }


def _metric_delta(after: Mapping[str, Any], before: Mapping[str, Any]) -> dict[str, float]:
    if set(after) != set(before):
        raise ValueError("cannot compare metrics with different keys")
    return {key: float(after[key]) - float(before[key]) for key in after}


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def run_verification_ablation(
    *,
    manifest_path: str | Path,
    retriever: BaseRetriever,
    output_path: str | Path,
    top_k: int = EXPECTED_GALLERY_COUNT,
    positive_distance_threshold_m: float = 100.0,
    verification_config: VerificationConfig | None = None,
    search_factory: Callable[[], ExactSearch] = IsolatedFaissExactSearch,
    localizer: SpatialLocalizer | None = None,
    reranker: BoundedReranker | None = None,
    expected_gallery_count: int = EXPECTED_GALLERY_COUNT,
    expected_query_count: int = EXPECTED_QUERY_COUNT,
) -> tuple[dict[str, Any], Path]:
    """Run both arms once over identical embeddings and exact-search results."""

    if top_k < 10:
        raise ValueError("top_k must be at least 10 to report Recall@10")
    if positive_distance_threshold_m <= 0:
        raise ValueError("positive_distance_threshold_m must be positive")
    if expected_gallery_count < 1 or expected_query_count < 1:
        raise ValueError("expected split counts must be positive")

    proxy = load_proxy_snapshot(manifest_path)
    if len(proxy.gallery) != expected_gallery_count or len(proxy.queries) != expected_query_count:
        raise ValueError(
            "frozen proxy shape mismatch: expected "
            f"{expected_query_count} queries/{expected_gallery_count} gallery, got "
            f"{len(proxy.queries)} queries/{len(proxy.gallery)} gallery"
        )
    if top_k > len(proxy.gallery):
        raise ValueError("top_k cannot exceed the frozen gallery size")

    config = verification_config or VerificationConfig(
        enabled=True,
        backend="opencv_sift",
        verify_top_k=10,
        geometric_weight=0.35,
    )
    if not config.enabled or config.backend != "opencv_sift":
        raise ValueError("this ablation requires enabled OpenCV SIFT verification")
    if config.verify_top_k > top_k:
        raise ValueError("verify_top_k cannot exceed top_k")
    active_reranker = reranker or GeometricReranker(config)
    if active_reranker.config != config:
        raise ValueError("injected reranker config does not match verification_config")

    active_localizer = localizer or SpatialLocalizer()
    gallery_images = {str(row["record_id"]): proxy.image_path(row) for row in proxy.gallery}
    truths: list[QueryGroundTruth] = []
    retrieval_only_predictions: dict[str, Sequence[RetrievalResult]] = {}
    verified_predictions: dict[str, Sequence[RetrievalResult]] = {}
    retrieval_only_observations: list[LocalizationObservation] = []
    verified_observations: list[LocalizationObservation] = []
    per_query: list[dict[str, Any]] = []
    shared_timings: dict[str, list[float]] = {
        "query_embedding": [],
        "exact_faiss_search": [],
    }
    retrieval_only_timings: dict[str, list[float]] = {
        "spatial_localization": [],
        "offline_query_pipeline": [],
    }
    verified_timings: dict[str, list[float]] = {
        "opencv_sift_verification_and_rerank": [],
        "opencv_sift_backend": [],
        "spatial_localization": [],
        "offline_query_pipeline": [],
    }

    # Spawn FAISS before loading the Torch retriever to avoid mixed OpenMP
    # runtimes in the same macOS process.
    with search_factory() as exact_search:
        started = time.perf_counter()
        retriever.load()
        model_load_ms = (time.perf_counter() - started) * 1_000
        model_metadata = _retriever_metadata(retriever)

        gallery_paths = [proxy.image_path(row) for row in proxy.gallery]
        started = time.perf_counter()
        gallery_descriptors = retriever.embed_batch(gallery_paths)
        gallery_embedding_ms = (time.perf_counter() - started) * 1_000

        started = time.perf_counter()
        exact_search.build(
            gallery_descriptors,
            [str(row["record_id"]) for row in proxy.gallery],
            [_reference_metadata(row) for row in proxy.gallery],
            model_metadata,
        )
        exact_faiss_build_ms = (time.perf_counter() - started) * 1_000

        for query in proxy.queries:
            query_id = str(query["record_id"])
            query_path = proxy.image_path(query)
            true_lat, true_lon = float(query["lat"]), float(query["lon"])
            truths.append(QueryGroundTruth(query_id, true_lat, true_lon))

            started = time.perf_counter()
            descriptor = retriever.embed_query(query_path)
            embedding_ms = (time.perf_counter() - started) * 1_000

            started = time.perf_counter()
            matches = exact_search.search_one(descriptor, k=top_k)
            search_ms = (time.perf_counter() - started) * 1_000

            started = time.perf_counter()
            retrieval_only_result = active_localizer.localize(matches)
            retrieval_only_localization_ms = (time.perf_counter() - started) * 1_000

            verification_candidates = _verification_candidates(matches, gallery_images)
            started = time.perf_counter()
            reranked = active_reranker.rerank(query_path, verification_candidates)
            verification_and_rerank_ms = (time.perf_counter() - started) * 1_000
            if not reranked.enabled or reranked.backend != "opencv_sift":
                raise RuntimeError("OpenCV SIFT reranker did not execute")
            if reranked.verified_count > config.verify_top_k:
                raise RuntimeError("reranker exceeded configured verification bound")
            verified_matches = localization_results_from_rerank(reranked)

            started = time.perf_counter()
            verified_result = active_localizer.localize(verified_matches)
            verified_localization_ms = (time.perf_counter() - started) * 1_000

            retrieval_only_total_ms = embedding_ms + search_ms + retrieval_only_localization_ms
            verified_total_ms = embedding_ms + search_ms + verification_and_rerank_ms + verified_localization_ms
            shared_timings["query_embedding"].append(embedding_ms)
            shared_timings["exact_faiss_search"].append(search_ms)
            retrieval_only_timings["spatial_localization"].append(retrieval_only_localization_ms)
            retrieval_only_timings["offline_query_pipeline"].append(retrieval_only_total_ms)
            verified_timings["opencv_sift_verification_and_rerank"].append(verification_and_rerank_ms)
            verified_timings["opencv_sift_backend"].append(reranked.total_latency_ms)
            verified_timings["spatial_localization"].append(verified_localization_ms)
            verified_timings["offline_query_pipeline"].append(verified_total_ms)

            retrieval_only_predictions[query_id] = matches
            verified_predictions[query_id] = verified_matches
            retrieval_only_observations.append(_observation(query, retrieval_only_result))
            verified_observations.append(_observation(query, verified_result))
            per_query.append(
                {
                    "query_id": query_id,
                    "landmark_id": query["landmark_id"],
                    "page_url": query["page_url"],
                    "retrieval_only": {
                        "localization": _localization_payload(query, retrieval_only_result),
                        "matches": [match.to_dict() for match in matches],
                    },
                    "retrieval_plus_verification": {
                        "localization": _localization_payload(query, verified_result),
                        "verified_count": reranked.verified_count,
                        "matches": [candidate.to_dict() for candidate in reranked.candidates],
                    },
                    "latency_ms": {
                        "query_embedding_shared": embedding_ms,
                        "exact_faiss_search_shared": search_ms,
                        "retrieval_only_spatial_localization": retrieval_only_localization_ms,
                        "opencv_sift_verification_and_rerank": verification_and_rerank_ms,
                        "opencv_sift_backend": reranked.total_latency_ms,
                        "verified_spatial_localization": verified_localization_ms,
                        "retrieval_only_offline_query_pipeline": retrieval_only_total_ms,
                        "verified_offline_query_pipeline": verified_total_ms,
                    },
                }
            )

    retrieval_only_retrieval = evaluate_retrieval(
        truths,
        retrieval_only_predictions,
        positive_distance_threshold_m=positive_distance_threshold_m,
        ks=(1, 5, 10),
    ).to_dict()
    verified_retrieval = evaluate_retrieval(
        truths,
        verified_predictions,
        positive_distance_threshold_m=positive_distance_threshold_m,
        ks=(1, 5, 10),
    ).to_dict()
    retrieval_only_localization = evaluate_localization(
        retrieval_only_observations, thresholds_m=(25, 50, 100)
    ).to_dict()
    verified_localization = evaluate_localization(verified_observations, thresholds_m=(25, 50, 100)).to_dict()
    shared_latency = summarize_latencies(shared_timings).to_dict()
    retrieval_only_latency = summarize_latencies(retrieval_only_timings).to_dict()
    verified_latency = summarize_latencies(verified_timings).to_dict()
    top1_changed_count = sum(
        retrieval_only_predictions[truth.query_id][0].reference_id
        != verified_predictions[truth.query_id][0].reference_id
        for truth in truths
    )

    payload: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "warning": PROXY_WARNING,
        "production_blocker": PRODUCTION_BLOCKER,
        "production_decision": "keep_verification_default_off",
        "dataset": {
            "dataset_id": proxy.manifest["dataset_id"],
            "purpose": proxy.manifest["purpose"],
            "manifest_path": str(proxy.manifest_path),
            "manifest_sha256": proxy.manifest_sha256,
            "canonical_dataset_sha256": proxy.manifest.get("canonical_dataset_sha256"),
            "snapshot_ledger_sha256": proxy.manifest.get("source", {}).get("snapshot_ledger_sha256"),
            "split_audit": audit_split(proxy.manifest["images"]),
            "domain": "wikimedia_commons_landmark_photos_not_street_view",
        },
        "model": model_metadata,
        "config": {
            "top_k": top_k,
            "positive_distance_threshold_m": positive_distance_threshold_m,
            "localizer": _localizer_config(active_localizer.config),
            "verification": {
                "enabled": config.enabled,
                "backend": config.backend,
                "verify_top_k": config.verify_top_k,
                "hard_max_verify_top_k": MAX_VERIFY_TOP_K,
                "geometric_weight": config.geometric_weight,
                "retrieval_score_min": config.retrieval_score_min,
                "retrieval_score_max": config.retrieval_score_max,
                "score_integration": (
                    "geometric evidence mapped to raw retrieval-score domain; combined rerank_score supplied once as localization_score; weight=0 is identity"
                ),
            },
        },
        "measurement_protocol": {
            "shared_work": (
                "Each query is embedded and exact-searched once; both arms consume the "
                "same raw matches. Offline totals are sums of the measured arm stages."
            ),
            "setup_excluded_from_per_query_latency": [
                "model_load",
                "gallery_embedding",
                "exact_faiss_build",
            ],
            "latency_population": "all_8_held_out_queries",
        },
        "runtime": {
            "model_load_ms": model_load_ms,
            "gallery_embedding_ms": gallery_embedding_ms,
            "exact_faiss_build_ms": exact_faiss_build_ms,
            "exact_search_backend": "faiss.IndexFlatIP (L2-normalized descriptors)",
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": sys.version.split()[0],
                "numpy": np.__version__,
                "torch": _package_version("torch"),
                "faiss_cpu": _package_version("faiss-cpu"),
                "opencv_python_headless": _package_version("opencv-python-headless"),
            },
        },
        "shared_latency": shared_latency,
        "arms": {
            "retrieval_only": {
                "retrieval": retrieval_only_retrieval,
                "localization": retrieval_only_localization,
                "latency": retrieval_only_latency,
            },
            "retrieval_plus_verification": {
                "retrieval": verified_retrieval,
                "localization": verified_localization,
                "latency": verified_latency,
            },
        },
        "comparison": {
            "retrieval_plus_verification_minus_retrieval_only": {
                "recall_at": _metric_delta(
                    verified_retrieval["recall_at"],
                    retrieval_only_retrieval["recall_at"],
                ),
                "accuracy_within_m": _metric_delta(
                    verified_localization["accuracy_within_m"],
                    retrieval_only_localization["accuracy_within_m"],
                ),
                "answer_rate": (verified_localization["answer_rate"] - retrieval_only_localization["answer_rate"]),
                "median_offline_query_pipeline_ms": (
                    verified_latency["stages"]["offline_query_pipeline"]["median_ms"]
                    - retrieval_only_latency["stages"]["offline_query_pipeline"]["median_ms"]
                ),
            },
            "top1_changed_query_count": top1_changed_count,
            "query_count": len(truths),
        },
        "per_query": per_query,
        "limitations": [
            "tiny hand-curated landmark-biased sample with only 8 queries and 12 gallery images",
            "Wikimedia Commons photos are not Moscow street-view or production gallery imagery",
            "the same four famous landmarks recur on both sides of the split",
            "latency is host-specific and excludes network and API overhead",
            "no model, threshold, or verification weight selection is justified by this proxy",
            PRODUCTION_BLOCKER,
        ],
    }
    resolved_output = Path(output_path)
    _atomic_json(resolved_output, payload)
    return payload, resolved_output


def _compact_result(payload: Mapping[str, Any], output_path: Path) -> dict[str, Any]:
    baseline = payload["arms"]["retrieval_only"]
    verified = payload["arms"]["retrieval_plus_verification"]
    return {
        "warning": payload["warning"],
        "production_blocker": payload["production_blocker"],
        "model": payload["model"]["model_name"],
        "device": payload["model"]["device"],
        "gallery_count": payload["dataset"]["split_audit"]["gallery_count"],
        "query_count": payload["dataset"]["split_audit"]["query_count"],
        "retrieval_only": {
            "recall_at": baseline["retrieval"]["recall_at"],
            "accuracy_within_m": baseline["localization"]["accuracy_within_m"],
            "answer_rate": baseline["localization"]["answer_rate"],
            "median_offline_query_pipeline_ms": baseline["latency"]["stages"]["offline_query_pipeline"]["median_ms"],
        },
        "retrieval_plus_verification": {
            "recall_at": verified["retrieval"]["recall_at"],
            "accuracy_within_m": verified["localization"]["accuracy_within_m"],
            "answer_rate": verified["localization"]["answer_rate"],
            "median_verification_ms": verified["latency"]["stages"]["opencv_sift_verification_and_rerank"]["median_ms"],
            "median_offline_query_pipeline_ms": verified["latency"]["stages"]["offline_query_pipeline"]["median_ms"],
        },
        "output": str(output_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ablate retrieval-only vs bounded OpenCV-SIFT verification on the frozen 8-query/12-gallery Commons proxy"
        )
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", choices=("megaloc", "dinov2-salad"), default="megaloc")
    parser.add_argument("--device", default="auto")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--top-k", type=int, default=EXPECTED_GALLERY_COUNT)
    parser.add_argument("--verify-top-k", type=int, default=10)
    parser.add_argument("--geometric-weight", type=float, default=0.35)
    parser.add_argument("--positive-distance-m", type=float, default=100.0)
    parser.add_argument(
        "--estimator",
        choices=tuple(value.value for value in CoordinateEstimator),
        default=CoordinateEstimator.WEIGHTED_MEDOID.value,
    )
    args = parser.parse_args()

    verification_config = VerificationConfig(
        enabled=True,
        backend="opencv_sift",
        verify_top_k=args.verify_top_k,
        geometric_weight=args.geometric_weight,
    )
    retriever = create_retriever(
        args.model,
        device=args.device,
        batch_size=args.batch_size,
        cache_dir=args.cache_dir,
    )
    try:
        payload, output_path = run_verification_ablation(
            manifest_path=args.manifest,
            retriever=retriever,
            output_path=args.output,
            top_k=args.top_k,
            positive_distance_threshold_m=args.positive_distance_m,
            verification_config=verification_config,
            localizer=SpatialLocalizer(LocalizerConfig(estimator=CoordinateEstimator(args.estimator))),
        )
    finally:
        retriever.close()
    print(
        json.dumps(
            _compact_result(payload, output_path),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
