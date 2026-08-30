"""Exact-FAISS benchmark for the tiny Moscow Commons landmark proxy.

Primary results use genuinely different Commons file pages for gallery and
query. Deterministic augmentations, when requested, are reported separately and
never mixed into the primary denominators.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing as mp
import os
import platform
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from ml.evaluation.augmentations import generate_robustness_variants
from ml.evaluation.commons import CommonsAcquisitionError, audit_split
from ml.evaluation.metrics import (
    EvaluationBundle,
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

PROXY_PURPOSE = "evaluation_only_tiny_landmark_biased_proxy"
PROXY_WARNING = (
    "Tiny hand-curated landmark-biased Wikimedia Commons proxy; not street-view, "
    "not representative of Moscow, and not evidence of production coverage."
)


def _package_version(name: str) -> str | None:
    try:
        from importlib.metadata import version

        return version(name)
    except Exception:
        return None


class BenchmarkError(RuntimeError):
    """A benchmark precondition or isolated exact-search operation failed."""


class ExactSearch(Protocol):
    def __enter__(self) -> ExactSearch: ...

    def __exit__(self, *args: Any) -> None: ...

    def build(
        self,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        reference_metadata: Sequence[Mapping[str, Any]],
        retriever_metadata: Mapping[str, Any],
    ) -> None: ...

    def search_one(self, query: np.ndarray, *, k: int) -> list[RetrievalResult]: ...


def _faiss_process(
    connection: Any,
    index_id: str,
    city_id: str | None,
    extra_metadata: Mapping[str, Any],
) -> None:
    """Own FAISS in a process that never imports PyTorch (important on macOS)."""

    try:
        from ml.indexing.faiss_index import FaissExactIndex, FaissExactIndexBuilder

        connection.send({"kind": "booted"})
        index: FaissExactIndex | None = None
        stream_builder: FaissExactIndexBuilder | None = None
        while True:
            request = connection.recv()
            operation = request.get("operation")
            if operation == "close":
                connection.send({"kind": "closed"})
                return
            request_id = request.get("request_id")
            try:
                if operation == "build":
                    if stream_builder is not None:
                        raise BenchmarkError("a streamed exact FAISS build is already in progress")
                    index = FaissExactIndex.build(
                        request["descriptors"],
                        request["reference_ids"],
                        reference_metadata=request["reference_metadata"],
                        retriever_metadata=request["retriever_metadata"],
                        index_id=index_id,
                        city_id=city_id,
                        extra_metadata=extra_metadata,
                    )
                    response: dict[str, Any] = {
                        "kind": "built",
                        "request_id": request_id,
                        "size": index.size,
                        "descriptor_dim": index.descriptor_dim,
                    }
                elif operation == "begin_build":
                    if stream_builder is not None:
                        raise BenchmarkError("a streamed exact FAISS build is already in progress")
                    stream_builder = FaissExactIndexBuilder(
                        descriptor_dim=int(request["descriptor_dim"]),
                        expected_size=int(request["expected_size"]),
                        retriever_metadata=request["retriever_metadata"],
                        index_id=index_id,
                        city_id=city_id,
                        extra_metadata=extra_metadata,
                    )
                    index = None
                    response = {
                        "kind": "build_started",
                        "request_id": request_id,
                        "descriptor_dim": int(request["descriptor_dim"]),
                        "expected_size": int(request["expected_size"]),
                    }
                elif operation == "add_batch":
                    if stream_builder is None:
                        raise BenchmarkError("a streamed exact FAISS build has not been started")
                    stream_builder.add_batch(
                        request["descriptors"],
                        request["reference_ids"],
                        reference_metadata=request["reference_metadata"],
                    )
                    response = {
                        "kind": "batch_added",
                        "request_id": request_id,
                        "size": stream_builder.size,
                    }
                elif operation == "finish_build":
                    if stream_builder is None:
                        raise BenchmarkError("a streamed exact FAISS build has not been started")
                    index = stream_builder.finish()
                    stream_builder = None
                    response = {
                        "kind": "built",
                        "request_id": request_id,
                        "size": index.size,
                        "descriptor_dim": index.descriptor_dim,
                    }
                elif operation == "search":
                    if index is None:
                        raise BenchmarkError("exact FAISS index has not been built")
                    matches = index.search_one(request["query"], k=int(request["k"]))
                    response = {
                        "kind": "result",
                        "request_id": request_id,
                        "matches": [match.to_dict() for match in matches],
                    }
                else:
                    raise BenchmarkError(f"unknown exact-search operation: {operation!r}")
            except BaseException as exc:
                response = {
                    "kind": "error",
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
            connection.send(response)
    except BaseException as exc:
        try:
            connection.send({"kind": "error", "error_type": type(exc).__name__, "message": str(exc)})
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        connection.close()


class IsolatedFaissExactSearch(AbstractContextManager["IsolatedFaissExactSearch"]):
    """Runtime-only normalized ``faiss.IndexFlatIP`` behind a spawn boundary."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 120.0,
        index_id: str = "moscow-commons-proxy-runtime",
        city_id: str | None = "moscow",
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not index_id.strip():
            raise ValueError("index_id must be non-empty")
        self.timeout_seconds = timeout_seconds
        self.index_id = index_id
        self.city_id = city_id
        self.extra_metadata = dict(
            extra_metadata
            if extra_metadata is not None
            else {
                "evaluation_only": True,
                "dataset_kind": "tiny_landmark_biased_proxy",
            }
        )
        self._context = mp.get_context("spawn")
        self._connection: Any | None = None
        self._process: Any | None = None
        self._request_counter = 0
        self._stream_expected_size: int | None = None
        self._stream_added_size = 0
        self._stream_descriptor_dim: int | None = None

    def __enter__(self) -> IsolatedFaissExactSearch:
        parent, child = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_faiss_process,
            args=(child, self.index_id, self.city_id, self.extra_metadata),
            name="geosnap-evaluation-faiss",
            daemon=True,
        )
        process.start()
        child.close()
        self._connection = parent
        self._process = process
        self._stream_expected_size = None
        self._stream_added_size = 0
        self._stream_descriptor_dim = None
        try:
            response = self._receive()
        except BaseException:
            self.close(force=True)
            raise
        if response.get("kind") != "booted":
            self.close(force=True)
            raise BenchmarkError(f"exact FAISS process failed: {response.get('error_type')}: {response.get('message')}")
        return self

    def _receive(self) -> Mapping[str, Any]:
        connection = self._connection
        if connection is None:
            raise BenchmarkError("exact FAISS process is not started")
        if not connection.poll(self.timeout_seconds):
            raise BenchmarkError("exact FAISS process timed out")
        return connection.recv()

    def _request(self, operation: str, **payload: Any) -> Mapping[str, Any]:
        connection = self._connection
        if connection is None:
            raise BenchmarkError("exact FAISS process is not started")
        self._request_counter += 1
        request_id = str(self._request_counter)
        connection.send({"operation": operation, "request_id": request_id, **payload})
        response = self._receive()
        if response.get("request_id") != request_id or response.get("kind") == "error":
            raise BenchmarkError(
                f"exact FAISS {operation} failed: {response.get('error_type')}: {response.get('message')}"
            )
        return response

    def build(
        self,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        reference_metadata: Sequence[Mapping[str, Any]],
        retriever_metadata: Mapping[str, Any],
    ) -> None:
        if self._stream_expected_size is not None:
            raise BenchmarkError("a streamed exact FAISS build is already in progress")
        response = self._request(
            "build",
            descriptors=np.asarray(descriptors, dtype=np.float32),
            reference_ids=list(reference_ids),
            reference_metadata=[dict(value) for value in reference_metadata],
            retriever_metadata=dict(retriever_metadata),
        )
        if response.get("kind") != "built":
            raise BenchmarkError(f"unexpected exact FAISS build response: {response}")

    def begin_build(
        self,
        *,
        descriptor_dim: int,
        expected_size: int,
        retriever_metadata: Mapping[str, Any],
    ) -> None:
        """Start a bounded-memory gallery build in the isolated FAISS process."""

        if self._stream_expected_size is not None:
            raise BenchmarkError("a streamed exact FAISS build is already in progress")
        if descriptor_dim < 1:
            raise ValueError("descriptor_dim must be positive")
        if expected_size < 1:
            raise ValueError("expected_size must be positive")
        response = self._request(
            "begin_build",
            descriptor_dim=int(descriptor_dim),
            expected_size=int(expected_size),
            retriever_metadata=dict(retriever_metadata),
        )
        if (
            response.get("kind") != "build_started"
            or response.get("descriptor_dim") != int(descriptor_dim)
            or response.get("expected_size") != int(expected_size)
        ):
            raise BenchmarkError(f"unexpected exact FAISS streamed-build response: {response}")
        self._stream_expected_size = int(expected_size)
        self._stream_added_size = 0
        self._stream_descriptor_dim = int(descriptor_dim)

    def add_batch(
        self,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        reference_metadata: Sequence[Mapping[str, Any]],
    ) -> None:
        """Append one descriptor/sidecar batch and wait for the child ACK."""

        if self._stream_expected_size is None:
            raise BenchmarkError("a streamed exact FAISS build has not been started")
        ids = [str(value) for value in reference_ids]
        response = self._request(
            "add_batch",
            descriptors=np.asarray(descriptors, dtype=np.float32),
            reference_ids=ids,
            reference_metadata=[dict(value) for value in reference_metadata],
        )
        expected_size = self._stream_added_size + len(ids)
        if response.get("kind") != "batch_added" or response.get("size") != expected_size:
            raise BenchmarkError(f"unexpected exact FAISS streamed-batch response: {response}")
        self._stream_added_size = expected_size

    def finish_build(self) -> None:
        """Fail closed unless every declared gallery row reached the FAISS child."""

        expected_size = self._stream_expected_size
        descriptor_dim = self._stream_descriptor_dim
        if expected_size is None or descriptor_dim is None:
            raise BenchmarkError("a streamed exact FAISS build has not been started")
        response = self._request("finish_build")
        if (
            response.get("kind") != "built"
            or response.get("size") != expected_size
            or response.get("descriptor_dim") != descriptor_dim
        ):
            raise BenchmarkError(f"unexpected exact FAISS streamed-finish response: {response}")
        self._stream_expected_size = None
        self._stream_added_size = 0
        self._stream_descriptor_dim = None

    def search_one(self, query: np.ndarray, *, k: int) -> list[RetrievalResult]:
        response = self._request("search", query=np.asarray(query, dtype=np.float32), k=int(k))
        if response.get("kind") != "result":
            raise BenchmarkError(f"unexpected exact FAISS search response: {response}")
        return [RetrievalResult(**row) for row in response["matches"]]

    def close(self, *, force: bool = False) -> None:
        process = self._process
        connection = self._connection
        if process is not None and process.is_alive() and connection is not None and not force:
            try:
                connection.send({"operation": "close"})
                if connection.poll(min(self.timeout_seconds, 5.0)):
                    connection.recv()
                process.join(timeout=5.0)
            except (BrokenPipeError, EOFError, OSError):
                pass
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
        if connection is not None:
            connection.close()
        self._process = None
        self._connection = None
        self._stream_expected_size = None
        self._stream_added_size = 0
        self._stream_descriptor_dim = None

    def __exit__(self, *args: Any) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class LoadedProxy:
    manifest_path: Path
    manifest_sha256: str
    manifest: Mapping[str, Any]
    gallery: tuple[Mapping[str, Any], ...]
    queries: tuple[Mapping[str, Any], ...]

    def image_path(self, row: Mapping[str, Any]) -> Path:
        relative = Path(str(row["local_path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise BenchmarkError(f"unsafe local_path in manifest: {relative}")
        return self.manifest_path.parent / relative


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_proxy_snapshot(path: str | Path, *, verify_files: bool = True) -> LoadedProxy:
    manifest_path = Path(path)
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest.get("purpose") != PROXY_PURPOSE:
        raise BenchmarkError("manifest is not the explicitly-labelled Commons proxy")
    records = manifest.get("images")
    if not isinstance(records, list) or not records:
        raise BenchmarkError("manifest images must be a non-empty list")
    try:
        current_audit = audit_split(records)
    except CommonsAcquisitionError as exc:
        raise BenchmarkError(str(exc)) from exc
    recorded_audit = manifest.get("split_audit", {})
    if current_audit["gallery_count"] != recorded_audit.get("gallery_count") or current_audit[
        "query_count"
    ] != recorded_audit.get("query_count"):
        raise BenchmarkError("manifest split audit counts are inconsistent")
    gallery = tuple(row for row in records if row.get("split") == "gallery")
    queries = tuple(row for row in records if row.get("split") == "query")
    if verify_files:
        for row in records:
            relative = Path(str(row["local_path"]))
            if relative.is_absolute() or ".." in relative.parts:
                raise BenchmarkError(f"unsafe image path in manifest: {relative}")
            image_path = manifest_path.parent / relative
            if not image_path.is_file():
                raise BenchmarkError(f"snapshot image is missing: {image_path}")
            actual = _sha256_file(image_path)
            if actual != row.get("downloaded_sha256"):
                raise BenchmarkError(f"snapshot SHA-256 mismatch: {image_path}")
    return LoadedProxy(
        manifest_path=manifest_path,
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        manifest=manifest,
        gallery=gallery,
        queries=queries,
    )


def _retriever_metadata(retriever: BaseRetriever) -> dict[str, Any]:
    metadata = retriever.metadata
    if hasattr(metadata, "to_dict"):
        return dict(metadata.to_dict())
    if isinstance(metadata, Mapping):
        return dict(metadata)
    raise BenchmarkError("retriever.metadata must be mapping-like or expose to_dict()")


def _reference_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "city_id": "moscow",
        "source": "wikimedia_commons_evaluation_only",
        "landmark_id": row["landmark_id"],
        "title": row["title"],
        "page_url": row["page_url"],
        "author": row["author"],
        "license_short_name": row["license_short_name"],
        "license_url": row["license_url"],
    }


def _observation(
    query_id: str,
    true_lat: float,
    true_lon: float,
    result: Any,
) -> LocalizationObservation:
    return LocalizationObservation(
        query_id=query_id,
        true_lat=true_lat,
        true_lon=true_lon,
        predicted_lat=result.lat,
        predicted_lon=result.lon,
        status=result.status.value,
        confidence=float(result.confidence),
    )


def _evaluate_one(
    *,
    query_id: str,
    query_image: Any,
    true_lat: float,
    true_lon: float,
    retriever: BaseRetriever,
    exact_search: ExactSearch,
    localizer: SpatialLocalizer,
    top_k: int,
) -> tuple[list[RetrievalResult], LocalizationObservation, dict[str, float], dict[str, Any]]:
    end_to_end_started = time.perf_counter()
    started = time.perf_counter()
    descriptor = retriever.embed_query(query_image)
    embedding_ms = (time.perf_counter() - started) * 1000.0

    started = time.perf_counter()
    matches = exact_search.search_one(descriptor, k=top_k)
    search_ms = (time.perf_counter() - started) * 1000.0

    started = time.perf_counter()
    localized = localizer.localize(matches)
    localization_ms = (time.perf_counter() - started) * 1000.0
    end_to_end_ms = (time.perf_counter() - end_to_end_started) * 1000.0
    observation = _observation(query_id, true_lat, true_lon, localized)
    error_m = (
        None
        if localized.lat is None or localized.lon is None
        else haversine_m(true_lat, true_lon, localized.lat, localized.lon)
    )
    per_query = {
        "query_id": query_id,
        "true_lat": true_lat,
        "true_lon": true_lon,
        "predicted_lat": localized.lat,
        "predicted_lon": localized.lon,
        "error_m": error_m,
        "status": localized.status.value,
        "confidence": localized.confidence,
        "reasons": list(localized.reasons),
        "latency_ms": {
            "query_embedding": embedding_ms,
            "exact_faiss_search": search_ms,
            "spatial_localization": localization_ms,
            "end_to_end": end_to_end_ms,
        },
        "matches": [
            {
                "rank": match.rank,
                "reference_id": match.reference_id,
                "score": match.score,
                "lat": match.lat,
                "lon": match.lon,
                "landmark_id": match.metadata.get("landmark_id"),
                "page_url": match.metadata.get("page_url"),
            }
            for match in matches
        ],
    }
    timings = {
        "query_embedding": embedding_ms,
        "exact_faiss_search": search_ms,
        "spatial_localization": localization_ms,
        "end_to_end": end_to_end_ms,
    }
    return matches, observation, timings, per_query


def _run_robustness(
    *,
    proxy: LoadedProxy,
    retriever: BaseRetriever,
    exact_search: ExactSearch,
    localizer: SpatialLocalizer,
    output_dir: Path,
    top_k: int,
    positive_distance_threshold_m: float,
) -> dict[str, Any]:
    truths: list[QueryGroundTruth] = []
    predictions: dict[str, Sequence[RetrievalResult]] = {}
    observations: list[LocalizationObservation] = []
    per_query: list[dict[str, Any]] = []
    timings: dict[str, list[float]] = {
        "query_embedding": [],
        "exact_faiss_search": [],
        "spatial_localization": [],
        "end_to_end": [],
    }
    variants_by_name: dict[str, list[str]] = {}
    for query in proxy.queries:
        variants = generate_robustness_variants(
            proxy.image_path(query),
            query_id=str(query["record_id"]),
            lat=float(query["lat"]),
            lon=float(query["lon"]),
            output_dir=output_dir / "robustness_images",
            seed=0,
        )
        for variant in variants:
            variant_id = f"{variant.query_id}::{variant.variant}"
            matches, observation, samples, row = _evaluate_one(
                query_id=variant_id,
                query_image=variant.image,
                true_lat=variant.lat,
                true_lon=variant.lon,
                retriever=retriever,
                exact_search=exact_search,
                localizer=localizer,
                top_k=top_k,
            )
            row["variant"] = variant.variant
            row["derived_from_query_id"] = variant.query_id
            row["augmentation_parameters"] = dict(variant.parameters)
            row["source_page_url"] = query["page_url"]
            row["source_author"] = query["author"]
            row["source_license_short_name"] = query["license_short_name"]
            row["source_license_url"] = query["license_url"]
            row["derivative_notice"] = (
                "Locally generated evaluation derivative; any redistribution must comply "
                "with the linked source license, including ShareAlike where applicable."
            )
            truths.append(QueryGroundTruth(variant_id, variant.lat, variant.lon))
            predictions[variant_id] = matches
            observations.append(observation)
            per_query.append(row)
            variants_by_name.setdefault(variant.variant, []).append(variant_id)
            for name, value in samples.items():
                timings[name].append(value)

    overall_retrieval = evaluate_retrieval(
        truths,
        predictions,
        positive_distance_threshold_m=positive_distance_threshold_m,
        ks=(1, 5, 10),
    )
    overall_localization = evaluate_localization(observations)
    by_variant: dict[str, Any] = {}
    truths_by_id = {truth.query_id: truth for truth in truths}
    observations_by_id = {observation.query_id: observation for observation in observations}
    for variant, ids in sorted(variants_by_name.items()):
        variant_truths = [truths_by_id[query_id] for query_id in ids]
        variant_observations = [observations_by_id[query_id] for query_id in ids]
        by_variant[variant] = {
            "retrieval": evaluate_retrieval(
                variant_truths,
                predictions,
                positive_distance_threshold_m=positive_distance_threshold_m,
                ks=(1, 5, 10),
            ).to_dict(),
            "localization": evaluate_localization(variant_observations).to_dict(),
        }
    attribution_lines = [
        "# Robustness derivative attribution",
        "",
        "These files are local evaluation derivatives of the held-out Commons images. ",
        "They are not additional geographic samples. Any redistribution must follow each ",
        "linked source license, including ShareAlike where applicable.",
        "",
    ]
    for query in proxy.queries:
        attribution_lines.extend(
            [
                f"- Source: <{query['page_url']}>",
                f"  - Author/credit: {query.get('attribution', query['author'])}",
                f"  - License: [{query['license_short_name']}]({query['license_url']})",
                f"  - Derived files: `{query['record_id'].replace(':', '_')}__*.jpg`",
            ]
        )
    _atomic_text(
        output_dir / "ROBUSTNESS_ATTRIBUTION.md",
        "\n".join(attribution_lines).rstrip() + "\n",
    )
    return {
        "warning": (
            "Synthetic perturbations of held-out real query images; reported separately "
            "from primary cross-image results and not additional geographic samples."
        ),
        "derived_query_count": len(per_query),
        "source_query_count": len(proxy.queries),
        "variants_per_query": len(variants_by_name),
        "overall": {
            "retrieval": overall_retrieval.to_dict(),
            "localization": overall_localization.to_dict(),
            "latency": summarize_latencies(timings).to_dict(),
        },
        "by_variant": by_variant,
        "per_query": per_query,
    }


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def _markdown_report(payload: Mapping[str, Any]) -> str:
    primary = payload["primary"]
    retrieval = primary["retrieval"]
    localization = primary["localization"]
    lines = [
        f"# {payload['dataset']['dataset_id']} — {payload['model']['model_name']}",
        "",
        f"> **Evaluation-only warning:** {payload['warning']}",
        "",
        "## Snapshot and split",
        "",
        f"- Manifest SHA-256: `{payload['dataset']['manifest_sha256']}`",
        f"- Gallery / query: {payload['split_audit']['gallery_count']} / {payload['split_audit']['query_count']}",
        f"- Landmarks: {payload['split_audit']['landmark_count']}",
        "- Gallery and query contain different Commons page IDs, Commons SHA-1s, and downloaded-byte SHA-256s.",
        f"- Same-author same-landmark cross-split pairs: "
        f"{payload['split_audit']['same_landmark_gallery_query_same_author_pair_count']}",
        f"- Same-time same-landmark cross-split pairs: "
        f"{payload['split_audit']['same_landmark_gallery_query_same_capture_time_pair_count']}",
        "",
        "## Primary cross-image results",
        "",
        f"Coordinate estimator: `{payload['localization']['estimator']}`. The remaining "
        "localizer settings are recorded in JSON.",
        "",
        f"Recall positives are gallery images within "
        f"{retrieval['positive_distance_threshold_m']:g} m of the held-out query geotag.",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for k, value in retrieval["recall_at"].items():
        lines.append(f"| Recall@{k} | {_percent(value)} |")
    for threshold, value in localization["accuracy_within_m"].items():
        lines.append(f"| Accuracy ≤ {threshold} m (all queries) | {_percent(value)} |")
    lines.extend(
        [
            f"| Median error (answered only) | "
            f"{localization['median_error_m'] if localization['median_error_m'] is not None else 'n/a'} m |",
            f"| P90 error (answered only) | "
            f"{localization['p90_error_m'] if localization['p90_error_m'] is not None else 'n/a'} m |",
            f"| Answer rate | {_percent(localization['answer_rate'])} |",
            "",
            "## Latency",
            "",
            "Measured on the report's stated device; model load and gallery/index setup are "
            "not hidden inside per-query latency.",
            "",
            "| Stage | N | Median ms | P90 ms |",
            "|---|---:|---:|---:|",
        ]
    )
    for stage, values in primary["latency"]["stages"].items():
        median = "n/a" if values["median_ms"] is None else f"{values['median_ms']:.2f}"
        p90 = "n/a" if values["p90_ms"] is None else f"{values['p90_ms']:.2f}"
        lines.append(f"| {stage} | {values['count']} | {median} | {p90} |")
    lines.extend(
        [
            "",
            "## Model",
            "",
            f"- Name: `{payload['model']['model_name']}`",
            f"- Device: `{payload['model']['device']}`",
            f"- Checkpoint: `{payload['model']['checkpoint']}`",
            f"- Revision: `{payload['model'].get('revision')}`",
            f"- Model load: {payload['runtime']['model_load_ms']:.2f} ms",
            f"- Gallery embedding: {payload['runtime']['gallery_embedding_ms']:.2f} ms",
            f"- Gallery embedding throughput: "
            f"{primary['latency']['gallery_embedding_images_per_second']:.2f} images/s",
            f"- Descriptor dimension / dtype: "
            f"{payload['runtime']['descriptor_dimension']} / `{payload['runtime']['descriptor_dtype']}`",
            f"- Gallery descriptor storage: "
            f"{payload['runtime']['gallery_descriptor_storage_bytes']} bytes",
            f"- Exact FAISS vector storage (payload only): "
            f"{payload['runtime']['exact_faiss_vector_storage_bytes']} bytes",
            f"- Exact FAISS build: {payload['runtime']['exact_faiss_build_ms']:.2f} ms",
            "",
        ]
    )
    robustness = payload.get("robustness")
    if robustness:
        lines.extend(
            [
                "## Separate augmentation robustness",
                "",
                f"> {robustness['warning']}",
                "",
                f"Derived queries: {robustness['derived_query_count']} from "
                f"{robustness['source_query_count']} held-out source queries.",
                "",
                "| Metric | Value |",
                "|---|---:|",
            ]
        )
        for k, value in robustness["overall"]["retrieval"]["recall_at"].items():
            lines.append(f"| Robustness Recall@{k} | {_percent(value)} |")
        for threshold, value in robustness["overall"]["localization"]["accuracy_within_m"].items():
            lines.append(f"| Robustness accuracy ≤ {threshold} m | {_percent(value)} |")
        lines.append("")
    lines.extend(
        [
            "## Limitations",
            "",
            "- Wikimedia Commons photos are evaluation-only fallback imagery, not the "
            "Mapillary/KartaView production domain.",
            "- The sample is tiny, landmark-selected, visually distinctive, and cannot "
            "estimate city-wide or ordinary-street accuracy.",
            "- Commons geotags and license metadata are contributor-supplied; follow every "
            "linked file page and verify current terms before reuse.",
            "- Spatial confidence uses the unchanged default, uncalibrated localizer; no "
            "threshold was tuned on this tiny proxy.",
            "- Accuracy denominators include all held-out queries; unanswered queries count "
            "as failures. Error percentiles are explicitly answered-query-only.",
            "- These numbers compare runs on this frozen snapshot only; they do not by "
            "themselves justify production model selection.",
            "",
        ]
    )
    return "\n".join(lines)


def write_commons_benchmark_reports(
    payload: Mapping[str, Any], output_dir: str | Path, *, stem: str
) -> tuple[Path, Path]:
    if not stem or Path(stem).name != stem:
        raise ValueError("stem must be a non-empty filename stem")
    output_dir = Path(output_dir)
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    _atomic_text(
        json_path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    _atomic_text(markdown_path, _markdown_report(payload))
    return json_path, markdown_path


def run_commons_benchmark(
    *,
    manifest_path: str | Path,
    retriever: BaseRetriever,
    output_dir: str | Path,
    report_stem: str | None = None,
    top_k: int = 10,
    positive_distance_threshold_m: float = 100.0,
    robustness: bool = False,
    search_factory: Callable[[], ExactSearch] = IsolatedFaissExactSearch,
    localizer: SpatialLocalizer | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    if top_k < 10:
        raise ValueError("top_k must be at least 10 to report Recall@10")
    if positive_distance_threshold_m <= 0:
        raise ValueError("positive_distance_threshold_m must be positive")
    proxy = load_proxy_snapshot(manifest_path)
    active_localizer = localizer or SpatialLocalizer()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timings: dict[str, list[float]] = {
        "query_embedding": [],
        "exact_faiss_search": [],
        "spatial_localization": [],
        "end_to_end": [],
    }
    query_truths: list[QueryGroundTruth] = []
    predictions: dict[str, Sequence[RetrievalResult]] = {}
    observations: list[LocalizationObservation] = []
    per_query: list[dict[str, Any]] = []

    # Spawn exact FAISS before retriever.load() imports/initializes PyTorch.
    with search_factory() as exact_search:
        started = time.perf_counter()
        retriever.load()
        model_load_ms = (time.perf_counter() - started) * 1000.0
        model_metadata = _retriever_metadata(retriever)

        gallery_paths = [proxy.image_path(row) for row in proxy.gallery]
        started = time.perf_counter()
        gallery_descriptors = retriever.embed_batch(gallery_paths)
        gallery_embedding_seconds = time.perf_counter() - started
        gallery_embedding_ms = gallery_embedding_seconds * 1000.0

        started = time.perf_counter()
        exact_search.build(
            gallery_descriptors,
            [str(row["record_id"]) for row in proxy.gallery],
            [_reference_metadata(row) for row in proxy.gallery],
            model_metadata,
        )
        exact_faiss_build_ms = (time.perf_counter() - started) * 1000.0

        for query in proxy.queries:
            query_id = str(query["record_id"])
            true_lat, true_lon = float(query["lat"]), float(query["lon"])
            matches, observation, samples, row = _evaluate_one(
                query_id=query_id,
                query_image=proxy.image_path(query),
                true_lat=true_lat,
                true_lon=true_lon,
                retriever=retriever,
                exact_search=exact_search,
                localizer=active_localizer,
                top_k=top_k,
            )
            row.update(
                {
                    "landmark_id": query["landmark_id"],
                    "title": query["title"],
                    "page_url": query["page_url"],
                    "author": query["author"],
                    "license_short_name": query["license_short_name"],
                    "license_url": query["license_url"],
                }
            )
            query_truths.append(QueryGroundTruth(query_id, true_lat, true_lon))
            predictions[query_id] = matches
            observations.append(observation)
            per_query.append(row)
            for name, value in samples.items():
                timings[name].append(value)

        retrieval_metrics = evaluate_retrieval(
            query_truths,
            predictions,
            positive_distance_threshold_m=positive_distance_threshold_m,
            ks=(1, 5, 10),
        )
        localization_metrics = evaluate_localization(observations, thresholds_m=(25, 50, 100))
        latency_metrics = summarize_latencies(
            timings,
            gallery_embedding_images=len(proxy.gallery),
            gallery_embedding_seconds=gallery_embedding_seconds,
        )
        bundle = EvaluationBundle(
            dataset_id=str(proxy.manifest["dataset_id"]),
            split_name="different_commons_page_cross_image",
            retrieval=retrieval_metrics,
            localization=localization_metrics,
            latency=latency_metrics,
            model=model_metadata,
            notes=(PROXY_WARNING,),
        )
        robustness_payload = (
            _run_robustness(
                proxy=proxy,
                retriever=retriever,
                exact_search=exact_search,
                localizer=active_localizer,
                output_dir=output_dir,
                top_k=top_k,
                positive_distance_threshold_m=positive_distance_threshold_m,
            )
            if robustness
            else None
        )

    split_audit = audit_split(proxy.manifest["images"])
    localizer_config = active_localizer.config
    payload: dict[str, Any] = {
        "schema_version": 1,
        "warning": PROXY_WARNING,
        "dataset": {
            "dataset_id": proxy.manifest["dataset_id"],
            "purpose": proxy.manifest["purpose"],
            "manifest_path": str(proxy.manifest_path),
            "manifest_sha256": proxy.manifest_sha256,
            "source": proxy.manifest.get("source"),
        },
        "split_audit": split_audit,
        "model": model_metadata,
        "localization": {
            "estimator": localizer_config.estimator.value,
            "cluster_radius_m": localizer_config.cluster_radius_m,
            "max_cluster_diameter_m": localizer_config.max_cluster_diameter_m,
            "out_of_coverage_similarity": localizer_config.out_of_coverage_similarity,
            "confidence_threshold": localizer_config.confidence_threshold,
            "confidence_calibrated": active_localizer.uncertainty_calibration is not None,
        },
        "runtime": {
            "model_load_ms": model_load_ms,
            "gallery_embedding_ms": gallery_embedding_ms,
            "descriptor_dimension": int(gallery_descriptors.shape[1]),
            "descriptor_dtype": str(gallery_descriptors.dtype),
            "gallery_descriptor_storage_bytes": int(gallery_descriptors.nbytes),
            # IndexFlatIP stores the same float32 vectors; this excludes small
            # implementation/process/metadata overhead and is labelled as such.
            "exact_faiss_vector_storage_bytes": int(gallery_descriptors.nbytes),
            "exact_faiss_build_ms": exact_faiss_build_ms,
            "exact_search_backend": "faiss.IndexFlatIP (L2-normalized descriptors)",
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": sys.version.split()[0],
                "numpy": np.__version__,
                "torch": _package_version("torch"),
                "faiss_cpu": _package_version("faiss-cpu"),
            },
        },
        "primary": bundle.to_dict(),
        "per_query": per_query,
        "robustness": robustness_payload,
        "limitations": [
            "evaluation-only Wikimedia Commons fallback; not production gallery data",
            "tiny hand-curated landmark-biased sample",
            "not representative of Moscow streets or product coverage",
            "geotags and license metadata are contributor-supplied",
            "default spatial confidence thresholds are uncalibrated and were not proxy-tuned",
            "no model-selection claim follows from this proxy alone",
        ],
    }
    stem = report_stem or f"{model_metadata['model_name']}_moscow_commons_proxy"
    json_path, markdown_path = write_commons_benchmark_reports(payload, output_dir, stem=stem)
    return payload, json_path, markdown_path


def _compact_result(payload: Mapping[str, Any], json_path: Path, markdown_path: Path) -> dict[str, Any]:
    primary = payload["primary"]
    latency = primary["latency"]["stages"].get("end_to_end", {})
    return {
        "warning": payload["warning"],
        "model": payload["model"]["model_name"],
        "device": payload["model"]["device"],
        "estimator": payload["localization"]["estimator"],
        "gallery_count": payload["split_audit"]["gallery_count"],
        "query_count": payload["split_audit"]["query_count"],
        "recall_at": primary["retrieval"]["recall_at"],
        "accuracy_within_m": primary["localization"]["accuracy_within_m"],
        "median_error_m_answered": primary["localization"]["median_error_m"],
        "p90_error_m_answered": primary["localization"]["p90_error_m"],
        "median_end_to_end_ms": latency.get("median_ms"),
        "robustness_derived_query_count": (
            payload["robustness"]["derived_query_count"] if payload["robustness"] else 0
        ),
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark a real model on the tiny Moscow Commons proxy")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", choices=("megaloc", "dinov2-salad"), required=True)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--positive-distance-m", type=float, default=100.0)
    parser.add_argument(
        "--estimator",
        choices=tuple(value.value for value in CoordinateEstimator),
        default=CoordinateEstimator.WEIGHTED_MEDOID.value,
    )
    parser.add_argument("--report-stem")
    parser.add_argument("--robustness", action="store_true")
    args = parser.parse_args()

    retriever = create_retriever(
        args.model,
        device=args.device,
        batch_size=args.batch_size,
        cache_dir=args.cache_dir,
    )
    try:
        payload, json_path, markdown_path = run_commons_benchmark(
            manifest_path=args.manifest,
            retriever=retriever,
            output_dir=args.output_dir,
            report_stem=args.report_stem,
            top_k=args.top_k,
            positive_distance_threshold_m=args.positive_distance_m,
            robustness=args.robustness,
            localizer=SpatialLocalizer(LocalizerConfig(estimator=CoordinateEstimator(args.estimator))),
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
