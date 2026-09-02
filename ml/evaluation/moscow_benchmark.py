"""Leakage-resistant benchmark for real Moscow street-view manifests.

Unlike :mod:`ml.evaluation.commons_benchmark`, this runner accepts two
canonical Parquet manifests and does not know about (or accept) the Commons
proxy purpose marker.  Gallery and query images are embedded by one loaded
official retriever, searched with the existing process-isolated exact FAISS
implementation, and localized with the production ``SpatialLocalizer``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import resource
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageOps, UnidentifiedImageError

from ml.evaluation.augmentations import generate_robustness_variants
from ml.evaluation.commons_benchmark import ExactSearch, IsolatedFaissExactSearch
from ml.evaluation.metrics import (
    LocalizationObservation,
    QueryGroundTruth,
    evaluate_localization,
    evaluate_retrieval,
    summarize_latencies,
)
from ml.indexing.faiss_index import RetrievalResult
from ml.ingestion.schema import (
    CANONICAL_COLUMNS,
    MOSCOW_BOUNDS,
    coerce_manifest_schema,
    is_missing_value,
    validate_manifest_schema,
)
from ml.localization import LocalizerConfig, SpatialLocalizer, haversine_m
from ml.localization.estimators import CoordinateEstimator
from ml.query_quality import measure_query_image_quality
from ml.retrieval import BaseRetriever, create_retriever
from ml.retrieval.base import l2_normalize
from ml.retrieval.embedding_job import load_embedding_artifacts
from ml.retrieval.query_aggregation import SUPPORTED_QUERY_AGGREGATIONS, embed_aggregated_query
from ml.runtime_config import FrozenRuntimeConfig

BENCHMARK_KIND = "real_moscow_street_view"
REPORT_SCHEMA_VERSION = 1
POSITIVE_DISTANCE_THRESHOLD_M = 100.0
LOCALIZATION_THRESHOLDS_M = (25, 50, 100)
RECALL_KS = (1, 5, 10, 20, 50)
AREA_COLUMNS = (
    "area_id",
    "evaluation_area_h3",
    "evaluation_sample_h3",
    "h3_coarse",
    "h3_fine",
)
DECLARED_HASH_COLUMNS = (
    "sha256",
    "image_sha256",
    "content_sha256",
    "downloaded_sha256",
    "file_sha256",
)
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class MoscowBenchmarkError(RuntimeError):
    """A real-data benchmark precondition was not satisfied."""


def _real_moscow_exact_search() -> IsolatedFaissExactSearch:
    """Create the shared isolated exact search with truthful run metadata."""

    return IsolatedFaissExactSearch(
        index_id="moscow-real-street-view-runtime",
        city_id="moscow",
        extra_metadata={
            "evaluation_only": True,
            "dataset_kind": BENCHMARK_KIND,
        },
    )


@dataclass(frozen=True, slots=True)
class ManifestRow:
    """One validated canonical row plus its resolved immutable image identity."""

    values: Mapping[str, Any]
    image_path: Path
    image_sha256: str

    @property
    def reference_id(self) -> str:
        return str(self.values["id"])


@dataclass(frozen=True, slots=True)
class LoadedManifest:
    """A validated Parquet manifest and its resolved records."""

    path: Path
    sha256: str
    columns: tuple[str, ...]
    rows: tuple[ManifestRow, ...]


@dataclass(frozen=True, slots=True)
class LoadedMoscowBenchmark:
    gallery: LoadedManifest
    queries: LoadedManifest
    leakage_audit: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class GalleryBuildStats:
    """Logical descriptor/storage facts from a materialized or streamed build."""

    gallery_embedding_seconds: float
    exact_faiss_build_ms: float
    descriptor_dimension: int
    descriptor_dtype: str
    descriptor_itemsize: int
    descriptor_storage_bytes: int
    embedding_source: str = "computed"
    embedding_artifact_generation: str | None = None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _package_version(name: str) -> str | None:
    try:
        from importlib.metadata import version

        return version(name)
    except Exception:
        return None


def _peak_process_rss_bytes() -> int:
    """Return peak parent-process RSS with Unix platform units normalized."""

    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _json_safe(value: Any) -> Any:
    if is_missing_value(value) or value.__class__.__name__ == "NaTType":
        return None
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if bool(value != value):  # NaN and pandas NA-like values
            return None
    except (TypeError, ValueError):
        return None
    text = str(value).strip()
    return text or None


def _resolve_image_path(raw_path: Any, manifest_path: Path) -> Path:
    text = _optional_text(raw_path)
    if text is None:
        raise MoscowBenchmarkError(f"manifest {manifest_path} contains a blank image_path")
    path = Path(text).expanduser()
    if path.is_absolute():
        return path.resolve()

    candidates = [Path.cwd() / path, manifest_path.parent / path]
    existing = [candidate.resolve() for candidate in candidates if candidate.is_file()]
    if not existing:
        # Return the conventional project-root-relative interpretation so the
        # caller can report one concrete missing path.
        return candidates[0].resolve()
    unique = list(dict.fromkeys(existing))
    if len(unique) > 1:
        raise MoscowBenchmarkError(
            f"ambiguous relative image_path {text!r} in {manifest_path}; "
            f"both {unique[0]} and {unique[1]} exist"
        )
    return unique[0]


def _validate_declared_hashes(row: Mapping[str, Any], *, manifest_path: Path, row_id: str) -> None:
    for column in DECLARED_HASH_COLUMNS:
        if column not in row:
            continue
        value = _optional_text(row[column])
        if value is not None and _SHA256_PATTERN.fullmatch(value.lower()) is None:
            raise MoscowBenchmarkError(
                f"manifest {manifest_path} row {row_id!r} has invalid {column}; expected SHA-256 hex"
            )


def _verify_declared_hashes_match_file(
    row: Mapping[str, Any],
    *,
    actual_sha256: str,
    manifest_path: Path,
    row_id: str,
) -> None:
    for column in DECLARED_HASH_COLUMNS:
        if column not in row:
            continue
        declared = _optional_text(row[column])
        if declared is not None and declared.lower() != actual_sha256:
            raise MoscowBenchmarkError(
                f"manifest {manifest_path} row {row_id!r} declared {column} does not match actual image bytes"
            )


def load_moscow_manifest(path: str | Path) -> LoadedManifest:
    """Load one canonical Moscow Parquet manifest and verify every local image."""

    manifest_path = Path(path).expanduser().resolve()
    if not manifest_path.is_file():
        raise MoscowBenchmarkError(f"manifest is missing: {manifest_path}")
    if manifest_path.suffix.lower() != ".parquet":
        raise MoscowBenchmarkError(f"manifest must be a Parquet file: {manifest_path}")
    try:
        import pandas as pd

        raw = pd.read_parquet(manifest_path)
    except Exception as exc:
        raise MoscowBenchmarkError(f"cannot read Parquet manifest {manifest_path}: {exc}") from exc

    missing_columns = [column for column in CANONICAL_COLUMNS if column not in raw.columns]
    if missing_columns:
        raise MoscowBenchmarkError(
            f"manifest {manifest_path} is missing canonical columns: {missing_columns}"
        )
    normalized = coerce_manifest_schema(raw)
    schema_errors = validate_manifest_schema(normalized, allow_empty=False, strict_reference=False)
    if schema_errors:
        raise MoscowBenchmarkError(
            f"manifest {manifest_path} failed canonical schema validation: {schema_errors}"
        )

    city_values = {
        str(value).strip().lower()
        for value in normalized["city_id"].tolist()
        if _optional_text(value) is not None
    }
    if city_values != {"moscow"}:
        raise MoscowBenchmarkError(
            f"manifest {manifest_path} must contain only city_id='moscow', got {sorted(city_values)}"
        )
    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    outside_moscow = normalized[
        ~normalized["lat"].between(min_lat, max_lat)
        | ~normalized["lon"].between(min_lon, max_lon)
    ]
    if not outside_moscow.empty:
        ids = outside_moscow["id"].astype("string").head(10).tolist()
        raise MoscowBenchmarkError(
            f"manifest {manifest_path} contains coordinates outside Moscow bounds: {ids}"
        )

    records: list[ManifestRow] = []
    for raw_row in normalized.to_dict(orient="records"):
        row = {str(key): _json_safe(value) for key, value in raw_row.items()}
        row_id = str(row["id"])
        _validate_declared_hashes(row, manifest_path=manifest_path, row_id=row_id)
        image_path = _resolve_image_path(row["image_path"], manifest_path)
        if not image_path.is_file():
            raise MoscowBenchmarkError(
                f"manifest {manifest_path} row {row_id!r} references a missing image: {image_path}"
            )
        try:
            with Image.open(image_path) as image:
                image.verify()
        except (OSError, UnidentifiedImageError) as exc:
            raise MoscowBenchmarkError(
                f"manifest {manifest_path} row {row_id!r} references an invalid image: {image_path}"
            ) from exc
        image_sha256 = _sha256_file(image_path)
        _verify_declared_hashes_match_file(
            row,
            actual_sha256=image_sha256,
            manifest_path=manifest_path,
            row_id=row_id,
        )
        records.append(
            ManifestRow(
                values=row,
                image_path=image_path,
                image_sha256=image_sha256,
            )
        )
    return LoadedManifest(
        path=manifest_path,
        sha256=_sha256_file(manifest_path),
        columns=tuple(str(column) for column in raw.columns),
        rows=tuple(records),
    )


def _normalized_values(rows: Sequence[ManifestRow], column: str) -> set[str]:
    return {
        text
        for row in rows
        if (text := _optional_text(row.values.get(column))) is not None
    }


def _identity_values(rows: Sequence[ManifestRow], *columns: str) -> set[str]:
    identities: set[str] = set()
    for row in rows:
        values = [_optional_text(row.values.get(column)) for column in columns]
        values = [
            value.lower() if value is not None and column == "source" else value
            for column, value in zip(columns, values, strict=True)
        ]
        if all(value is not None for value in values):
            identities.add("::".join(str(value) for value in values))
    return identities


def _limited_overlap(left: set[str], right: set[str]) -> list[str]:
    return sorted(left.intersection(right))[:100]


def audit_query_gallery_leakage(
    gallery: LoadedManifest,
    queries: LoadedManifest,
) -> dict[str, Any]:
    """Reject exact cross-split identity/content/sequence leakage.

    Sequence identity is namespaced by provider.  Rejecting a whole shared
    sequence is intentionally stronger than trying to guess an adjacent-frame
    cutoff from provider-specific metadata.
    """

    declared_hash_overlaps: dict[str, list[str]] = {}
    declared_hash_checked: list[str] = []
    for column in DECLARED_HASH_COLUMNS:
        if column in gallery.columns and column in queries.columns:
            gallery_values = {value.lower() for value in _normalized_values(gallery.rows, column)}
            query_values = {value.lower() for value in _normalized_values(queries.rows, column)}
            if gallery_values or query_values:
                declared_hash_checked.append(column)
                declared_hash_overlaps[column] = _limited_overlap(gallery_values, query_values)

    overlaps: dict[str, Any] = {
        "id": _limited_overlap(
            _normalized_values(gallery.rows, "id"),
            _normalized_values(queries.rows, "id"),
        ),
        "source_image_identity": _limited_overlap(
            _identity_values(gallery.rows, "source", "source_image_id"),
            _identity_values(queries.rows, "source", "source_image_id"),
        ),
        "source_url": _limited_overlap(
            _normalized_values(gallery.rows, "source_url"),
            _normalized_values(queries.rows, "source_url"),
        ),
        "resolved_image_path": _limited_overlap(
            {str(row.image_path) for row in gallery.rows},
            {str(row.image_path) for row in queries.rows},
        ),
        "computed_image_sha256": _limited_overlap(
            {row.image_sha256 for row in gallery.rows},
            {row.image_sha256 for row in queries.rows},
        ),
        "sequence_identity": _limited_overlap(
            _identity_values(gallery.rows, "source", "sequence_id"),
            _identity_values(queries.rows, "source", "sequence_id"),
        ),
        "declared_hash_columns": declared_hash_overlaps,
    }
    failing = {
        name: values
        for name, values in overlaps.items()
        if (isinstance(values, list) and values)
        or (isinstance(values, Mapping) and any(values.values()))
    }
    if failing:
        rendered = json.dumps(failing, ensure_ascii=False, sort_keys=True)
        raise MoscowBenchmarkError(f"query/gallery leakage detected; refusing benchmark: {rendered}")

    gallery_sequences = _identity_values(gallery.rows, "source", "sequence_id")
    query_sequences = _identity_values(queries.rows, "source", "sequence_id")
    return {
        "passed": True,
        "policy": (
            "fail_closed_on_cross_split_id_source_image_source_url_path_exact_bytes_"
            "declared_hash_or_whole_provider_sequence_overlap"
        ),
        "checks": {
            "id": True,
            "source_image_identity": True,
            "source_url": bool(
                _normalized_values(gallery.rows, "source_url")
                or _normalized_values(queries.rows, "source_url")
            ),
            "resolved_image_path": True,
            "computed_image_sha256": True,
            "sequence_identity": bool(gallery_sequences or query_sequences),
            "declared_hash_columns": declared_hash_checked,
        },
        "overlaps": overlaps,
        "gallery_sequence_count": len(gallery_sequences),
        "query_sequence_count": len(query_sequences),
    }


def load_moscow_benchmark(
    gallery_manifest_path: str | Path,
    query_manifest_path: str | Path,
) -> LoadedMoscowBenchmark:
    """Load and leakage-audit two independent real street-view manifests."""

    gallery_path = Path(gallery_manifest_path).expanduser().resolve()
    query_path = Path(query_manifest_path).expanduser().resolve()
    if gallery_path == query_path:
        raise MoscowBenchmarkError("gallery and query manifests must be different files")
    gallery = load_moscow_manifest(gallery_path)
    queries = load_moscow_manifest(query_path)
    leakage_audit = audit_query_gallery_leakage(gallery, queries)
    return LoadedMoscowBenchmark(gallery=gallery, queries=queries, leakage_audit=leakage_audit)


def _counter(rows: Sequence[ManifestRow], column: str) -> dict[str, int]:
    return dict(
        sorted(
            Counter(
                value.lower() if column == "source" else value
                for row in rows
                if (value := _optional_text(row.values.get(column))) is not None
            ).items()
        )
    )


def _manifest_profile(manifest: LoadedManifest) -> dict[str, Any]:
    area_counts = {
        column: counts
        for column in AREA_COLUMNS
        if column in manifest.columns and (counts := _counter(manifest.rows, column))
    }
    sequences_by_source: dict[str, int] = {}
    for source in _counter(manifest.rows, "source"):
        values = {
            sequence
            for row in manifest.rows
            if _optional_text(row.values.get("source")) == source
            and (sequence := _optional_text(row.values.get("sequence_id"))) is not None
        }
        sequences_by_source[source] = len(values)
    content_hashes = sorted(row.image_sha256 for row in manifest.rows)
    content_set_digest = hashlib.sha256("\n".join(content_hashes).encode("ascii")).hexdigest()
    return {
        "manifest_path": str(manifest.path),
        "manifest_sha256": manifest.sha256,
        "count": len(manifest.rows),
        "source_counts": _counter(manifest.rows, "source"),
        "area_counts": area_counts,
        "sequence_count": len(_identity_values(manifest.rows, "source", "sequence_id")),
        "sequences_by_source": sequences_by_source,
        "unique_computed_image_sha256_count": len(set(content_hashes)),
        "ordered_image_sha256_digest": content_set_digest,
        "columns": list(manifest.columns),
    }


def _retriever_metadata(retriever: BaseRetriever) -> dict[str, Any]:
    metadata = retriever.metadata
    if hasattr(metadata, "to_dict"):
        payload = dict(metadata.to_dict())
    elif isinstance(metadata, Mapping):
        payload = dict(metadata)
    else:
        raise MoscowBenchmarkError("retriever.metadata must be mapping-like or expose to_dict()")
    metadata_name = str(payload.get("model_name", "")).strip().lower().replace("_", "-")
    retriever_name = str(retriever.model_name).strip().lower().replace("_", "-")
    if metadata_name != retriever_name:
        raise MoscowBenchmarkError(
            f"retriever model mismatch: runtime={retriever_name!r}, metadata={metadata_name!r}"
        )
    try:
        descriptor_dim = int(payload["descriptor_dim"])
    except (KeyError, TypeError, ValueError) as exc:
        raise MoscowBenchmarkError("retriever metadata lacks a valid descriptor_dim") from exc
    if descriptor_dim != int(retriever.descriptor_dim):
        raise MoscowBenchmarkError(
            "retriever descriptor mismatch: runtime and metadata dimensions differ"
        )
    return payload


def _validate_expected_model(retriever: BaseRetriever, expected_model: str | None) -> None:
    if expected_model is None:
        return
    expected = expected_model.strip().lower().replace("_", "-")
    actual = str(retriever.model_name).strip().lower().replace("_", "-")
    if expected != actual:
        raise MoscowBenchmarkError(
            f"configured model {expected!r} does not match retriever {actual!r}"
        )


def _reference_metadata(row: ManifestRow) -> dict[str, Any]:
    values = row.values
    return {
        "lat": float(values["lat"]),
        "lon": float(values["lon"]),
        "city_id": str(values["city_id"]),
        "source": str(values["source"]),
        "source_image_id": str(values["source_image_id"]),
        "sequence_id": _optional_text(values.get("sequence_id")),
        "captured_at": _optional_text(values.get("captured_at")),
        "heading": values.get("heading"),
        "quality_score": values.get("quality_score"),
        "license": values.get("license"),
        "attribution": values.get("attribution"),
        "source_url": values.get("source_url"),
        "area_id": values.get("area_id"),
        "evaluation_area_h3": values.get("evaluation_area_h3"),
        "evaluation_sample_h3": values.get("evaluation_sample_h3"),
        "h3_coarse": values.get("h3_coarse"),
        "h3_fine": values.get("h3_fine"),
        "computed_image_sha256": row.image_sha256,
    }


def _local_gallery_density(queries: LoadedManifest, gallery: LoadedManifest) -> dict[str, int]:
    gallery_lat = np.radians([float(row.values["lat"]) for row in gallery.rows])
    gallery_lon = np.radians([float(row.values["lon"]) for row in gallery.rows])
    densities: dict[str, int] = {}
    for query in queries.rows:
        query_lat = np.radians(float(query.values["lat"]))
        query_lon = np.radians(float(query.values["lon"]))
        delta_lat = gallery_lat - query_lat
        delta_lon = gallery_lon - query_lon
        haversine = np.sin(delta_lat / 2.0) ** 2 + np.cos(query_lat) * np.cos(gallery_lat) * (
            np.sin(delta_lon / 2.0) ** 2
        )
        distances = 2.0 * 6_371_008.8 * np.arcsin(np.sqrt(np.clip(haversine, 0.0, 1.0)))
        densities[query.reference_id] = int(np.sum(distances <= 100.0))
    return densities


def _optional_finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def _positive_slice_fields(
    query_values: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    *,
    local_density: int,
) -> dict[str, Any]:
    positive = diagnostics["by_positive_distance_m"]["100"].get("positive_reference")
    metadata = positive.get("metadata", {}) if isinstance(positive, Mapping) else {}
    query_provider = str(query_values.get("source") or "unknown").lower()
    positive_provider = str(metadata.get("source") or "missing_positive").lower()
    query_heading = _optional_finite_float(query_values.get("heading"))
    positive_heading = _optional_finite_float(metadata.get("heading"))
    if query_heading is None or positive_heading is None:
        heading_bucket = "missing"
    else:
        gap = abs((query_heading - positive_heading + 180.0) % 360.0 - 180.0)
        heading_bucket = "lt45" if gap < 45 else ("45_89" if gap < 90 else "ge90")
    query_time = _optional_text(query_values.get("captured_at"))
    positive_time = _optional_text(metadata.get("captured_at"))
    try:
        gap_days = abs(
            (
                datetime.fromisoformat(query_time.replace("Z", "+00:00"))
                - datetime.fromisoformat(positive_time.replace("Z", "+00:00"))
            ).total_seconds()
        ) / 86_400.0
    except (AttributeError, TypeError, ValueError):
        temporal_bucket = "missing"
    else:
        temporal_bucket = "lt30d" if gap_days < 30 else ("30_364d" if gap_days < 365 else "ge365d")
    density_bucket = "1" if local_density <= 1 else ("2_4" if local_density <= 4 else "ge5")
    return {
        "local_gallery_density_100m": local_density,
        "local_gallery_density_bucket": density_bucket,
        "positive_provider_pair": f"{query_provider}->{positive_provider}",
        "positive_heading_gap_bucket": heading_bucket,
        "positive_temporal_gap_bucket": temporal_bucket,
    }


def _streamed_build_methods(exact_search: ExactSearch) -> tuple[Callable[..., Any], ...] | None:
    """Return the optional bounded-build lifecycle offered by the FAISS worker."""

    methods = tuple(
        getattr(exact_search, name, None)
        for name in ("begin_build", "add_batch", "finish_build")
    )
    return methods if all(callable(method) for method in methods) else None


def _build_gallery_search(
    *,
    gallery_rows: Sequence[ManifestRow],
    retriever: BaseRetriever,
    exact_search: ExactSearch,
    model_metadata: Mapping[str, Any],
    gallery_embedding_dir: str | Path | None = None,
) -> GalleryBuildStats:
    """Embed the gallery without retaining a full descriptor matrix when supported.

    ``IsolatedFaissExactSearch`` acknowledges every append, which bounds the
    Torch-side matrix and the IPC payload to one model batch.  The legacy
    fallback intentionally preserves the existing injected test/custom-search
    contract.
    """

    expected_dimension = int(model_metadata["descriptor_dim"])
    if gallery_embedding_dir is not None:
        descriptors, ids, _, artifact_metadata = load_embedding_artifacts(gallery_embedding_dir)
        expected_ids = [row.reference_id for row in gallery_rows]
        if ids != expected_ids:
            raise MoscowBenchmarkError(
                "precomputed gallery embedding IDs do not exactly match the ordered gallery manifest"
            )
        expected_model = dict(model_metadata)
        actual_model = dict(artifact_metadata.get("retriever", {}))
        expected_model.pop("device", None)
        actual_model.pop("device", None)
        if actual_model != expected_model:
            raise MoscowBenchmarkError("precomputed gallery embeddings do not match the loaded retriever")
        if descriptors.shape != (len(gallery_rows), expected_dimension):
            raise MoscowBenchmarkError("precomputed gallery descriptor shape is inconsistent")
        streamed_methods = _streamed_build_methods(exact_search)
        started = time.perf_counter()
        if streamed_methods is None:
            exact_search.build(
                descriptors,
                expected_ids,
                [_reference_metadata(row) for row in gallery_rows],
                model_metadata,
            )
        else:
            begin_build, add_batch, finish_build = streamed_methods
            begin_build(
                descriptor_dim=expected_dimension,
                expected_size=len(gallery_rows),
                retriever_metadata=model_metadata,
            )
            batch_size = max(int(getattr(retriever, "batch_size", 1)), 1)
            for offset in range(0, len(gallery_rows), batch_size):
                batch = gallery_rows[offset : offset + batch_size]
                add_batch(
                    np.asarray(descriptors[offset : offset + len(batch)], dtype=np.float32),
                    expected_ids[offset : offset + len(batch)],
                    [_reference_metadata(row) for row in batch],
                )
            finish_build()
        dtype = np.dtype(descriptors.dtype)
        return GalleryBuildStats(
            gallery_embedding_seconds=0.0,
            exact_faiss_build_ms=(time.perf_counter() - started) * 1000.0,
            descriptor_dimension=expected_dimension,
            descriptor_dtype=str(dtype),
            descriptor_itemsize=int(dtype.itemsize),
            descriptor_storage_bytes=int(descriptors.nbytes),
            embedding_source="validated_precomputed_artifact",
            embedding_artifact_generation=str(artifact_metadata["artifact_generation"]),
        )

    streamed_methods = _streamed_build_methods(exact_search)
    if streamed_methods is None:
        gallery_paths = [row.image_path for row in gallery_rows]
        started = time.perf_counter()
        gallery_descriptors = retriever.embed_batch(gallery_paths)
        gallery_descriptors = retriever.validate_descriptors(
            gallery_descriptors,
            expected_rows=len(gallery_paths),
            normalize=True,
        )
        gallery_embedding_seconds = time.perf_counter() - started
        if gallery_descriptors.shape[1] != expected_dimension:
            raise MoscowBenchmarkError(
                "gallery descriptor dimension does not match loaded model metadata"
            )
        started = time.perf_counter()
        exact_search.build(
            gallery_descriptors,
            [row.reference_id for row in gallery_rows],
            [_reference_metadata(row) for row in gallery_rows],
            model_metadata,
        )
        return GalleryBuildStats(
            gallery_embedding_seconds=gallery_embedding_seconds,
            exact_faiss_build_ms=(time.perf_counter() - started) * 1000.0,
            descriptor_dimension=int(gallery_descriptors.shape[1]),
            descriptor_dtype=str(gallery_descriptors.dtype),
            descriptor_itemsize=int(gallery_descriptors.dtype.itemsize),
            descriptor_storage_bytes=int(gallery_descriptors.nbytes),
        )

    begin_build, add_batch, finish_build = streamed_methods
    try:
        batch_size = int(getattr(retriever, "batch_size", 1))
    except (TypeError, ValueError) as exc:
        raise MoscowBenchmarkError("retriever batch_size must be a positive integer") from exc
    if batch_size < 1:
        raise MoscowBenchmarkError("retriever batch_size must be a positive integer")

    exact_faiss_build_ms = 0.0
    started = time.perf_counter()
    begin_build(
        descriptor_dim=expected_dimension,
        expected_size=len(gallery_rows),
        retriever_metadata=model_metadata,
    )
    exact_faiss_build_ms += (time.perf_counter() - started) * 1000.0
    gallery_embedding_seconds = 0.0
    descriptor_dtype = np.dtype(np.float32)
    for start in range(0, len(gallery_rows), batch_size):
        batch = gallery_rows[start : start + batch_size]
        started = time.perf_counter()
        descriptors = retriever.embed_batch([row.image_path for row in batch])
        descriptors = retriever.validate_descriptors(
            descriptors,
            expected_rows=len(batch),
            normalize=True,
        )
        gallery_embedding_seconds += time.perf_counter() - started
        if descriptors.shape[1] != expected_dimension:
            raise MoscowBenchmarkError(
                "gallery descriptor dimension does not match loaded model metadata"
            )
        descriptor_dtype = descriptors.dtype
        started = time.perf_counter()
        add_batch(
            descriptors,
            [row.reference_id for row in batch],
            [_reference_metadata(row) for row in batch],
        )
        exact_faiss_build_ms += (time.perf_counter() - started) * 1000.0
    started = time.perf_counter()
    finish_build()
    exact_faiss_build_ms += (time.perf_counter() - started) * 1000.0
    descriptor_itemsize = int(descriptor_dtype.itemsize)
    return GalleryBuildStats(
        gallery_embedding_seconds=gallery_embedding_seconds,
        exact_faiss_build_ms=exact_faiss_build_ms,
        descriptor_dimension=expected_dimension,
        descriptor_dtype=str(descriptor_dtype),
        descriptor_itemsize=descriptor_itemsize,
        descriptor_storage_bytes=len(gallery_rows) * expected_dimension * descriptor_itemsize,
    )


def _observation(row: ManifestRow, result: Any) -> LocalizationObservation:
    return LocalizationObservation(
        query_id=row.reference_id,
        true_lat=float(row.values["lat"]),
        true_lon=float(row.values["lon"]),
        predicted_lat=result.lat,
        predicted_lon=result.lon,
        status=result.status.value,
        confidence=float(result.confidence),
    )


def _evaluate_image(
    *,
    query_id: str,
    query_image: Any,
    true_lat: float,
    true_lon: float,
    retriever: BaseRetriever,
    exact_search: ExactSearch,
    localizer: SpatialLocalizer,
    top_k: int,
    query_aggregation: str = "single",
) -> tuple[list[RetrievalResult], LocalizationObservation, dict[str, float], dict[str, Any]]:
    end_to_end_started = time.perf_counter()
    started = time.perf_counter()
    try:
        if isinstance(query_image, Image.Image):
            prepared_image = ImageOps.exif_transpose(query_image).convert("RGB")
        else:
            with Image.open(query_image) as opened:
                opened.load()
                prepared_image = ImageOps.exif_transpose(opened).convert("RGB")
    except (OSError, SyntaxError, ValueError, UnidentifiedImageError) as exc:
        raise MoscowBenchmarkError(f"cannot prepare query image {query_id!r}") from exc
    quality = measure_query_image_quality(prepared_image)
    query_preprocessing_ms = (time.perf_counter() - started) * 1000.0

    started = time.perf_counter()
    descriptor = embed_aggregated_query(retriever, prepared_image, policy=query_aggregation)
    descriptor = l2_normalize(descriptor)[0]
    query_embedding_ms = (time.perf_counter() - started) * 1000.0
    if descriptor.shape != (retriever.descriptor_dim,):
        raise MoscowBenchmarkError(
            f"query {query_id!r} descriptor shape mismatch: {descriptor.shape}"
        )

    started = time.perf_counter()
    matches = exact_search.search_one(descriptor, k=top_k)
    diagnose = getattr(exact_search, "diagnose_one", None)
    retrieval_diagnostics = (
        diagnose(
            descriptor,
            true_lat=true_lat,
            true_lon=true_lon,
            distance_thresholds_m=(25.0, 50.0, 100.0),
        )
        if callable(diagnose)
        else _top_k_rank_diagnostics(matches, true_lat=true_lat, true_lon=true_lon)
    )
    exact_faiss_search_ms = (time.perf_counter() - started) * 1000.0

    started = time.perf_counter()
    localized = localizer.localize(matches, query_quality=quality.confidence_signal)
    spatial_localization_ms = (time.perf_counter() - started) * 1000.0
    end_to_end_ms = (time.perf_counter() - end_to_end_started) * 1000.0
    observation = LocalizationObservation(
        query_id=query_id,
        true_lat=true_lat,
        true_lon=true_lon,
        predicted_lat=localized.lat,
        predicted_lon=localized.lon,
        status=localized.status.value,
        confidence=float(localized.confidence),
    )
    error_m = (
        None
        if localized.lat is None or localized.lon is None
        else haversine_m(true_lat, true_lon, localized.lat, localized.lon)
    )
    timings = {
        "query_preprocessing": query_preprocessing_ms,
        "query_embedding": query_embedding_ms,
        "exact_faiss_search": exact_faiss_search_ms,
        "spatial_localization": spatial_localization_ms,
        "end_to_end": end_to_end_ms,
    }
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
        "query_quality": {
            "sharpness": quality.sharpness,
            "brightness": quality.brightness,
            "exposure": quality.exposure,
            "confidence_signal": quality.confidence_signal,
            "method": "shared_production_query_quality_v1",
        },
        "latency_ms": timings,
        "matches": [
            {
                "rank": match.rank,
                "reference_id": match.reference_id,
                "score": match.score,
                "lat": match.lat,
                "lon": match.lon,
                "source": match.metadata.get("source"),
                "source_image_id": match.metadata.get("source_image_id"),
                "sequence_id": match.metadata.get("sequence_id"),
                "source_url": match.metadata.get("source_url"),
            }
            for match in matches
        ],
        "retrieval_diagnostics": retrieval_diagnostics,
    }
    return matches, observation, timings, per_query


def _top_k_rank_diagnostics(
    matches: Sequence[RetrievalResult],
    *,
    true_lat: float,
    true_lon: float,
) -> dict[str, Any]:
    """Fallback for injected searches; positive ranks beyond top-k remain censored."""

    by_distance: dict[str, Any] = {}
    distances = [
        haversine_m(true_lat, true_lon, float(match.lat), float(match.lon)) for match in matches
    ]
    for threshold in (25.0, 50.0, 100.0):
        positives = [index for index, distance in enumerate(distances) if distance <= threshold]
        negatives = [index for index, distance in enumerate(distances) if distance > threshold]
        key = str(int(threshold))
        if not positives:
            by_distance[key] = {
                "positive_rank": None,
                "positive_score": None,
                "best_incorrect_score": None if not negatives else matches[negatives[0]].score,
                "positive_minus_best_incorrect_margin": None,
                "positive_reference": None,
            }
            continue
        position = positives[0]
        match = matches[position]
        incorrect_score = None if not negatives else matches[negatives[0]].score
        by_distance[key] = {
            "positive_rank": position + 1,
            "positive_score": match.score,
            "best_incorrect_score": incorrect_score,
            "positive_minus_best_incorrect_margin": (
                None if incorrect_score is None else match.score - incorrect_score
            ),
            "positive_reference": {
                "reference_id": match.reference_id,
                "distance_m": distances[position],
                "metadata": match.metadata,
            },
        }
    return {
        "gallery_size": None,
        "rank_scope": f"top_{len(matches)}_censored",
        "by_positive_distance_m": by_distance,
    }


def _false_confident_errors(
    rows: Sequence[Mapping[str, Any]],
    *,
    confidence_threshold: float,
    error_threshold_m: float = 100.0,
) -> dict[str, Any]:
    failures = [
        {
            "query_id": str(row["query_id"]),
            "confidence": float(row["confidence"]),
            "error_m": float(row["error_m"]),
            "status": str(row["status"]),
        }
        for row in rows
        if row.get("status") == "ok"
        and row.get("error_m") is not None
        and float(row["confidence"]) >= confidence_threshold
        and float(row["error_m"]) > error_threshold_m
    ]
    answered_count = sum(row.get("status") == "ok" for row in rows)
    return {
        "definition": (
            f"status_ok_and_confidence>={confidence_threshold:g}_and_error>{error_threshold_m:g}m"
        ),
        "confidence_threshold": confidence_threshold,
        "error_threshold_m": error_threshold_m,
        "count": len(failures),
        "rate_all_queries": len(failures) / len(rows) if rows else 0.0,
        "rate_answered_queries": len(failures) / answered_count if answered_count else 0.0,
        "errors": failures,
    }


def _metrics_payload(
    *,
    truths: Sequence[QueryGroundTruth],
    predictions: Mapping[str, Sequence[RetrievalResult]],
    observations: Sequence[LocalizationObservation],
    timings: Mapping[str, Sequence[float]],
    per_query: Sequence[Mapping[str, Any]],
    confidence_threshold: float,
    gallery_embedding_images: int | None = None,
    gallery_embedding_seconds: float | None = None,
) -> dict[str, Any]:
    localization = evaluate_localization(
        observations,
        thresholds_m=LOCALIZATION_THRESHOLDS_M,
    ).to_dict()
    answered_errors = [
        float(row["error_m"])
        for row in per_query
        if row.get("status") == "ok" and row.get("error_m") is not None
    ]
    correct = sum(value <= 100.0 for value in answered_errors)
    localization["conditional_accuracy_within_100m_wilson_interval_95"] = _wilson_interval(
        correct, len(answered_errors)
    )
    localization["accepted_error_m"] = _summary(answered_errors)
    available_k = min((len(matches) for matches in predictions.values()), default=0)
    recall_ks = tuple(value for value in RECALL_KS if value <= available_k)
    return {
        "retrieval": evaluate_retrieval(
            truths,
            predictions,
            positive_distance_threshold_m=POSITIVE_DISTANCE_THRESHOLD_M,
            ks=recall_ks,
        ).to_dict(),
        "retrieval_diagnostics": _retrieval_diagnostics_payload(per_query, recall_ks=recall_ks),
        "localization": localization,
        "confidence_buckets": localization["confidence_buckets"],
        "false_confident_errors": _false_confident_errors(
            per_query,
            confidence_threshold=confidence_threshold,
        ),
        "latency": summarize_latencies(
            timings,
            gallery_embedding_images=gallery_embedding_images,
            gallery_embedding_seconds=gallery_embedding_seconds,
        ).to_dict(),
    }


def _summary(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "median": None, "p75": None, "p90": None, "p95": None}
    array = np.asarray(values, dtype=np.float64)
    return {
        "count": len(values),
        "median": float(np.median(array)),
        "p75": float(np.percentile(array, 75)),
        "p90": float(np.percentile(array, 90)),
        "p95": float(np.percentile(array, 95)),
    }


def _wilson_interval(successes: int, trials: int) -> list[float | None]:
    if trials == 0:
        return [None, None]
    z = 1.959963984540054
    probability = successes / trials
    z_squared = z * z
    denominator = 1.0 + z_squared / trials
    center = probability + z_squared / (2.0 * trials)
    margin = z * np.sqrt((probability * (1.0 - probability) + z_squared / (4.0 * trials)) / trials)
    return [float((center - margin) / denominator), float((center + margin) / denominator)]


def _slice_recall(rows: Sequence[Mapping[str, Any]], *, key: str) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or "unknown"), []).append(row)
    result: dict[str, Any] = {}
    for label, group in sorted(grouped.items()):
        ranks = [row["retrieval_diagnostics"]["by_positive_distance_m"]["100"]["positive_rank"] for row in group]
        result[label] = {
            "queries": len(group),
            "recall_at_10": sum(rank is not None and int(rank) <= 10 for rank in ranks) / len(group),
            "recall_at_20": sum(rank is not None and int(rank) <= 20 for rank in ranks) / len(group),
            "recall_at_50": sum(rank is not None and int(rank) <= 50 for rank in ranks) / len(group),
        }
    return result


def _retrieval_diagnostics_payload(
    rows: Sequence[Mapping[str, Any]],
    *,
    recall_ks: Sequence[int],
) -> dict[str, Any]:
    by_threshold: dict[str, Any] = {}
    for threshold in (25, 50, 100):
        key = str(threshold)
        ranks = [row["retrieval_diagnostics"]["by_positive_distance_m"][key]["positive_rank"] for row in rows]
        observed_ranks = [float(rank) for rank in ranks if rank is not None]
        margins = [
            row["retrieval_diagnostics"]["by_positive_distance_m"][key][
                "positive_minus_best_incorrect_margin"
            ]
            for row in rows
        ]
        observed_margins = [float(value) for value in margins if value is not None]
        by_threshold[key] = {
            "recall_at": {
                str(k): sum(rank is not None and int(rank) <= k for rank in ranks) / len(rows)
                for k in recall_ks
            },
            "positive_rank": _summary(observed_ranks),
            "missing_positive_count": sum(rank is None for rank in ranks),
            "positive_minus_best_incorrect_similarity_margin": _summary(observed_margins),
        }
    return {
        "rank_scope": sorted(
            {str(row["retrieval_diagnostics"]["rank_scope"]) for row in rows}
        ),
        "by_positive_distance_m": by_threshold,
        "slices": {
            "query_provider": _slice_recall(rows, key="source"),
            "region_h3_coarse": _slice_recall(rows, key="h3_coarse"),
            "local_gallery_density_100m": _slice_recall(rows, key="local_gallery_density_bucket"),
            "provider_pair": _slice_recall(rows, key="positive_provider_pair"),
            "heading_gap": _slice_recall(rows, key="positive_heading_gap_bucket"),
            "temporal_gap": _slice_recall(rows, key="positive_temporal_gap_bucket"),
        },
    }


def _run_robustness(
    *,
    queries: LoadedManifest,
    retriever: BaseRetriever,
    exact_search: ExactSearch,
    localizer: SpatialLocalizer,
    output_dir: Path,
    top_k: int,
    query_aggregation: str,
) -> dict[str, Any]:
    truths: list[QueryGroundTruth] = []
    predictions: dict[str, Sequence[RetrievalResult]] = {}
    observations: list[LocalizationObservation] = []
    per_query: list[dict[str, Any]] = []
    timings: dict[str, list[float]] = {
        "query_preprocessing": [],
        "query_embedding": [],
        "exact_faiss_search": [],
        "spatial_localization": [],
        "end_to_end": [],
    }
    query_ids_by_variant: dict[str, list[str]] = {}
    for query in queries.rows:
        values = query.values
        variants = generate_robustness_variants(
            query.image_path,
            query_id=query.reference_id,
            lat=float(values["lat"]),
            lon=float(values["lon"]),
            output_dir=output_dir / "robustness_images",
            seed=0,
        )
        for variant in variants:
            variant_id = f"{variant.query_id}::{variant.variant}"
            matches, observation, samples, row = _evaluate_image(
                query_id=variant_id,
                query_image=variant.image,
                true_lat=variant.lat,
                true_lon=variant.lon,
                retriever=retriever,
                exact_search=exact_search,
                localizer=localizer,
                top_k=top_k,
                query_aggregation=query_aggregation,
            )
            row.update(
                {
                    "variant": variant.variant,
                    "derived_from_query_id": variant.query_id,
                    "augmentation_parameters": dict(variant.parameters),
                    "source": values.get("source"),
                    "source_image_id": values.get("source_image_id"),
                    "source_url": values.get("source_url"),
                    "license": values.get("license"),
                    "attribution": values.get("attribution"),
                }
            )
            truths.append(QueryGroundTruth(variant_id, variant.lat, variant.lon))
            predictions[variant_id] = matches
            observations.append(observation)
            per_query.append(row)
            query_ids_by_variant.setdefault(variant.variant, []).append(variant_id)
            for name, value in samples.items():
                timings[name].append(value)

    overall = _metrics_payload(
        truths=truths,
        predictions=predictions,
        observations=observations,
        timings=timings,
        per_query=per_query,
        confidence_threshold=localizer.config.confidence_threshold,
    )
    truths_by_id = {truth.query_id: truth for truth in truths}
    observations_by_id = {observation.query_id: observation for observation in observations}
    rows_by_id = {str(row["query_id"]): row for row in per_query}
    by_variant: dict[str, Any] = {}
    for variant, ids in sorted(query_ids_by_variant.items()):
        variant_truths = [truths_by_id[query_id] for query_id in ids]
        variant_observations = [observations_by_id[query_id] for query_id in ids]
        variant_rows = [rows_by_id[query_id] for query_id in ids]
        variant_timings = {
            stage: [float(rows_by_id[query_id]["latency_ms"][stage]) for query_id in ids]
            for stage in timings
        }
        by_variant[variant] = _metrics_payload(
            truths=variant_truths,
            predictions=predictions,
            observations=variant_observations,
            timings=variant_timings,
            per_query=variant_rows,
            confidence_threshold=localizer.config.confidence_threshold,
        )
    return {
        "warning": (
            "Deterministic derivatives of held-out real queries; kept separate from primary metrics "
            "and not counted as additional independent geographic samples."
        ),
        "source_query_count": len(queries.rows),
        "derived_query_count": len(per_query),
        "variants_per_query": len(query_ids_by_variant),
        "overall": overall,
        "by_variant": by_variant,
        "per_query": per_query,
    }


def _localizer_payload(config: LocalizerConfig) -> dict[str, Any]:
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
    dataset = payload["dataset"]
    primary = payload["primary"]
    retrieval = primary["retrieval"]
    localization = primary["localization"]
    leakage = payload["leakage_audit"]
    lines = [
        f"# Moscow real street-view benchmark — {payload['model']['model_name']}",
        "",
        "> Primary metrics contain only original held-out real street-view queries. "
        "Synthetic robustness variants, when enabled, are reported separately.",
        "",
        "## Dataset and leakage gate",
        "",
        f"- Gallery manifest: `{dataset['gallery']['manifest_path']}`",
        f"- Gallery SHA-256 / rows: `{dataset['gallery']['manifest_sha256']}` / {dataset['gallery']['count']}",
        f"- Query manifest: `{dataset['queries']['manifest_path']}`",
        f"- Query SHA-256 / rows: `{dataset['queries']['manifest_sha256']}` / {dataset['queries']['count']}",
        f"- Dataset fingerprint: `{dataset['fingerprint_sha256']}`",
        f"- Sources (gallery / query): `{json.dumps(dataset['gallery']['source_counts'], sort_keys=True)}` / "
        f"`{json.dumps(dataset['queries']['source_counts'], sort_keys=True)}`",
        f"- Areas (gallery): `{json.dumps(dataset['gallery']['area_counts'], sort_keys=True)}`",
        f"- Areas (query): `{json.dumps(dataset['queries']['area_counts'], sort_keys=True)}`",
        f"- Provider sequences (gallery / query): {dataset['gallery']['sequence_count']} / "
        f"{dataset['queries']['sequence_count']}",
        f"- Leakage audit: **{'PASS' if leakage['passed'] else 'FAIL'}**; policy `{leakage['policy']}`",
        "",
        "## Primary metrics",
        "",
        f"- Estimator: `{payload['localization']['estimator']}`",
        f"- Confidence threshold: `{payload['localization']['confidence_threshold']}`",
        f"- Recall positive: gallery coordinate within {retrieval['positive_distance_threshold_m']:g} m",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for k, value in retrieval["recall_at"].items():
        lines.append(f"| Recall@{k} | {_percent(value)} |")
    for threshold, value in localization["accuracy_within_m"].items():
        lines.append(f"| Localization <= {threshold} m (all queries) | {_percent(value)} |")
    lines.extend(
        [
            f"| Median error (answered only) | {localization['median_error_m'] if localization['median_error_m'] is not None else 'n/a'} m |",
            f"| P90 error (answered only) | {localization['p90_error_m'] if localization['p90_error_m'] is not None else 'n/a'} m |",
            f"| Answer rate | {_percent(localization['answer_rate'])} |",
            f"| Low-confidence rate | {_percent(localization['low_confidence_rate'])} |",
            f"| Out-of-coverage rate | {_percent(localization['out_of_coverage_rate'])} |",
            f"| False-confident errors (>100 m) | {primary['false_confident_errors']['count']} |",
            "",
            "## Confidence buckets",
            "",
            "| Confidence interval | Queries | Answer rate | Accuracy <=50 m (all) | Median error answered |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for bucket in primary["confidence_buckets"]:
        median = bucket["median_error_m_answered"]
        lines.append(
            f"| [{bucket['min_confidence']:.2f}, {bucket['max_confidence']:.2f}] | "
            f"{bucket['query_count']} | {_percent(bucket['answer_rate'])} | "
            f"{_percent(bucket['accuracy_within_50m_all_queries'])} | "
            f"{'n/a' if median is None else f'{median:.2f} m'} |"
        )
    lines.extend(
        [
            "",
            "## Latency",
            "",
            "| Stage | N | Median ms | P90 ms |",
            "|---|---:|---:|---:|",
        ]
    )
    for stage, values in primary["latency"]["stages"].items():
        median = "n/a" if values["median_ms"] is None else f"{values['median_ms']:.2f}"
        p90 = "n/a" if values["p90_ms"] is None else f"{values['p90_ms']:.2f}"
        lines.append(f"| {stage} | {values['count']} | {median} | {p90} |")
    runtime = payload["runtime"]
    embedding_throughput = primary["latency"]["gallery_embedding_images_per_second"]
    embedding_throughput_text = (
        "n/a" if embedding_throughput is None else f"{embedding_throughput:.2f} images/s"
    )
    lines.extend(
        [
            "",
            "## Model and storage",
            "",
            f"- Device: `{payload['model']['device']}`",
            f"- Checkpoint: `{payload['model']['checkpoint']}`",
            f"- Revision: `{payload['model'].get('revision')}`",
            f"- Model load: {runtime['model_load_ms']:.2f} ms",
            f"- Gallery embedding: {runtime['gallery_embedding_ms']:.2f} ms "
            f"({embedding_throughput_text})",
            f"- Exact FAISS build: {runtime['exact_faiss_build_ms']:.2f} ms",
            f"- Descriptor dimension / dtype: {runtime['descriptor_dimension']} / `{runtime['descriptor_dtype']}`",
            f"- Gallery descriptors: {runtime['gallery_descriptor_storage_bytes']} bytes",
            f"- Query descriptors (logical total): {runtime['query_descriptor_storage_bytes']} bytes",
            f"- Exact FAISS vector payload: {runtime['exact_faiss_vector_storage_bytes']} bytes",
            f"- Peak benchmark-process RSS: {runtime['peak_process_rss_bytes']} bytes",
            "",
        ]
    )
    robustness = payload.get("robustness")
    if robustness is not None:
        lines.extend(
            [
                "## Separate robustness results",
                "",
                f"> {robustness['warning']}",
                "",
                f"Derived queries: {robustness['derived_query_count']} from "
                f"{robustness['source_query_count']} originals.",
                "",
                "| Metric | Value |",
                "|---|---:|",
            ]
        )
        for k, value in robustness["overall"]["retrieval"]["recall_at"].items():
            lines.append(f"| Robustness Recall@{k} | {_percent(value)} |")
        for threshold, value in robustness["overall"]["localization"]["accuracy_within_m"].items():
            lines.append(f"| Robustness localization <= {threshold} m | {_percent(value)} |")
        lines.append("")
    lines.extend(
        [
            "## Metric semantics",
            "",
            "- Recall and product accuracy use all original query rows as the denominator.",
            "- Only `status=ok` is answered; low-confidence and out-of-coverage are failures in unconditional accuracy.",
            "- Median and P90 localization errors use answered queries only.",
            "- A false-confident error is an answered (`status=ok`) query at or above the configured confidence threshold with error >100 m.",
            "- Whole provider sequences are disjoint across query and gallery; no adjacent-frame exception is made.",
            "",
        ]
    )
    return "\n".join(lines)


def write_moscow_benchmark_reports(
    payload: Mapping[str, Any],
    output_dir: str | Path,
    *,
    stem: str,
) -> tuple[Path, Path]:
    if not stem or Path(stem).name != stem:
        raise ValueError("stem must be a non-empty filename stem")
    destination = Path(output_dir)
    json_path = destination / f"{stem}.json"
    markdown_path = destination / f"{stem}.md"
    _atomic_text(
        json_path,
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    _atomic_text(markdown_path, _markdown_report(payload))
    return json_path, markdown_path


def run_moscow_benchmark(
    *,
    gallery_manifest_path: str | Path,
    query_manifest_path: str | Path,
    retriever: BaseRetriever,
    output_dir: str | Path,
    expected_model: str | None = None,
    report_stem: str | None = None,
    top_k: int = 10,
    estimator: CoordinateEstimator | str = CoordinateEstimator.WEIGHTED_MEDOID,
    confidence_threshold: float = LocalizerConfig().confidence_threshold,
    robustness: bool = False,
    search_factory: Callable[[], ExactSearch] = _real_moscow_exact_search,
    localizer: SpatialLocalizer | None = None,
    gallery_embedding_dir: str | Path | None = None,
    runtime_config_path: str | Path | None = None,
    query_aggregation: str = "single",
) -> tuple[dict[str, Any], Path, Path]:
    """Run the primary and optional robustness benchmarks once.

    Data/schema/leakage checks happen before the model is loaded.  The optional
    injected localizer is supported for deterministic tests, but its estimator
    and confidence threshold must exactly match the requested configuration.
    """

    if top_k < 10:
        raise ValueError("top_k must be at least 10 to report Recall@10")
    if query_aggregation not in SUPPORTED_QUERY_AGGREGATIONS:
        raise ValueError(f"unsupported query aggregation {query_aggregation!r}")
    estimator_value = CoordinateEstimator(estimator)
    if not 0.0 <= confidence_threshold <= 1.0:
        raise ValueError("confidence_threshold must be in [0, 1]")
    _validate_expected_model(retriever, expected_model)
    requested_config = LocalizerConfig(
        estimator=estimator_value,
        confidence_threshold=confidence_threshold,
    )
    runtime_config = (
        None
        if runtime_config_path is None
        else FrozenRuntimeConfig.load(runtime_config_path, verify_index=True)
    )
    if runtime_config is not None:
        runtime_config.assert_benchmark_contract(
            retriever=retriever.model_name,
            top_k=top_k,
            estimator=estimator_value.value,
            confidence_threshold=confidence_threshold,
            query_aggregation=query_aggregation,
            gallery_manifest=gallery_manifest_path,
            localization=_localizer_payload(requested_config),
        )
    loaded = load_moscow_benchmark(gallery_manifest_path, query_manifest_path)
    if len(loaded.gallery.rows) < 10:
        raise MoscowBenchmarkError("gallery must contain at least 10 references to report Recall@10")
    if top_k > len(loaded.gallery.rows):
        raise MoscowBenchmarkError("top_k cannot exceed gallery size")

    active_localizer = localizer or SpatialLocalizer(requested_config)
    if (
        active_localizer.config.estimator != estimator_value
        or active_localizer.config.confidence_threshold != confidence_threshold
    ):
        raise MoscowBenchmarkError(
            "injected localizer does not match requested estimator/confidence threshold"
        )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    timings: dict[str, list[float]] = {
        "query_preprocessing": [],
        "query_embedding": [],
        "exact_faiss_search": [],
        "spatial_localization": [],
        "end_to_end": [],
    }
    truths: list[QueryGroundTruth] = []
    predictions: dict[str, Sequence[RetrievalResult]] = {}
    observations: list[LocalizationObservation] = []
    per_query: list[dict[str, Any]] = []
    local_density_by_query = _local_gallery_density(loaded.queries, loaded.gallery)

    # Start FAISS before importing/initializing Torch in the parent process.
    with search_factory() as exact_search:
        started = time.perf_counter()
        retriever.load()
        model_load_ms = (time.perf_counter() - started) * 1000.0
        model_metadata = _retriever_metadata(retriever)

        gallery_build = _build_gallery_search(
            gallery_rows=loaded.gallery.rows,
            retriever=retriever,
            exact_search=exact_search,
            model_metadata=model_metadata,
            gallery_embedding_dir=gallery_embedding_dir,
        )
        gallery_embedding_seconds = gallery_build.gallery_embedding_seconds
        gallery_embedding_ms = gallery_embedding_seconds * 1000.0
        exact_faiss_build_ms = gallery_build.exact_faiss_build_ms

        for query in loaded.queries.rows:
            values = query.values
            query_id = query.reference_id
            true_lat = float(values["lat"])
            true_lon = float(values["lon"])
            matches, observation, samples, row = _evaluate_image(
                query_id=query_id,
                query_image=query.image_path,
                true_lat=true_lat,
                true_lon=true_lon,
                retriever=retriever,
                exact_search=exact_search,
                localizer=active_localizer,
                top_k=top_k,
                query_aggregation=query_aggregation,
            )
            row.update(
                {
                    "source": values.get("source"),
                    "source_image_id": values.get("source_image_id"),
                    "sequence_id": values.get("sequence_id"),
                    "captured_at": values.get("captured_at"),
                    "area_id": values.get("area_id"),
                    "evaluation_area_h3": values.get("evaluation_area_h3"),
                    "evaluation_sample_h3": values.get("evaluation_sample_h3"),
                    "evaluation_split": values.get("evaluation_split"),
                    "h3_coarse": values.get("h3_coarse"),
                    "h3_fine": values.get("h3_fine"),
                    "source_url": values.get("source_url"),
                    "license": values.get("license"),
                    "attribution": values.get("attribution"),
                    "computed_image_sha256": query.image_sha256,
                }
            )
            row.update(
                _positive_slice_fields(
                    values,
                    row["retrieval_diagnostics"],
                    local_density=local_density_by_query[query_id],
                )
            )
            truths.append(
                QueryGroundTruth(
                    query_id,
                    true_lat,
                    true_lon,
                    sequence_id=_optional_text(values.get("sequence_id")),
                    captured_at=_optional_text(values.get("captured_at")),
                )
            )
            predictions[query_id] = matches
            observations.append(observation)
            per_query.append(row)
            for name, value in samples.items():
                timings[name].append(value)

        primary = _metrics_payload(
            truths=truths,
            predictions=predictions,
            observations=observations,
            timings=timings,
            per_query=per_query,
            confidence_threshold=active_localizer.config.confidence_threshold,
            gallery_embedding_images=(
                len(loaded.gallery.rows) if gallery_embedding_seconds > 0 else None
            ),
            gallery_embedding_seconds=(gallery_embedding_seconds if gallery_embedding_seconds > 0 else None),
        )
        robustness_payload = (
            _run_robustness(
                queries=loaded.queries,
                retriever=retriever,
                exact_search=exact_search,
                localizer=active_localizer,
                output_dir=output_path,
                top_k=top_k,
                query_aggregation=query_aggregation,
            )
            if robustness
            else None
        )

    gallery_profile = _manifest_profile(loaded.gallery)
    query_profile = _manifest_profile(loaded.queries)
    fingerprint_payload = {
        "gallery_manifest_sha256": loaded.gallery.sha256,
        "gallery_images_digest": gallery_profile["ordered_image_sha256_digest"],
        "query_manifest_sha256": loaded.queries.sha256,
        "query_images_digest": query_profile["ordered_image_sha256_digest"],
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    descriptor_dimension = gallery_build.descriptor_dimension
    descriptor_itemsize = gallery_build.descriptor_itemsize
    payload: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "benchmark_kind": BENCHMARK_KIND,
        "dataset": {
            "gallery": gallery_profile,
            "queries": query_profile,
            "fingerprint_sha256": fingerprint,
            "fingerprint_inputs": fingerprint_payload,
        },
        "leakage_audit": loaded.leakage_audit,
        "model": model_metadata,
        "retrieval_configuration": {
            "top_k": top_k,
            "query_aggregation": query_aggregation,
        },
        "localization": _localizer_payload(active_localizer.config),
        "runtime": {
            "model_load_ms": model_load_ms,
            "gallery_embedding_ms": gallery_embedding_ms,
            "gallery_embedding_source": gallery_build.embedding_source,
            "gallery_embedding_artifact_generation": gallery_build.embedding_artifact_generation,
            "exact_faiss_build_ms": exact_faiss_build_ms,
            "descriptor_dimension": descriptor_dimension,
            "descriptor_dtype": gallery_build.descriptor_dtype,
            "gallery_descriptor_storage_bytes": gallery_build.descriptor_storage_bytes,
            "query_descriptor_storage_bytes": (
                len(loaded.queries.rows) * descriptor_dimension * descriptor_itemsize
            ),
            "exact_faiss_vector_storage_bytes": gallery_build.descriptor_storage_bytes,
            "peak_process_rss_bytes": _peak_process_rss_bytes(),
            "exact_search_backend": getattr(
                exact_search,
                "backend_name",
                "faiss.IndexFlatIP (L2-normalized descriptors, isolated process)",
            ),
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": sys.version.split()[0],
                "numpy": np.__version__,
                "torch": _package_version("torch"),
                "faiss_cpu": _package_version("faiss-cpu"),
            },
        },
        "primary": primary,
        "per_query": per_query,
        "robustness": robustness_payload,
        "runtime_configuration": (
            None
            if runtime_config is None
            else {
                "path": str(runtime_config.path),
                "sha256": runtime_config.sha256,
                "contract_validated": True,
            }
        ),
    }
    stem = report_stem or f"{model_metadata['model_name']}_moscow_real_street_view"
    json_path, markdown_path = write_moscow_benchmark_reports(payload, output_path, stem=stem)
    return payload, json_path, markdown_path


def _compact_result(
    payload: Mapping[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Any]:
    localization = payload["primary"]["localization"]
    return {
        "benchmark_kind": payload["benchmark_kind"],
        "model": payload["model"]["model_name"],
        "device": payload["model"]["device"],
        "gallery_count": payload["dataset"]["gallery"]["count"],
        "query_count": payload["dataset"]["queries"]["count"],
        "leakage_audit_passed": payload["leakage_audit"]["passed"],
        "recall_at": payload["primary"]["retrieval"]["recall_at"],
        "accuracy_within_m": localization["accuracy_within_m"],
        "answer_rate": localization["answer_rate"],
        "false_confident_error_count": payload["primary"]["false_confident_errors"]["count"],
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark one official retriever on leakage-clean real Moscow street-view manifests"
    )
    parser.add_argument("--gallery-manifest", type=Path, required=True)
    parser.add_argument("--query-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--model",
        choices=(
            "megaloc",
            "dinov2-salad",
            "sage-vitb",
            "selavprplusplus-base",
            "selavprplusplus-base-rerank",
        ),
        required=True,
    )
    parser.add_argument("--device", default="auto")
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--top-k", type=int, default=10)
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
    parser.add_argument("--report-stem")
    parser.add_argument("--robustness", action="store_true")
    parser.add_argument(
        "--query-aggregation",
        choices=SUPPORTED_QUERY_AGGREGATIONS,
        default="single",
    )
    parser.add_argument(
        "--gallery-embedding-dir",
        type=Path,
        help="validated exact-order embedding artifact used instead of regenerating gallery descriptors",
    )
    parser.add_argument(
        "--runtime-config",
        type=Path,
        help="frozen configuration contract; required for the one final test run",
    )
    args = parser.parse_args()

    retriever = create_retriever(
        args.model,
        device=args.device,
        batch_size=args.batch_size,
        cache_dir=args.cache_dir,
    )
    search_factory: Callable[[], ExactSearch]
    if args.model == "selavprplusplus-base-rerank":
        from ml.evaluation.selavpr_two_stage import SelaVPRPlusPlusTwoStageSearch

        def search_factory() -> ExactSearch:
            return SelaVPRPlusPlusTwoStageSearch(candidate_pool=100)
    else:
        search_factory = _real_moscow_exact_search
    try:
        payload, json_path, markdown_path = run_moscow_benchmark(
            gallery_manifest_path=args.gallery_manifest,
            query_manifest_path=args.query_manifest,
            retriever=retriever,
            output_dir=args.output_dir,
            expected_model=args.model,
            report_stem=args.report_stem,
            top_k=args.top_k,
            estimator=args.estimator,
            confidence_threshold=args.confidence_threshold,
            robustness=args.robustness,
            gallery_embedding_dir=args.gallery_embedding_dir,
            runtime_config_path=args.runtime_config,
            query_aggregation=args.query_aggregation,
            search_factory=search_factory,
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
