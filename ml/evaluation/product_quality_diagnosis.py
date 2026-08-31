"""Calibration-only product-quality diagnosis for the persisted Moscow bundle.

This module deliberately separates two processes on macOS:

* the parent loads PyTorch only to re-embed the 493 calibration images;
* ``--search-only`` loads FAISS only to replay the immutable production index.

That is the same OpenMP isolation principle used by the production service.  It
never rebuilds the gallery/index, never reads test performance labels, and
never changes a production threshold.  The test manifest is read only for
metadata distribution and coverage comparisons.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC
from pathlib import Path
from typing import Any

import numpy as np

SCHEMA_VERSION = 1
CALIBRATION_LABEL = "calibration"
TEST_LABEL = "test"
POSITIVE_THRESHOLDS_M = (25, 50, 100)
RETRIEVAL_DEPTHS = (1, 5, 10, 20, 50)
FIXED_GRID_BINS = 20
SELECTED_CONFIDENCE_THRESHOLD = 0.5548002022369389
DEFAULT_OUTPUT_DIR = Path("data/evaluation/generated/product_quality_diagnosis")


class DiagnosisError(RuntimeError):
    """An artifact is insufficient or inconsistent for this diagnosis."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, np.ndarray):
        return [_json_safe(item) for item in value.tolist()]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    return str(value)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_safe(row), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if bool(value != value):
            return None
    except (TypeError, ValueError):
        pass
    normalized = str(value).strip()
    return normalized or None


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def _pct_count(count: int, total: int) -> dict[str, float | int]:
    return {"count": count, "rate": count / total if total else 0.0}


def _quantiles(values: Sequence[float]) -> dict[str, float | int | None]:
    finite = np.asarray([item for item in values if math.isfinite(item)], dtype=float)
    if not len(finite):
        return {"count": 0, "min": None, "p10": None, "p25": None, "p50": None, "p75": None, "p90": None,
                "p95": None, "max": None, "mean": None}
    return {
        "count": int(len(finite)),
        "min": float(np.min(finite)),
        "p10": float(np.percentile(finite, 10)),
        "p25": float(np.percentile(finite, 25)),
        "p50": float(np.percentile(finite, 50)),
        "p75": float(np.percentile(finite, 75)),
        "p90": float(np.percentile(finite, 90)),
        "p95": float(np.percentile(finite, 95)),
        "max": float(np.max(finite)),
        "mean": float(np.mean(finite)),
    }


def _wilson_interval(successes: int, total: int, *, z: float = 1.959963984540054) -> dict[str, float | int | None]:
    if total == 0:
        return {"successes": successes, "total": total, "rate": None, "lower": None, "upper": None}
    rate = successes / total
    denominator = 1.0 + z * z / total
    center = (rate + z * z / (2.0 * total)) / denominator
    radius = z * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total)) / denominator
    return {
        "successes": successes,
        "total": total,
        "rate": rate,
        "lower": max(0.0, center - radius),
        "upper": min(1.0, center + radius),
    }


def _haversine_np(
    lat1: np.ndarray | float,
    lon1: np.ndarray | float,
    lat2: np.ndarray | float,
    lon2: np.ndarray | float,
) -> np.ndarray:
    radius_m = 6_371_008.8
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    a = np.sin((lat2_rad - lat1_rad) / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(
        (lon2_rad - lon1_rad) / 2.0
    ) ** 2
    return 2.0 * radius_m * np.arcsin(np.sqrt(np.minimum(1.0, a)))


def _heading_delta_degrees(left: Any, right: Any) -> float | None:
    left_value = _finite(left)
    right_value = _finite(right)
    if left_value is None or right_value is None:
        return None
    return abs((left_value - right_value + 180.0) % 360.0 - 180.0)


def _parse_date_to_days(value: Any) -> float | None:
    text = _text(value)
    if text is None:
        return None
    try:
        import pandas as pd

        parsed = pd.to_datetime(text, utc=True, errors="coerce")
    except (ImportError, TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return float(parsed.timestamp() / 86_400.0)


def _read_manifest(path: Path) -> Any:
    import pandas as pd

    if not path.is_file():
        raise DiagnosisError(f"missing manifest: {path}")
    frame = pd.read_parquet(path)
    required = {"id", "lat", "lon", "source", "sequence_id", "evaluation_split"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise DiagnosisError(f"manifest {path} lacks required columns: {missing}")
    if frame["id"].isna().any() or frame["id"].duplicated().any():
        raise DiagnosisError(f"manifest {path} has missing or duplicate ids")
    return frame.copy()


def _frame_records(frame: Any) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        record = {str(key): _json_safe(value) for key, value in row.items()}
        records[str(record["id"])] = record
    return records


def _validate_split(frame: Any, expected: str, path: Path) -> None:
    labels = {str(value).strip().lower() for value in frame["evaluation_split"].dropna().tolist()}
    if labels != {expected}:
        raise DiagnosisError(f"{path} must contain only evaluation_split={expected!r}; got {sorted(labels)}")


def _load_json(path: Path) -> Any:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiagnosisError(f"cannot read JSON {path}: {exc}") from exc
    return payload


def _validate_calibration_report(report: Mapping[str, Any], calibration_count: int) -> None:
    if report.get("benchmark_kind") != "real_moscow_street_view":
        raise DiagnosisError("benchmark JSON is not a real Moscow benchmark")
    rows = report.get("per_query")
    if not isinstance(rows, list) or len(rows) != calibration_count:
        raise DiagnosisError("benchmark per_query rows do not match calibration manifest")
    labels = {str(row.get("evaluation_split", "")).lower() for row in rows if isinstance(row, Mapping)}
    if labels != {CALIBRATION_LABEL}:
        raise DiagnosisError("benchmark JSON is not proven calibration-only")
    localization = report.get("localization")
    if not isinstance(localization, Mapping):
        raise DiagnosisError("benchmark JSON lacks localization config")
    base_threshold = _finite(localization.get("confidence_threshold"))
    if base_threshold != 0.0:
        raise DiagnosisError(
            "diagnosis requires threshold-0 calibration evidence; the supplied benchmark has a non-zero threshold"
        )


def _index_sidecar(index_dir: Path) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    mapping_path = index_dir / "id_mapping.json"
    metadata_path = index_dir / "reference_metadata.jsonl"
    index_metadata_path = index_dir / "index_metadata.json"
    if not all(path.is_file() for path in (mapping_path, metadata_path, index_metadata_path, index_dir / "index.faiss")):
        raise DiagnosisError(f"incomplete persisted index: {index_dir}")
    mapping = _load_json(mapping_path)
    if not isinstance(mapping, list):
        raise DiagnosisError("index id_mapping must be a list")
    ids = [str(row["reference_id"]) for row in mapping]
    if [int(row["row"]) for row in mapping] != list(range(len(ids))):
        raise DiagnosisError("index mapping rows are not contiguous")
    metadata_rows: list[dict[str, Any]] = []
    for line in metadata_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            item = json.loads(line)
            if not isinstance(item, dict):
                raise DiagnosisError("index metadata line is not an object")
            metadata_rows.append(item)
    if [str(row.get("reference_id")) for row in metadata_rows] != ids:
        raise DiagnosisError("index metadata order does not match id mapping")
    metadata = _load_json(index_metadata_path)
    if not isinstance(metadata, dict):
        raise DiagnosisError("index metadata must be an object")
    return ids, [dict(row.get("metadata", {})) for row in metadata_rows], metadata


def _config_from_report(report: Mapping[str, Any], *, confidence_threshold: float) -> Any:
    from ml.localization.estimators import CoordinateEstimator
    from ml.localization.pipeline import LocalizerConfig

    raw = dict(report["localization"])
    raw["estimator"] = CoordinateEstimator(str(raw["estimator"]))
    raw["confidence_threshold"] = confidence_threshold
    return LocalizerConfig(**raw)


@dataclass(frozen=True, slots=True)
class LocalizerDetail:
    predicted_lat: float
    predicted_lon: float
    confidence: float
    diagnostics: Mapping[str, Any]
    hard_reasons: tuple[str, ...]
    winning_ids: tuple[str, ...]
    cluster_ids: tuple[tuple[str, ...], ...]


def _analysis_localize(candidates: Sequence[Any], *, config: Any, query_quality: float) -> LocalizerDetail:
    """Replay post-retrieval production math without early abstention.

    This is intentionally an analysis helper rather than a new estimator: it
    reproduces ``SpatialLocalizer.localize`` after its OOC early return so that
    component values remain observable for OOC rows.  The original OOC
    similarity threshold is retained in the score formula.
    """

    from ml.localization.clustering import group_compact_candidates
    from ml.localization.estimators import estimate_coordinate
    from ml.localization.geo import haversine_m, percentile

    if not candidates:
        raise DiagnosisError("analysis localizer needs at least one candidate")
    scores = {candidate.reference_id: float(candidate.retrieval_score) for candidate in candidates}
    top_score = max(scores.values())
    maximum = max(scores.values())
    evidence = {
        candidate.reference_id: math.exp((scores[candidate.reference_id] - maximum) / config.score_temperature)
        for candidate in candidates
    }
    clusters = group_compact_candidates(
        candidates,
        evidence,
        cluster_radius_m=config.cluster_radius_m,
        max_cluster_diameter_m=config.max_cluster_diameter_m,
    )
    values: list[tuple[Any, float, float, float]] = []
    for cluster in clusters:
        lat, lon = estimate_coordinate(cluster.candidates, evidence, config.estimator)
        spread = percentile([haversine_m(lat, lon, candidate.lat, candidate.lon) for candidate in cluster.candidates], 0.9)
        values.append((cluster, lat, lon, spread))
    winning, lat, lon, spread = values[0]
    second_mass = clusters[1].mass_fraction if len(clusters) >= 2 else None
    mass_margin = winning.mass_fraction - second_mass if second_mass is not None else None
    outside = [candidate for candidate in candidates if candidate not in winning.candidates]
    winning_top = max(candidate.retrieval_score for candidate in winning.candidates)
    outside_top = max((candidate.retrieval_score for candidate in outside), default=None)
    geographic_margin = winning_top - outside_top if outside_top is not None else None
    separation = None
    if len(values) >= 2:
        _, second_lat, second_lon, _ = values[1]
        separation = haversine_m(lat, lon, second_lat, second_lon)

    def clip(value: float) -> float:
        return max(0.0, min(1.0, value))

    similarity_signal = clip(
        (top_score - config.out_of_coverage_similarity)
        / (config.confident_similarity - config.out_of_coverage_similarity)
    )
    margin_signal = clip(geographic_margin / config.good_geographic_margin) if geographic_margin is not None else 0.0
    mass_signal = clip(winning.mass_fraction)
    compactness_signal = math.exp(-spread / config.cluster_radius_m)
    dominance_signal = clip((mass_margin or 0.0) / 0.25)
    separation_signal = clip(separation / config.good_hypothesis_separation_m) * dominance_signal if separation is not None else 0.0
    confidence = clip(
        0.30 * similarity_signal
        + 0.20 * margin_signal
        + 0.25 * mass_signal
        + 0.10 * compactness_signal
        + 0.10 * separation_signal
        + 0.05 * query_quality
    )
    reasons: list[str] = []
    if winning.mass_fraction < config.minimum_cluster_mass:
        reasons.append("winning_geographic_mode_has_low_evidence_mass")
    if mass_margin is not None and mass_margin < config.minimum_cluster_mass_margin:
        reasons.append("winning_geographic_mode_is_not_dominant")
    if len(winning.candidates) < config.minimum_cluster_candidates:
        reasons.append("winning_geographic_mode_has_insufficient_support")
    diagnostics = {
        "candidate_count": len(candidates),
        "spatial_mode_count": len(clusters),
        "top1_similarity": top_score,
        "geographic_margin": geographic_margin,
        "winning_cluster_mass": winning.mass_fraction,
        "winning_cluster_mass_margin": mass_margin,
        "winning_cluster_candidate_count": len(winning.candidates),
        "winning_cluster_p90_spread_m": spread,
        "second_hypothesis_distance_m": separation,
        "confidence_components": {
            "similarity": similarity_signal,
            "geographic_margin": margin_signal,
            "cluster_mass": mass_signal,
            "compactness": compactness_signal,
            "hypothesis_separation": separation_signal,
            "geographic_mode_dominance": dominance_signal,
            "query_quality": query_quality,
        },
    }
    return LocalizerDetail(
        predicted_lat=lat,
        predicted_lon=lon,
        confidence=confidence,
        diagnostics=diagnostics,
        hard_reasons=tuple(reasons),
        winning_ids=tuple(candidate.reference_id for candidate in winning.candidates),
        cluster_ids=tuple(tuple(candidate.reference_id for candidate in cluster.candidates) for cluster in clusters),
    )


def _candidate_rows(matches: Sequence[Mapping[str, Any]]) -> list[Any]:
    from ml.localization.models import Candidate

    candidates: list[Any] = []
    for match in matches:
        candidates.append(
            Candidate(
                reference_id=str(match["reference_id"]),
                lat=float(match["lat"]),
                lon=float(match["lon"]),
                retrieval_score=float(match["score"]),
                rank=int(match["rank"]),
                city_id="moscow",
                index_id="moscow",
                source=_text(match.get("source")),
                metadata={
                    "source": _text(match.get("source")),
                    "sequence_id": _text(match.get("sequence_id")),
                },
            )
        )
    return candidates


def _positive_ids(matches: Sequence[Mapping[str, Any]], true_lat: float, true_lon: float, threshold_m: float = 100.0) -> set[str]:
    return {
        str(match["reference_id"])
        for match in matches
        if float(_haversine_np(true_lat, true_lon, float(match["lat"]), float(match["lon"]))) <= threshold_m
    }


def _status_from_detail(detail: LocalizerDetail, *, config: Any) -> tuple[str, tuple[str, ...]]:
    top_similarity = float(detail.diagnostics["top1_similarity"])
    if top_similarity < config.out_of_coverage_similarity:
        return "out_of_coverage", ("retrieval_evidence_below_out_of_coverage_threshold",)
    reasons = list(detail.hard_reasons)
    if detail.confidence < config.confidence_threshold:
        reasons.append("confidence_below_threshold")
    return ("ok", ()) if not reasons else ("low_confidence", tuple(reasons))


def _matches_from_report(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    matches = row.get("matches")
    if not isinstance(matches, list) or not matches:
        raise DiagnosisError(f"benchmark row {row.get('query_id')} has no matches")
    return [dict(match) for match in matches]


def _base_replay(
    report: Mapping[str, Any],
    *,
    selected_threshold: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Reconstruct the stored top-10 behavior and prove the replay is exact."""

    from ml.localization.geo import haversine_m
    from ml.localization.pipeline import SpatialLocalizer

    base_config = _config_from_report(report, confidence_threshold=0.0)
    selected_config = _config_from_report(report, confidence_threshold=selected_threshold)
    base_localizer = SpatialLocalizer(base_config)
    selected_localizer = SpatialLocalizer(selected_config)
    rows: list[dict[str, Any]] = []
    mismatch_count = 0
    for stored in report["per_query"]:
        matches = _matches_from_report(stored)
        query_quality = float(stored["query_quality"]["confidence_signal"])
        candidates = _candidate_rows(matches)
        stored_replay = base_localizer.localize(candidates, query_quality=query_quality)
        detail = _analysis_localize(candidates, config=selected_config, query_quality=query_quality)
        status, reasons = _status_from_detail(detail, config=selected_config)
        selected = selected_localizer.localize(candidates, query_quality=query_quality)
        raw_error = haversine_m(float(stored["true_lat"]), float(stored["true_lon"]), detail.predicted_lat, detail.predicted_lon)
        stored_matches = (
            stored_replay.status.value == str(stored["status"])
            and list(stored_replay.reasons) == list(stored["reasons"])
            and abs(stored_replay.confidence - float(stored["confidence"])) < 1e-10
        )
        selected_matches = selected.status.value == status and tuple(selected.reasons) == reasons
        if status != "out_of_coverage":
            selected_matches = selected_matches and abs(selected.confidence - detail.confidence) < 1e-10
        if not stored_matches or not selected_matches:
            mismatch_count += 1
        positive_ids = _positive_ids(matches, float(stored["true_lat"]), float(stored["true_lon"]))
        rows.append(
            {
                "query_id": str(stored["query_id"]),
                "true_lat": float(stored["true_lat"]),
                "true_lon": float(stored["true_lon"]),
                "query_quality": query_quality,
                "matches": matches,
                "detail": detail,
                "stored_status": str(stored["status"]),
                "stored_reasons": tuple(str(reason) for reason in stored["reasons"]),
                "selected_status": status,
                "selected_reasons": reasons,
                "status": status,
                "reasons": reasons,
                "error_m": raw_error,
                "raw_error_m": raw_error,
                "positive_ids": positive_ids,
                "replay_matches_stored": stored_matches,
                "replay_matches_selected": selected_matches,
            }
        )
    return rows, {"query_count": len(rows), "mismatch_count": mismatch_count, "exact": mismatch_count == 0}


def _embed_calibration_queries(
    calibration: Any,
    *,
    output_path: Path,
    model_name: str,
    device: str,
    cache_dir: Path | None,
) -> dict[str, Any]:
    """Embed calibration images one at a time to reproduce the benchmark path."""

    from PIL import Image, ImageOps, UnidentifiedImageError

    from ml.retrieval import create_retriever
    from ml.retrieval.base import l2_normalize

    output_path.parent.mkdir(parents=True, exist_ok=True)
    retriever = create_retriever(model_name, device=device, batch_size=1, cache_dir=cache_dir)
    started = time.perf_counter()
    retriever.load()
    model_load_ms = (time.perf_counter() - started) * 1000.0
    descriptor_rows: list[np.ndarray] = []
    timings: list[float] = []
    try:
        for index, row in enumerate(calibration.to_dict(orient="records"), start=1):
            image_path = Path(str(row["image_path"]))
            try:
                with Image.open(image_path) as opened:
                    opened.load()
                    image = ImageOps.exif_transpose(opened).convert("RGB")
            except (OSError, SyntaxError, ValueError, UnidentifiedImageError) as exc:
                raise DiagnosisError(f"cannot load calibration image {row['id']}: {image_path}") from exc
            query_started = time.perf_counter()
            descriptor = retriever.embed_query(image)
            descriptor = l2_normalize(descriptor)[0]
            timings.append((time.perf_counter() - query_started) * 1000.0)
            descriptor_rows.append(np.asarray(descriptor, dtype=np.float32))
            if index % 50 == 0 or index == len(calibration):
                print(f"embedded_calibration_queries={index}/{len(calibration)}", flush=True)
        matrix = np.ascontiguousarray(np.stack(descriptor_rows, axis=0), dtype=np.float32)
        np.save(output_path, matrix, allow_pickle=False)
        metadata = retriever.metadata.to_dict()
    finally:
        retriever.close()
    return {
        "path": str(output_path),
        "sha256": _sha256_file(output_path),
        "shape": list(matrix.shape),
        "dtype": str(matrix.dtype),
        "model_load_ms": model_load_ms,
        "model": metadata,
        "per_query_embedding_ms": _quantiles(timings),
    }


def _search_only(args: argparse.Namespace) -> None:
    """Run in a fresh interpreter where FAISS can safely coexist without Torch."""

    import faiss

    index_dir = args.index_dir.resolve()
    descriptor_path = args.descriptors.resolve()
    query_path = args.query_manifest.resolve()
    output_path = args.search_output.resolve()
    ids, metadata_rows, metadata = _index_sidecar(index_dir)
    descriptors = np.load(descriptor_path, mmap_mode="r")
    queries = _read_manifest(query_path)
    _validate_split(queries, CALIBRATION_LABEL, query_path)
    if descriptors.ndim != 2 or descriptors.shape[0] != len(queries):
        raise DiagnosisError("query descriptor matrix does not match calibration manifest")
    if descriptors.shape[1] != int(metadata.get("descriptor_dim", -1)):
        raise DiagnosisError("query descriptor dimension does not match index")
    index = faiss.read_index(str(index_dir / "index.faiss"))
    if int(index.ntotal) != len(ids) or int(index.d) != descriptors.shape[1]:
        raise DiagnosisError("FAISS index dimensions do not match sidecars")
    gallery_lat = np.asarray([float(row["lat"]) for row in metadata_rows], dtype=np.float64)
    gallery_lon = np.asarray([float(row["lon"]) for row in metadata_rows], dtype=np.float64)
    top_k = min(int(args.replay_top_k), len(ids))
    full_k = len(ids)
    records = queries.to_dict(orient="records")
    result_rows: list[dict[str, Any]] = []
    search_times: list[float] = []
    for start in range(0, len(records), int(args.search_batch_size)):
        end = min(len(records), start + int(args.search_batch_size))
        batch = np.ascontiguousarray(descriptors[start:end], dtype=np.float32)
        started = time.perf_counter()
        scores, positions = index.search(batch, full_k)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        search_times.extend([elapsed_ms / len(batch)] * len(batch))
        for row, ranked_scores, ranked_positions in zip(records[start:end], scores, positions, strict=True):
            true_lat = float(row["lat"])
            true_lon = float(row["lon"])
            ordered_lat = gallery_lat[ranked_positions]
            ordered_lon = gallery_lon[ranked_positions]
            distances = _haversine_np(true_lat, true_lon, ordered_lat, ordered_lon)
            top_matches: list[dict[str, Any]] = []
            for rank, (score, position) in enumerate(zip(ranked_scores[:top_k], ranked_positions[:top_k], strict=True), start=1):
                item = metadata_rows[int(position)]
                top_matches.append(
                    {
                        "rank": rank,
                        "reference_id": ids[int(position)],
                        "score": float(score),
                        "lat": float(item["lat"]),
                        "lon": float(item["lon"]),
                        "source": _text(item.get("source")),
                        "sequence_id": _text(item.get("sequence_id")),
                        "captured_at": _text(item.get("captured_at")),
                        "heading": _finite(item.get("heading")),
                    }
                )
            geo: dict[str, dict[str, Any]] = {}
            for threshold in POSITIVE_THRESHOLDS_M:
                valid = np.flatnonzero(distances <= threshold)
                first = int(valid[0]) if len(valid) else None
                if first is None:
                    geo[str(threshold)] = {
                        "first_correct_rank": None,
                        "best_similarity": None,
                        "reference_id": None,
                    }
                else:
                    position = int(ranked_positions[first])
                    geo[str(threshold)] = {
                        "first_correct_rank": first + 1,
                        "best_similarity": float(ranked_scores[first]),
                        "reference_id": ids[position],
                    }
            correct_mask = distances <= 100.0
            incorrect_positions = np.flatnonzero(~correct_mask)
            best_incorrect = float(ranked_scores[int(incorrect_positions[0])]) if len(incorrect_positions) else None
            best_correct = geo["100"]["best_similarity"]
            result_rows.append(
                {
                    "query_id": str(row["id"]),
                    "top_matches": top_matches,
                    "geo": geo,
                    "best_incorrect_similarity_100m": best_incorrect,
                    "correct_vs_incorrect_similarity_gap_100m": (
                        None if best_correct is None or best_incorrect is None else float(best_correct) - best_incorrect
                    ),
                }
            )
        print(f"faiss_full_rank_queries={end}/{len(records)}", flush=True)
    output = {
        "schema_version": SCHEMA_VERSION,
        "index_dir": str(index_dir),
        "index_sha256": _sha256_file(index_dir / "index.faiss"),
        "index_generation": metadata.get("artifact_generation"),
        "index_size": len(ids),
        "descriptor_path": str(descriptor_path),
        "descriptor_sha256": _sha256_file(descriptor_path),
        "replay_top_k": top_k,
        "full_rank_search": True,
        "query_count": len(result_rows),
        "faiss_search_ms_per_query": _quantiles(search_times),
        "per_query": result_rows,
    }
    _atomic_json(output_path, output)


def _run_search_subprocess(args: argparse.Namespace, descriptors_path: Path, output_path: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "ml.evaluation.product_quality_diagnosis",
        "--search-only",
        "--index-dir",
        str(args.index_dir),
        "--descriptors",
        str(descriptors_path),
        "--query-manifest",
        str(args.calibration_manifest),
        "--search-output",
        str(output_path),
        "--replay-top-k",
        str(args.replay_top_k),
        "--search-batch-size",
        str(args.search_batch_size),
    ]
    completed = subprocess.run(command, check=False, text=True)
    if completed.returncode != 0:
        raise DiagnosisError(f"isolated full-rank FAISS replay failed with exit code {completed.returncode}")
    return _load_json(output_path)


def _top_k_localization_rows(
    search: Mapping[str, Any],
    calibration_records: Mapping[str, Mapping[str, Any]],
    report: Mapping[str, Any],
    *,
    top_k: int,
    selected_threshold: float,
) -> list[dict[str, Any]]:
    from ml.localization.geo import haversine_m

    config = _config_from_report(report, confidence_threshold=selected_threshold)
    output: list[dict[str, Any]] = []
    for row in search["per_query"]:
        query_id = str(row["query_id"])
        query = calibration_records[query_id]
        matches = list(row["top_matches"][:top_k])
        candidates = _candidate_rows(matches)
        quality = _finite(query.get("_query_confidence_signal"))
        if quality is None:
            raise DiagnosisError(f"query quality is missing for {query_id}")
        detail = _analysis_localize(candidates, config=config, query_quality=quality)
        status, reasons = _status_from_detail(detail, config=config)
        positive_ids = _positive_ids(matches, float(query["lat"]), float(query["lon"]))
        error_m = haversine_m(float(query["lat"]), float(query["lon"]), detail.predicted_lat, detail.predicted_lon)
        output.append(
            {
                "query_id": query_id,
                "top_k": top_k,
                "matches": matches,
                "detail": detail,
                "status": status,
                "reasons": reasons,
                "error_m": error_m,
                "positive_ids": positive_ids,
            }
        )
    return output


def _abstention_decomposition(rows: Sequence[Mapping[str, Any]], *, total_queries: int) -> dict[str, Any]:
    reason_labels = {
        "low_top_similarity": "retrieval_evidence_below_out_of_coverage_threshold",
        "insufficient_winning_cluster_mass": "winning_geographic_mode_has_low_evidence_mass",
        "insufficient_mass_margin": "winning_geographic_mode_is_not_dominant",
        "insufficient_candidate_count": "winning_geographic_mode_has_insufficient_support",
        "confidence_threshold": "confidence_below_threshold",
    }
    reason_counts: Counter[str] = Counter()
    combination_counts: Counter[tuple[str, ...]] = Counter()
    correction = {
        "correct_candidate_in_top_k_then_rejected": 0,
        "no_correct_candidate_in_top_k_then_rejected": 0,
        "correct_candidate_in_top_k_and_answered": 0,
        "no_correct_candidate_in_top_k_and_answered": 0,
    }
    by_status: Counter[str] = Counter()
    for row in rows:
        status = str(row["status"])
        by_status[status] += 1
        reasons = tuple(str(reason) for reason in row["reasons"])
        if status != "ok":
            normalized = tuple(name for name, reason in reason_labels.items() if reason in reasons)
            additional = tuple(reason for reason in reasons if reason not in reason_labels.values())
            combination_counts[normalized + additional] += 1
            for name in normalized:
                reason_counts[name] += 1
            for reason in additional:
                reason_counts[f"other:{reason}"] += 1
        has_correct = bool(row["positive_ids"])
        if status == "ok":
            correction["correct_candidate_in_top_k_and_answered" if has_correct else "no_correct_candidate_in_top_k_and_answered"] += 1
        else:
            correction["correct_candidate_in_top_k_then_rejected" if has_correct else "no_correct_candidate_in_top_k_then_rejected"] += 1
    abstentions = total_queries - by_status["ok"]
    combinations = [
        {
            "conditions": list(combo),
            **_pct_count(count, total_queries),
            "rate_among_abstentions": count / abstentions if abstentions else 0.0,
        }
        for combo, count in combination_counts.most_common()
    ]
    return {
        "query_count": total_queries,
        "answered": _pct_count(by_status["ok"], total_queries),
        "abstained": _pct_count(abstentions, total_queries),
        "status": {key: _pct_count(value, total_queries) for key, value in sorted(by_status.items())},
        "individual_conditions": {key: _pct_count(value, total_queries) for key, value in sorted(reason_counts.items())},
        "combinations": combinations,
        "retrieval_vs_rejection": {key: _pct_count(value, total_queries) for key, value in correction.items()},
        "retrieval_vs_rejection_among_abstentions": {
            key: _pct_count(value, abstentions)
            for key, value in correction.items()
            if key.endswith("then_rejected")
        },
    }


def _retrieval_oracle(search: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(search["per_query"])
    count = len(rows)
    by_threshold: dict[str, Any] = {}
    for threshold in POSITIVE_THRESHOLDS_M:
        key = str(threshold)
        ranks = [row["geo"][key]["first_correct_rank"] for row in rows]
        present = [int(rank) for rank in ranks if rank is not None]
        best_scores = [row["geo"][key]["best_similarity"] for row in rows if row["geo"][key]["best_similarity"] is not None]
        by_threshold[key] = {
            "first_correct_rank": _quantiles([float(rank) for rank in present]),
            "best_correct_similarity": _quantiles([float(score) for score in best_scores]),
            "recall_at": {
                str(depth): _pct_count(sum(rank is not None and rank <= depth for rank in ranks), count)
                for depth in RETRIEVAL_DEPTHS
            },
            "retrieval_depth_oracle_upper_bound": {
                str(depth): _pct_count(sum(rank is not None and rank <= depth for rank in ranks), count)
                for depth in RETRIEVAL_DEPTHS
            },
        }
    gaps = [row["correct_vs_incorrect_similarity_gap_100m"] for row in rows]
    return {
        "query_count": count,
        "definition": "A correct candidate is a gallery coordinate within the stated distance of the query.",
        "by_distance_m": by_threshold,
        "correct_vs_best_incorrect_similarity_gap_100m": _quantiles([float(item) for item in gaps if item is not None]),
        "interpretation": (
            "The finite-depth oracle is the maximum answer rate if a perfect downstream component could select a "
            "geographically correct candidate already returned by the unchanged retriever at that depth."
        ),
    }


def _estimator_comparison(rows: Sequence[Mapping[str, Any]], report: Mapping[str, Any]) -> dict[str, Any]:
    from ml.localization.estimators import CoordinateEstimator
    from ml.localization.geo import haversine_m

    config = _config_from_report(report, confidence_threshold=0.0)
    results: dict[str, Any] = {}
    for estimator in CoordinateEstimator:
        estimator_config = type(config)(**(asdict(config) | {"estimator": estimator, "confidence_threshold": 0.0}))
        errors_all: list[float] = []
        errors_with_positive: list[float] = []
        correct = 0
        correct_with_positive = 0
        for row in rows:
            candidates = _candidate_rows(row["matches"])
            detail = _analysis_localize(candidates, config=estimator_config, query_quality=0.75)
            # The calling code adds ground truth directly to every row.
            error = haversine_m(float(row["true_lat"]), float(row["true_lon"]), detail.predicted_lat, detail.predicted_lon)
            errors_all.append(error)
            correct += error <= 100.0
            if row["positive_ids"]:
                errors_with_positive.append(error)
                correct_with_positive += error <= 100.0
        results[estimator.value] = {
            "all_queries": {"within_100m": _pct_count(correct, len(rows)), "error_m": _quantiles(errors_all)},
            "queries_with_correct_candidate_in_top_k": {
                "within_100m": _pct_count(correct_with_positive, len(errors_with_positive)),
                "error_m": _quantiles(errors_with_positive),
            },
        }
    return results


def _localization_oracle(rows: Sequence[Mapping[str, Any]], report: Mapping[str, Any]) -> dict[str, Any]:

    positive_rows = [row for row in rows if row["positive_ids"]]
    selected_wrong_mode = 0
    correct_not_winning = 0
    estimate_miss_despite_winning = 0
    ambiguous = 0
    positive_cluster_counts: list[int] = []
    correct_support_counts: list[int] = []
    for row in positive_rows:
        detail: LocalizerDetail = row["detail"]
        winning_ids = set(detail.winning_ids)
        positives = set(row["positive_ids"])
        if not winning_ids.intersection(positives):
            selected_wrong_mode += 1
            correct_not_winning += 1
        else:
            error = float(row["error_m"])
            if error > 100.0:
                estimate_miss_despite_winning += 1
        positive_clusters = sum(bool(positives.intersection(cluster)) for cluster in detail.cluster_ids)
        positive_cluster_counts.append(positive_clusters)
        correct_support_counts.append(len(positives.intersection(winning_ids)))
        margin = _finite(detail.diagnostics["winning_cluster_mass_margin"])
        separation = _finite(detail.diagnostics["second_hypothesis_distance_m"])
        if margin is not None and margin < 0.10 and separation is not None and separation >= 500.0:
            ambiguous += 1
    estimator_rows: list[dict[str, Any]] = []
    for row in rows:
        base = dict(row)
        base["true_lat"] = row["true_lat"]
        base["true_lon"] = row["true_lon"]
        estimator_rows.append(base)
    return {
        "query_count": len(rows),
        "queries_with_correct_candidate_in_top_k": _pct_count(len(positive_rows), len(rows)),
        "winning_mode_wrong_among_retrieval_positive": _pct_count(selected_wrong_mode, len(positive_rows)),
        "correct_candidate_only_in_nonwinning_mode": _pct_count(correct_not_winning, len(positive_rows)),
        "estimated_over_100m_despite_correct_candidate_in_winning_mode": _pct_count(
            estimate_miss_despite_winning, len(positive_rows)
        ),
        "positive_clusters_per_query": _quantiles([float(value) for value in positive_cluster_counts]),
        "positive_candidates_in_winning_cluster": _quantiles([float(value) for value in correct_support_counts]),
        "ambiguous_multimodal_cases": {
            **_pct_count(ambiguous, len(positive_rows)),
            "definition": "second geographic mode >=500m away and winning-vs-second mass margin <0.10",
        },
        "estimator_counterfactual": _estimator_comparison(estimator_rows, report),
        "cluster_policy": {
            "radius_m": float(report["localization"]["cluster_radius_m"]),
            "max_diameter_m": float(report["localization"]["max_cluster_diameter_m"]),
            "note": "The current non-chaining clustering is replayed exactly; no radius or diameter was tuned.",
        },
    }


def _component_distributions(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    components = (
        "similarity",
        "geographic_margin",
        "cluster_mass",
        "compactness",
        "hypothesis_separation",
        "geographic_mode_dominance",
        "query_quality",
    )
    output: dict[str, Any] = {}
    classes = {
        "correct_raw_localization_within_100m": [row for row in rows if float(row["error_m"]) <= 100.0],
        "incorrect_raw_localization_over_100m": [row for row in rows if float(row["error_m"]) > 100.0],
        "retrieval_positive_but_raw_localization_wrong": [
            row for row in rows if row["positive_ids"] and float(row["error_m"]) > 100.0
        ],
    }
    for label, subset in classes.items():
        output[label] = {
            "count": len(subset),
            "components": {
                component: _quantiles(
                    [float(row["detail"].diagnostics["confidence_components"][component]) for row in subset]
                )
                for component in components
            },
            "raw_diagnostics": {
                key: _quantiles(
                    [float(value) for row in subset if (value := _finite(row["detail"].diagnostics[key])) is not None]
                )
                for key in (
                    "top1_similarity",
                    "geographic_margin",
                    "winning_cluster_mass",
                    "winning_cluster_mass_margin",
                    "winning_cluster_candidate_count",
                    "winning_cluster_p90_spread_m",
                    "second_hypothesis_distance_m",
                )
            },
            "confidence": _quantiles([float(row["detail"].confidence) for row in subset]),
        }
    return output


def _confidence_curve(rows: Sequence[Mapping[str, Any]], *, thresholds: Sequence[float]) -> list[dict[str, Any]]:
    curve: list[dict[str, Any]] = []
    total = len(rows)
    for threshold in sorted(set(float(item) for item in thresholds)):
        accepted = [
            row
            for row in rows
            if not row["detail"].hard_reasons
            and float(row["detail"].diagnostics["top1_similarity"]) >= 0.15
            and float(row["detail"].confidence) >= threshold
        ]
        correct = sum(float(row["error_m"]) <= 100.0 for row in accepted)
        errors = [float(row["error_m"]) for row in accepted]
        curve.append(
            {
                "threshold": threshold,
                "answer_rate": _pct_count(len(accepted), total),
                "correct_all_queries_within_100m": _pct_count(correct, total),
                "conditional_precision_within_100m": _wilson_interval(correct, len(accepted)),
                "accepted_error_m": _quantiles(errors),
                "false_confident_over_100m": _pct_count(len(accepted) - correct, total),
            }
        )
    return curve


def _confidence_analysis(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    # The curve uses every distinct hard-gate-eligible score plus a concise
    # reader-facing grid. It does not reselect a production threshold.
    eligible_scores = sorted(
        {
            float(row["detail"].confidence)
            for row in rows
            if not row["detail"].hard_reasons and float(row["detail"].diagnostics["top1_similarity"]) >= 0.15
        }
    )
    full_curve = _confidence_curve(rows, thresholds=(0.0, *eligible_scores, 1.0))
    display_thresholds = (0.0, 0.25, 0.50, SELECTED_CONFIDENCE_THRESHOLD, 0.60, 0.70, 0.80, 0.90)
    display_curve = _confidence_curve(rows, thresholds=display_thresholds)
    objectives: dict[str, Any] = {}
    for target in (0.90, 0.95, 0.975):
        candidates = [
            row
            for row in full_curve
            if row["conditional_precision_within_100m"]["rate"] is not None
            and float(row["conditional_precision_within_100m"]["rate"]) >= target
        ]
        best = max(candidates, key=lambda row: (row["answer_rate"]["count"], -float(row["threshold"])), default=None)
        objectives[f"point_precision_at_least_{target:.3f}"] = best
        lower_candidates = [
            row
            for row in full_curve
            if row["conditional_precision_within_100m"]["lower"] is not None
            and float(row["conditional_precision_within_100m"]["lower"]) >= target
        ]
        objectives[f"wilson_lower_bound_at_least_{target:.3f}"] = max(
            lower_candidates,
            key=lambda row: (row["answer_rate"]["count"], -float(row["threshold"])),
            default=None,
        )
    # Reliability is descriptive only: the confidence is explicitly not a
    # calibrated probability. Bin all rows for which the pipeline can create a
    # coordinate, including hard-gate abstentions.
    bins = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.55), (0.55, 0.7), (0.7, 1.0000001)]
    reliability: list[dict[str, Any]] = []
    brier_values: list[float] = []
    for lower, upper in bins:
        subset = [
            row
            for row in rows
            if lower <= float(row["detail"].confidence) < upper
        ]
        correct = sum(float(row["error_m"]) <= 100.0 for row in subset)
        mean_confidence = float(np.mean([float(row["detail"].confidence) for row in subset])) if subset else None
        reliability.append(
            {
                "min": lower,
                "max": min(upper, 1.0),
                "count": len(subset),
                "mean_confidence": mean_confidence,
                "observed_within_100m": _wilson_interval(correct, len(subset)),
            }
        )
        brier_values.extend((float(row["detail"].confidence) - float(float(row["error_m"]) <= 100.0)) ** 2 for row in subset)
    ece = sum(
        entry["count"] / len(rows) * abs(float(entry["mean_confidence"]) - float(entry["observed_within_100m"]["rate"]))
        for entry in reliability
        if entry["count"]
    )
    return {
        "scope": "Calibration only. The confidence score remains interpretable-v1-uncalibrated.",
        "component_distributions": _component_distributions(rows),
        "display_curve": display_curve,
        "full_curve": full_curve,
        "alternative_product_objectives": objectives,
        "descriptive_reliability": {
            "bins": reliability,
            "brier_score": float(np.mean(brier_values)) if brier_values else None,
            "expected_calibration_error": ece,
            "warning": "These are descriptive diagnostics, not validation of a probabilistic confidence model.",
        },
    }


def _spatial_features(queries: Any, gallery: Any) -> dict[str, dict[str, Any]]:
    """Compute nearest-reference, source-pairing and local-density metadata."""

    gallery_records = gallery.to_dict(orient="records")
    gallery_lat = np.asarray([float(row["lat"]) for row in gallery_records], dtype=np.float64)
    gallery_lon = np.asarray([float(row["lon"]) for row in gallery_records], dtype=np.float64)
    gallery_sources = np.asarray([str(row["source"]).lower() for row in gallery_records], dtype=object)
    gallery_sequences = np.asarray(
        [f"{str(row['source']).lower()}::{_text(row.get('sequence_id')) or '<missing>'}" for row in gallery_records], dtype=object
    )
    output: dict[str, dict[str, Any]] = {}
    query_records = queries.to_dict(orient="records")
    for start in range(0, len(query_records), 32):
        batch = query_records[start : start + 32]
        lat = np.asarray([float(row["lat"]) for row in batch], dtype=np.float64)[:, None]
        lon = np.asarray([float(row["lon"]) for row in batch], dtype=np.float64)[:, None]
        distances = _haversine_np(lat, lon, gallery_lat[None, :], gallery_lon[None, :])
        for row, row_distances in zip(batch, distances, strict=True):
            nearest_index = int(np.argmin(row_distances))
            nearest = gallery_records[nearest_index]
            within_100 = row_distances <= 100.0
            within_250 = row_distances <= 250.0
            query_source = str(row["source"]).lower()
            temporal_left = _parse_date_to_days(row.get("captured_at"))
            temporal_right = _parse_date_to_days(nearest.get("captured_at"))
            output[str(row["id"])] = {
                "nearest_gallery_distance_m_recomputed": float(row_distances[nearest_index]),
                "nearest_gallery_id": str(nearest["id"]),
                "nearest_gallery_source": str(nearest["source"]).lower(),
                "source_pairing": f"{query_source}->{str(nearest['source']).lower()}",
                "nearest_gallery_heading_gap_degrees": _heading_delta_degrees(row.get("heading"), nearest.get("heading")),
                "nearest_gallery_capture_gap_days": (
                    None if temporal_left is None or temporal_right is None else abs(temporal_left - temporal_right)
                ),
                "gallery_count_within_100m": int(np.count_nonzero(within_100)),
                "gallery_sequence_count_within_100m": int(len(set(gallery_sequences[within_100].tolist()))),
                "gallery_provider_count_within_100m": int(len(set(gallery_sources[within_100].tolist()))),
                "gallery_count_within_250m": int(np.count_nonzero(within_250)),
                "gallery_sequence_count_within_250m": int(len(set(gallery_sequences[within_250].tolist()))),
            }
    return output


def _distribution(values: Sequence[Any]) -> dict[str, Any]:
    counter = Counter(_text(value) or "<missing>" for value in values)
    total = sum(counter.values())
    return {key: {"count": value, "rate": value / total if total else 0.0} for key, value in sorted(counter.items())}


def _total_variation(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    keys = set(left).union(right)
    return 0.5 * sum(abs(float(left.get(key, {}).get("rate", 0.0)) - float(right.get(key, {}).get("rate", 0.0))) for key in keys)


def _continuous_shift(calibration: Sequence[float | None], test: Sequence[float | None]) -> dict[str, Any]:
    cal = [float(value) for value in calibration if value is not None and math.isfinite(float(value))]
    tst = [float(value) for value in test if value is not None and math.isfinite(float(value))]
    result = {"calibration": _quantiles(cal), "test": _quantiles(tst)}
    if cal and tst:
        result["median_delta_test_minus_calibration"] = float(np.median(tst) - np.median(cal))
    else:
        result["median_delta_test_minus_calibration"] = None
    return result


def _capture_month(value: Any) -> str:
    text = _text(value)
    if text is None or len(text) < 7:
        return "<missing>"
    return text[:7]


def _size_bucket(row: Mapping[str, Any]) -> str:
    width = _finite(row.get("width"))
    height = _finite(row.get("height"))
    if width is None or height is None:
        return "<missing>"
    longest = max(width, height)
    if longest < 1000:
        return "<1000px"
    if longest < 1600:
        return "1000-1599px"
    if longest < 2500:
        return "1600-2499px"
    return ">=2500px"


def _region_bucket(row: Mapping[str, Any]) -> str:
    from ml.ingestion.schema import MOSCOW_BOUNDS

    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    lat = float(row["lat"])
    lon = float(row["lon"])
    x = max(0, min(3, int((lon - min_lon) / (max_lon - min_lon) * 4)))
    y = max(0, min(3, int((lat - min_lat) / (max_lat - min_lat) * 4)))
    return f"r{x}{y}"


def _metadata_shift(calibration: Any, test: Any, gallery: Any) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    calibration_spatial = _spatial_features(calibration, gallery)
    test_spatial = _spatial_features(test, gallery)
    cal_records = calibration.to_dict(orient="records")
    test_records = test.to_dict(orient="records")
    categorical_extractors: dict[str, Any] = {
        "provider": lambda row, spatial: str(row["source"]).lower(),
        "evaluation_area_h3": lambda row, spatial: _text(row.get("evaluation_area_h3")),
        "coarse_h3": lambda row, spatial: _text(row.get("h3_coarse")),
        "coarse_4x4_region": lambda row, spatial: _region_bucket(row),
        "image_size_bucket": lambda row, spatial: _size_bucket(row),
        "capture_month": lambda row, spatial: _capture_month(row.get("captured_at")),
        "query_to_nearest_gallery_provider": lambda row, spatial: spatial[str(row["id"])]["source_pairing"],
    }
    categorical: dict[str, Any] = {}
    for name, extractor in categorical_extractors.items():
        cal_dist = _distribution([extractor(row, calibration_spatial) for row in cal_records])
        test_dist = _distribution([extractor(row, test_spatial) for row in test_records])
        deltas = [
            {
                "value": key,
                "test_minus_calibration": float(test_dist.get(key, {}).get("rate", 0.0))
                - float(cal_dist.get(key, {}).get("rate", 0.0)),
                "calibration_rate": float(cal_dist.get(key, {}).get("rate", 0.0)),
                "test_rate": float(test_dist.get(key, {}).get("rate", 0.0)),
            }
            for key in set(cal_dist).union(test_dist)
        ]
        categorical[name] = {
            "calibration": cal_dist,
            "test": test_dist,
            "total_variation_distance": _total_variation(cal_dist, test_dist),
            "largest_rate_shifts": sorted(deltas, key=lambda item: abs(float(item["test_minus_calibration"])), reverse=True)[:12],
        }
    continuous_extractors: dict[str, Any] = {
        "nearest_gallery_distance_m": lambda row, spatial: spatial[str(row["id"])]["nearest_gallery_distance_m_recomputed"],
        "gallery_count_within_100m": lambda row, spatial: spatial[str(row["id"])]["gallery_count_within_100m"],
        "gallery_sequence_count_within_100m": lambda row, spatial: spatial[str(row["id"])]["gallery_sequence_count_within_100m"],
        "gallery_provider_count_within_100m": lambda row, spatial: spatial[str(row["id"])]["gallery_provider_count_within_100m"],
        "quality_score": lambda row, spatial: _finite(row.get("quality_score")),
        "blur_score": lambda row, spatial: _finite(row.get("blur_score")),
        "brightness": lambda row, spatial: _finite(row.get("brightness")),
        "exposure_score": lambda row, spatial: _finite(row.get("exposure_score")),
        "image_width": lambda row, spatial: _finite(row.get("width")),
        "image_height": lambda row, spatial: _finite(row.get("height")),
        "nearest_gallery_heading_gap_degrees": lambda row, spatial: spatial[str(row["id"])]["nearest_gallery_heading_gap_degrees"],
        "nearest_gallery_capture_gap_days": lambda row, spatial: spatial[str(row["id"])]["nearest_gallery_capture_gap_days"],
    }
    continuous = {
        name: _continuous_shift(
            [extractor(row, calibration_spatial) for row in cal_records],
            [extractor(row, test_spatial) for row in test_records],
        )
        for name, extractor in continuous_extractors.items()
    }
    sequence_stats = {
        "calibration": {
            "query_count": len(cal_records),
            "unique_provider_sequences": len({f"{row['source']}::{row['sequence_id']}" for row in cal_records}),
            "frames_per_sequence": _quantiles(
                [
                    float(count)
                    for count in Counter(f"{row['source']}::{row['sequence_id']}" for row in cal_records).values()
                ]
            ),
        },
        "test": {
            "query_count": len(test_records),
            "unique_provider_sequences": len({f"{row['source']}::{row['sequence_id']}" for row in test_records}),
            "frames_per_sequence": _quantiles(
                [float(count) for count in Counter(f"{row['source']}::{row['sequence_id']}" for row in test_records).values()]
            ),
        },
    }
    return {"categorical": categorical, "continuous": continuous, "sequence": sequence_stats}, calibration_spatial, test_spatial


def _fixed_grid_index(lat: float, lon: float) -> tuple[int, int]:
    from ml.ingestion.schema import MOSCOW_BOUNDS

    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    x = max(0, min(FIXED_GRID_BINS - 1, int((lon - min_lon) / (max_lon - min_lon) * FIXED_GRID_BINS)))
    y = max(0, min(FIXED_GRID_BINS - 1, int((lat - min_lat) / (max_lat - min_lat) * FIXED_GRID_BINS)))
    return x, y


def _heading_bin_count(rows: Sequence[Mapping[str, Any]]) -> int:
    headings = [_finite(row.get("heading")) for row in rows]
    values = [float(value) for value in headings if value is not None]
    return len({int(value % 360.0 // 45.0) for value in values})


def _coverage_grid(gallery: Any, calibration: Any, test: Any, source_manifest: Any | None) -> dict[str, Any]:
    from ml.ingestion.schema import MOSCOW_BOUNDS

    gallery_rows = gallery.to_dict(orient="records")
    source_rows = source_manifest.to_dict(orient="records") if source_manifest is not None else []
    query_rows = calibration.to_dict(orient="records") + test.to_dict(orient="records")
    gallery_by_cell: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    source_by_cell: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    queries_by_cell: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in gallery_rows:
        gallery_by_cell[_fixed_grid_index(float(row["lat"]), float(row["lon"]))].append(row)
    for row in source_rows:
        source_by_cell[_fixed_grid_index(float(row["lat"]), float(row["lon"]))].append(row)
    for row in query_rows:
        queries_by_cell[_fixed_grid_index(float(row["lat"]), float(row["lon"]))].append(row)
    cells: list[dict[str, Any]] = []
    category_counts: Counter[str] = Counter()
    flag_counts: Counter[str] = Counter()
    for y in range(FIXED_GRID_BINS):
        for x in range(FIXED_GRID_BINS):
            rows = gallery_by_cell[(x, y)]
            sources = {str(row["source"]).lower() for row in rows}
            sequences = {
                f"{str(row['source']).lower()}::{_text(row.get('sequence_id')) or '<missing>'}" for row in rows
            }
            heading_count = _heading_bin_count(rows)
            query_count = len(queries_by_cell[(x, y)])
            reference_count = len(rows)
            # These operational classifications are deliberately transparent
            # rather than optimized: 10 images, two independent sequences, two
            # providers, and two 45-degree heading bins are minimum evidence of
            # diversity for a cell this large.
            if reference_count == 0:
                category = "empty"
            elif len(sequences) < 2:
                category = "insufficient_sequence_diversity"
            elif len(sources) < 2:
                category = "single_provider"
            elif heading_count < 2:
                category = "concentrated_viewpoints"
            elif query_count == 0:
                category = "no_independent_heldout_query"
            elif reference_count >= 10:
                category = "dense_healthy"
            else:
                category = "independent_but_sparse"
            category_counts[category] += 1
            min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
            flags = {
                "empty": reference_count == 0,
                "insufficient_sequence_diversity": reference_count > 0 and len(sequences) < 2,
                "single_provider": reference_count > 0 and len(sources) < 2,
                "concentrated_viewpoints": reference_count > 0 and heading_count < 2,
                "supports_independent_heldout_queries": query_count > 0,
                "dense_healthy": category == "dense_healthy",
            }
            flag_counts.update(key for key, value in flags.items() if value)
            cells.append(
                {
                    "x": x,
                    "y": y,
                    "lon_min": min_lon + (max_lon - min_lon) * x / FIXED_GRID_BINS,
                    "lon_max": min_lon + (max_lon - min_lon) * (x + 1) / FIXED_GRID_BINS,
                    "lat_min": min_lat + (max_lat - min_lat) * y / FIXED_GRID_BINS,
                    "lat_max": min_lat + (max_lat - min_lat) * (y + 1) / FIXED_GRID_BINS,
                    "reference_count": reference_count,
                    "source_clean_reference_count": len(source_by_cell[(x, y)]),
                    "sequence_count": len(sequences),
                    "provider_count": len(sources),
                    "providers": sorted(sources),
                    "heading_bin_count_45deg": heading_count,
                    "heading_known_count": sum(_finite(row.get("heading")) is not None for row in rows),
                    "independent_heldout_query_count": query_count,
                    "calibration_query_count": sum(row["evaluation_split"] == CALIBRATION_LABEL for row in queries_by_cell[(x, y)]),
                    "test_query_count": sum(row["evaluation_split"] == TEST_LABEL for row in queries_by_cell[(x, y)]),
                    "category": category,
                    "flags": flags,
                }
            )
    return {
        "definition": {
            "grid": "Fixed 20x20 grid over ml.ingestion.schema.MOSCOW_BOUNDS, matching the tracked data report.",
            "independent_heldout_query": "A calibration or test query in the cell; every such query is split-audited to have a <=100m gallery positive from a different sequence.",
            "dense_healthy": "At least 10 deployed references, >=2 provider-namespaced sequences, >=2 providers, >=2 heading 45-degree bins, and >=1 independent held-out query.",
            "concentrated_viewpoints": "Fewer than two observed 45-degree heading bins; missing headings remain explicit rather than treated as coverage.",
        },
        "bounds": list(MOSCOW_BOUNDS),
        "grid_bins_per_axis": FIXED_GRID_BINS,
        "category_counts": dict(sorted(category_counts.items())),
        "flag_counts": dict(sorted(flag_counts.items())),
        "deployed_gallery_count": len(gallery_rows),
        "source_clean_count": len(source_rows) if source_manifest is not None else None,
        "cells": cells,
    }


def _write_coverage_map(grid: Mapping[str, Any], output_path: Path) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch, Rectangle

    colors = {
        "empty": "#f1f5f9",
        "insufficient_sequence_diversity": "#fb923c",
        "single_provider": "#facc15",
        "concentrated_viewpoints": "#c084fc",
        "no_independent_heldout_query": "#94a3b8",
        "independent_but_sparse": "#38bdf8",
        "dense_healthy": "#22c55e",
    }
    figure, axis = plt.subplots(figsize=(11, 10))
    for cell in grid["cells"]:
        axis.add_patch(
            Rectangle(
                (cell["lon_min"], cell["lat_min"]),
                cell["lon_max"] - cell["lon_min"],
                cell["lat_max"] - cell["lat_min"],
                facecolor=colors[cell["category"]],
                edgecolor="#cbd5e1",
                linewidth=0.25,
            )
        )
    min_lat, max_lat, min_lon, max_lon = grid["bounds"]
    axis.set(xlim=(min_lon, max_lon), ylim=(min_lat, max_lat), xlabel="Longitude", ylabel="Latitude")
    axis.set_title("Deployed Moscow gallery: coverage and diversity diagnostics (20×20 fixed grid)")
    axis.legend(
        handles=[Patch(facecolor=color, label=label.replace("_", " ")) for label, color in colors.items()],
        loc="upper left",
        bbox_to_anchor=(1.02, 1.0),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(figure)


def _calibration_slices(rows: Sequence[Mapping[str, Any]], calibration: Any, spatial: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    metadata = _frame_records(calibration)
    enriched: list[dict[str, Any]] = []
    for row in rows:
        record = dict(metadata[str(row["query_id"])])
        record.update(row)
        record["spatial"] = spatial[str(row["query_id"])]
        enriched.append(record)

    def summarize(subset: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        total = len(subset)
        if not total:
            return {"query_count": 0}
        return {
            "query_count": total,
            "top_k_retrieval_positive_within_100m": _pct_count(sum(bool(row["positive_ids"]) for row in subset), total),
            "raw_localization_within_100m": _pct_count(sum(float(row["error_m"]) <= 100.0 for row in subset), total),
            "selected_policy_answer_rate": _pct_count(sum(row["status"] == "ok" for row in subset), total),
            "selected_policy_precision_within_100m": _wilson_interval(
                sum(row["status"] == "ok" and float(row["error_m"]) <= 100.0 for row in subset),
                sum(row["status"] == "ok" for row in subset),
            ),
            "top1_similarity": _quantiles([float(row["detail"].diagnostics["top1_similarity"]) for row in subset]),
            "nearest_gallery_distance_m": _quantiles(
                [float(row["spatial"]["nearest_gallery_distance_m_recomputed"]) for row in subset]
            ),
            "gallery_count_within_100m": _quantiles([float(row["spatial"]["gallery_count_within_100m"]) for row in subset]),
        }

    provider = {key: summarize([row for row in enriched if str(row["source"]).lower() == key]) for key in sorted({str(row["source"]).lower() for row in enriched})}
    paired = {
        key: summarize([row for row in enriched if row["spatial"]["source_pairing"] == key])
        for key in sorted({str(row["spatial"]["source_pairing"]) for row in enriched})
    }
    areas: list[dict[str, Any]] = []
    for area, subset in sorted(
        ((key, [row for row in enriched if _text(row.get("evaluation_area_h3")) == key]) for key in {_text(row.get("evaluation_area_h3")) for row in enriched}),
        key=lambda item: str(item[0]),
    ):
        if area is not None and len(subset) >= 5:
            item = {"evaluation_area_h3": area, **summarize(subset)}
            areas.append(item)
    areas_sorted = sorted(areas, key=lambda item: (item["raw_localization_within_100m"]["rate"], -item["query_count"]))
    return {
        "by_query_provider": provider,
        "by_query_to_nearest_gallery_provider_pair": paired,
        "area_groups_minimum_5_queries": {
            "count": len(areas),
            "lowest_raw_localization": areas_sorted[:20],
            "highest_raw_localization": list(reversed(areas_sorted[-20:])),
        },
    }


def _process_rss_kib(process_id: int) -> int | None:
    completed = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(process_id)],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    try:
        return int(value)
    except ValueError:
        return None


def _tree_size_bytes(path: Path) -> int | None:
    if not path.exists():
        return None
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def _file_sizes(paths: Mapping[str, Path]) -> dict[str, int | None]:
    return {name: path.stat().st_size if path.exists() and path.is_file() else None for name, path in paths.items()}


def _runtime_only(args: argparse.Namespace) -> None:
    """Measure the actual production-shaped K=20 service in a clean process."""

    from PIL import Image, ImageOps, UnidentifiedImageError

    from ml.localization.pipeline import SpatialLocalizer
    from ml.localization.service import LocalizationService
    from ml.query_quality import measure_query_image_quality
    from ml.retrieval import create_retriever

    calibration = _read_manifest(args.calibration_manifest)
    _validate_split(calibration, CALIBRATION_LABEL, args.calibration_manifest)
    report = _load_json(args.calibration_benchmark)
    if not isinstance(report, dict):
        raise DiagnosisError("calibration benchmark must be a JSON object")
    config = _config_from_report(report, confidence_threshold=args.selected_confidence_threshold)
    retriever = create_retriever(args.model, device=args.device, batch_size=8, cache_dir=args.cache_dir)
    service = LocalizationService(
        retriever,
        args.index_dir,
        localizer=SpatialLocalizer(config),
        top_k=20,
        process_isolate_faiss=True,
        expected_city_id="moscow",
        expected_index_id="moscow",
    )
    load_started = time.perf_counter()
    service.load()
    load_ms = (time.perf_counter() - load_started) * 1000.0
    try:
        worker_pid = getattr(getattr(service, "index", None), "_process", None)
        worker_pid = getattr(worker_pid, "pid", None)
        memory = {
            "service_parent_rss_kib": _process_rss_kib(os.getpid()),
            "faiss_worker_rss_kib": _process_rss_kib(int(worker_pid)) if worker_pid else None,
        }
        if memory["service_parent_rss_kib"] is not None and memory["faiss_worker_rss_kib"] is not None:
            memory["combined_rss_kib"] = int(memory["service_parent_rss_kib"]) + int(memory["faiss_worker_rss_kib"])
        else:
            memory["combined_rss_kib"] = None
        rows = sorted(calibration.to_dict(orient="records"), key=lambda row: str(row["id"]))[: int(args.runtime_sample_size)]
        latency_samples: defaultdict[str, list[float]] = defaultdict(list)
        statuses: Counter[str] = Counter()
        preprocessing_ms: list[float] = []
        for position, row in enumerate(rows, start=1):
            image_path = Path(str(row["image_path"]))
            started = time.perf_counter()
            try:
                with Image.open(image_path) as opened:
                    opened.load()
                    image = ImageOps.exif_transpose(opened).convert("RGB")
            except (OSError, SyntaxError, ValueError, UnidentifiedImageError) as exc:
                raise DiagnosisError(f"cannot prepare runtime image {row['id']}") from exc
            quality = measure_query_image_quality(image)
            preprocessing_ms.append((time.perf_counter() - started) * 1000.0)

            @dataclass(frozen=True, slots=True)
            class Prepared:
                image: Any
                diagnostics: Any

            @dataclass(frozen=True, slots=True)
            class Diagnostics:
                sharpness: float
                exposure: float

            result = service.localize(Prepared(image=image, diagnostics=Diagnostics(quality.sharpness, quality.exposure)))
            statuses[str(result["status"])] += 1
            diagnostics = result["diagnostics"]
            for key in ("query_ms", "embedding_ms", "retrieval_ms"):
                value = _finite(diagnostics.get(key))
                if value is not None:
                    latency_samples[key].append(value)
            if position % 10 == 0 or position == len(rows):
                print(f"runtime_probe_queries={position}/{len(rows)}", flush=True)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "configuration": {
                "retriever": args.model,
                "top_k": 20,
                "coordinate_estimator": config.estimator.value,
                "confidence_threshold": config.confidence_threshold,
                "verification_enabled": False,
                "faiss_process_isolation": True,
            },
            "model_index_load_ms": load_ms,
            "rss_after_model_and_index_load": memory,
            "sample": {
                "count": len(rows),
                "selection": "First 50 calibration IDs in stable lexical order; labels not used for selection.",
                "status_counts": dict(sorted(statuses.items())),
                "validation_preprocessing_ms": _quantiles(preprocessing_ms),
                "service_latency_ms": {key: _quantiles(values) for key, values in sorted(latency_samples.items())},
            },
            "disk_logical_bytes": {
                "index_directory": _tree_size_bytes(args.index_dir),
                "embedding_directory": _tree_size_bytes(Path("data/embeddings/moscow/megaloc")),
                "reference_imagery": _tree_size_bytes(Path("data/raw/moscow/images")),
                "torch_hub_cache": _tree_size_bytes(args.cache_dir),
                "huggingface_cache": _tree_size_bytes(Path(".cache/huggingface")),
            },
        }
    finally:
        service.close()
    _atomic_json(args.runtime_output, payload)
    print(json.dumps({"runtime_probe": str(args.runtime_output)}, indent=2))


def _run_runtime_subprocess(args: argparse.Namespace, output_path: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "ml.evaluation.product_quality_diagnosis",
        "--runtime-only",
        "--index-dir",
        str(args.index_dir),
        "--calibration-manifest",
        str(args.calibration_manifest),
        "--calibration-benchmark",
        str(args.calibration_benchmark),
        "--runtime-output",
        str(output_path),
        "--runtime-sample-size",
        str(args.runtime_sample_size),
        "--model",
        args.model,
        "--device",
        args.device,
        "--cache-dir",
        str(args.cache_dir),
        "--selected-confidence-threshold",
        str(args.selected_confidence_threshold),
    ]
    completed = subprocess.run(command, check=False, text=True)
    if completed.returncode != 0:
        raise DiagnosisError(f"production-shaped runtime probe failed with exit code {completed.returncode}")
    payload = _load_json(output_path)
    if not isinstance(payload, dict):
        raise DiagnosisError("runtime probe output must be an object")
    return payload


def _runtime_from_existing_report(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "historical_calibration_run": {
            "model_load_ms": report.get("runtime", {}).get("model_load_ms"),
            "gallery_embedding_ms": report.get("runtime", {}).get("gallery_embedding_ms"),
            "exact_faiss_build_ms": report.get("runtime", {}).get("exact_faiss_build_ms"),
            "latency": report.get("primary", {}).get("latency"),
            "warning": "Historical K=10 benchmark timing; an equivalent production K=20 probe is reported separately when it runs.",
        }
    }


def _render_generated_markdown(payload: Mapping[str, Any]) -> str:
    abstention = payload["calibration_top10"]["abstention_decomposition"]
    retrieval = payload["calibration_retrieval_oracle"]
    grid = payload["coverage_grid"]
    shift = payload["distribution_shift"]
    lines = [
        "# Generated product-quality diagnosis evidence",
        "",
        "This generated report is calibration-only for performance analysis. The test manifest is used only for metadata/coverage shift.",
        "",
        "## Calibration top-10 abstentions at the frozen threshold",
        "",
        f"- Answered: {_percent(abstention['answered']['rate'])} ({abstention['answered']['count']}/{abstention['query_count']})",
        f"- Abstained: {_percent(abstention['abstained']['rate'])} ({abstention['abstained']['count']}/{abstention['query_count']})",
        "",
        "| Condition | Count | Rate |",
        "|---|---:|---:|",
    ]
    for reason, value in abstention["individual_conditions"].items():
        lines.append(f"| {reason} | {value['count']} | {_percent(value['rate'])} |")
    lines.extend(
        [
            "",
            "## Retrieval oracle",
            "",
            "| Positive distance | Recall@10 | Recall@20 | Recall@50 |",
            "|---|---:|---:|---:|",
        ]
    )
    for distance, values in retrieval["by_distance_m"].items():
        recall = values["recall_at"]
        lines.append(
            f"| <= {distance} m | {_percent(recall['10']['rate'])} | {_percent(recall['20']['rate'])} | {_percent(recall['50']['rate'])} |"
        )
    lines.extend(
        [
            "",
            "## Fixed-grid coverage",
            "",
            "| Category | Cells |",
            "|---|---:|",
        ]
    )
    for category, count in grid["category_counts"].items():
        lines.append(f"| {category} | {count} |")
    lines.extend(
        [
            "",
            "## Calibration/test metadata shift",
            "",
            "| Categorical variable | Total variation distance |",
            "|---|---:|",
        ]
    )
    for name, values in shift["categorical"].items():
        lines.append(f"| {name} | {values['total_variation_distance']:.3f} |")
    return "\n".join(lines) + "\n"


def _append_ground_truth(rows: Sequence[dict[str, Any]], calibration_records: Mapping[str, Mapping[str, Any]]) -> None:
    for row in rows:
        source = calibration_records[str(row["query_id"])]
        row["true_lat"] = float(source["lat"])
        row["true_lon"] = float(source["lon"])


def _diagnose(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    gallery = _read_manifest(args.gallery_manifest)
    calibration = _read_manifest(args.calibration_manifest)
    test = _read_manifest(args.test_manifest)
    _validate_split(gallery, "gallery", args.gallery_manifest)
    _validate_split(calibration, CALIBRATION_LABEL, args.calibration_manifest)
    _validate_split(test, TEST_LABEL, args.test_manifest)
    report = _load_json(args.calibration_benchmark)
    if not isinstance(report, dict):
        raise DiagnosisError("calibration benchmark must be a JSON object")
    _validate_calibration_report(report, len(calibration))
    calibration_records = _frame_records(calibration)
    source_manifest = None
    if args.source_clean_manifest and args.source_clean_manifest.is_file():
        import pandas as pd

        source_manifest = pd.read_parquet(args.source_clean_manifest)
        required_source_columns = {"id", "lat", "lon", "source", "sequence_id"}
        missing_source_columns = sorted(required_source_columns.difference(source_manifest.columns))
        if missing_source_columns:
            raise DiagnosisError(
                f"source clean manifest lacks required coverage columns: {missing_source_columns}"
            )
    if source_manifest is not None:
        # The source manifest predates split assignment, so only schema/data are relevant here.
        if len(source_manifest) < len(gallery):
            raise DiagnosisError("source clean manifest is smaller than deployed gallery")

    top10_rows, replay = _base_replay(report, selected_threshold=args.selected_confidence_threshold)
    if not replay["exact"]:
        raise DiagnosisError(f"stored calibration replay diverged for {replay['mismatch_count']} queries")
    for row in top10_rows:
        source = calibration_records[str(row["query_id"])]
        row["true_lat"] = float(source["lat"])
        row["true_lon"] = float(source["lon"])
        source["_query_confidence_signal"] = row["query_quality"]

    descriptor_path = output_dir / "calibration_query_descriptors.npy"
    embedding = _embed_calibration_queries(
        calibration,
        output_path=descriptor_path,
        model_name=args.model,
        device=args.device,
        cache_dir=args.cache_dir,
    )
    search_path = output_dir / "calibration_full_rank_search.json"
    search = _run_search_subprocess(args, descriptor_path, search_path)
    if len(search.get("per_query", [])) != len(calibration):
        raise DiagnosisError("full-rank FAISS replay did not return every calibration query")
    search_by_id = {str(row["query_id"]): row for row in search["per_query"]}
    historical_by_id = {str(row["query_id"]): row for row in report["per_query"]}
    top10_identity_mismatch = 0
    for query_id, search_row in search_by_id.items():
        historical_ids = [str(match["reference_id"]) for match in historical_by_id[query_id]["matches"]]
        replay_ids = [str(match["reference_id"]) for match in search_row["top_matches"][:10]]
        if historical_ids != replay_ids:
            top10_identity_mismatch += 1
    top20_rows = _top_k_localization_rows(
        search,
        calibration_records,
        report,
        top_k=20,
        selected_threshold=args.selected_confidence_threshold,
    )
    top50_rows = _top_k_localization_rows(
        search,
        calibration_records,
        report,
        top_k=50,
        selected_threshold=args.selected_confidence_threshold,
    )
    _append_ground_truth(top20_rows, calibration_records)
    _append_ground_truth(top50_rows, calibration_records)

    shift, calibration_spatial, test_spatial = _metadata_shift(calibration, test, gallery)
    coverage = _coverage_grid(gallery, calibration, test, source_manifest)
    coverage_map_path = output_dir / "coverage_diversity_grid.png"
    _write_coverage_map(coverage, coverage_map_path)

    # Attach deployed-gallery metadata to candidate records for compact useful
    # per-query evidence, including temporal/viewpoint fields.
    gallery_records = _frame_records(gallery)
    for collection in (top10_rows, top20_rows, top50_rows):
        for row in collection:
            for match in row["matches"]:
                reference = gallery_records.get(str(match["reference_id"]))
                if reference:
                    match["captured_at"] = reference.get("captured_at")
                    match["heading"] = reference.get("heading")
                    match["quality_score"] = reference.get("quality_score")
    # The internal temporal helper needs a reference lookup; calculate directly
    # after gallery metadata has been attached.
    temporal_viewpoint: dict[str, Any] = {}
    retrieval_positive = [row for row in top50_rows if row["positive_ids"]]
    heading_gaps: list[float] = []
    time_gaps: list[float] = []
    for row in retrieval_positive:
        match = next(match for match in row["matches"] if str(match["reference_id"]) in set(row["positive_ids"]))
        query = calibration_records[str(row["query_id"])]
        if (gap := _heading_delta_degrees(query.get("heading"), match.get("heading"))) is not None:
            heading_gaps.append(gap)
        query_day = _parse_date_to_days(query.get("captured_at"))
        match_day = _parse_date_to_days(match.get("captured_at"))
        if query_day is not None and match_day is not None:
            time_gaps.append(abs(query_day - match_day))
    temporal_viewpoint = {
        "queries_with_top50_geographic_positive": _pct_count(len(retrieval_positive), len(top50_rows)),
        "highest_ranked_geographic_positive_heading_gap_degrees": _quantiles(heading_gaps),
        "highest_ranked_geographic_positive_capture_gap_days": _quantiles(time_gaps),
    }
    runtime_probe = _run_runtime_subprocess(args, output_dir / "runtime_probe.json")

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "purpose": (
            "Calibration-only product-quality diagnosis. Frozen test labels/errors were not used for any model, "
            "threshold, estimator, or data-selection decision."
        ),
        "generated_at_utc": __import__("datetime").datetime.now(UTC).isoformat(),
        "inputs": {
            "gallery_manifest": {"path": str(args.gallery_manifest), "sha256": _sha256_file(args.gallery_manifest), "count": len(gallery)},
            "calibration_manifest": {
                "path": str(args.calibration_manifest),
                "sha256": _sha256_file(args.calibration_manifest),
                "count": len(calibration),
            },
            "test_manifest_metadata_only": {
                "path": str(args.test_manifest),
                "sha256": _sha256_file(args.test_manifest),
                "count": len(test),
            },
            "calibration_benchmark_threshold_zero": {
                "path": str(args.calibration_benchmark),
                "sha256": _sha256_file(args.calibration_benchmark),
            },
            "source_clean_manifest": (
                None
                if source_manifest is None
                else {"path": str(args.source_clean_manifest), "sha256": _sha256_file(args.source_clean_manifest), "count": len(source_manifest)}
            ),
            "persisted_index": {
                "path": str(args.index_dir),
                "index_sha256": _sha256_file(args.index_dir / "index.faiss"),
                "metadata_sha256": _sha256_file(args.index_dir / "index_metadata.json"),
            },
        },
        "frozen_config_replayed": {
            "retriever": args.model,
            "coordinate_estimator": report["localization"]["estimator"],
            "benchmark_top_k": 10,
            "production_default_top_k": 20,
            "selected_confidence_threshold": args.selected_confidence_threshold,
            "verification_enabled": False,
            "top10_replay": replay,
            "top10_identity_reproducibility_after_reembedding": {
                "mismatch_count": top10_identity_mismatch,
                "exact": top10_identity_mismatch == 0,
            },
        },
        "query_embedding_replay": embedding,
        "full_rank_faiss_replay": {key: value for key, value in search.items() if key != "per_query"},
        "calibration_top10": {
            "abstention_decomposition": _abstention_decomposition(top10_rows, total_queries=len(top10_rows)),
            "localization_oracle": _localization_oracle(top10_rows, report),
            "confidence": _confidence_analysis(top10_rows),
        },
        "calibration_retrieval_oracle": _retrieval_oracle(search),
        "calibration_top20_production_depth_diagnostic": {
            "abstention_decomposition": _abstention_decomposition(top20_rows, total_queries=len(top20_rows)),
            "localization_oracle": _localization_oracle(top20_rows, report),
            "confidence": _confidence_analysis(top20_rows),
            "warning": "Diagnostic only: top-20 was not the frozen benchmark depth and no threshold/config was selected from it.",
        },
        "calibration_top50_retrieval_depth_diagnostic": {
            "abstention_decomposition": _abstention_decomposition(top50_rows, total_queries=len(top50_rows)),
            "localization_oracle": _localization_oracle(top50_rows, report),
            "warning": "Diagnostic only; a 50-candidate input is not a proposed production configuration.",
        },
        "distribution_shift": shift,
        "coverage_grid": coverage,
        "calibration_slices": _calibration_slices(top50_rows, calibration, calibration_spatial),
        "temporal_viewpoint_retrieval_positive": temporal_viewpoint,
        "runtime_resources": {
            **_runtime_from_existing_report(report),
            "production_shaped_runtime_probe": runtime_probe,
            "disk_bytes": _file_sizes(
                {
                    "index_faiss": args.index_dir / "index.faiss",
                    "index_mapping": args.index_dir / "id_mapping.json",
                    "index_metadata": args.index_dir / "index_metadata.json",
                    "gallery_manifest": args.gallery_manifest,
                    "reference_images_directory": Path("data/raw/moscow/images"),
                }
            ),
            "coverage_map": str(coverage_map_path),
        },
        "environment": {"platform": platform.platform(), "python": sys.version.split()[0]},
    }
    per_query_rows: list[dict[str, Any]] = []
    top20_by_id = {str(row["query_id"]): row for row in top20_rows}
    top50_by_id = {str(row["query_id"]): row for row in top50_rows}
    top10_by_id = {str(row["query_id"]): row for row in top10_rows}
    for search_row in search["per_query"]:
        query_id = str(search_row["query_id"])
        query = calibration_records[query_id]
        per_query_rows.append(
            {
                "query_id": query_id,
                "query_metadata": {
                    key: query.get(key)
                    for key in (
                        "source",
                        "sequence_id",
                        "captured_at",
                        "heading",
                        "quality_score",
                        "width",
                        "height",
                        "blur_score",
                        "brightness",
                        "exposure_score",
                        "evaluation_area_h3",
                    )
                },
                "spatial_metadata": calibration_spatial[query_id],
                "full_rank_retrieval": {
                    "geo": search_row["geo"],
                    "correct_vs_best_incorrect_similarity_gap_100m": search_row["correct_vs_incorrect_similarity_gap_100m"],
                },
                "top10": _serialize_localization_row(top10_by_id[query_id]),
                "top20": _serialize_localization_row(top20_by_id[query_id]),
                "top50": _serialize_localization_row(top50_by_id[query_id]),
            }
        )
    _atomic_json(output_dir / "diagnosis.json", payload)
    _atomic_jsonl(output_dir / "per_query.jsonl", per_query_rows)
    _atomic_json(output_dir / "coverage_grid.json", coverage)
    _atomic_text(output_dir / "diagnosis.md", _render_generated_markdown(payload))
    print(json.dumps({"diagnosis": str(output_dir / "diagnosis.json"), "coverage_map": str(coverage_map_path)}, indent=2))
    return payload


def _serialize_localization_row(row: Mapping[str, Any]) -> dict[str, Any]:
    detail: LocalizerDetail = row["detail"]
    return {
        "status": row["status"],
        "reasons": list(row["reasons"]),
        "error_m": row["error_m"],
        "positive_ids": sorted(row["positive_ids"]),
        "confidence": detail.confidence,
        "diagnostics": dict(detail.diagnostics),
        "winning_ids": list(detail.winning_ids),
        "cluster_ids": [list(cluster) for cluster in detail.cluster_ids],
        "matches": row["matches"],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a calibration-only Moscow product-quality diagnosis")
    parser.add_argument("--search-only", action="store_true", help="internal isolated FAISS full-rank worker")
    parser.add_argument("--runtime-only", action="store_true", help="internal production-shaped runtime worker")
    parser.add_argument("--index-dir", type=Path, default=Path("data/indexes/moscow/megaloc"))
    parser.add_argument("--descriptors", type=Path)
    parser.add_argument("--query-manifest", type=Path)
    parser.add_argument("--search-output", type=Path)
    parser.add_argument("--runtime-output", type=Path)
    parser.add_argument("--replay-top-k", type=int, default=50)
    parser.add_argument("--search-batch-size", type=int, default=8)
    parser.add_argument("--runtime-sample-size", type=int, default=50)
    parser.add_argument("--gallery-manifest", type=Path, default=Path("data/evaluation/moscow_real_v1/gallery.parquet"))
    parser.add_argument(
        "--calibration-manifest", type=Path, default=Path("data/evaluation/moscow_real_v1/calibration_queries.parquet")
    )
    parser.add_argument("--test-manifest", type=Path, default=Path("data/evaluation/moscow_real_v1/test_queries.parquet"))
    parser.add_argument(
        "--calibration-benchmark",
        type=Path,
        default=Path("data/evaluation/moscow_real_v1/reports/megaloc_moscow_real_calibration.json"),
    )
    parser.add_argument("--source-clean-manifest", type=Path, default=Path("data/processed/moscow/manifest_clean.parquet"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--model", choices=("megaloc",), default="megaloc")
    parser.add_argument("--device", default="auto")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/torch/hub"))
    parser.add_argument("--selected-confidence-threshold", type=float, default=SELECTED_CONFIDENCE_THRESHOLD)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.search_only:
        if args.descriptors is None or args.query_manifest is None or args.search_output is None:
            raise SystemExit("--search-only requires --descriptors, --query-manifest, and --search-output")
        _search_only(args)
        return
    if args.runtime_only:
        if args.runtime_output is None:
            raise SystemExit("--runtime-only requires --runtime-output")
        _runtime_only(args)
        return
    _diagnose(args)


if __name__ == "__main__":
    main()
