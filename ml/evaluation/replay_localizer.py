"""Replay calibration retrieval matches through another production coordinate estimator."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from ml.evaluation.metrics import LocalizationObservation, evaluate_localization
from ml.evaluation.moscow_benchmark import (
    BENCHMARK_KIND,
    _false_confident_errors,
    _localizer_payload,
    write_moscow_benchmark_reports,
)
from ml.indexing.faiss_index import RetrievalResult
from ml.localization import LocalizerConfig, SpatialLocalizer, haversine_m
from ml.localization.estimators import CoordinateEstimator


class ReplayError(RuntimeError):
    """A benchmark report is unsafe or incomplete for calibration replay."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def replay_payload(
    source: dict[str, Any],
    *,
    estimator: CoordinateEstimator | str,
    top_k: int,
) -> dict[str, Any]:
    if source.get("benchmark_kind") != BENCHMARK_KIND:
        raise ReplayError("source must be a Moscow real-street-view benchmark")
    if source.get("leakage_audit", {}).get("passed") is not True:
        raise ReplayError("source leakage audit must pass")
    rows = source.get("per_query")
    if not isinstance(rows, list) or not rows:
        raise ReplayError("source per_query rows are required")
    if {str(row.get("evaluation_split", "")).lower() for row in rows} != {"calibration"}:
        raise ReplayError("estimator replay is calibration-only")
    if top_k < 10:
        raise ValueError("top_k must be at least 10")

    estimator_value = CoordinateEstimator(estimator)
    localizer = SpatialLocalizer(LocalizerConfig(estimator=estimator_value, confidence_threshold=0.0))
    replayed_rows: list[dict[str, Any]] = []
    observations: list[LocalizationObservation] = []
    for source_row in rows:
        matches_value = source_row.get("matches")
        if not isinstance(matches_value, list) or len(matches_value) < min(top_k, 10):
            raise ReplayError("every query must carry the requested top-k retrieval matches")
        matches = [
            RetrievalResult(
                reference_id=str(match["reference_id"]),
                score=float(match["score"]),
                rank=int(match["rank"]),
                metadata={
                    "lat": float(match["lat"]),
                    "lon": float(match["lon"]),
                    "source": match.get("source"),
                    "source_image_id": match.get("source_image_id"),
                    "sequence_id": match.get("sequence_id"),
                    "source_url": match.get("source_url"),
                    "index_id": "calibration-replay",
                },
            )
            for match in matches_value[:top_k]
        ]
        quality = float(source_row.get("query_quality", {}).get("confidence_signal", 1.0))
        result = localizer.localize(matches, query_quality=quality)
        true_lat = float(source_row["true_lat"])
        true_lon = float(source_row["true_lon"])
        error_m = (
            None
            if result.lat is None or result.lon is None
            else haversine_m(true_lat, true_lon, result.lat, result.lon)
        )
        row = copy.deepcopy(source_row)
        row.update(
            {
                "predicted_lat": result.lat,
                "predicted_lon": result.lon,
                "error_m": error_m,
                "status": result.status.value,
                "confidence": result.confidence,
                "reasons": list(result.reasons),
                "matches": matches_value[:top_k],
            }
        )
        replayed_rows.append(row)
        observations.append(
            LocalizationObservation(
                query_id=str(row["query_id"]),
                true_lat=true_lat,
                true_lon=true_lon,
                predicted_lat=result.lat,
                predicted_lon=result.lon,
                status=result.status.value,
                confidence=float(result.confidence),
            )
        )

    payload = copy.deepcopy(source)
    localization = evaluate_localization(observations, thresholds_m=(25, 50, 100)).to_dict()
    payload["localization"] = _localizer_payload(localizer.config)
    payload.setdefault("retrieval_configuration", {})["top_k"] = top_k
    source_recall = payload["primary"]["retrieval"]["recall_at"]
    payload["primary"]["retrieval"]["recall_at"] = {
        key: value for key, value in source_recall.items() if int(key) <= top_k
    }
    payload["primary"]["localization"] = localization
    payload["primary"]["confidence_buckets"] = localization["confidence_buckets"]
    payload["primary"]["false_confident_errors"] = _false_confident_errors(
        replayed_rows,
        confidence_threshold=0.0,
    )
    payload["per_query"] = replayed_rows
    payload["experiment_replay"] = {
        "scope": "calibration_only",
        "retrieval_matches_reused": True,
        "estimator": estimator_value.value,
        "top_k": top_k,
    }
    return payload


def replay_report(
    source_json: str | Path,
    output_dir: str | Path,
    *,
    estimator: CoordinateEstimator | str,
    top_k: int,
    stem: str,
) -> tuple[dict[str, Any], Path, Path]:
    source_path = Path(source_json).resolve()
    loaded = json.loads(source_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ReplayError("source report must be a JSON object")
    payload = replay_payload(loaded, estimator=estimator, top_k=top_k)
    payload["experiment_replay"]["source_report"] = str(source_path)
    payload["experiment_replay"]["source_report_sha256"] = _sha256(source_path)
    json_path, markdown_path = write_moscow_benchmark_reports(payload, output_dir, stem=stem)
    return payload, json_path, markdown_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--estimator", choices=tuple(value.value for value in CoordinateEstimator), required=True)
    parser.add_argument("--top-k", type=int, required=True)
    parser.add_argument("--report-stem", required=True)
    args = parser.parse_args()
    payload, json_path, markdown_path = replay_report(
        args.source_json,
        args.output_dir,
        estimator=args.estimator,
        top_k=args.top_k,
        stem=args.report_stem,
    )
    print(
        json.dumps(
            {
                "retrieval": payload["primary"]["retrieval"],
                "localization": payload["primary"]["localization"],
                "json_report": str(json_path),
                "markdown_report": str(markdown_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
