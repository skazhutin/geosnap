"""Calibration-only evidence for the retrieval contribution of a versioned gallery tranche."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

THRESHOLDS_M = (25.0, 50.0, 100.0)
EARTH_RADIUS_M = 6_371_008.8


class GalleryContributionError(RuntimeError):
    """Contribution inputs do not support a safe comparison."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row["source"]).strip().lower(), str(row["source_image_id"]).strip()


def _nearest_distances(queries: pd.DataFrame, gallery: pd.DataFrame) -> np.ndarray:
    if gallery.empty:
        return np.full(len(queries), np.inf, dtype=np.float64)
    gallery_lat = np.radians(gallery["lat"].to_numpy(dtype=np.float64))
    gallery_lon = np.radians(gallery["lon"].to_numpy(dtype=np.float64))
    result = np.empty(len(queries), dtype=np.float64)
    for output_row, (_, query) in enumerate(queries.iterrows()):
        query_lat = math.radians(float(query["lat"]))
        query_lon = math.radians(float(query["lon"]))
        delta_lat = gallery_lat - query_lat
        delta_lon = gallery_lon - query_lon
        haversine = np.sin(delta_lat / 2.0) ** 2 + np.cos(query_lat) * np.cos(gallery_lat) * (
            np.sin(delta_lon / 2.0) ** 2
        )
        result[output_row] = 2.0 * EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(haversine, 0.0, 1.0))).min()
    return result


def _coverage(distances: np.ndarray) -> dict[str, Any]:
    finite = distances[np.isfinite(distances)]
    return {
        "query_count": int(len(distances)),
        "coverage_within_m": {
            str(int(threshold)): float(np.mean(distances <= threshold)) for threshold in THRESHOLDS_M
        },
        "nearest_distance_m": {
            "median": None if not len(finite) else float(np.median(finite)),
            "p90": None if not len(finite) else float(np.percentile(finite, 90)),
            "max": None if not len(finite) else float(np.max(finite)),
        },
    }


def _retrieval_contribution(
    benchmark_json: Path,
    *,
    new_reference_ids: set[str],
) -> dict[str, Any]:
    benchmark = json.loads(benchmark_json.read_text(encoding="utf-8"))
    queries = benchmark.get("dataset", {}).get("queries", {})
    if Path(str(queries.get("manifest_path", ""))).name != "calibration_queries.parquet":
        raise GalleryContributionError("retrieval contribution accepts calibration benchmark reports only")
    rows = benchmark.get("per_query")
    if not isinstance(rows, list) or not rows:
        raise GalleryContributionError("benchmark per_query rows are required")
    new_positive_queries = 0
    new_any_queries = 0
    new_first_positive_queries = 0
    for row in rows:
        matches = row.get("matches", [])
        new_matches = [match for match in matches if str(match.get("reference_id")) in new_reference_ids]
        new_any_queries += bool(new_matches)
        positive_new = [
            match
            for match in new_matches
            if match.get("lat") is not None
            and match.get("lon") is not None
            and _point_distance_m(
                float(row["true_lat"]),
                float(row["true_lon"]),
                float(match["lat"]),
                float(match["lon"]),
            )
            <= 100.0
        ]
        new_positive_queries += bool(positive_new)
        positives = [
            match
            for match in matches
            if match.get("lat") is not None
            and match.get("lon") is not None
            and _point_distance_m(
                float(row["true_lat"]),
                float(row["true_lon"]),
                float(match["lat"]),
                float(match["lon"]),
            )
            <= 100.0
        ]
        new_first_positive_queries += bool(positives and str(positives[0].get("reference_id")) in new_reference_ids)
    return {
        "benchmark_path": str(benchmark_json),
        "benchmark_sha256": _sha256(benchmark_json),
        "query_count": len(rows),
        "queries_retrieving_any_new_reference_at_top_k": new_any_queries,
        "queries_retrieving_a_new_positive_within_100m_at_top_k": new_positive_queries,
        "queries_whose_highest_ranked_positive_is_new": new_first_positive_queries,
    }


def _point_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = math.radians(lon2 - lon1)
    value = math.sin(delta_lat / 2.0) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(
        delta_lon / 2.0
    ) ** 2
    return 2.0 * EARTH_RADIUS_M * math.asin(math.sqrt(min(max(value, 0.0), 1.0)))


def build_contribution_report(
    *,
    legacy_manifest: str | Path,
    gallery_manifest: str | Path,
    calibration_manifest: str | Path,
    benchmark_json: str | Path | None = None,
) -> dict[str, Any]:
    legacy_path = Path(legacy_manifest)
    gallery_path = Path(gallery_manifest)
    calibration_path = Path(calibration_manifest)
    legacy = pd.read_parquet(legacy_path)
    gallery = pd.read_parquet(gallery_path)
    calibration = pd.read_parquet(calibration_path)
    if set(calibration.get("evaluation_split", [])) != {"calibration"}:
        raise GalleryContributionError("calibration manifest must contain calibration rows only")
    legacy_identities = {_identity(row) for row in legacy.to_dict(orient="records")}
    legacy_mask = np.asarray(
        [_identity(row) in legacy_identities for row in gallery.to_dict(orient="records")], dtype=bool
    )
    legacy_gallery = gallery.loc[legacy_mask].reset_index(drop=True)
    new_gallery = gallery.loc[~legacy_mask].reset_index(drop=True)
    legacy_distances = _nearest_distances(calibration, legacy_gallery)
    full_distances = _nearest_distances(calibration, gallery)
    legacy_coverage = _coverage(legacy_distances)
    full_coverage = _coverage(full_distances)
    report: dict[str, Any] = {
        "schema_version": 1,
        "scope": "calibration_only",
        "inputs": {
            "legacy_manifest": str(legacy_path),
            "legacy_manifest_sha256": _sha256(legacy_path),
            "gallery_manifest": str(gallery_path),
            "gallery_manifest_sha256": _sha256(gallery_path),
            "calibration_manifest": str(calibration_path),
            "calibration_manifest_sha256": _sha256(calibration_path),
        },
        "gallery_rows": {
            "full": len(gallery),
            "legacy_origin": len(legacy_gallery),
            "new_tranche_origin": len(new_gallery),
            "new_reference_ids": sorted(str(value) for value in new_gallery["id"]),
        },
        "coordinate_oracle": {
            "legacy_origin_gallery": legacy_coverage,
            "full_v2_gallery": full_coverage,
            "coverage_absolute_gain": {
                key: full_coverage["coverage_within_m"][key] - legacy_coverage["coverage_within_m"][key]
                for key in legacy_coverage["coverage_within_m"]
            },
            "queries_with_strictly_closer_new_gallery_coverage": int(
                np.sum(full_distances + 1e-9 < legacy_distances)
            ),
        },
    }
    if benchmark_json is not None:
        report["retrieval"] = _retrieval_contribution(
            Path(benchmark_json),
            new_reference_ids=set(report["gallery_rows"]["new_reference_ids"]),
        )
    return report


def _write(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-manifest", type=Path, required=True)
    parser.add_argument("--gallery-manifest", type=Path, required=True)
    parser.add_argument("--calibration-manifest", type=Path, required=True)
    parser.add_argument("--benchmark-json", type=Path)
    parser.add_argument("--output-json", type=Path, required=True)
    args = parser.parse_args()
    report = build_contribution_report(
        legacy_manifest=args.legacy_manifest,
        gallery_manifest=args.gallery_manifest,
        calibration_manifest=args.calibration_manifest,
        benchmark_json=args.benchmark_json,
    )
    _write(args.output_json, report)
    print(json.dumps({key: value for key, value in report.items() if key != "gallery_rows"}, sort_keys=True))


if __name__ == "__main__":
    main()
