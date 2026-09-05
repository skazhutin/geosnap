"""Build a versioned, diversity-aware Moscow acquisition plan.

The plan is intentionally based on cells, provider-sequence diversity, heading
coverage, and historical *calibration-only* retrieval strata.  Counts alone do
not make a cell healthy, and cells outside the exact administrative polygon are
reported separately instead of being mistaken for unsupported Moscow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd
from shapely.geometry import box

from ml.ingestion.common import read_json, write_json
from ml.ingestion.mapillary_citywide import load_aoi_boundary

SCHEMA_VERSION = 1
GRID_BINS = 20
DEFAULT_TRANCH_QUOTAS = {
    "empty_available": 6,
    "single_provider": 6,
    "low_density_or_diversity": 6,
    "healthy_control": 4,
}


class MoscowAcquisitionPlanError(RuntimeError):
    """The plan inputs are missing, incompatible, or malformed."""


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if bool(value != value):
            return ""
    except (TypeError, ValueError):
        return ""
    return str(value).strip()


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stable_score(*values: object) -> str:
    return hashlib.sha256("\0".join(str(value) for value in values).encode("utf-8")).hexdigest()


def _cell_index(lat: float, lon: float, bounds: Sequence[float]) -> tuple[int, int]:
    west, south, east, north = (float(value) for value in bounds)
    x = min(GRID_BINS - 1, max(0, int((lon - west) / (east - west) * GRID_BINS)))
    y = min(GRID_BINS - 1, max(0, int((lat - south) / (north - south) * GRID_BINS)))
    return x, y


def _cell_bounds(x: int, y: int, bounds: Sequence[float]) -> tuple[float, float, float, float]:
    west, south, east, north = (float(value) for value in bounds)
    width = (east - west) / GRID_BINS
    height = (north - south) / GRID_BINS
    return west + x * width, south + y * height, west + (x + 1) * width, south + (y + 1) * height


def _heading_bins(rows: Sequence[Mapping[str, Any]]) -> int:
    bins = {
        int(value % 360 // 45)
        for row in rows
        if (value := _finite_float(row.get("heading"))) is not None
    }
    return len(bins)


def _sequence_ids(rows: Sequence[Mapping[str, Any]], source: str | None = None) -> list[str]:
    values = {
        _text(row.get("sequence_id"))
        for row in rows
        if (source is None or _text(row.get("source")).lower() == source) and _text(row.get("sequence_id"))
    }
    return sorted(values)


def _load_json_rows(paths: Sequence[Path], source: str) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in paths:
        payload = read_json(path, default=[])
        if not isinstance(payload, list):
            raise MoscowAcquisitionPlanError(f"source availability input must be an array: {path}")
        for value in payload:
            if not isinstance(value, Mapping):
                continue
            identity = _text(value.get("source_image_id") or value.get("id"))
            lat = _finite_float(value.get("lat"))
            lon = _finite_float(value.get("lon"))
            if not identity or lat is None or lon is None:
                continue
            result[f"{source}:{identity}"] = dict(value) | {"source": source, "lat": lat, "lon": lon}
    return list(result.values())


def _dataframe_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_parquet(path).to_dict(orient="records")


def _historical_retrieval_by_cell(
    calibration_manifest: Path | None,
    per_query_jsonl: Path | None,
    bounds: Sequence[float],
) -> dict[tuple[int, int], dict[str, Any]]:
    if calibration_manifest is None or per_query_jsonl is None:
        return {}
    queries = {_text(row["id"]): row for row in _dataframe_rows(calibration_manifest)}
    values: dict[tuple[int, int], list[int]] = defaultdict(list)
    recalled: Counter[tuple[int, int]] = Counter()
    with per_query_jsonl.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            query = queries.get(_text(row.get("query_id")))
            if query is None:
                continue
            cell = _cell_index(float(query["lat"]), float(query["lon"]), bounds)
            rank = int(row["full_rank_retrieval"]["geo"]["100"]["first_correct_rank"])
            values[cell].append(rank)
            if rank <= 10:
                recalled[cell] += 1
    return {
        cell: {
            "query_count": len(ranks),
            "recall_at_10_within_100m": recalled[cell] / len(ranks),
            "median_nearest_positive_rank_within_100m": float(median(ranks)),
        }
        for cell, ranks in values.items()
    }


def _source_availability(mapillary_count: int, kartaview_count: int) -> str:
    if mapillary_count and kartaview_count:
        return "both_available"
    if mapillary_count:
        return "mapillary_available"
    if kartaview_count:
        return "kartaview_available"
    return "no_source_observed"


def _need_category(
    *,
    intersects_aoi: bool,
    gallery_count: int,
    gallery_provider_count: int,
    gallery_sequence_count: int,
    gallery_heading_bins: int,
    availability_count: int,
) -> str:
    if not intersects_aoi:
        return "outside_exact_aoi"
    if availability_count == 0:
        return "unsupported_no_source_observed"
    if gallery_count == 0:
        return "empty_available"
    if gallery_count < 10 or gallery_sequence_count < 2 or gallery_heading_bins < 2:
        return "low_density_or_diversity"
    if gallery_provider_count == 1:
        return "single_provider"
    return "healthy_control"


def build_plan(
    *,
    gallery_rows: Sequence[Mapping[str, Any]],
    mapillary_rows: Sequence[Mapping[str, Any]],
    kartaview_rows: Sequence[Mapping[str, Any]],
    aoi_geometry: Any,
    aoi_sha256: str,
    bounds: Sequence[float],
    historical_retrieval: Mapping[tuple[int, int], Mapping[str, Any]] | None = None,
    input_fingerprints: Mapping[str, str] | None = None,
    tranche_quotas: Mapping[str, int] = DEFAULT_TRANCH_QUOTAS,
) -> dict[str, Any]:
    gallery_by_cell: dict[tuple[int, int], list[Mapping[str, Any]]] = defaultdict(list)
    mapillary_by_cell: dict[tuple[int, int], list[Mapping[str, Any]]] = defaultdict(list)
    kartaview_by_cell: dict[tuple[int, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in gallery_rows:
        gallery_by_cell[_cell_index(float(row["lat"]), float(row["lon"]), bounds)].append(row)
    for row in mapillary_rows:
        mapillary_by_cell[_cell_index(float(row["lat"]), float(row["lon"]), bounds)].append(row)
    for row in kartaview_rows:
        kartaview_by_cell[_cell_index(float(row["lat"]), float(row["lon"]), bounds)].append(row)

    cells: list[dict[str, Any]] = []
    for y in range(GRID_BINS):
        for x in range(GRID_BINS):
            cell_bounds = _cell_bounds(x, y, bounds)
            cell_geometry = box(*cell_bounds)
            intersection = aoi_geometry.intersection(cell_geometry)
            intersects = not intersection.is_empty and intersection.area > 0
            aoi_fraction = intersection.area / cell_geometry.area if intersects else 0.0
            gallery = gallery_by_cell[(x, y)]
            mapillary = mapillary_by_cell[(x, y)]
            kartaview = kartaview_by_cell[(x, y)]
            gallery_providers = sorted({_text(row.get("source")).lower() for row in gallery if _text(row.get("source"))})
            gallery_sequences = {
                f"{_text(row.get('source')).lower()}:{_text(row.get('sequence_id'))}"
                for row in gallery
                if _text(row.get("source")) and _text(row.get("sequence_id"))
            }
            heading_bins = _heading_bins(gallery)
            availability_count = len(mapillary) + len(kartaview)
            category = _need_category(
                intersects_aoi=intersects,
                gallery_count=len(gallery),
                gallery_provider_count=len(gallery_providers),
                gallery_sequence_count=len(gallery_sequences),
                gallery_heading_bins=heading_bins,
                availability_count=availability_count,
            )
            availability_sequences = len(_sequence_ids(mapillary)) + len(_sequence_ids(kartaview))
            many_images_low_diversity = availability_count >= 20 and (
                availability_sequences < 2 or _heading_bins([*mapillary, *kartaview]) < 2
            )
            historical = dict((historical_retrieval or {}).get((x, y), {}))
            poor_rank = float(historical.get("median_nearest_positive_rank_within_100m", 0.0) or 0.0)
            priority_score = (
                {"empty_available": 4, "single_provider": 3, "low_density_or_diversity": 2, "healthy_control": 1}.get(
                    category, 0
                )
                + min(poor_rank / 1000.0, 3.0)
                + (1.0 if many_images_low_diversity else 0.0)
                + min(availability_sequences / 20.0, 1.0)
            )
            cells.append(
                {
                    "x": x,
                    "y": y,
                    "bounds_west_south_east_north": list(cell_bounds),
                    "aoi_intersection_fraction": round(aoi_fraction, 8),
                    "need_category": category,
                    "source_availability": _source_availability(len(mapillary), len(kartaview)),
                    "gallery": {
                        "references": len(gallery),
                        "providers": gallery_providers,
                        "provider_count": len(gallery_providers),
                        "provider_sequence_count": len(gallery_sequences),
                        "heading_bin_count_45deg": heading_bins,
                    },
                    "available": {
                        "mapillary_representatives": len(mapillary),
                        "mapillary_sequences": len(_sequence_ids(mapillary)),
                        "kartaview_frames": len(kartaview),
                        "kartaview_sequences": len(_sequence_ids(kartaview)),
                        "heading_bin_count_45deg": _heading_bins([*mapillary, *kartaview]),
                        "many_images_but_low_visual_proxy_diversity": many_images_low_diversity,
                    },
                    "historical_v1_calibration_retrieval": historical or None,
                    "priority_score": round(priority_score, 6),
                    "mapillary_sequence_ids": sorted(
                        _sequence_ids(mapillary), key=lambda value: (_stable_score("mapillary", x, y, value), value)
                    )[:12],
                    "kartaview_sequence_ids": sorted(
                        _sequence_ids(kartaview), key=lambda value: (_stable_score("kartaview", x, y, value), value)
                    )[:12],
                }
            )

    tranche_cells: list[dict[str, Any]] = []
    for category, limit in tranche_quotas.items():
        candidates = [cell for cell in cells if cell["need_category"] == category]
        candidates.sort(
            key=lambda cell: (
                -float(cell["priority_score"]),
                _stable_score("tranche", category, cell["x"], cell["y"]),
            )
        )
        tranche_cells.extend(candidates[: int(limit)])
    category_counts = Counter(cell["need_category"] for cell in cells)
    availability_counts = Counter(cell["source_availability"] for cell in cells if cell["need_category"] != "outside_exact_aoi")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "moscow_real_v2",
        "purpose": "targeted acquisition before any v2 retrieval tuning",
        "exact_aoi_sha256": aoi_sha256,
        "grid": {"bins_per_axis": GRID_BINS, "bounds_west_south_east_north": list(bounds)},
        "input_fingerprints": dict(sorted((input_fingerprints or {}).items())),
        "rules": {
            "healthy": ">=10 gallery refs, >=2 provider-scoped sequences, >=2 providers, >=2 heading bins",
            "availability_is_not_coverage": True,
            "historical_retrieval_scope": "moscow_real_v1 calibration only; never v1 frozen test outcomes",
        },
        "summary": {
            "cell_categories": dict(sorted(category_counts.items())),
            "source_availability_inside_or_intersecting_aoi": dict(sorted(availability_counts.items())),
            "tranche_cells": len(tranche_cells),
            "tranche_category_counts": dict(sorted(Counter(cell["need_category"] for cell in tranche_cells).items())),
        },
        "tranche": {
            "status": "planned",
            "selection": "deterministic category quotas, poor historical calibration rank, and source diversity",
            "cells": tranche_cells,
        },
        "cells": cells,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    payload["plan_fingerprint_sha256"] = hashlib.sha256(canonical).hexdigest()
    return payload


def run(
    *,
    gallery_manifest: Path,
    mapillary_jsons: Sequence[Path],
    kartaview_jsons: Sequence[Path],
    aoi_geojson: Path,
    output_json: Path,
    historical_calibration_manifest: Path | None = None,
    historical_per_query_jsonl: Path | None = None,
) -> dict[str, Any]:
    boundary = load_aoi_boundary(aoi_geojson)
    bounds = boundary.bounds.to_list()
    input_paths = [gallery_manifest, *mapillary_jsons, *kartaview_jsons, aoi_geojson]
    if historical_calibration_manifest:
        input_paths.append(historical_calibration_manifest)
    if historical_per_query_jsonl:
        input_paths.append(historical_per_query_jsonl)
    plan = build_plan(
        gallery_rows=_dataframe_rows(gallery_manifest),
        mapillary_rows=_load_json_rows(mapillary_jsons, "mapillary"),
        kartaview_rows=_load_json_rows(kartaview_jsons, "kartaview"),
        aoi_geometry=boundary.geometry,
        aoi_sha256=boundary.sha256,
        bounds=bounds,
        historical_retrieval=_historical_retrieval_by_cell(
            historical_calibration_manifest, historical_per_query_jsonl, bounds
        ),
        input_fingerprints={str(path): _sha256(path) for path in input_paths},
    )
    write_json(output_json, plan)
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Moscow v2 targeted acquisition plan")
    parser.add_argument("--gallery-manifest", type=Path, required=True)
    parser.add_argument("--mapillary-json", type=Path, action="append", default=[])
    parser.add_argument("--kartaview-json", type=Path, action="append", default=[])
    parser.add_argument("--aoi-geojson", type=Path, required=True)
    parser.add_argument("--historical-calibration-manifest", type=Path)
    parser.add_argument("--historical-per-query-jsonl", type=Path)
    parser.add_argument("--output-json", type=Path, required=True)
    args = parser.parse_args()
    plan = run(
        gallery_manifest=args.gallery_manifest,
        mapillary_jsons=args.mapillary_json,
        kartaview_jsons=args.kartaview_json,
        aoi_geojson=args.aoi_geojson,
        output_json=args.output_json,
        historical_calibration_manifest=args.historical_calibration_manifest,
        historical_per_query_jsonl=args.historical_per_query_jsonl,
    )
    print(json.dumps(plan["summary"], ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
