"""Conservative hard validation before quality scoring."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from ml.cleaning.reporting import update_cleaning_report
from ml.ingestion.common import validate_image_file, write_json
from ml.ingestion.mapillary_citywide import AoiBoundary, load_aoi_boundary
from ml.ingestion.schema import MOSCOW_BOUNDS, read_manifest, write_manifest


def _valid_coordinate(
    city_id: str,
    lat: Any,
    lon: Any,
    *,
    aoi_boundary: AoiBoundary | None = None,
) -> bool:
    try:
        latitude = float(lat)
        longitude = float(lon)
    except (TypeError, ValueError):
        return False
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return False
    if aoi_boundary is not None:
        return aoi_boundary.covers(longitude, latitude)
    if city_id == "moscow":
        min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
        return min_lat <= latitude <= max_lat and min_lon <= longitude <= max_lon
    return True


def is_valid_image(path: Path) -> bool:
    return validate_image_file(path).valid


def run(
    input_manifest: Path,
    output_manifest: Path,
    report_path: Path,
    min_size_bytes: int,
    pipeline_report_path: Path,
    *,
    min_width: int = 64,
    min_height: int = 64,
    aoi_geojson: Path | None = None,
) -> dict[str, Any]:
    if min_size_bytes < 0:
        raise ValueError("min_size_bytes must be >= 0")
    if min_width < 1 or min_height < 1:
        raise ValueError("min_width and min_height must be >= 1")
    df = read_manifest(input_manifest, allow_empty=True)
    aoi_boundary = load_aoi_boundary(aoi_geojson) if aoi_geojson is not None else None
    if "width" not in df.columns:
        df["width"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    if "height" not in df.columns:
        df["height"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    keep_indices: list[int] = []
    issues: dict[str, list[str]] = {
        "invalid_coordinate": [],
        "missing_file": [],
        "too_small_file": [],
        "decode_failed": [],
        "too_small_dimensions": [],
        "unsupported_image_format": [],
    }
    for index, row in df.iterrows():
        row_id = str(row.get("id") or "")
        if not _valid_coordinate(
            str(row.get("city_id") or ""),
            row.get("lat"),
            row.get("lon"),
            aoi_boundary=aoi_boundary,
        ):
            issues["invalid_coordinate"].append(row_id)
            continue
        validation = validate_image_file(
            Path(str(row.get("image_path") or "")),
            min_valid_size_bytes=min_size_bytes,
            min_width=min_width,
            min_height=min_height,
        )
        if not validation.valid:
            reason = validation.reason or "decode_failed"
            issues.setdefault(reason, []).append(row_id)
            continue
        df.at[index, "width"] = validation.width
        df.at[index, "height"] = validation.height
        keep_indices.append(index)

    cleaned = df.loc[keep_indices].copy()
    write_manifest(cleaned, output_manifest, allow_empty=True)
    summary: dict[str, Any] = {
        "input_rows": int(len(df)),
        "output_rows": int(len(cleaned)),
        "dropped_rows": int(len(df) - len(cleaned)),
        "coordinate_scope": "exact_aoi_polygon" if aoi_boundary is not None else "legacy_city_bounds",
        "aoi_sha256": aoi_boundary.sha256 if aoi_boundary is not None else None,
        **{key: len(value) for key, value in issues.items()},
    }
    write_json(report_path, {"summary": summary, "issues": issues})
    report_path.with_suffix(".md").write_text(
        "# Basic cleaning\n\n" + "\n".join(f"- {key}: {value}" for key, value in summary.items()) + "\n",
        encoding="utf-8",
    )
    update_cleaning_report(
        report_path=pipeline_report_path,
        before_clean=len(df),
        after_clean=len(cleaned),
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Conservative validation for raw geotagged images")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step1.parquet")
    parser.add_argument("--report", default="data/processed/reports/cleaning_step1.json")
    parser.add_argument("--min-size-bytes", type=int, default=1)
    parser.add_argument("--min-width", type=int, default=64)
    parser.add_argument("--min-height", type=int, default=64)
    parser.add_argument("--pipeline-report", default="data/processed/cleaning_report.json")
    parser.add_argument("--aoi-geojson", type=Path, help="optional exact Polygon/MultiPolygon coordinate gate")
    args = parser.parse_args()
    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        report_path=Path(args.report),
        min_size_bytes=args.min_size_bytes,
        min_width=args.min_width,
        min_height=args.min_height,
        pipeline_report_path=Path(args.pipeline_report),
        aoi_geojson=args.aoi_geojson,
    )


if __name__ == "__main__":
    main()
