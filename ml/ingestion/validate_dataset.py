"""Validate canonical schema, geography, attribution and downloaded images."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from ml.ingestion.common import validate_image_file, write_json
from ml.ingestion.schema import MOSCOW_BOUNDS, coerce_manifest_schema, validate_manifest_schema


class DatasetValidationError(RuntimeError):
    pass


def is_valid_coord(lat: float, lon: float, city_id: str = "moscow") -> bool:
    if city_id == "moscow":
        min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
        return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
    return -90 <= lat <= 90 and -180 <= lon <= 180


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Dataset validation", ""]
    lines.extend(f"- {key.replace('_', ' ').title()}: {value}" for key, value in summary.items())
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    manifest_path: Path,
    min_size_bytes: int,
    report_path: Path,
    *,
    min_width: int = 64,
    min_height: int = 64,
    markdown_path: Path | None = None,
    fail_on_error: bool = False,
) -> dict[str, Any]:
    if min_size_bytes < 0:
        raise ValueError("min_size_bytes must be >= 0")
    if min_width < 1 or min_height < 1:
        raise ValueError("min_width and min_height must be >= 1")
    raw_df = pd.read_parquet(manifest_path)
    schema_errors = validate_manifest_schema(
        raw_df,
        allow_empty=True,
        strict_reference=True,
    )
    df = coerce_manifest_schema(raw_df)
    issues: dict[str, list[str]] = {
        "schema": schema_errors,
        "invalid_coords": [],
        "missing_attribution": [],
        "missing_images": [],
        "invalid_images": [],
    }

    for row in df.to_dict(orient="records"):
        row_id = str(row.get("id") or "")
        try:
            valid_coord = is_valid_coord(float(row["lat"]), float(row["lon"]), str(row.get("city_id") or ""))
        except (TypeError, ValueError, KeyError):
            valid_coord = False
        if not valid_coord:
            issues["invalid_coords"].append(row_id)
        if not str(row.get("license") or "").strip() or not str(row.get("attribution") or "").strip():
            issues["missing_attribution"].append(row_id)

        path = Path(str(row.get("image_path") or ""))
        validation = validate_image_file(
            path,
            min_valid_size_bytes=min_size_bytes,
            min_width=min_width,
            min_height=min_height,
        )
        if validation.reason == "missing_file":
            issues["missing_images"].append(row_id)
        elif not validation.valid:
            issues["invalid_images"].append(row_id)

    source_counts = Counter(str(value) for value in df.get("source", pd.Series(dtype="string")).dropna())
    summary: dict[str, Any] = {
        "rows": int(len(df)),
        "empty_manifest": len(df) == 0,
        "schema_errors": len(issues["schema"]),
        "invalid_coords": len(issues["invalid_coords"]),
        "missing_attribution": len(issues["missing_attribution"]),
        "missing_images": len(issues["missing_images"]),
        "invalid_images": len(issues["invalid_images"]),
        "sources": dict(sorted(source_counts.items())),
    }
    summary["valid"] = not summary["empty_manifest"] and not any(
        summary[key]
        for key in ("schema_errors", "invalid_coords", "missing_attribution", "missing_images", "invalid_images")
    )
    write_json(report_path, {"summary": summary, "issues": issues})
    _write_markdown(markdown_path or report_path.with_suffix(".md"), summary)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if fail_on_error and not summary["valid"]:
        raise DatasetValidationError(f"dataset validation failed; see {report_path}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate downloaded canonical dataset")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--min-size-bytes", type=int, default=10_000)
    parser.add_argument("--min-width", type=int, default=64)
    parser.add_argument("--min-height", type=int, default=64)
    parser.add_argument("--report", default="data/raw/validation_report.json")
    parser.add_argument("--markdown")
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    run(
        Path(args.manifest),
        min_size_bytes=args.min_size_bytes,
        report_path=Path(args.report),
        min_width=args.min_width,
        min_height=args.min_height,
        markdown_path=Path(args.markdown) if args.markdown else None,
        fail_on_error=args.fail_on_error,
    )


if __name__ == "__main__":
    main()
