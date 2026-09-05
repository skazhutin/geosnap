"""Assign validated H3 cells for coverage analysis and spatial bucketing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import h3

from ml.ingestion.common import write_json
from ml.ingestion.schema import read_manifest, write_manifest


def validate_resolution(resolution: int) -> None:
    if not isinstance(resolution, int) or isinstance(resolution, bool) or not 0 <= resolution <= 15:
        raise ValueError("H3 resolution must be an integer between 0 and 15")


def to_h3(lat: float, lon: float, resolution: int) -> str:
    validate_resolution(resolution)
    if hasattr(h3, "latlng_to_cell"):
        return str(h3.latlng_to_cell(lat, lon, resolution))
    return str(h3.geo_to_h3(lat, lon, resolution))


def run(
    input_manifest: Path,
    output_manifest: Path,
    coarse_resolution: int,
    fine_resolution: int,
    *,
    report_path: Path | None = None,
) -> dict[str, Any]:
    validate_resolution(coarse_resolution)
    validate_resolution(fine_resolution)
    if coarse_resolution >= fine_resolution:
        raise ValueError("coarse_resolution must be less than fine_resolution")
    df = read_manifest(input_manifest, allow_empty=True)
    if len(df):
        df["h3_coarse"] = [
            to_h3(float(lat), float(lon), coarse_resolution) for lat, lon in zip(df["lat"], df["lon"], strict=True)
        ]
        df["h3_fine"] = [
            to_h3(float(lat), float(lon), fine_resolution) for lat, lon in zip(df["lat"], df["lon"], strict=True)
        ]
    else:
        df["h3_coarse"] = df["id"].astype("string")
        df["h3_fine"] = df["id"].astype("string")
    write_manifest(df, output_manifest, allow_empty=True)
    summary: dict[str, Any] = {
        "rows": len(df),
        "coarse_resolution": coarse_resolution,
        "fine_resolution": fine_resolution,
        "unique_h3_coarse": int(df["h3_coarse"].nunique()),
        "unique_h3_fine": int(df["h3_fine"].nunique()),
        "note": "H3 resolutions are index scales, not semantic district/street labels",
    }
    target = report_path or output_manifest.with_suffix(".h3.report.json")
    write_json(target, summary)
    target.with_suffix(".md").write_text(
        "# H3 assignment\n\n" + "\n".join(f"- {key}: {value}" for key, value in summary.items()) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Assign H3 cells to a canonical manifest")
    parser.add_argument("--manifest", default="data/processed/manifest_step3.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step4.parquet")
    parser.add_argument("--coarse-resolution", type=int, default=6)
    parser.add_argument("--fine-resolution", type=int, default=9)
    parser.add_argument("--report")
    args = parser.parse_args()
    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        coarse_resolution=args.coarse_resolution,
        fine_resolution=args.fine_resolution,
        report_path=Path(args.report) if args.report else None,
    )


if __name__ == "__main__":
    main()
