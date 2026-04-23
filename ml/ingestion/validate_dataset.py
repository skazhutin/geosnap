"""Quick dataset validator for unified ingestion manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from PIL import Image


MOSCOW_BOUNDS = {
    "min_lat": 55.55,
    "max_lat": 55.95,
    "min_lon": 37.30,
    "max_lon": 37.90,
}


def is_valid_coord(lat: float, lon: float) -> bool:
    return MOSCOW_BOUNDS["min_lat"] <= lat <= MOSCOW_BOUNDS["max_lat"] and MOSCOW_BOUNDS["min_lon"] <= lon <= MOSCOW_BOUNDS["max_lon"]


def run(manifest_path: Path, min_size_bytes: int, report_path: Path) -> None:
    df = pd.read_parquet(manifest_path)

    issues: dict[str, list[str]] = {
        "missing_fields": [],
        "invalid_coords": [],
        "missing_images": [],
        "broken_images": [],
        "too_small_images": [],
    }

    required = ["id", "source", "source_image_id", "lat", "lon", "captured_at", "image_path"]
    for col in required:
        if col not in df.columns:
            issues["missing_fields"].append(f"missing_column:{col}")

    for _, row in df.iterrows():
        row_id = str(row.get("id"))
        lat = row.get("lat")
        lon = row.get("lon")
        if pd.isna(lat) or pd.isna(lon) or not is_valid_coord(float(lat), float(lon)):
            issues["invalid_coords"].append(row_id)

        path = Path(str(row.get("image_path")))
        if not path.exists():
            issues["missing_images"].append(row_id)
            continue

        if path.stat().st_size < min_size_bytes:
            issues["too_small_images"].append(row_id)

        try:
            with Image.open(path) as img:
                img.verify()
        except Exception:  # noqa: BLE001
            issues["broken_images"].append(row_id)

    summary = {
        "rows": len(df),
        "missing_fields": len(issues["missing_fields"]),
        "invalid_coords": len(issues["invalid_coords"]),
        "missing_images": len(issues["missing_images"]),
        "broken_images": len(issues["broken_images"]),
        "too_small_images": len(issues["too_small_images"]),
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as fp:
        json.dump({"summary": summary, "issues": issues}, fp, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate downloaded dataset")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--min-size-bytes", type=int, default=10_000)
    parser.add_argument("--report", default="data/raw/validation_report.json")
    args = parser.parse_args()

    run(Path(args.manifest), min_size_bytes=args.min_size_bytes, report_path=Path(args.report))


if __name__ == "__main__":
    main()
