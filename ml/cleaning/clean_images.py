"""Stage 3.1: basic image cleaning for raw manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from PIL import Image

from ml.cleaning.reporting import update_cleaning_report


REQUIRED_COLUMNS = {"id", "image_path", "lat", "lon", "source"}


def is_valid_image(path: Path) -> bool:
    """Return True when image can be opened and verified."""
    try:
        with Image.open(path) as img:
            img.verify()
        return True
    except Exception:  # noqa: BLE001
        return False


def run(
    input_manifest: Path,
    output_manifest: Path,
    report_path: Path,
    min_size_bytes: int,
    pipeline_report_path: Path,
) -> None:
    df = pd.read_parquet(input_manifest)

    missing_columns = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing_columns:
        raise ValueError(f"Manifest missing required columns: {missing_columns}")

    records: list[dict] = []
    issues: dict[str, list[str]] = {
        "missing_file": [],
        "empty_file": [],
        "broken_image": [],
        "missing_image_path": [],
    }

    for _, row in df.iterrows():
        row_id = str(row.get("id"))
        image_path_raw = row.get("image_path")

        if pd.isna(image_path_raw) or str(image_path_raw).strip() == "":
            issues["missing_image_path"].append(row_id)
            continue

        image_path = Path(str(image_path_raw))
        if not image_path.exists():
            issues["missing_file"].append(row_id)
            continue

        if image_path.stat().st_size <= min_size_bytes:
            issues["empty_file"].append(row_id)
            continue

        if not is_valid_image(image_path):
            issues["broken_image"].append(row_id)
            continue

        records.append(row.to_dict())

    cleaned_df = pd.DataFrame(records)
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    cleaned_df.to_parquet(output_manifest, index=False)

    summary = {
        "input_rows": int(len(df)),
        "output_rows": int(len(cleaned_df)),
        "dropped_rows": int(len(df) - len(cleaned_df)),
        **{k: len(v) for k, v in issues.items()},
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"summary": summary, "issues": issues}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    update_cleaning_report(
        report_path=pipeline_report_path,
        before_clean=len(df),
        after_clean=len(cleaned_df),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Basic cleaning for raw geotagged images")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step1.parquet")
    parser.add_argument("--report", default="data/processed/reports/cleaning_step1.json")
    parser.add_argument("--min-size-bytes", type=int, default=0)
    parser.add_argument("--pipeline-report", default="data/processed/cleaning_report.json")
    args = parser.parse_args()

    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        report_path=Path(args.report),
        min_size_bytes=args.min_size_bytes,
        pipeline_report_path=Path(args.pipeline_report),
    )


if __name__ == "__main__":
    main()
