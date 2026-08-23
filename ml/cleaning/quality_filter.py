"""Vectorized, conservative image-quality scoring."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from PIL import Image

from ml.cleaning.reporting import update_cleaning_report
from ml.ingestion.common import write_json
from ml.ingestion.schema import read_manifest, write_manifest

QUALITY_WEIGHTS = {"sharpness": 0.5, "resolution": 0.3, "exposure": 0.2}


def variance_of_laplacian(gray: np.ndarray) -> float:
    if gray.ndim != 2 or gray.size == 0:
        raise ValueError("gray image must be a non-empty 2D array")
    padded = np.pad(gray.astype(np.float32, copy=False), 1, mode="edge")
    laplacian = -4.0 * padded[1:-1, 1:-1] + padded[:-2, 1:-1] + padded[2:, 1:-1] + padded[1:-1, :-2] + padded[1:-1, 2:]
    return float(laplacian.var())


def image_quality_metrics(path: Path) -> tuple[int, int, float, float, float]:
    with Image.open(path) as image:
        gray = np.asarray(image.convert("L"), dtype=np.float32)
        width, height = image.size
    sharpness = variance_of_laplacian(gray)
    brightness = float(gray.mean())
    # Fraction not crushed into near-black or near-white; analysis-only signal.
    exposure_score = float(np.mean((gray >= 8.0) & (gray <= 247.0)))
    return width, height, sharpness, brightness, exposure_score


def _resolution_score(width: int, height: int, min_width: int, min_height: int) -> float:
    ratio = min(width / float(min_width), height / float(min_height))
    return float(np.clip(np.log2(max(ratio, 1.0)) / 2.0, 0.0, 1.0))


def _sharpness_score(sharpness: float) -> float:
    # Soft saturation; hard blur rejection is opt-in through min_blur_score.
    return float(np.clip(np.log1p(max(sharpness, 0.0)) / np.log1p(500.0), 0.0, 1.0))


def run(
    input_manifest: Path,
    output_manifest: Path,
    report_path: Path,
    min_width: int,
    min_height: int,
    min_blur_score: float,
    pipeline_report_path: Path,
) -> dict[str, Any]:
    if min_width < 1 or min_height < 1:
        raise ValueError("min_width and min_height must be >= 1")
    if min_blur_score < 0:
        raise ValueError("min_blur_score must be >= 0")
    df = read_manifest(input_manifest, allow_empty=True)
    for column, dtype in (
        ("width", "Int64"),
        ("height", "Int64"),
        ("blur_score", "float64"),
        ("brightness", "float64"),
        ("exposure_score", "float64"),
    ):
        if column not in df.columns:
            df[column] = pd.Series(index=df.index, dtype=dtype)

    kept: list[int] = []
    dropped: dict[str, list[str]] = {"small": [], "hard_blur": [], "metrics_error": []}
    for index, row in df.iterrows():
        row_id = str(row.get("id") or "")
        try:
            width, height, sharpness, brightness, exposure = image_quality_metrics(Path(str(row["image_path"])))
        except (OSError, ValueError):
            dropped["metrics_error"].append(row_id)
            continue
        if width < min_width or height < min_height:
            dropped["small"].append(row_id)
            continue
        if min_blur_score > 0 and sharpness < min_blur_score:
            dropped["hard_blur"].append(row_id)
            continue

        resolution = _resolution_score(width, height, min_width, min_height)
        quality = (
            QUALITY_WEIGHTS["sharpness"] * _sharpness_score(sharpness)
            + QUALITY_WEIGHTS["resolution"] * resolution
            + QUALITY_WEIGHTS["exposure"] * exposure
        )
        df.at[index, "width"] = width
        df.at[index, "height"] = height
        df.at[index, "blur_score"] = sharpness
        df.at[index, "brightness"] = brightness
        df.at[index, "exposure_score"] = exposure
        df.at[index, "quality_score"] = float(np.clip(quality, 0.0, 1.0))
        kept.append(index)

    result = df.loc[kept].copy()
    write_manifest(result, output_manifest, allow_empty=True)
    summary: dict[str, Any] = {
        "formula": "0.5*sharpness_log_norm + 0.3*resolution_norm + 0.2*usable_exposure_fraction",
        "input_rows": int(len(df)),
        "output_rows": int(len(result)),
        "dropped_small": len(dropped["small"]),
        "dropped_hard_blur": len(dropped["hard_blur"]),
        "dropped_metrics_error": len(dropped["metrics_error"]),
        "hard_blur_threshold": min_blur_score,
    }
    write_json(report_path, {"summary": summary, "dropped": dropped})
    report_path.with_suffix(".md").write_text(
        "# Quality scoring\n\n" + "\n".join(f"- {key}: {value}" for key, value in summary.items()) + "\n",
        encoding="utf-8",
    )
    update_cleaning_report(report_path=pipeline_report_path, after_quality=len(result))
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Score image quality with conservative optional hard filters")
    parser.add_argument("--manifest", default="data/processed/manifest_step1.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step2.parquet")
    parser.add_argument("--report", default="data/processed/reports/quality_step2.json")
    parser.add_argument("--min-width", type=int, default=64)
    parser.add_argument("--min-height", type=int, default=64)
    parser.add_argument("--min-blur-score", type=float, default=0.0)
    parser.add_argument("--pipeline-report", default="data/processed/cleaning_report.json")
    args = parser.parse_args()
    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        report_path=Path(args.report),
        min_width=args.min_width,
        min_height=args.min_height,
        min_blur_score=args.min_blur_score,
        pipeline_report_path=Path(args.pipeline_report),
    )


if __name__ == "__main__":
    main()
