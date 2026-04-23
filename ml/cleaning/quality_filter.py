"""Stage 3.2: image quality filtering (before deduplication)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image

from ml.cleaning.reporting import update_cleaning_report


QUALITY_WEIGHTS = {
    "blur": 0.6,
    "resolution": 0.3,
    "brightness": 0.1,
}


def variance_of_laplacian(gray: np.ndarray) -> float:
    """Discrete Laplacian variance without OpenCV dependency."""
    padded = np.pad(gray.astype(np.float32), 1, mode="edge")
    lap = (
        -4.0 * padded[1:-1, 1:-1]
        + padded[:-2, 1:-1]
        + padded[2:, 1:-1]
        + padded[1:-1, :-2]
        + padded[1:-1, 2:]
    )
    return float(lap.var())


def image_quality_metrics(path: Path) -> tuple[int, int, float, float]:
    with Image.open(path) as img:
        rgb = img.convert("RGB")
        width, height = rgb.size
        gray = np.asarray(rgb.convert("L"), dtype=np.float32)
    blur = variance_of_laplacian(gray)
    brightness = float(gray.mean())
    return width, height, blur, brightness


def _resolution_score(width: int, height: int, min_width: int, min_height: int) -> float:
    ratio = min(width, height) / max(float(min(min_width, min_height)), 1.0)
    return float(np.clip(np.log2(max(ratio, 1.0)) / 2.0, 0.0, 1.0))


def _brightness_score(brightness: float) -> float:
    # Soft penalty only. No hard drop by brightness.
    return float(np.clip(1.0 - abs(brightness - 127.5) / 127.5, 0.0, 1.0))


def _blur_score(blur: float, min_blur_score: float) -> float:
    return float(np.clip(blur / max(min_blur_score * 3.0, 1.0), 0.0, 1.0))


def run(
    input_manifest: Path,
    output_manifest: Path,
    report_path: Path,
    min_width: int,
    min_height: int,
    min_blur_score: float,
    pipeline_report_path: Path,
) -> None:
    df = pd.read_parquet(input_manifest)

    kept_rows: list[dict] = []
    dropped = {"small": [], "blur": []}

    for _, row in df.iterrows():
        row_id = str(row["id"])
        path = Path(str(row["image_path"]))
        width, height, blur, brightness = image_quality_metrics(path)

        if width < min_width or height < min_height:
            dropped["small"].append(row_id)
            continue
        if blur < min_blur_score:
            dropped["blur"].append(row_id)
            continue

        blur_norm = _blur_score(blur, min_blur_score)
        brightness_norm = _brightness_score(brightness)
        resolution_norm = _resolution_score(width, height, min_width, min_height)

        quality_score = (
            QUALITY_WEIGHTS["blur"] * blur_norm
            + QUALITY_WEIGHTS["resolution"] * resolution_norm
            + QUALITY_WEIGHTS["brightness"] * brightness_norm
        )

        row_dict = row.to_dict()
        row_dict["quality_score"] = float(np.clip(quality_score, 0.0, 1.0))
        row_dict["blur_score"] = float(blur)
        row_dict["brightness"] = float(brightness)
        kept_rows.append(row_dict)

    result_columns = list(dict.fromkeys([*df.columns, "quality_score", "blur_score", "brightness"]))
    result = pd.DataFrame(kept_rows, columns=result_columns)
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_manifest, index=False)

    summary = {
        "formula": "0.6*blur_norm + 0.3*resolution_norm + 0.1*brightness_norm",
        "input_rows": int(len(df)),
        "output_rows": int(len(result)),
        "dropped_small": int(len(dropped["small"])),
        "dropped_blur": int(len(dropped["blur"])),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"summary": summary, "dropped": dropped}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    update_cleaning_report(
        report_path=pipeline_report_path,
        after_quality=len(result),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter low-quality images and compute quality_score")
    parser.add_argument("--manifest", default="data/processed/manifest_step1.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step2.parquet")
    parser.add_argument("--report", default="data/processed/reports/quality_step2.json")
    parser.add_argument("--min-width", type=int, default=224)
    parser.add_argument("--min-height", type=int, default=224)
    parser.add_argument("--min-blur-score", type=float, default=75.0)
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
