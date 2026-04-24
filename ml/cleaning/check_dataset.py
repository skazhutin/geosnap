"""Stage 3.7: sanity checks and quick visualizations for processed dataset."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw, ImageOps

MOSCOW_BOUNDS = {
    "min_lat": 55.55,
    "max_lat": 55.95,
    "min_lon": 37.30,
    "max_lon": 37.90,
}
EARTH_RADIUS_M = 6_371_000


def in_moscow(lat: float, lon: float) -> bool:
    return MOSCOW_BOUNDS["min_lat"] <= lat <= MOSCOW_BOUNDS["max_lat"] and MOSCOW_BOUNDS["min_lon"] <= lon <= MOSCOW_BOUNDS["max_lon"]


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(d_lon / 2.0) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def estimate_mean_nearest_distance(df: pd.DataFrame, sample_limit: int = 1200) -> float:
    if len(df) < 2:
        return 0.0

    sample = df[["lat", "lon"]]
    if len(sample) > sample_limit:
        sample = sample.sample(n=sample_limit, random_state=42)

    points = list(sample.itertuples(index=False, name=None))
    nearest_distances: list[float] = []
    for i, (lat_i, lon_i) in enumerate(points):
        best = float("inf")
        for j, (lat_j, lon_j) in enumerate(points):
            if i == j:
                continue
            dist = haversine_distance_m(float(lat_i), float(lon_i), float(lat_j), float(lon_j))
            if dist < best:
                best = dist
        nearest_distances.append(best)

    return float(sum(nearest_distances) / max(len(nearest_distances), 1))


def build_contact_sheet(df: pd.DataFrame, output_path: Path, sample_size: int = 20, thumb_size: tuple[int, int] = (256, 256)) -> None:
    sample = df.sample(n=min(sample_size, len(df)), random_state=42)

    cols = 5
    rows = (len(sample) + cols - 1) // cols
    canvas = Image.new("RGB", (cols * thumb_size[0], rows * thumb_size[1]), color=(20, 20, 20))

    for idx, (_, row) in enumerate(sample.iterrows()):
        x = (idx % cols) * thumb_size[0]
        y = (idx // cols) * thumb_size[1]
        path = Path(str(row["image_path"]))
        with Image.open(path) as img:
            thumb = ImageOps.fit(img.convert("RGB"), thumb_size)
        draw = ImageDraw.Draw(thumb)
        draw.rectangle((0, thumb_size[1] - 28, thumb_size[0], thumb_size[1]), fill=(0, 0, 0))
        draw.text((8, thumb_size[1] - 22), str(row["source"]), fill=(255, 255, 255))
        canvas.paste(thumb, (x, y))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path)


def build_scatter(df: pd.DataFrame, output_path: Path) -> str:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return "matplotlib_not_installed"

    plt.figure(figsize=(8, 8))
    plt.scatter(df["lon"], df["lat"], s=1, alpha=0.35)
    plt.title("Dataset geo spread")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(alpha=0.2)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()
    return "ok"


def run(manifest_path: Path, report_path: Path, scatter_path: Path, preview_path: Path) -> None:
    df = pd.read_parquet(manifest_path)

    inside_mask = df.apply(lambda r: in_moscow(float(r["lat"]), float(r["lon"])), axis=1)
    in_bounds_df = df[inside_mask]

    lat_bins = pd.cut(df["lat"], bins=6)
    lon_bins = pd.cut(df["lon"], bins=6)
    grid = df.groupby([lat_bins, lon_bins], observed=False).size().reset_index(name="count")
    empty_cells = int((grid["count"] == 0).sum())

    center_mask = (df["lat"].between(55.70, 55.80)) & (df["lon"].between(37.55, 37.70))
    center_ratio = float(center_mask.mean()) if len(df) else 0.0

    mean_nearest_distance_m = estimate_mean_nearest_distance(df)

    h3_distribution: dict[str, dict[str, int]] = {}
    for col in ("h3_coarse", "h3_fine"):
        if col in df.columns:
            vc = df[col].value_counts()
            h3_distribution[col] = {str(k): int(v) for k, v in vc.items()}

    scatter_status = build_scatter(df, scatter_path)
    build_contact_sheet(df, preview_path)

    summary = {
        "rows": int(len(df)),
        "total_images": int(len(df)),
        "inside_moscow_ratio": float(len(in_bounds_df) / max(len(df), 1)),
        "empty_grid_cells": empty_cells,
        "center_ratio": center_ratio,
        "mean_nearest_distance_m": mean_nearest_distance_m,
        "h3_distribution": h3_distribution,
        "scatter_status": scatter_status,
        "scatter_path": str(scatter_path),
        "preview_path": str(preview_path),
    }

    if 10.0 <= mean_nearest_distance_m <= 30.0:
        print("Dataset density looks OK")
    else:
        print("WARNING: Dataset density is outside expected range (10-30 m)")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Sanity checks for processed dataset")
    parser.add_argument("--manifest", default="data/processed/manifest_clean.parquet")
    parser.add_argument("--report", default="data/processed/reports/dataset_sanity.json")
    parser.add_argument("--scatter", default="data/processed/reports/dataset_scatter.png")
    parser.add_argument("--preview", default="data/processed/reports/dataset_preview_20.jpg")
    args = parser.parse_args()

    run(
        manifest_path=Path(args.manifest),
        report_path=Path(args.report),
        scatter_path=Path(args.scatter),
        preview_path=Path(args.preview),
    )


if __name__ == "__main__":
    main()
