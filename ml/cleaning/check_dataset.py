"""Efficient coverage statistics, source comparison and visual QA artifacts."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageOps

from ml.ingestion.common import read_json, write_json
from ml.ingestion.schema import MOSCOW_BOUNDS, read_manifest

EARTH_RADIUS_M = 6_371_000.0


@dataclass
class _KDNode:
    index: int
    axis: int
    left: _KDNode | None = None
    right: _KDNode | None = None


def _build_kd(indices: list[int], points: np.ndarray, depth: int = 0) -> _KDNode | None:
    if not indices:
        return None
    axis = depth % 2
    indices.sort(key=lambda index: float(points[index, axis]))
    middle = len(indices) // 2
    return _KDNode(
        index=indices[middle],
        axis=axis,
        left=_build_kd(indices[:middle], points, depth + 1),
        right=_build_kd(indices[middle + 1 :], points, depth + 1),
    )


def _nearest_sq(
    node: _KDNode | None,
    points: np.ndarray,
    target: np.ndarray,
    target_index: int,
    best_sq: float,
) -> float:
    if node is None:
        return best_sq
    point = points[node.index]
    if node.index != target_index:
        distance_sq = float(np.dot(point - target, point - target))
        best_sq = min(best_sq, distance_sq)
    delta = float(target[node.axis] - point[node.axis])
    near, far = (node.left, node.right) if delta <= 0 else (node.right, node.left)
    best_sq = _nearest_sq(near, points, target, target_index, best_sq)
    if delta * delta < best_sq:
        best_sq = _nearest_sq(far, points, target, target_index, best_sq)
    return best_sq


def _metric_points(df: pd.DataFrame) -> np.ndarray:
    if len(df) == 0:
        return np.empty((0, 2), dtype=np.float64)
    lat = df["lat"].to_numpy(dtype=np.float64)
    lon = df["lon"].to_numpy(dtype=np.float64)
    reference_lat = math.radians(float(lat.mean()))
    return np.column_stack(
        (
            np.radians(lon) * EARTH_RADIUS_M * math.cos(reference_lat),
            np.radians(lat) * EARTH_RADIUS_M,
        )
    )


def nearest_reference_distances(df: pd.DataFrame) -> np.ndarray:
    if len(df) < 2:
        return np.empty(0, dtype=np.float64)
    points = _metric_points(df)
    tree = _build_kd(list(range(len(points))), points)
    distances = [
        math.sqrt(_nearest_sq(tree, points, points[index], index, float("inf"))) for index in range(len(points))
    ]
    return np.asarray(distances, dtype=np.float64)


def estimate_mean_nearest_distance(df: pd.DataFrame, sample_limit: int | None = None) -> float:
    # sample_limit is retained for compatibility; the KD-tree computes the full
    # dataset efficiently and avoids the old sampling bias.
    distances = nearest_reference_distances(df)
    return float(distances.mean()) if len(distances) else 0.0


def build_contact_sheet(
    df: pd.DataFrame,
    output_path: Path,
    sample_size: int = 20,
    thumb_size: tuple[int, int] = (256, 256),
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if len(df) == 0:
        canvas = Image.new("RGB", (thumb_size[0] * 2, thumb_size[1]), color=(20, 20, 20))
        ImageDraw.Draw(canvas).text((20, 20), "Empty manifest — no preview images", fill=(255, 255, 255))
        canvas.save(output_path)
        return
    sample = df.sample(n=min(sample_size, len(df)), random_state=42)
    columns = 5
    rows = max(1, math.ceil(len(sample) / columns))
    canvas = Image.new("RGB", (columns * thumb_size[0], rows * thumb_size[1]), color=(20, 20, 20))
    for position, (_, row) in enumerate(sample.iterrows()):
        x = (position % columns) * thumb_size[0]
        y = (position // columns) * thumb_size[1]
        try:
            with Image.open(Path(str(row["image_path"]))) as image:
                thumb = ImageOps.fit(image.convert("RGB"), thumb_size)
        except OSError:
            thumb = Image.new("RGB", thumb_size, color=(80, 20, 20))
        draw = ImageDraw.Draw(thumb)
        draw.rectangle((0, thumb_size[1] - 28, thumb_size[0], thumb_size[1]), fill=(0, 0, 0))
        draw.text((8, thumb_size[1] - 22), str(row["source"]), fill=(255, 255, 255))
        canvas.paste(thumb, (x, y))
    canvas.save(output_path)


def build_visualizations(
    df: pd.DataFrame,
    *,
    scatter_path: Path,
    density_path: Path,
    source_comparison_path: Path,
) -> str:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return "matplotlib_not_installed"

    for path in (scatter_path, density_path, source_comparison_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    sources = sorted(str(value) for value in df["source"].dropna().unique()) if len(df) else []

    figure, axis = plt.subplots(figsize=(8, 8))
    if len(df):
        for source in sources:
            subset = df[df["source"] == source]
            axis.scatter(subset["lon"], subset["lat"], s=2, alpha=0.45, label=source)
        axis.legend(markerscale=4)
    else:
        axis.text(0.5, 0.5, "No references", ha="center", va="center", transform=axis.transAxes)
    axis.set(title="Moscow reference coverage by source", xlabel="Longitude", ylabel="Latitude")
    axis.set_xlim(MOSCOW_BOUNDS[2], MOSCOW_BOUNDS[3])
    axis.set_ylim(MOSCOW_BOUNDS[0], MOSCOW_BOUNDS[1])
    axis.grid(alpha=0.2)
    figure.savefig(scatter_path, dpi=180, bbox_inches="tight")
    plt.close(figure)

    figure, axis = plt.subplots(figsize=(8, 8))
    if len(df):
        density = axis.hexbin(df["lon"], df["lat"], gridsize=45, bins="log", mincnt=1, cmap="viridis")
        figure.colorbar(density, ax=axis, label="log10(reference count)")
    else:
        axis.text(0.5, 0.5, "No references", ha="center", va="center", transform=axis.transAxes)
    axis.set(title="Moscow reference density", xlabel="Longitude", ylabel="Latitude")
    axis.set_xlim(MOSCOW_BOUNDS[2], MOSCOW_BOUNDS[3])
    axis.set_ylim(MOSCOW_BOUNDS[0], MOSCOW_BOUNDS[1])
    figure.savefig(density_path, dpi=180, bbox_inches="tight")
    plt.close(figure)

    counts = Counter(str(value) for value in df["source"].dropna())
    figure, axis = plt.subplots(figsize=(7, 4))
    if counts:
        labels = sorted(counts)
        axis.bar(labels, [counts[label] for label in labels])
    else:
        axis.text(0.5, 0.5, "No references", ha="center", va="center", transform=axis.transAxes)
    axis.set(title="Reference count by source", ylabel="References")
    figure.savefig(source_comparison_path, dpi=180, bbox_inches="tight")
    plt.close(figure)
    return "ok"


def _fixed_grid_stats(df: pd.DataFrame, bins: int = 20) -> dict[str, Any]:
    if bins < 1:
        raise ValueError("bins must be >= 1")
    if len(df):
        counts, _, _ = np.histogram2d(
            df["lat"].to_numpy(dtype=float),
            df["lon"].to_numpy(dtype=float),
            bins=(bins, bins),
            range=((MOSCOW_BOUNDS[0], MOSCOW_BOUNDS[1]), (MOSCOW_BOUNDS[2], MOSCOW_BOUNDS[3])),
        )
    else:
        counts = np.zeros((bins, bins), dtype=int)
    occupied = int(np.count_nonzero(counts))
    total = int(counts.size)
    return {
        "grid_bins_per_axis": bins,
        "occupied_grid_cells": occupied,
        "empty_grid_cells": total - occupied,
        "grid_occupancy_ratio": occupied / total,
        "references_per_occupied_cell_mean": float(counts.sum() / occupied) if occupied else 0.0,
    }


def _load_ingestion_stats(paths: Iterable[Path]) -> dict[str, Any]:
    combined: dict[str, Any] = {}
    for path in paths:
        payload = read_json(path, default={})
        if isinstance(payload, dict):
            combined[path.stem] = payload
    return combined


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    nearest = summary["nearest_reference_distance_m"]
    sources = summary["sources"]
    lines = [
        "# Moscow reference data report",
        "",
        f"- Total references: {summary['total_references']}",
        f"- Empty gallery: {summary['empty_gallery']}",
        f"- Unique H3 fine cells: {summary['unique_h3_fine_cells']}",
        f"- Nearest-reference p50: {nearest['p50']}",
        f"- Nearest-reference p90: {nearest['p90']}",
        f"- Grid occupancy: {summary['fixed_moscow_grid']['grid_occupancy_ratio']:.4f}",
        "",
        "## Sources",
        "",
        "| Source | References | Proportion | H3 fine cells |",
        "|---|---:|---:|---:|",
    ]
    for source, values in sorted(sources.items()):
        lines.append(
            f"| {source} | {values['count']} | {values['proportion']:.4f} | {values['unique_h3_fine_cells']} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    manifest_path: Path,
    report_path: Path,
    scatter_path: Path,
    preview_path: Path,
    *,
    density_path: Path | None = None,
    source_comparison_path: Path | None = None,
    markdown_path: Path | None = None,
    ingestion_stats_paths: Iterable[Path] = (),
) -> dict[str, Any]:
    df = read_manifest(manifest_path, allow_empty=True)
    distances = nearest_reference_distances(df)
    nearest = {
        "mean": float(distances.mean()) if len(distances) else None,
        "p50": float(np.percentile(distances, 50)) if len(distances) else None,
        "p90": float(np.percentile(distances, 90)) if len(distances) else None,
        "max": float(distances.max()) if len(distances) else None,
        "count": len(distances),
    }
    source_counts = Counter(str(value) for value in df["source"].dropna())
    source_stats: dict[str, dict[str, Any]] = {}
    for source, count in sorted(source_counts.items()):
        subset = df[df["source"] == source]
        source_stats[source] = {
            "count": count,
            "proportion": count / len(df) if len(df) else 0.0,
            "unique_h3_fine_cells": int(subset["h3_fine"].nunique()) if "h3_fine" in subset.columns else None,
        }

    density_target = density_path or scatter_path.with_name("dataset_density.png")
    source_target = source_comparison_path or scatter_path.with_name("dataset_sources.png")
    visualization_status = build_visualizations(
        df,
        scatter_path=scatter_path,
        density_path=density_target,
        source_comparison_path=source_target,
    )
    build_contact_sheet(df, preview_path)
    summary: dict[str, Any] = {
        "total_references": len(df),
        "empty_gallery": len(df) == 0,
        "unique_h3_coarse_cells": int(df["h3_coarse"].nunique()) if "h3_coarse" in df.columns else None,
        "unique_h3_fine_cells": int(df["h3_fine"].nunique()) if "h3_fine" in df.columns else None,
        "nearest_reference_distance_m": nearest,
        "fixed_moscow_grid": _fixed_grid_stats(df),
        "sources": source_stats,
        "ingestion": _load_ingestion_stats(ingestion_stats_paths),
        "visualization_status": visualization_status,
        "artifacts": {
            "scatter": str(scatter_path),
            "density": str(density_target),
            "source_comparison": str(source_target),
            "preview": str(preview_path),
        },
    }
    write_json(report_path, summary)
    _write_markdown(markdown_path or report_path.with_suffix(".md"), summary)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Moscow coverage and source-comparison reports")
    parser.add_argument("--manifest", default="data/processed/manifest_clean.parquet")
    parser.add_argument("--report", default="data/processed/reports/dataset_report.json")
    parser.add_argument("--markdown")
    parser.add_argument("--scatter", default="data/processed/reports/dataset_scatter.png")
    parser.add_argument("--density", default="data/processed/reports/dataset_density.png")
    parser.add_argument("--source-comparison", default="data/processed/reports/dataset_sources.png")
    parser.add_argument("--preview", default="data/processed/reports/dataset_preview_20.jpg")
    parser.add_argument("--ingestion-stats", action="append", default=[])
    args = parser.parse_args()
    run(
        manifest_path=Path(args.manifest),
        report_path=Path(args.report),
        scatter_path=Path(args.scatter),
        density_path=Path(args.density),
        source_comparison_path=Path(args.source_comparison),
        preview_path=Path(args.preview),
        markdown_path=Path(args.markdown) if args.markdown else None,
        ingestion_stats_paths=[Path(value) for value in args.ingestion_stats],
    )


if __name__ == "__main__":
    main()
