"""Stage 3.3: remove geo and visual duplicates after quality filtering."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from PIL import Image

from ml.cleaning.reporting import update_cleaning_report

try:
    import imagehash
except ImportError:  # pragma: no cover
    imagehash = None

EARTH_RADIUS_M = 6_371_000


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(d_lon / 2.0) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def heading_diff_deg(a: float, b: float) -> float:
    diff = abs((a - b) % 360)
    return min(diff, 360 - diff)


def compute_visual_hash(path: Path, hash_size: int = 8) -> Any:
    with Image.open(path) as img:
        if imagehash is not None:
            return imagehash.phash(img, hash_size=hash_size)
        gray = img.convert("L").resize((hash_size + 1, hash_size))
        px = np.asarray(gray, dtype=np.float32)
    bits = (px[:, 1:] > px[:, :-1]).astype(np.uint8).flatten()
    packed = np.packbits(bits)
    return packed.tobytes().hex()


def hash_distance(left: Any, right: Any) -> int:
    if imagehash is not None:
        return int(left - right)

    left_int = int(str(left), 16)
    right_int = int(str(right), 16)
    return int((left_int ^ right_int).bit_count())


def _cluster_geo(df: pd.DataFrame, dedup_radius_m: float) -> list[list[int]]:
    n = len(df)
    neighbors: list[list[int]] = [[] for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            dist = haversine_distance_m(float(df.loc[i, "lat"]), float(df.loc[i, "lon"]), float(df.loc[j, "lat"]), float(df.loc[j, "lon"]))
            if dist <= dedup_radius_m:
                neighbors[i].append(j)
                neighbors[j].append(i)

    visited = [False] * n
    clusters: list[list[int]] = []
    for i in range(n):
        if visited[i]:
            continue
        stack = [i]
        visited[i] = True
        component: list[int] = []
        while stack:
            cur = stack.pop()
            component.append(cur)
            for nxt in neighbors[cur]:
                if not visited[nxt]:
                    visited[nxt] = True
                    stack.append(nxt)
        clusters.append(component)
    return clusters


def _cluster_keep_indices(df: pd.DataFrame, cluster: list[int], max_per_geo_point: int, heading_threshold: float) -> list[int]:
    sorted_cluster = sorted(cluster, key=lambda i: float(df.loc[i, "quality_score"]), reverse=True)
    keep = sorted_cluster[:max_per_geo_point]

    if "heading" not in df.columns:
        return keep

    for idx in sorted_cluster[max_per_geo_point:]:
        heading = df.loc[idx, "heading"]
        if pd.isna(heading):
            continue

        selected_headings = [float(df.loc[selected, "heading"]) for selected in keep if not pd.isna(df.loc[selected, "heading"])]
        if not selected_headings:
            continue

        if all(heading_diff_deg(float(heading), selected_heading) > heading_threshold for selected_heading in selected_headings):
            keep.append(idx)

    return keep


def run(
    input_manifest: Path,
    output_manifest: Path,
    report_path: Path,
    dedup_radius_m: float,
    max_per_geo_point: int,
    hash_distance_threshold: int,
    heading_threshold_deg: float,
    pipeline_report_path: Path,
) -> None:
    df = pd.read_parquet(input_manifest).copy().reset_index(drop=True)
    if "quality_score" not in df.columns:
        df["quality_score"] = 0.0

    geo_clusters = _cluster_geo(df, dedup_radius_m)
    keep_geo: set[int] = set()
    for cluster in geo_clusters:
        keep_geo.update(_cluster_keep_indices(df, cluster, max_per_geo_point, heading_threshold_deg))

    df_geo = df.loc[sorted(keep_geo)].reset_index(drop=True)

    kept_hashes: list[Any] = []
    drop_visual: set[int] = set()
    for idx, row in df_geo.iterrows():
        path = Path(str(row["image_path"]))
        current_hash = compute_visual_hash(path)
        too_similar = any(hash_distance(current_hash, existing_hash) < hash_distance_threshold for existing_hash in kept_hashes)
        if too_similar:
            drop_visual.add(idx)
            continue
        kept_hashes.append(current_hash)

    result = df_geo.drop(index=sorted(drop_visual)).reset_index(drop=True)

    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_manifest, index=False)

    summary = {
        "input_rows": int(len(df)),
        "geo_clusters": int(len(geo_clusters)),
        "after_geo_rows": int(len(df_geo)),
        "after_visual_rows": int(len(result)),
        "geo_duplicates_removed": int(len(df) - len(df_geo)),
        "visual_duplicates_removed": int(len(drop_visual)),
        "dedup_radius_m": dedup_radius_m,
        "max_per_geo_point": max_per_geo_point,
        "heading_threshold_deg": heading_threshold_deg,
        "hash_distance_threshold": hash_distance_threshold,
        "visual_hash": "phash" if imagehash is not None else "dhash_fallback",
    }

    if len(df) > 0 and len(result) < 0.5 * len(df):
        summary["warning"] = "Deduplication removed more than 50% of rows. Consider increasing radius/thresholds."
        print(f"WARNING: {summary['warning']}")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    update_cleaning_report(report_path=pipeline_report_path, after_dedup=len(result))


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate by geo distance + visual hash")
    parser.add_argument("--manifest", default="data/processed/manifest_step2.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step3.parquet")
    parser.add_argument("--report", default="data/processed/reports/dedup_step3.json")
    parser.add_argument("--dedup-radius-m", type=float, default=15.0)
    parser.add_argument("--max-per-geo-point", type=int, default=5)
    parser.add_argument("--hash-distance-threshold", type=int, default=4)
    parser.add_argument("--heading-threshold-deg", type=float, default=60.0)
    parser.add_argument("--pipeline-report", default="data/processed/cleaning_report.json")
    args = parser.parse_args()

    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        report_path=Path(args.report),
        dedup_radius_m=args.dedup_radius_m,
        max_per_geo_point=args.max_per_geo_point,
        hash_distance_threshold=args.hash_distance_threshold,
        heading_threshold_deg=args.heading_threshold_deg,
        pipeline_report_path=Path(args.pipeline_report),
    )


if __name__ == "__main__":
    main()
