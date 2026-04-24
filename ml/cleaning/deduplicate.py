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


def _bucket_key(lat: float, lon: float, radius_m: float) -> tuple[int, int]:
    # Approximate metric grid in degrees.
    lat_step = radius_m / 111_320.0
    lon_step = radius_m / (111_320.0 * max(math.cos(math.radians(lat)), 0.2))
    return (int(lat / max(lat_step, 1e-9)), int(lon / max(lon_step, 1e-9)))


def _cluster_geo(df: pd.DataFrame, dedup_radius_m: float) -> list[list[int]]:
    """Greedy local clusters (center-based), avoids transitive chain collapse."""
    clusters: list[dict[str, Any]] = []
    bucket_to_clusters: dict[tuple[int, int], list[int]] = {}

    sorted_idx = sorted(df.index, key=lambda i: float(df.loc[i, "quality_score"]), reverse=True)
    for idx in sorted_idx:
        lat = float(df.loc[idx, "lat"])
        lon = float(df.loc[idx, "lon"])
        bucket = _bucket_key(lat, lon, dedup_radius_m)

        candidate_cluster_ids: set[int] = set()
        for d_lat in (-1, 0, 1):
            for d_lon in (-1, 0, 1):
                candidate_cluster_ids.update(bucket_to_clusters.get((bucket[0] + d_lat, bucket[1] + d_lon), []))

        best_cluster: int | None = None
        best_dist = float("inf")
        for cluster_id in candidate_cluster_ids:
            center_lat = float(clusters[cluster_id]["center_lat"])
            center_lon = float(clusters[cluster_id]["center_lon"])
            dist = haversine_distance_m(lat, lon, center_lat, center_lon)
            if dist <= dedup_radius_m and dist < best_dist:
                best_dist = dist
                best_cluster = cluster_id

        if best_cluster is None:
            cluster_id = len(clusters)
            clusters.append({"indices": [idx], "center_lat": lat, "center_lon": lon})
            bucket_to_clusters.setdefault(bucket, []).append(cluster_id)
        else:
            clusters[best_cluster]["indices"].append(idx)
            members = clusters[best_cluster]["indices"]
            clusters[best_cluster]["center_lat"] = float(df.loc[members, "lat"].astype(float).mean())
            clusters[best_cluster]["center_lon"] = float(df.loc[members, "lon"].astype(float).mean())

    return [cluster["indices"] for cluster in clusters]


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
    if max_per_geo_point < 1:
        raise ValueError("max_per_geo_point must be >= 1")

    df = pd.read_parquet(input_manifest).copy().reset_index(drop=True)
    if "quality_score" not in df.columns:
        df["quality_score"] = 0.0

    geo_clusters = _cluster_geo(df, dedup_radius_m)
    keep_geo: set[int] = set()
    for cluster in geo_clusters:
        keep_geo.update(_cluster_keep_indices(df, cluster, max_per_geo_point, heading_threshold_deg))

    df_geo = df.loc[sorted(keep_geo)].copy()
    df_geo = df_geo.sort_values(by="quality_score", ascending=False).reset_index(drop=True)

    kept_hashes: list[Any] = []
    keep_visual_indices: list[int] = []
    for idx, row in df_geo.iterrows():
        path = Path(str(row["image_path"]))
        current_hash = compute_visual_hash(path)
        too_similar = any(hash_distance(current_hash, existing_hash) <= hash_distance_threshold for existing_hash in kept_hashes)
        if too_similar:
            continue
        kept_hashes.append(current_hash)
        keep_visual_indices.append(idx)

    result = df_geo.loc[keep_visual_indices].sort_index().reset_index(drop=True)

    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_manifest, index=False)

    summary = {
        "input_rows": int(len(df)),
        "geo_clusters": int(len(geo_clusters)),
        "after_geo_rows": int(len(df_geo)),
        "after_visual_rows": int(len(result)),
        "geo_duplicates_removed": int(len(df) - len(df_geo)),
        "visual_duplicates_removed": int(len(df_geo) - len(result)),
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
