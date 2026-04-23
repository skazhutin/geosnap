"""Merge Mapillary and KartaView raw metadata into a unified manifest."""

from __future__ import annotations

import argparse
import math
import re
import uuid
from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json


SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(value: str) -> str:
    sanitized = SAFE_FILENAME_RE.sub("_", value).strip("._")
    return sanitized or "unknown"


def normalize_record(source: str, row: dict[str, Any]) -> dict[str, Any] | None:
    lat = row.get("lat")
    lon = row.get("lon")
    source_image_id = row.get("id")
    if lat is None or lon is None or source_image_id is None:
        return None
    download_url = row.get("image_url") or row.get("url")
    if download_url is None:
        return None

    source_l = source.lower()
    attribution = "Mapillary" if source_l == "mapillary" else "KartaView"
    record_id = str(uuid.uuid4())
    safe_source_image_id = safe_filename(str(source_image_id))

    return {
        "id": record_id,
        "source": source_l,
        "source_image_id": str(source_image_id),
        "lat": float(lat),
        "lon": float(lon),
        "captured_at": row.get("timestamp"),
        "heading": row.get("heading"),
        "image_path": f"data/raw/images/{source_l}/{record_id}_{safe_source_image_id}.jpg",
        "thumb_path": row.get("thumb_path"),
        "license": row.get("license") or "CC-BY-SA",
        "attribution": row.get("attribution") or attribution,
        "metadata_json": row,
        "download_url": download_url,
    }


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6_371_000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius_m * math.asin(math.sqrt(a))


def deduplicate_spatial(
    rows: list[dict[str, Any]],
    *,
    radius_m: float = 7.0,
    max_per_cluster: int = 2,
) -> list[dict[str, Any]]:
    """Deduplicate cross-source near-identical points by small-radius clustering."""
    if max_per_cluster < 1:
        raise ValueError("max_per_cluster must be >= 1")
    if radius_m <= 0:
        return rows

    lat_deg = radius_m / 111_320
    lon_deg = radius_m / 65_000  # rough Moscow latitude approximation

    clusters: dict[tuple[int, int], list[dict[str, Any]]] = {}
    result: list[dict[str, Any]] = []

    def cluster_key(lat: float, lon: float) -> tuple[int, int]:
        return (int(lat / lat_deg), int(lon / lon_deg))

    for row in rows:
        lat = float(row["lat"])
        lon = float(row["lon"])
        base_key = cluster_key(lat, lon)

        nearby: list[dict[str, Any]] = []
        bx, by = base_key
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                nearby.extend(clusters.get((bx + dx, by + dy), []))

        near_count = 0
        for accepted in nearby:
            if haversine_meters(lat, lon, float(accepted["lat"]), float(accepted["lon"])) <= radius_m:
                near_count += 1

        if near_count < max_per_cluster:
            clusters.setdefault(base_key, []).append(row)
            result.append(row)

    return result


def run(
    mapillary_json: Path,
    kartaview_json: Path,
    output_manifest: Path,
    dedup_radius_m: float = 7.0,
    max_per_cluster: int = 2,
) -> None:
    import pandas as pd

    rows: list[dict[str, Any]] = []
    for source, path in (("mapillary", mapillary_json), ("kartaview", kartaview_json)):
        for item in read_json(path, default=[]):
            normalized = normalize_record(source, item)
            if normalized:
                rows.append(normalized)

    rows = deduplicate_spatial(rows, radius_m=dedup_radius_m, max_per_cluster=max_per_cluster)
    manifest = pd.DataFrame(rows)
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_parquet(output_manifest, index=False)
    print(f"wrote {len(manifest)} rows to {output_manifest}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge raw source metadata into manifest.parquet")
    parser.add_argument("--mapillary-json", default="data/raw/mapillary_raw.json")
    parser.add_argument("--kartaview-json", default="data/raw/kartaview_raw.json")
    parser.add_argument("--output-manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--dedup-radius-m", type=float, default=7.0)
    parser.add_argument("--max-per-cluster", type=int, default=2)
    args = parser.parse_args()

    run(
        mapillary_json=Path(args.mapillary_json),
        kartaview_json=Path(args.kartaview_json),
        output_manifest=Path(args.output_manifest),
        dedup_radius_m=args.dedup_radius_m,
        max_per_cluster=args.max_per_cluster,
    )


if __name__ == "__main__":
    main()
