"""Quality-first exact and local perceptual duplicate suppression.

There is deliberately no geographic per-location cap.  Nearby views are only
removed when direct visual evidence says they are redundant, and suppressed
records never become transitive clustering anchors.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import imagehash
import pandas as pd
from PIL import Image

from ml.cleaning.reporting import update_cleaning_report
from ml.ingestion.common import write_json
from ml.ingestion.schema import is_missing_value, normalize_captured_at, read_manifest, write_manifest

EARTH_RADIUS_M = 6_371_000.0
PHASH_BITS = 64


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    value = math.sin(d_lat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(d_lon / 2.0) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(value))


def heading_diff_deg(a: float, b: float) -> float:
    difference = abs((float(a) - float(b)) % 360.0)
    return min(difference, 360.0 - difference)


def compute_visual_hash(path: Path, hash_size: int = 8) -> int:
    if hash_size != 8:
        raise ValueError("only 64-bit perceptual hashes (hash_size=8) are supported")
    with Image.open(path) as image:
        return int(str(imagehash.phash(image.convert("RGB"), hash_size=hash_size)), 16)


def hash_distance(left: Any, right: Any) -> int:
    if isinstance(left, int) and isinstance(right, int):
        return int((left ^ right).bit_count())
    return int(left - right)


def exact_file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quality(row: pd.Series) -> float:
    try:
        value = float(row.get("quality_score"))
    except (TypeError, ValueError):
        return -1.0
    return value if math.isfinite(value) else -1.0


def _metric_xy(lat: float, lon: float, reference_lat: float) -> tuple[float, float]:
    y = math.radians(lat) * EARTH_RADIUS_M
    x = math.radians(lon) * EARTH_RADIUS_M * math.cos(math.radians(reference_lat))
    return x, y


def _cell_key(lat: float, lon: float, reference_lat: float, cell_size_m: float) -> tuple[int, int]:
    x, y = _metric_xy(lat, lon, reference_lat)
    return math.floor(x / cell_size_m), math.floor(y / cell_size_m)


def _hash_band_keys(value: int, threshold: int) -> list[tuple[int, int]]:
    """LSH bands with no false negatives for Hamming distance <= threshold."""
    if threshold >= PHASH_BITS:
        return [(0, 0)]
    bands = threshold + 1
    base_width, remainder = divmod(PHASH_BITS, bands)
    result: list[tuple[int, int]] = []
    offset = 0
    for band in range(bands):
        width = base_width + (1 if band < remainder else 0)
        mask = (1 << width) - 1
        result.append((band, (value >> offset) & mask))
        offset += width
    return result


def _parse_time(value: Any) -> datetime | None:
    normalized = normalize_captured_at(value)
    if normalized is None:
        return None
    return datetime.fromisoformat(normalized.replace("Z", "+00:00"))


def _optional_text(value: Any) -> str:
    return "" if is_missing_value(value) else str(value).strip()


def _eligible_pair(
    left: pd.Series,
    right: pd.Series,
    *,
    radius_m: float,
    heading_threshold_deg: float,
    temporal_preserve_days: float,
    sequence_time_window_sec: float,
) -> bool:
    if (
        haversine_distance_m(float(left["lat"]), float(left["lon"]), float(right["lat"]), float(right["lon"]))
        > radius_m
    ):
        return False

    left_heading = left.get("heading")
    right_heading = right.get("heading")
    if not pd.isna(left_heading) and not pd.isna(right_heading):
        if heading_diff_deg(float(left_heading), float(right_heading)) > heading_threshold_deg:
            return False

    left_sequence = _optional_text(left.get("sequence_id"))
    right_sequence = _optional_text(right.get("sequence_id"))
    left_time = _parse_time(left.get("captured_at"))
    right_time = _parse_time(right.get("captured_at"))
    delta_seconds = abs((left_time - right_time).total_seconds()) if left_time and right_time else None

    if left_sequence and right_sequence:
        if left_sequence == right_sequence:
            return delta_seconds is None or delta_seconds <= sequence_time_window_sec
        # Different traversals are useful temporal diversity unless timestamps
        # show that they are effectively contemporaneous.
        return delta_seconds is not None and delta_seconds <= temporal_preserve_days * 86_400.0
    if delta_seconds is not None and delta_seconds > temporal_preserve_days * 86_400.0:
        return False
    return True


def _cluster_geo(df: pd.DataFrame, dedup_radius_m: float) -> list[list[int]]:
    """Complete-link local groups used by regression tests and diagnostics.

    A point may join a group only if it is within the radius of *every* member,
    so A-B-C-D chains cannot collapse into one component.
    """
    if dedup_radius_m <= 0:
        raise ValueError("dedup_radius_m must be > 0")
    if len(df) == 0:
        return []
    reference_lat = float(pd.to_numeric(df["lat"], errors="raise").mean())
    order = sorted(df.index, key=lambda index: (-_quality(df.loc[index]), str(df.loc[index].get("id") or index)))
    groups: list[list[int]] = []
    group_cells: dict[tuple[int, int], list[int]] = {}
    for index in order:
        row = df.loc[index]
        cell = _cell_key(float(row["lat"]), float(row["lon"]), reference_lat, dedup_radius_m)
        candidate_groups: set[int] = set()
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                candidate_groups.update(group_cells.get((cell[0] + dx, cell[1] + dy), []))
        selected: int | None = None
        for group_id in sorted(candidate_groups):
            if all(
                haversine_distance_m(
                    float(row["lat"]),
                    float(row["lon"]),
                    float(df.loc[member, "lat"]),
                    float(df.loc[member, "lon"]),
                )
                <= dedup_radius_m
                for member in groups[group_id]
            ):
                selected = group_id
                break
        if selected is None:
            selected = len(groups)
            groups.append([])
            group_cells.setdefault(cell, []).append(selected)
        groups[selected].append(index)
    return groups


def run(
    input_manifest: Path,
    output_manifest: Path,
    report_path: Path,
    dedup_radius_m: float,
    max_per_geo_point: int | None,
    hash_distance_threshold: int,
    heading_threshold_deg: float,
    pipeline_report_path: Path,
    *,
    temporal_preserve_days: float = 7.0,
    sequence_time_window_sec: float = 180.0,
) -> dict[str, Any]:
    if dedup_radius_m <= 0:
        raise ValueError("dedup_radius_m must be > 0")
    if max_per_geo_point is not None and max_per_geo_point < 1:
        raise ValueError("max_per_geo_point must be >= 1 when provided")
    if not 0 <= hash_distance_threshold <= PHASH_BITS:
        raise ValueError("hash_distance_threshold must be between 0 and 64 inclusive")
    if not 0 <= heading_threshold_deg <= 180:
        raise ValueError("heading_threshold_deg must be between 0 and 180")
    if temporal_preserve_days < 0:
        raise ValueError("temporal_preserve_days must be >= 0")
    if sequence_time_window_sec < 0:
        raise ValueError("sequence_time_window_sec must be >= 0")

    df = read_manifest(input_manifest, allow_empty=True).reset_index(drop=True)
    order = sorted(range(len(df)), key=lambda index: (-_quality(df.loc[index]), str(df.loc[index, "id"])))
    exact_keeper: dict[str, int] = {}
    exact_survivors: list[int] = []
    removed_exact: dict[str, str] = {}
    hash_failures: list[str] = []
    for index in order:
        row = df.loc[index]
        try:
            digest = exact_file_hash(Path(str(row["image_path"])))
        except OSError:
            hash_failures.append(str(row["id"]))
            exact_survivors.append(index)
            continue
        keeper = exact_keeper.get(digest)
        if keeper is not None:
            removed_exact[str(row["id"])] = str(df.loc[keeper, "id"])
            continue
        exact_keeper[digest] = index
        exact_survivors.append(index)

    reference_lat = float(df["lat"].mean()) if len(df) else 0.0
    # (cell x, cell y, band number, band value) -> quality-selected exemplars.
    local_hash_index: dict[tuple[int, int, int, int], list[int]] = {}
    phashes: dict[int, int] = {}
    perceptual_survivors: list[int] = []
    removed_perceptual: dict[str, str] = {}
    visual_comparisons = 0

    for index in exact_survivors:
        row = df.loc[index]
        try:
            current_hash = compute_visual_hash(Path(str(row["image_path"])))
        except (OSError, ValueError):
            hash_failures.append(str(row["id"]))
            perceptual_survivors.append(index)
            continue
        phashes[index] = current_hash
        cell = _cell_key(float(row["lat"]), float(row["lon"]), reference_lat, dedup_radius_m)
        bands = _hash_band_keys(current_hash, hash_distance_threshold)
        candidates: set[int] = set()
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for band, value in bands:
                    candidates.update(local_hash_index.get((cell[0] + dx, cell[1] + dy, band, value), []))
        duplicate_of: int | None = None
        for candidate in sorted(candidates, key=lambda item: (-_quality(df.loc[item]), str(df.loc[item, "id"]))):
            candidate_row = df.loc[candidate]
            if not _eligible_pair(
                row,
                candidate_row,
                radius_m=dedup_radius_m,
                heading_threshold_deg=heading_threshold_deg,
                temporal_preserve_days=temporal_preserve_days,
                sequence_time_window_sec=sequence_time_window_sec,
            ):
                continue
            visual_comparisons += 1
            if hash_distance(current_hash, phashes[candidate]) <= hash_distance_threshold:
                duplicate_of = candidate
                break
        if duplicate_of is not None:
            removed_perceptual[str(row["id"])] = str(df.loc[duplicate_of, "id"])
            continue

        perceptual_survivors.append(index)
        for band, value in bands:
            local_hash_index.setdefault((cell[0], cell[1], band, value), []).append(index)

    # Preserve input ordering while retaining the quality-selected winner.
    result = df.loc[sorted(perceptual_survivors)].copy()
    write_manifest(result, output_manifest, allow_empty=True)
    summary: dict[str, Any] = {
        "input_rows": len(df),
        "after_exact_rows": len(exact_survivors),
        "after_perceptual_rows": len(result),
        "exact_duplicates_removed": len(removed_exact),
        "perceptual_duplicates_removed": len(removed_perceptual),
        "hash_failures": len(set(hash_failures)),
        "visual_comparisons": visual_comparisons,
        "dedup_radius_m": dedup_radius_m,
        "heading_threshold_deg": heading_threshold_deg,
        "hash_distance_threshold": hash_distance_threshold,
        "hash_threshold_zero_semantics": "remove only equal 64-bit perceptual hashes",
        "temporal_preserve_days": temporal_preserve_days,
        "sequence_time_window_sec": sequence_time_window_sec,
        "geo_cap_applied": False,
    }
    write_json(
        report_path,
        {
            "summary": summary,
            "removed_exact": removed_exact,
            "removed_perceptual": removed_perceptual,
            "hash_failure_ids": sorted(set(hash_failures)),
        },
    )
    report_path.with_suffix(".md").write_text(
        "# Deduplication\n\n" + "\n".join(f"- {key}: {value}" for key, value in summary.items()) + "\n",
        encoding="utf-8",
    )
    update_cleaning_report(report_path=pipeline_report_path, after_dedup=len(result))
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Quality-first exact and local perceptual deduplication")
    parser.add_argument("--manifest", default="data/processed/manifest_step2.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step3.parquet")
    parser.add_argument("--report", default="data/processed/reports/dedup_step3.json")
    parser.add_argument("--dedup-radius-m", type=float, default=15.0)
    parser.add_argument("--max-per-geo-point", type=int)
    parser.add_argument("--hash-distance-threshold", type=int, default=4)
    parser.add_argument("--heading-threshold-deg", type=float, default=45.0)
    parser.add_argument("--temporal-preserve-days", type=float, default=7.0)
    parser.add_argument("--sequence-time-window-sec", type=float, default=180.0)
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
        temporal_preserve_days=args.temporal_preserve_days,
        sequence_time_window_sec=args.sequence_time_window_sec,
        pipeline_report_path=Path(args.pipeline_report),
    )


if __name__ == "__main__":
    main()
