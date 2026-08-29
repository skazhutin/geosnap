"""Conservatively thin near-identical KartaView sequence neighbours.

The default path deliberately has no gallery-size or per-sequence cap and only
removes repeated source IDs.  Before the images exist locally, spatial
proximity and heading can only provide a near-duplicate proxy, so proximity
thinning is opt-in; direct visual deduplication remains the job of
:mod:`ml.cleaning.deduplicate` after download.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json
from ml.ingestion.merge_sources import haversine_meters
from ml.ingestion.schema import MOSCOW_BOUNDS

DEFAULT_MIN_SPACING_M = 0.0
DEFAULT_HEADING_DIVERSITY_DEG = 30.0
DEFAULT_MIN_HEADING_SPACING_M = 0.0


def _prefer_cdn_url(row: dict[str, Any]) -> dict[str, Any]:
    """Refresh legacy normalized rows from the raw API metadata they retain."""

    try:
        metadata = json.loads(str(row.get("metadata_json") or "{}"))
    except (json.JSONDecodeError, TypeError):
        return row
    if not isinstance(metadata, dict):
        return row
    for key in ("imageProcUrl", "imageLthUrl"):
        value = metadata.get(key)
        if isinstance(value, str) and value.startswith("https://cdn.kartaview.org/"):
            row["download_url"] = value
            row["image_url"] = value
            return row
    return row


def _sequence_index(row: dict[str, Any]) -> tuple[int, str]:
    value = row.get("sequence_index")
    if value is None:
        try:
            value = json.loads(str(row.get("metadata_json") or "{}"))["sequenceIndex"]
        except (json.JSONDecodeError, KeyError, TypeError):
            value = None
    try:
        return int(value), str(row.get("id") or "")
    except (TypeError, ValueError):
        return 2**63 - 1, str(row.get("id") or "")


def _area_id(row: dict[str, Any]) -> str:
    try:
        metadata = json.loads(str(row.get("metadata_json") or "{}"))
    except json.JSONDecodeError:
        metadata = {}
    explicit = metadata.get("_geosnap_acquisition_area")
    if isinstance(explicit, str) and explicit:
        return explicit
    query = metadata.get("_geosnap_acquisition_query")
    if isinstance(query, str):
        parts = query.split(":")
        if len(parts) >= 4 and parts[0] == "config":
            return parts[2]
    return "unassigned"


def _heading_delta(first: Any, second: Any) -> float | None:
    try:
        left, right = float(first), float(second)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(left) and math.isfinite(right)):
        return None
    delta = abs((left - right) % 360.0)
    return min(delta, 360.0 - delta)


def _sample_sequence(
    rows: list[dict[str, Any]],
    *,
    min_spacing_m: float,
    heading_diversity_deg: float,
    min_heading_spacing_m: float,
    max_per_sequence: int | None,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in sorted(rows, key=_sequence_index):
        source_id = str(row.get("source_image_id") or row.get("id") or "")
        if not source_id or source_id in seen_ids:
            continue
        seen_ids.add(source_id)
        if not selected:
            selected.append(row)
            if max_per_sequence is not None and len(selected) >= max_per_sequence:
                break
            continue
        previous = selected[-1]
        distance = haversine_meters(
            float(previous["lat"]),
            float(previous["lon"]),
            float(row["lat"]),
            float(row["lon"]),
        )
        heading_delta = _heading_delta(previous.get("heading"), row.get("heading"))
        heading_keep = (
            heading_delta is not None
            and heading_delta >= heading_diversity_deg
            and distance >= min_heading_spacing_m
        )
        if distance >= min_spacing_m or heading_keep:
            selected.append(row)
        if max_per_sequence is not None and len(selected) >= max_per_sequence:
            break
    return selected


def _balanced_take(
    sampled_by_area_sequence: dict[str, dict[str, list[dict[str, Any]]]],
    max_records: int | None,
) -> list[dict[str, Any]]:
    queues: dict[str, dict[str, deque[dict[str, Any]]]] = {
        area: {sequence: deque(rows) for sequence, rows in sorted(sequences.items())}
        for area, sequences in sorted(sampled_by_area_sequence.items())
    }
    selected: list[dict[str, Any]] = []
    while max_records is None or len(selected) < max_records:
        progressed = False
        for area in sorted(queues):
            for sequence in sorted(queues[area]):
                queue = queues[area][sequence]
                if not queue:
                    continue
                selected.append(queue.popleft())
                progressed = True
                if max_records is not None and len(selected) >= max_records:
                    break
            if max_records is not None and len(selected) >= max_records:
                break
        if not progressed:
            break
    return selected


def run(
    input_json: Path,
    output_json: Path,
    report_path: Path,
    *,
    max_records: int | None = None,
    min_spacing_m: float = DEFAULT_MIN_SPACING_M,
    heading_diversity_deg: float = DEFAULT_HEADING_DIVERSITY_DEG,
    min_heading_spacing_m: float = DEFAULT_MIN_HEADING_SPACING_M,
    max_per_sequence: int | None = None,
) -> dict[str, Any]:
    if max_records is not None and max_records < 1:
        raise ValueError("max_records must be >= 1 when configured")
    if max_per_sequence is not None and max_per_sequence < 1:
        raise ValueError("max_per_sequence must be >= 1 when configured")
    if min_spacing_m < 0 or min_heading_spacing_m < 0:
        raise ValueError("spacing thresholds are invalid")
    if min_heading_spacing_m > min_spacing_m:
        raise ValueError("min_heading_spacing_m must not exceed min_spacing_m")
    if not 0 <= heading_diversity_deg <= 180:
        raise ValueError("heading_diversity_deg must be in [0, 180]")
    payload = read_json(input_json, default=[])
    if not isinstance(payload, list):
        raise ValueError(f"{input_json} must contain a JSON array")

    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    valid: list[dict[str, Any]] = []
    invalid_rows = 0
    outside_moscow = 0
    duplicate_source_ids = 0
    seen_source_ids: set[str] = set()
    for value in payload:
        if not isinstance(value, dict):
            invalid_rows += 1
            continue
        try:
            lat, lon = float(value["lat"]), float(value["lon"])
        except (KeyError, TypeError, ValueError):
            invalid_rows += 1
            continue
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            outside_moscow += 1
            continue
        if not value.get("sequence_id") or not (value.get("source_image_id") or value.get("id")):
            invalid_rows += 1
            continue
        source_id = str(value.get("source_image_id") or value.get("id"))
        if source_id in seen_source_ids:
            duplicate_source_ids += 1
            continue
        seen_source_ids.add(source_id)
        valid.append(_prefer_cdn_url(dict(value)))
    if not valid:
        raise ValueError("no valid Moscow KartaView frames are available for sampling")

    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in valid:
        grouped[_area_id(row)][str(row["sequence_id"])].append(row)
    sampled: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    sampled_before_optional_sequence_limit = 0
    for area, sequences in grouped.items():
        for sequence, rows in sequences.items():
            spatially_novel = _sample_sequence(
                rows,
                min_spacing_m=min_spacing_m,
                heading_diversity_deg=heading_diversity_deg,
                min_heading_spacing_m=min_heading_spacing_m,
                max_per_sequence=None,
            )
            sampled_before_optional_sequence_limit += len(spatially_novel)
            sampled[area][sequence] = (
                spatially_novel[:max_per_sequence]
                if max_per_sequence is not None
                else spatially_novel
            )
    sampled_after_optional_sequence_limit = sum(
        len(rows) for values in sampled.values() for rows in values.values()
    )
    selected = _balanced_take(sampled, max_records)
    selected.sort(key=lambda row: (_area_id(row), str(row["sequence_id"]), _sequence_index(row)))
    write_json(output_json, selected)

    canonical = json.dumps(selected, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    area_counts = Counter(_area_id(row) for row in selected)
    sequence_counts = Counter(str(row["sequence_id"]) for row in selected)
    report: dict[str, Any] = {
        "schema_version": 2,
        "input_json": str(input_json),
        "output_json": str(output_json),
        "input_records": len(payload),
        "valid_moscow_records": len(valid),
        "invalid_rows": invalid_rows,
        "outside_moscow_rows": outside_moscow,
        "duplicate_source_ids_removed": duplicate_source_ids,
        "candidate_areas": len(grouped),
        "candidate_sequences": sum(len(value) for value in grouped.values()),
        "after_sequence_neighbor_thinning": sampled_before_optional_sequence_limit,
        "sequence_neighbor_proxies_removed": len(valid) - sampled_before_optional_sequence_limit,
        "sampled_before_global_cap": sampled_after_optional_sequence_limit,
        "optional_sequence_limit_removed": (
            sampled_before_optional_sequence_limit - sampled_after_optional_sequence_limit
        ),
        "optional_global_limit_removed": sampled_after_optional_sequence_limit - len(selected),
        "global_cap_configured": max_records is not None,
        "per_sequence_cap_configured": max_per_sequence is not None,
        "selected_records": len(selected),
        "selected_sequences": len(sequence_counts),
        "selected_areas": len(area_counts),
        "selected_per_area": dict(sorted(area_counts.items())),
        "max_records": max_records,
        "max_per_sequence": max_per_sequence,
        "min_spacing_m": min_spacing_m,
        "heading_diversity_deg": heading_diversity_deg,
        "min_heading_spacing_m": min_heading_spacing_m,
        "selected_content_sha256": hashlib.sha256(canonical).hexdigest(),
    }
    write_json(report_path, report)
    report_path.with_suffix(".md").write_text(
        "# KartaView frame selection\n\n"
        + "\n".join(f"- {key}: {value}" for key, value in report.items() if key != "selected_per_area")
        + "\n\n## Selected per area\n\n"
        + "\n".join(f"- {area}: {count}" for area, count in sorted(area_counts.items()))
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Conservatively thin near-identical KartaView sequence neighbours"
    )
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--max-records",
        type=int,
        help="optional explicit output limit; omitted by default",
    )
    parser.add_argument("--min-spacing-m", type=float, default=DEFAULT_MIN_SPACING_M)
    parser.add_argument(
        "--heading-diversity-deg", type=float, default=DEFAULT_HEADING_DIVERSITY_DEG
    )
    parser.add_argument(
        "--min-heading-spacing-m", type=float, default=DEFAULT_MIN_HEADING_SPACING_M
    )
    parser.add_argument(
        "--max-per-sequence",
        type=int,
        help="optional explicit per-sequence limit; omitted by default",
    )
    args = parser.parse_args()
    run(
        args.input_json,
        args.output_json,
        args.report,
        max_records=args.max_records,
        min_spacing_m=args.min_spacing_m,
        heading_diversity_deg=args.heading_diversity_deg,
        min_heading_spacing_m=args.min_heading_spacing_m,
        max_per_sequence=args.max_per_sequence,
    )


if __name__ == "__main__":
    main()
