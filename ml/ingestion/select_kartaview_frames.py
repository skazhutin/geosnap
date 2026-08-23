"""Deterministically sample expanded KartaView frames before image download."""

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
    max_per_sequence: int,
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
        if len(selected) >= max_per_sequence:
            break
    return selected


def _balanced_take(
    sampled_by_area_sequence: dict[str, dict[str, list[dict[str, Any]]]],
    max_records: int,
) -> list[dict[str, Any]]:
    queues: dict[str, dict[str, deque[dict[str, Any]]]] = {
        area: {sequence: deque(rows) for sequence, rows in sorted(sequences.items())}
        for area, sequences in sorted(sampled_by_area_sequence.items())
    }
    selected: list[dict[str, Any]] = []
    while len(selected) < max_records:
        progressed = False
        for area in sorted(queues):
            for sequence in sorted(queues[area]):
                queue = queues[area][sequence]
                if not queue:
                    continue
                selected.append(queue.popleft())
                progressed = True
                if len(selected) >= max_records:
                    break
            if len(selected) >= max_records:
                break
        if not progressed:
            break
    return selected


def run(
    input_json: Path,
    output_json: Path,
    report_path: Path,
    *,
    max_records: int = 1_200,
    min_spacing_m: float = 45.0,
    heading_diversity_deg: float = 60.0,
    min_heading_spacing_m: float = 15.0,
    max_per_sequence: int = 75,
) -> dict[str, Any]:
    if max_records < 1 or max_per_sequence < 1:
        raise ValueError("record limits must be >= 1")
    if min_spacing_m <= 0 or min_heading_spacing_m < 0:
        raise ValueError("spacing thresholds are invalid")
    if not 0 <= heading_diversity_deg <= 180:
        raise ValueError("heading_diversity_deg must be in [0, 180]")
    payload = read_json(input_json, default=[])
    if not isinstance(payload, list):
        raise ValueError(f"{input_json} must contain a JSON array")

    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    valid: list[dict[str, Any]] = []
    invalid_rows = 0
    outside_moscow = 0
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
        valid.append(dict(value))
    if not valid:
        raise ValueError("no valid Moscow KartaView frames are available for sampling")

    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in valid:
        grouped[_area_id(row)][str(row["sequence_id"])].append(row)
    sampled: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    for area, sequences in grouped.items():
        for sequence, rows in sequences.items():
            sampled[area][sequence] = _sample_sequence(
                rows,
                min_spacing_m=min_spacing_m,
                heading_diversity_deg=heading_diversity_deg,
                min_heading_spacing_m=min_heading_spacing_m,
                max_per_sequence=max_per_sequence,
            )
    selected = _balanced_take(sampled, max_records)
    selected.sort(key=lambda row: (_area_id(row), str(row["sequence_id"]), _sequence_index(row)))
    write_json(output_json, selected)

    canonical = json.dumps(selected, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    area_counts = Counter(_area_id(row) for row in selected)
    sequence_counts = Counter(str(row["sequence_id"]) for row in selected)
    report: dict[str, Any] = {
        "schema_version": 1,
        "input_json": str(input_json),
        "output_json": str(output_json),
        "input_records": len(payload),
        "valid_moscow_records": len(valid),
        "invalid_rows": invalid_rows,
        "outside_moscow_rows": outside_moscow,
        "candidate_areas": len(grouped),
        "candidate_sequences": sum(len(value) for value in grouped.values()),
        "sampled_before_global_cap": sum(len(rows) for values in sampled.values() for rows in values.values()),
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
    parser = argparse.ArgumentParser(description="Spatially sample expanded KartaView sequence frames")
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--max-records", type=int, default=1_200)
    parser.add_argument("--min-spacing-m", type=float, default=45.0)
    parser.add_argument("--heading-diversity-deg", type=float, default=60.0)
    parser.add_argument("--min-heading-spacing-m", type=float, default=15.0)
    parser.add_argument("--max-per-sequence", type=int, default=75)
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
