"""Build the compact public coverage grid used by the GeoSnap map.

The output deliberately contains aggregate occupied cells, never individual
reference coordinates or source URLs. It is tied to the frozen production
gallery hash supplied on the command line.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

MOSCOW_BOUNDS = (55.1421745, 56.0212238, 36.8031012, 37.9674277)


def build_coverage(metadata_path: Path, gallery_sha256: str, *, bins: int = 20) -> dict[str, Any]:
    if bins < 1 or bins > 100:
        raise ValueError("bins must be between 1 and 100")
    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    lat_step = (max_lat - min_lat) / bins
    lon_step = (max_lon - min_lon) / bins
    cells: dict[tuple[int, int], dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "providers": set()}
    )
    total = 0

    with metadata_path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            try:
                row = json.loads(line)
                metadata = row["metadata"]
                lat = float(metadata["lat"])
                lon = float(metadata["lon"])
                provider = str(metadata["source"]).lower()
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"invalid reference metadata on line {line_number}") from exc
            if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                raise ValueError(f"reference outside frozen Moscow bounds on line {line_number}")
            x = min(bins - 1, int((lon - min_lon) / lon_step))
            y = min(bins - 1, int((lat - min_lat) / lat_step))
            cells[(x, y)]["count"] += 1
            cells[(x, y)]["providers"].add(provider)
            total += 1

    if total != 20_487:
        raise ValueError(f"expected 20,487 production references, found {total:,}")

    occupied_counts = sorted(cell["count"] for cell in cells.values())
    low_cutoff = occupied_counts[max(0, len(occupied_counts) // 3 - 1)]
    high_cutoff = occupied_counts[max(0, (len(occupied_counts) * 2) // 3 - 1)]
    features: list[dict[str, Any]] = []
    for (x, y), cell in sorted(cells.items(), key=lambda item: (item[0][1], item[0][0])):
        west = min_lon + x * lon_step
        east = west + lon_step
        south = min_lat + y * lat_step
        north = south + lat_step
        count = int(cell["count"])
        density = "sparse" if count <= low_cutoff else "medium" if count <= high_cutoff else "dense"
        providers = sorted(cell["providers"])
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "cell": f"{x}:{y}",
                    "references": count,
                    "provider_count": len(providers),
                    "providers": ", ".join(providers),
                    "density": density,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [round(west, 7), round(south, 7)],
                            [round(east, 7), round(south, 7)],
                            [round(east, 7), round(north, 7)],
                            [round(west, 7), round(north, 7)],
                            [round(west, 7), round(south, 7)],
                        ]
                    ],
                },
            }
        )

    return {
        "schema_version": 1,
        "kind": "geosnap-production-gallery-coverage-grid",
        "gallery_sha256": gallery_sha256,
        "reference_count": total,
        "grid": {
            "bins_per_axis": bins,
            "bounds_west_south_east_north": [min_lon, min_lat, max_lon, max_lat],
            "occupied_cells": len(features),
            "total_cells": bins * bins,
            "density_thresholds": {
                "sparse_max": low_cutoff,
                "medium_max": high_cutoff,
            },
        },
        "methodology": (
            "Counts of frozen production references in occupied cells of the existing 20x20 Moscow grid. "
            "Density is relative among occupied cells; coverage does not imply localization accuracy."
        ),
        "geojson": {"type": "FeatureCollection", "features": features},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--gallery-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bins", type=int, default=20)
    args = parser.parse_args()
    if len(args.gallery_sha256) != 64 or any(char not in "0123456789abcdef" for char in args.gallery_sha256):
        raise SystemExit("--gallery-sha256 must be a lowercase SHA-256")
    payload = build_coverage(args.metadata, args.gallery_sha256, bins=args.bins)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    print(
        f"wrote {args.output}: {payload['reference_count']} references, "
        f"{payload['grid']['occupied_cells']} occupied cells"
    )


if __name__ == "__main__":
    main()
