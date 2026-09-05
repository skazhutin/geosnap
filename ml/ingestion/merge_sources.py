"""Normalize source metadata into the one canonical reference manifest."""

from __future__ import annotations

import argparse
import json
import math
import re
import warnings
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json
from ml.ingestion.schema import canonical_record, manifest_dataframe, write_manifest

SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(value: str) -> str:
    sanitized = SAFE_FILENAME_RE.sub("_", value).strip("._")
    return sanitized or "unknown"


def normalize_record(
    source: str,
    row: dict[str, Any],
    *,
    city_id: str = "moscow",
    image_root: Path = Path("data/raw/images"),
) -> dict[str, Any] | None:
    source_image_id = row.get("source_image_id") or row.get("id")
    download_url = row.get("download_url") or row.get("image_url") or row.get("url")
    if source_image_id in (None, "") or not isinstance(download_url, str) or not download_url:
        return None

    source_name = source.strip().lower()
    if source_name not in {"mapillary", "kartaview"}:
        return None
    attribution = row.get("attribution")
    source_url = row.get("source_url")
    if not isinstance(attribution, str) or not isinstance(source_url, str):
        return None
    if source_name == "mapillary":
        author = attribution.removeprefix("Mapillary image by ").strip()
        if not attribution.startswith("Mapillary image by ") or not author:
            return None
        if not source_url.startswith("https://www.mapillary.com/app/") or "pKey=" not in source_url:
            return None
    elif (
        attribution != "© Grab and KartaView Contributors"
        or not source_url.startswith("https://kartaview.org/details/")
    ):
        return None
    reference_id = row.get("reference_id") or row.get("canonical_id")
    # canonical_record creates the stable UUID before the path is finalized.
    preliminary = canonical_record(
        source=source_name,
        source_image_id=source_image_id,
        city_id=city_id,
        lat=row.get("lat"),
        lon=row.get("lon"),
        image_path="",
        sequence_id=row.get("sequence_id") or row.get("sequenceId"),
        captured_at=row.get("captured_at") or row.get("timestamp"),
        heading=row.get("heading"),
        quality_score=row.get("quality_score"),
        license_name="CC BY-SA 4.0",
        attribution=attribution,
        source_url=source_url,
        metadata=row.get("metadata_json", row),
        download_url=download_url,
        reference_id=str(reference_id) if reference_id else None,
    )
    preliminary["image_path"] = str(
        image_root / source_name / f"{preliminary['id']}_{safe_filename(str(source_image_id))}.jpg"
    )
    return preliminary


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6_371_000.0
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
    """Deprecated compatibility shim; merge no longer deletes nearby views."""
    if max_per_cluster < 1:
        raise ValueError("max_per_cluster must be >= 1")
    if radius_m < 0:
        raise ValueError("radius_m must be >= 0")
    warnings.warn(
        "spatial caps before quality/heading analysis are disabled; use ml.cleaning.deduplicate",
        DeprecationWarning,
        stacklevel=2,
    )
    return list(rows)


def _write_markdown_report(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sources = summary.get("sources", {})
    lines = [
        "# Ingestion merge report",
        "",
        f"- Source rows read: {summary['source_rows_read']}",
        f"- Normalized rows: {summary['normalized_rows']}",
        f"- Invalid rows: {summary['invalid_rows']}",
        f"- Duplicate source IDs: {summary['duplicate_source_ids']}",
        "",
        "## Sources",
        "",
        "| Source | References |",
        "|---|---:|",
    ]
    lines.extend(f"| {source} | {count} |" for source, count in sorted(sources.items()))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    mapillary_json: Path | Sequence[Path],
    kartaview_json: Path | Sequence[Path],
    output_manifest: Path,
    dedup_radius_m: float = 7.0,
    max_per_cluster: int = 2,
    *,
    city_id: str = "moscow",
    report_json: Path | None = None,
    report_markdown: Path | None = None,
    image_root: Path = Path("data/raw/images"),
) -> dict[str, Any]:
    if max_per_cluster < 1:
        raise ValueError("max_per_cluster must be >= 1")
    if dedup_radius_m < 0:
        raise ValueError("dedup_radius_m must be >= 0")

    rows: list[dict[str, Any]] = []
    seen_reference_ids: set[str] = set()
    source_rows_read = 0
    invalid_rows = 0
    duplicate_source_ids = 0
    source_counts: Counter[str] = Counter()
    def source_paths(value: Path | Sequence[Path]) -> tuple[Path, ...]:
        return (value,) if isinstance(value, Path) else tuple(Path(path) for path in value)

    inputs = {
        "mapillary": source_paths(mapillary_json),
        "kartaview": source_paths(kartaview_json),
    }
    if not all(inputs.values()):
        raise ValueError("at least one JSON input is required for each live source")
    for source, paths in inputs.items():
        for path in paths:
            if not path.is_file():
                raise FileNotFoundError(f"missing {source} input: {path}")
            payload = read_json(path, default=None)
            if not isinstance(payload, list):
                raise ValueError(f"{path} must contain a JSON array")
            for item in payload:
                source_rows_read += 1
                if not isinstance(item, dict):
                    invalid_rows += 1
                    continue
                try:
                    normalized = normalize_record(source, item, city_id=city_id, image_root=image_root)
                except (TypeError, ValueError):
                    normalized = None
                if normalized is None:
                    invalid_rows += 1
                    continue
                if normalized["id"] in seen_reference_ids:
                    duplicate_source_ids += 1
                    continue
                seen_reference_ids.add(normalized["id"])
                rows.append(normalized)
                source_counts[source] += 1

    manifest = manifest_dataframe(rows)
    write_manifest(manifest, output_manifest, allow_empty=True)
    summary: dict[str, Any] = {
        "source_rows_read": source_rows_read,
        "normalized_rows": len(manifest),
        "invalid_rows": invalid_rows,
        "duplicate_source_ids": duplicate_source_ids,
        "spatial_pre_quality_removals": 0,
        "sources": dict(sorted(source_counts.items())),
        "source_files": {
            source: [str(path) for path in paths] for source, paths in sorted(inputs.items())
        },
        "output_manifest": str(output_manifest),
    }
    json_path = report_json or output_manifest.with_suffix(".report.json")
    markdown_path = report_markdown or output_manifest.with_suffix(".report.md")
    write_json(json_path, summary)
    _write_markdown_report(markdown_path, summary)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge raw source metadata into a canonical manifest")
    parser.add_argument("--mapillary-json", action="append", type=Path)
    parser.add_argument("--kartaview-json", action="append", type=Path)
    parser.add_argument("--output-manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--city-id", default="moscow")
    parser.add_argument("--image-root", default="data/raw/images")
    # Kept so old commands fail safely instead of silently changing behavior.
    parser.add_argument("--dedup-radius-m", type=float, default=7.0)
    parser.add_argument("--max-per-cluster", type=int, default=2)
    parser.add_argument("--report-json")
    parser.add_argument("--report-markdown")
    args = parser.parse_args()

    run(
        mapillary_json=args.mapillary_json or [Path("data/raw/mapillary_raw.json")],
        kartaview_json=args.kartaview_json or [Path("data/raw/kartaview_raw.json")],
        output_manifest=Path(args.output_manifest),
        dedup_radius_m=args.dedup_radius_m,
        max_per_cluster=args.max_per_cluster,
        city_id=args.city_id,
        report_json=Path(args.report_json) if args.report_json else None,
        report_markdown=Path(args.report_markdown) if args.report_markdown else None,
        image_root=Path(args.image_root),
    )


if __name__ == "__main__":
    main()
