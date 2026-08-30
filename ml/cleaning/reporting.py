"""Atomic JSON/Markdown reporting for cleaning stages."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json
from ml.ingestion.schema import read_manifest

DEFAULT_REPORT_PATH = Path("data/processed/cleaning_report.json")


def update_cleaning_report(report_path: Path = DEFAULT_REPORT_PATH, **values: Any) -> dict[str, Any]:
    payload = read_json(report_path, default={})
    if not isinstance(payload, dict):
        payload = {}
    payload.update(values)
    before = payload.get("before_clean")
    after_clean = payload.get("after_clean")
    after_quality = payload.get("after_quality")
    after_dedup = payload.get("after_dedup")
    payload["removed_clean"] = (
        before - after_clean if isinstance(before, int) and isinstance(after_clean, int) else None
    )
    payload["removed_quality"] = (
        after_clean - after_quality if isinstance(after_clean, int) and isinstance(after_quality, int) else None
    )
    payload["removed_dedup"] = (
        after_quality - after_dedup if isinstance(after_quality, int) and isinstance(after_dedup, int) else None
    )
    write_json(report_path, payload)
    markdown = report_path.with_suffix(".md")
    lines = ["# Cleaning reduction report", ""]
    lines.extend(f"- {key.replace('_', ' ').title()}: {value}" for key, value in sorted(payload.items()))
    markdown.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def initialize_cleaning_report_from_manifest(
    report_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    """Start a fresh cleaning reduction report from one already-gated manifest.

    This is useful after an upstream publication gate (such as an exact AOI
    filter) has intentionally removed rows for a reason that must not be
    mislabeled as a quality-filter removal.
    """

    count = int(len(read_manifest(manifest_path, allow_empty=True)))
    return update_cleaning_report(
        report_path,
        before_clean=count,
        after_clean=count,
        after_quality=None,
        after_dedup=None,
    )


def rebase_cleaning_report_from_manifest(
    report_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    """Rebase an existing report while retaining completed downstream stages.

    This repairs an interrupted pipeline that began its quality stages before
    an upstream publication gate was introduced.  The gate's exclusions remain
    provenance/scope facts, while quality and deduplication retain their actual
    completed counts.
    """

    payload = read_json(report_path, default={})
    if not isinstance(payload, dict):
        payload = {}
    count = int(len(read_manifest(manifest_path, allow_empty=True)))
    return update_cleaning_report(
        report_path,
        before_clean=count,
        after_clean=count,
        after_quality=payload.get("after_quality"),
        after_dedup=payload.get("after_dedup"),
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Initialize a cleaning reduction report from a manifest")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline-manifest", type=Path, required=True)
    parser.add_argument(
        "--preserve-downstream",
        action="store_true",
        help="rebase a completed report while retaining quality/dedup stage counts",
    )
    args = parser.parse_args()
    operation = (
        rebase_cleaning_report_from_manifest
        if args.preserve_downstream
        else initialize_cleaning_report_from_manifest
    )
    result = operation(args.report, args.baseline_manifest)
    print(result)


if __name__ == "__main__":
    main()
