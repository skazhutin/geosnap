"""Atomic JSON/Markdown reporting for cleaning stages."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json

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
