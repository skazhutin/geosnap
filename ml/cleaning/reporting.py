"""Helpers for Stage 3 reduction report across cleaning scripts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_REPORT_PATH = Path("data/processed/cleaning_report.json")


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def update_cleaning_report(report_path: Path = DEFAULT_REPORT_PATH, **values: int) -> dict[str, int | None]:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, int | None] = {}
    if report_path.exists():
        payload = json.loads(report_path.read_text(encoding="utf-8"))

    for key, value in values.items():
        payload[key] = int(value)

    before_clean = _safe_int(payload.get("before_clean"))
    after_clean = _safe_int(payload.get("after_clean"))
    after_quality = _safe_int(payload.get("after_quality"))
    after_dedup = _safe_int(payload.get("after_dedup"))

    payload["removed_clean"] = (before_clean - after_clean) if before_clean is not None and after_clean is not None else None
    payload["removed_quality"] = (after_clean - after_quality) if after_clean is not None and after_quality is not None else None
    payload["removed_dedup"] = (after_quality - after_dedup) if after_quality is not None and after_dedup is not None else None

    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Stage 3 reduction summary:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    return payload
