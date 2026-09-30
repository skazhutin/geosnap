"""Publish progress from any GeoSnap research job through one atomic JSON file.

Usage in a job:
    publish("job-id", title="Extract descriptors", phase="Encoding", completed=1200, total=5000)
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RUN_FILES = ROOT / "data/evaluation/research_run_dashboard/runs"
RUN_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def publish(run_id: str, *, title: str, phase: str, completed: int, total: int,
            state: str = "running", unit: str = "элементов", description: str = "",
            rate_per_second: float | None = None, eta_seconds: float | None = None,
            note: str = "") -> Path:
    if not RUN_ID.fullmatch(run_id):
        raise ValueError("run_id must be lowercase letters, digits, hyphens or underscores")
    if not 0 <= completed <= total or total <= 0:
        raise ValueError("Require 0 <= completed <= total and total > 0")
    if state not in {"running", "waiting", "stalled", "stopped", "complete"}:
        raise ValueError("Invalid run state")
    RUN_FILES.mkdir(parents=True, exist_ok=True)
    path = RUN_FILES / f"{run_id}.json"
    value = {"id": run_id, "title": title, "phase": phase, "state": state,
             "completed": completed, "total": total, "unit": unit,
             "description": description, "rate_per_second": rate_per_second,
             "eta_seconds": eta_seconds, "note": note,
             "updated_at": datetime.now(UTC).isoformat()}
    temporary = path.with_suffix(f".json.tmp.{os.getpid()}")
    try:
        with temporary.open("x") as output:
            json.dump(value, output, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish one GeoSnap research progress update")
    parser.add_argument("--id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--completed", type=int, required=True)
    parser.add_argument("--total", type=int, required=True)
    parser.add_argument("--state", default="running")
    parser.add_argument("--unit", default="элементов")
    parser.add_argument("--description", default="")
    parser.add_argument("--rate-per-second", type=float)
    parser.add_argument("--eta-seconds", type=float)
    parser.add_argument("--note", default="")
    args = parser.parse_args()
    publish(args.id, title=args.title, phase=args.phase, completed=args.completed,
            total=args.total, state=args.state, unit=args.unit,
            description=args.description, rate_per_second=args.rate_per_second,
            eta_seconds=args.eta_seconds, note=args.note)


if __name__ == "__main__":
    main()
