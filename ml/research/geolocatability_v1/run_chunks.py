"""Restart the VLM every 20 images while preserving per-pass resume points."""
from __future__ import annotations

import json
import subprocess
import sys

from .common import OUT, now


def status() -> tuple[int, int]:
    rows = [json.loads(x) for x in (OUT / "annotation_input_manifest.jsonl").read_text().splitlines()]
    complete = failures = 0
    for row in rows:
        folder = OUT / "annotations/raw_qwen35_full1024" / row["query_id"]
        if all((folder / f"pass{n}.json").is_file() for n in (1, 2, 3)):
            complete += 1
        elif (folder / "failure.json").exists() or (folder / "recovery_failure.json").exists():
            failures += 1
    return complete, failures


def run() -> None:
    for chunk in range(1, 80):
        before = status()
        if sum(before) == 1184:
            print(json.dumps({"status": "finished", "complete": before[0],
                              "failures": before[1], "at": now()}), flush=True)
            return
        print(json.dumps({"chunk": chunk, "before": before, "at": now()}), flush=True)
        subprocess.run([sys.executable, "-m", "ml.research.geolocatability_v1.annotate",
                        "--limit", "20"], check=True)
        after = status()
        print(json.dumps({"chunk": chunk, "after": after, "at": now()}), flush=True)
        if sum(after) <= sum(before):
            raise RuntimeError("No new completed/failed images in chunk")
    raise RuntimeError("80 chunks insufficient for 1,184 images")


if __name__ == "__main__":
    run()
