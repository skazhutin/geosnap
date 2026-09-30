"""Finish the blind freeze and separate outcome analysis when the local run ends."""
from __future__ import annotations

import json
import subprocess
import sys
import time

from .common import OUT, now
from .run_chunks import status

STAGES = ("recover_missing_reasons", "freeze", "analyze", "contact_sheets", "report", "verify")


def run() -> None:
    previous = (-1, -1)
    unchanged_since = time.monotonic()
    while True:
        complete, failed = status()
        current = (complete, failed)
        if current != previous:
            print(json.dumps({"at": now(), "complete": complete, "failed": failed}), flush=True)
            previous = current
            unchanged_since = time.monotonic()
        if complete + failed == 1184:
            break
        if time.monotonic() - unchanged_since > 43200:
            raise RuntimeError("Annotation stalled for over twelve hours; not freezing incomplete evidence")
        time.sleep(60)
    for stage in STAGES:
        print(json.dumps({"at": now(), "stage": stage, "status": "starting"}), flush=True)
        interpreter = (OUT / "runtime/bin/python") if stage == "recover_missing_reasons" else sys.executable
        subprocess.run([str(interpreter), "-m", f"ml.research.geolocatability_v1.{stage}"], check=True)
        print(json.dumps({"at": now(), "stage": stage, "status": "done"}), flush=True)
    print(json.dumps({"at": now(), "status": "all_deliverables_complete",
                      "report": str(OUT.parents[2] / "docs/geolocatability_v1_report.md")}), flush=True)


if __name__ == "__main__":
    run()
