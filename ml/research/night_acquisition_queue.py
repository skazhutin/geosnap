"""Standalone bounded download -> CPU audit -> serialized GPU addition queue."""
import fcntl
import json
import os
import subprocess
import sys
import time

from ml.research.gallery_scale_storage import WORKSPACE, save
from ml.research.night_v7 import LOCAL, paths


def state(phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(LOCAL / "acquisition_queue_status.json", value)
    print(json.dumps(value), flush=True)


def run():
    *_, night = paths()
    with (LOCAL / "acquisition_queue.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for module, action in (("night_acquisition", "download"), ("night_gallery_addition", "audit"),
                               ("night_gallery_addition", "run")):
            state("running", module=module, action=action)
            with (LOCAL / f"{module}_{action}.log").open("a") as log:
                result = subprocess.run([sys.executable, "-u", "-m", f"ml.research.{module}", action],
                    cwd=WORKSPACE, stdout=log, stderr=subprocess.STDOUT, check=False)
            if result.returncode:
                if module == "night_acquisition" and (night / "reference_acquisition/download.closed.json").exists():
                    state("closed_failed_network_smoke_no_broad_download")
                    return
                raise RuntimeError(f"{module} {action} failed; inspect its committed checkpoint before resuming")
        state("completed")


if __name__ == "__main__":
    try:
        run()
    except BaseException as exc:
        state("failed_needs_review", error=repr(exc))
        raise
