"""Resume the second bounded acquisition, encoding, and fixed pipeline transfer."""
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
    save(LOCAL / "live_expansion2_queue_status.json", value)
    print(json.dumps(value), flush=True)


def run():
    *_, night = paths()
    out = night / "live_expansion2"
    with (LOCAL / "live_expansion2_queue.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        phases = (("night_live_expansion", ["download", "--limit", "6000"]),
                  ("night_live_expansion", ["audit", "--limit", "6000"]),
                  ("night_live_expansion_eval", []),
                  ("night_pipeline_added", ["live_expansion2"]))
        for module, arguments in phases:
            if (out / "download.closed.json").exists() or (out / "closed.json").exists():
                state("closed", stage=str(out))
                return
            state("running", module=module, arguments=arguments)
            suffix = arguments[0] if arguments else "run"
            with (LOCAL / f"{module}_{suffix}.log").open("a") as log:
                result = subprocess.run([sys.executable, "-u", "-m", f"ml.research.{module}", *arguments],
                    cwd=WORKSPACE, stdout=log, stderr=subprocess.STDOUT, check=False)
            if result.returncode:
                if (out / "download.closed.json").exists():
                    state("closed_failed_network_smoke")
                    return
                raise RuntimeError(f"{module} failed; inspect checkpoint before resuming")
        state("completed")


if __name__ == "__main__":
    try:
        run()
    except BaseException as exc:
        state("failed_needs_review", error=repr(exc))
        raise
