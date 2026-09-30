"""Continue the registered night queue immediately, without a heartbeat gap."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time

from ml.research.gallery_scale_storage import WORKSPACE, save
from ml.research.night_v7 import LOCAL, paths


def state(phase, **extra):
    value = {"pid": os.getpid(), "phase": phase, "updated": time.time(), **extra}
    save(LOCAL / "followthrough_status.json", value)
    print(json.dumps(value), flush=True)


def invoke(module, *args):
    state("running_registered_worker", module=module, arguments=list(args))
    with (LOCAL / (module.rsplit(".", 1)[-1] + ".log")).open("a") as log:
        result = subprocess.run([sys.executable, "-u", "-m", module, *args], cwd=WORKSPACE,
                                stdout=log, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        raise RuntimeError(f"{module} exited {result.returncode}; inspect its existing checkpoint before resuming")


def run():
    *_, out = paths()
    with (LOCAL / "followthrough.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state("waiting_for_initial_fol_queue")
        with (LOCAL / "dispatcher.lock").open("a") as dispatcher:
            fcntl.flock(dispatcher, fcntl.LOCK_EX)
            # A verified CPU-only FoL continuation frees the GPU for the
            # already-registered query experiment, without sharing encoders.
            if (out / "fol_cpu/intent.json").exists():
                state("waiting_for_verified_fol_cpu_handoff")
                while not (out / "fol_cpu/ready.json").exists():
                    if (out / "fol_cpu/failure.json").exists():
                        raise RuntimeError("FoL CPU handoff failed; inspect its preserved checkpoint")
                    time.sleep(2)
            if not (out / "fol.done.json").exists() and not (out / "fol.closed.json").exists():
                if not (out / "fol_cpu/ready.json").exists():
                    raise RuntimeError("initial FoL queue stopped without a completed experiment or verified CPU handoff")
            # Cheap fixed-query scaling precedes the costly optional extension.
            # Top100 adds36k images/~64GB; review the actual top20/scale results
            # and remaining night budget before committing that larger job.
            invoke("ml.research.night_query_scale")
        state("waiting_for_parallel_cpu_results")
        for name in ("place", "multiview"):
            with (LOCAL / f"{name}.lock").open("a") as branch:
                fcntl.flock(branch, fcntl.LOCK_EX)
        if (out / "cpu_place/place.started.json").exists():
            if not (out / "cpu_place/done.json").exists():
                raise RuntimeError("place worker stopped before completion; inspect checkpoint")
            invoke("ml.research.night_place", "publish")
        if (out / "aux_multiview/multiview.started.json").exists() and not (out / "aux_multiview/done.json").exists():
            raise RuntimeError("auxiliary multiview worker stopped before completion; inspect checkpoint")
        state("registered_queue_finished_awaiting_research_review", final_or_production_access=False,
              fol100_requires_result_and_cost_review=True)


if __name__ == "__main__":
    try:
        run()
    except BaseException as exc:
        state("failed_needs_review", error=repr(exc))
        raise
