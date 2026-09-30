"""Wait for the audited third tranche and run its single fixed evaluation once."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from ml.research import night_reference_tranche3 as audit
from ml.research import night_tranche3_eval as evaluation
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import WORKSPACE, digest, save


def stage():
    cfg, _, _, _, night = evaluator.paths()
    return cfg, night / "reference_tranche3"


def source_contract():
    modules = (sys.modules[__name__], audit, evaluation, evaluation.common, evaluation.context,
               evaluation.night_live_expansion_eval, evaluation.gallery_scale, evaluator)
    return {str(Path(module.__file__).resolve()): digest(Path(module.__file__)) for module in modules}


def assert_sources(expected):
    for filename, sha in expected.items():
        if digest(Path(filename)) != sha:
            raise RuntimeError(f"third evaluation queue source changed: {Path(filename).name}")


def register(out, cfg):
    value = {"sources": source_contract(), "cutoff": cfg["cutoff"],
        "action": "night_tranche3_eval run after third-tranche audit; no fourth tranche",
        "selection_or_download_started_by_queue": False,
        "preparation_does_not_count_as_evaluation_start": True}
    path = out / "evaluation_queue/started.json"
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise RuntimeError("third evaluation queue source or scheduling contract changed")
    else:
        save(path, value)
    return value


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "evaluation_queue/status.json", value)
    save(evaluator.LOCAL / "tranche3_queue_status.json", value)
    print(json.dumps(value), flush=True)


def available(path):
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(handle, fcntl.LOCK_UN)
    return True


def complete(out, expected):
    done_path = out / "evaluation.done.json"
    done = json.loads(done_path.read_text())
    if done["contract"]["source_sha256"] != expected["sources"][str(Path(evaluation.__file__).resolve())]:
        raise RuntimeError("completed third evaluation used a different implementation")
    for name, sha in done["artifacts"].items():
        if Path(name).name != name or digest(out / name) != sha:
            raise RuntimeError("completed third evaluation artifact changed")
    save(out / "evaluation_queue/done.json", {"completed": time.time(),
         "evaluation_done_sha256": digest(done_path), "queue_source_contract": expected})
    state(out, "tranche3_queue_completed")


def close(out, reason):
    save(out / "evaluation_queue/closed.json", {"closed": time.time(), "reason": reason,
         "new_evaluation_started": False})
    state(out, "tranche3_queue_closed", reason=reason)


def run():
    cfg, out = stage()
    out.mkdir(exist_ok=True)
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    with (evaluator.LOCAL / "tranche3_queue.lock").open("a") as own:
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        expected = register(out, cfg)
        try:
            while True:
                assert_sources(expected["sources"])
                if (out / "evaluation.done.json").exists():
                    complete(out, expected)
                    return
                if (out / "evaluation.closed.json").exists():
                    close(out, "evaluation explicitly closed; no restart")
                    return
                if (out / "evaluation_queue/closed.json").exists():
                    return
                if not evaluator.allowed_to_start(cfg, "evaluation", out):
                    close(out, "06:00 cutoff before actual new evaluation; waiting is not a start")
                    return
                if not (out / "audit.done.json").exists():
                    failed = [name for name in ("failure.json", "download.closed.json") if (out / name).exists()]
                    if failed:
                        raise RuntimeError("third acquisition/audit needs review: " + ", ".join(failed))
                    state(out, "tranche3_queue_waiting_audit")
                    time.sleep(30)
                    continue
                if not available(evaluator.LOCAL / "tranche3_evaluation.lock"):
                    state(out, "tranche3_queue_waiting_existing_evaluator")
                    time.sleep(30)
                    continue
                # Do not start the evaluator's preflight while another MPS job
                # owns the device. The evaluator still enforces its own lock and
                # checks cutoff after its full source/input validation.
                if not available(evaluator.LOCAL / "controller.lock"):
                    state(out, "tranche3_queue_waiting_gpu")
                    time.sleep(30)
                    continue
                if not evaluator.allowed_to_start(cfg, "evaluation", out):
                    close(out, "06:00 cutoff after queues, before child dispatch")
                    return
                assert_sources(expected["sources"])
                break
            log_path = out / "evaluation_queue/worker.log"
            with log_path.open("ab", buffering=0) as log:
                process = subprocess.Popen([sys.executable, "-u", "-m", "ml.research.night_tranche3_eval", "run"],
                    cwd=WORKSPACE, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
                save(out / "evaluation_queue/dispatched.json", {"dispatched": time.time(), "child_pid": process.pid,
                     "source_contract": expected, "log": str(log_path)})
                state(out, "tranche3_queue_evaluating", child_pid=process.pid)
                while process.poll() is None:
                    time.sleep(30)
                if process.returncode:
                    raise RuntimeError(f"third evaluator stopped with code{process.returncode}; resume only from its checkpoint after review")
            assert_sources(expected["sources"])
            if (out / "evaluation.done.json").exists():
                complete(out, expected)
            elif (out / "evaluation.closed.json").exists():
                close(out, "evaluator reached06:00 cutoff after input validation")
            else:
                raise RuntimeError("third evaluator exited without committed completion or explicit closure")
        except BaseException as exc:
            save(out / "evaluation_queue/failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            state(out, "tranche3_queue_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    run()
