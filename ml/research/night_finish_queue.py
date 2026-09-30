"""Finish already registered research work and render its verified result, without new trials."""
import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import LOCAL, paths


def ready(third):
    if (third / "closed.json").exists():
        return True
    queue = third / "evaluation_queue"
    failures = [p for p in (third / "failure.json", queue / "failure.json") if p.exists()]
    if failures:
        raise RuntimeError("third tranche requires explicit failure review before final verification")
    return (third / "audit.done.json").exists() and (
        ((third / "evaluation.done.json").exists() and (queue / "done.json").exists())
        or (queue / "closed.json").exists())


def licensed_ready(night):
    stage = night / "licensed_third"
    if not (stage / "intent.json").exists():
        return not (night / "finish_queue/licensed_third_extension.registration.json").exists()
    if (stage / "closed.json").exists():
        return True
    if (stage / "failure.json").exists():
        raise RuntimeError("third source-compatible assessment requires explicit failure review")
    return (stage / "done.json").exists()


def run():
    *_, night = paths()
    out = night / "finish_queue"
    out.mkdir(exist_ok=True)
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    with (LOCAL / "finish_queue.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sources = [Path(__file__), *(WORKSPACE / "ml/research" / name for name in (
            "night_v7_verify.py", "night_quarantine_report.py", "night_report_artifacts.py", "night_licensed_third.py"))]
        contract = {str(p.resolve()): digest(p) for p in sources}
        intent = out / "intent.json"
        if intent.exists() and json.loads(intent.read_text())["sources"] != contract:
            raise RuntimeError("finish queue source contract changed; explicit review required")
        if not intent.exists():
            save(intent, {"sources": contract, "started": time.time(), "new_experiments": False,
                "calibration_final_access": False, "production_changes": False})

        def state(phase, **extra):
            value = dict(phase=phase, updated=time.time(), pid=os.getpid(), **extra)
            save(out / "status.json", value)
            save(LOCAL / "finish_queue_status.json", value)
            print(json.dumps(value), flush=True)

        def validate():
            if any(digest(Path(p)) != sha for p, sha in contract.items()):
                raise RuntimeError("final verification/report implementation changed after scheduling")

        try:
            while not (ready(night / "reference_tranche3") and licensed_ready(night)):
                validate()
                state("waiting_for_registered_third_tranche")
                time.sleep(30)
            for phase, module, artifact in (
                ("verify", "night_v7_verify", night / "candidate_frozen.json"),
                ("render", "night_report_artifacts", night / "report/receipt.json"),
            ):
                validate()
                marker = out / f"{phase}.done.json"
                if marker.exists():
                    if digest(artifact) != json.loads(marker.read_text())["artifact_sha256"]:
                        raise RuntimeError("completed final verification artifact changed")
                    continue
                state(phase)
                with (out / f"{phase}.log").open("ab", buffering=0) as log:
                    subprocess.run([sys.executable, "-u", "-m", f"ml.research.{module}"], cwd=WORKSPACE,
                        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, check=True)
                validate()
                save(marker, {"completed": time.time(), "artifact_sha256": digest(artifact)})
            state("completed_pending_human_readable_report_review")
            save(out / "done.json", {"completed": time.time(), "sources": contract,
                "candidate_frozen_sha256": digest(night / "candidate_frozen.json"),
                "report_receipt_sha256": digest(night / "report/receipt.json")})
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "error": repr(exc)})
            state("failed_needs_review", error=repr(exc))
            raise


if __name__ == "__main__":
    run()
