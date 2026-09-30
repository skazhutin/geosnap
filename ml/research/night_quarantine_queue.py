"""Resume the audited data tranche, then decide expansion with a fixed gate."""
import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import LOCAL, paths

RULE = {"baseline": "quarantine_first_context", "candidate": "quarantine_expanded2_context",
        "minimum_gain_pp": .5, "require_paired_ci_lower_above_zero": True,
        "maximum_new_references": 6000, "latest_network_start": "2026-09-10T04:45:00+03:00",
        "common_quarantine": True, "same_1184_development_queries": True,
        "query_coordinates_for_acquisition": False, "calibration_final_access": False}


def status(phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(LOCAL / "quarantine_queue_status.json", value)
    print(json.dumps(value), flush=True)


def register(stage):
    from ml.research import night_quarantine_eval as evaluation

    target = stage / "expansion3_gate.plan.json"
    plan = {"rule": RULE, "source_sha256": digest(Path(__file__)),
            "evaluation_source_sha256": digest(Path(evaluation.__file__)),
            "audit_sha256": digest(stage / "audit.done.json"),
            "quarantine_sha256": digest(stage / "quarantine.receipt.json"),
            "scope": "same data-growth rule, applied to both commonly quarantined galleries; registered before new data outcomes"}
    if target.exists():
        if json.loads(target.read_text()) != plan:
            raise RuntimeError("registered common-quarantine data gate changed")
    else:
        if (stage / "evaluation/expanded2/done.json").exists():
            raise RuntimeError("register data gate before expanded outcomes")
        save(target, plan)
    return plan


def run():
    stage = paths()[-1] / "live_expansion2_quarantine"
    with (LOCAL / "quarantine_queue.lock").open("a") as own:
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        register(stage)
        for action in ("encode", "expanded"):
            if action == "expanded":
                status("waiting_for_common_baselines")
                while not (stage / "baseline.done.json").exists():
                    time.sleep(30)
            status(f"running_{action}")
            with (LOCAL / f"quarantine_{action}.log").open("ab") as log:
                result = subprocess.run([sys.executable, "-u", "-m", "ml.research.night_quarantine_eval", action],
                    cwd=WORKSPACE, stdout=log, stderr=subprocess.STDOUT)
            if result.returncode:
                status("failed_needs_review", action=action, returncode=result.returncode)
                raise RuntimeError("quarantined data continuation failed; inspect checkpoint before resume")
        board = json.loads((stage / "leaderboard.json").read_text())
        comparison = board["paired_same_architecture_data_gain"][RULE["candidate"]]
        now = datetime.now(ZoneInfo("Europe/Moscow"))
        data_gate = comparison["gain_pp"] >= RULE["minimum_gain_pp"] and comparison["ci95_pp"][0] > 0
        time_gate = now < datetime.fromisoformat(RULE["latest_network_start"])
        save(stage / "expansion3_gate.json", {"rule": RULE, "registered_plan_sha256": digest(stage / "expansion3_gate.plan.json"),
            "paired_data_gain": comparison, "data_gate_passed": data_gate, "time_gate_passed": time_gate,
            "eligible": data_gate and time_gate, "evaluated_at": now.isoformat(),
            "baseline_rows_sha256": digest(stage / "evaluation/first/context_rows.json"),
            "candidate_rows_sha256": digest(stage / "evaluation/expanded2/context_rows.json"),
            "additional_network_or_encoding_started": False})
        status("completed", next_data_tranche_eligible=data_gate and time_gate,
               best=board["records"][0]["name"], raw100=board["records"][0]["raw"]["accuracy_100m"])


if __name__ == "__main__":
    run()
