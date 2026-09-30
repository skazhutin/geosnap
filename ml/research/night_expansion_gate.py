"""A predeclared stopping rule for one further reference-only data tranche."""
import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ml.research.gallery_scale import fixed_development
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_v7 import paths

RULE = {"baseline": "scale_mean_context_added", "candidate": "scale_mean_context_expanded2",
        "baseline_correct100": 376, "query_count": 1184, "minimum_gain_pp": .5,
        "require_positive_paired_ci_lower": True, "maximum_new_references": 6000,
        "latest_network_start": "2026-09-10T04:45:00+03:00",
        "inference": "same mean322/504 + context30/mix0.5; only reference gallery differs",
        "calibration_final_access": False, "query_coordinates_for_acquisition": False}


def passes(comparison):
    return comparison["gain_pp"] >= RULE["minimum_gain_pp"] and comparison["ci95_pp"][0] > 0


def plan():
    *_, night = paths()
    out = night / "expansion3_gate.plan.json"
    source = night / "results" / f"{RULE['baseline']}_rows.json"
    value = {"rule": RULE, "source_sha256": digest(Path(__file__)), "baseline_rows_sha256": digest(source)}
    if out.exists():
        if json.loads(out.read_text()) != value:
            raise RuntimeError("registered data-expansion stopping rule changed")
    else:
        if (night / "cpu_pipeline_added/live_expansion2/done.json").exists():
            raise RuntimeError("third-tranche rule must be registered before second pipeline outcomes")
        save(out, value)
    return night, value


def evaluate():
    night, registered = plan()
    source = night / "cpu_pipeline_added/live_expansion2"
    if not (source / "published.json").exists():
        raise RuntimeError("finish and publish the second fixed pipeline before deciding further expansion")
    _, gc, *_ = paths()
    queries = fixed_development(gc)
    rows = [json.loads((night / "results" / f"{RULE[key]}_rows.json").read_text()) for key in ("baseline", "candidate")]
    if any(r["query_ids"] != queries.id.tolist() for r in rows) or len(queries) != RULE["query_count"]:
        raise RuntimeError("data-expansion decision population changed")
    if sum(error <= 100 for error in rows[0]["errors_m"]) != RULE["baseline_correct100"]:
        raise RuntimeError("first fixed pipeline baseline changed")
    comparison = paired_group_bootstrap(rows[0]["errors_m"], rows[1]["errors_m"], queries.h3_coarse.astype(str).tolist())
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    time_available = now < datetime.fromisoformat(RULE["latest_network_start"])
    value = {"registered_rule_sha256": digest(night / "expansion3_gate.plan.json"),
        "candidate_rows_sha256": digest(night / "results" / f"{RULE['candidate']}_rows.json"),
        "baseline_rows_sha256": registered["baseline_rows_sha256"], "paired_data_gain": comparison,
        "data_gain_gate_passed": passes(comparison), "time_gate_passed": time_available,
        "eligible": passes(comparison) and time_available, "evaluated_at": now.isoformat(),
        "network_or_encoding_started_by_this_gate": False,
        "note": "passing only makes one bounded reference-only tranche eligible; it does not open heldout data"}
    save(night / "expansion3_gate.json", value)
    print(json.dumps(value))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["plan", "evaluate"])
    action = parser.parse_args().action
    (plan if action == "plan" else evaluate)()
