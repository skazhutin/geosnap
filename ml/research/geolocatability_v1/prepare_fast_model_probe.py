"""Freeze an outcome-blind, geographically held-out image input for fast models."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from sklearn.model_selection import GroupShuffleSplit

from .common import json_once, revision, sha256, write_once
from .validate_small_encoder import PILOT, V1, read_rows

OUT = V1.parent / "geolocatability_fast_models_v1_20260929"


def main() -> None:
    frame, freeze = read_rows()
    valid = frame[frame.annotation_status == "valid"].copy()
    sample = json.loads((PILOT / "quick20_private_sample.json").read_text())
    token_to_id = {item["token"]: item["query_id"] for item in sample["items"]}
    human = [json.loads(line) for line in
             (PILOT / "quick20_rater_reviewer1.jsonl").read_text().splitlines()]
    human_ids = [token_to_id[item["token"]] for item in human]
    human_groups = set(valid.set_index("query_id").loc[human_ids].evaluation_geo_group_id)
    nonhuman = valid[~valid.evaluation_geo_group_id.isin(human_groups)].reset_index(drop=True)
    groups = nonhuman.evaluation_geo_group_id.to_numpy()
    train, test = next(GroupShuffleSplit(n_splits=1, test_size=0.2,
                                          random_state=20260929).split(nonhuman, groups=groups))
    assert len(train) == 828 and len(test) == 220
    assert not set(groups[train]) & set(groups[test])
    input_rows = {r["query_id"]: r for r in
                  (json.loads(line) for line in (V1 / "vlm_input_manifest_1024.jsonl").read_text().splitlines())}
    ids = nonhuman.iloc[test].query_id.tolist()
    assert len(ids) == len(set(ids)) == 220
    rows = [{k: input_rows[query_id][k] for k in
             ("query_id", "vlm_image_path", "vlm_image_sha256", "original_image_sha256")}
            for query_id in ids]
    assert all(Path(row["vlm_image_path"]).is_file() for row in rows)
    lines = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    target = OUT / "holdout220_image_input.jsonl"
    write_once(target, lines.encode())
    # Fixed order independent of Qwen scores, human scores, and GeoSnap outcomes.
    speed_ids = sorted(ids, key=lambda value: hashlib.sha256(value.encode()).hexdigest())[:32]
    speed_rows = [next(row for row in rows if row["query_id"] == query_id) for query_id in speed_ids]
    speed_target = OUT / "speed32_image_input.jsonl"
    write_once(speed_target, "".join(json.dumps(row, sort_keys=True) + "\n"
                                     for row in speed_rows).encode())
    json_once(OUT / "input_receipt.json", {
        "code_revision": revision(), "source_script_sha256": sha256(Path(__file__)),
        "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
        "split": "same geographically grouped 828/220 as validate_small_encoder.py, seed=20260929",
        "train_n": len(train), "holdout_n": len(test), "speed_n": len(speed_rows),
        "holdout_manifest_sha256": sha256(target), "speed_manifest_sha256": sha256(speed_target),
        "selection": "32 smallest SHA-256(query_id) values inside the fixed holdout",
        "outcome_data_accessed": False,
        "human_pilot_usage": "IDs only to exclude their geographic groups; no human labels used",
    })
    print(json.dumps({"holdout": len(rows), "speed": len(speed_rows),
                      "manifest": str(speed_target)}), flush=True)


if __name__ == "__main__":
    main()
