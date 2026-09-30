"""Evaluate frozen zero-shot image/text contrast scores on grouped Qwen holdout."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupShuffleSplit

from .common import json_once, sha256
from .score_semantic_prompts import OUT
from .validate_small_encoder import PILOT, read_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=("siglip2_base", "mobileclip2_s0"))
    args = parser.parse_args()
    scores_file = OUT / f"semantic_prompt_scores_{args.model}.jsonl"
    receipt = json.loads((OUT / f"semantic_prompt_scores_{args.model}_receipt.json").read_text())
    if sha256(scores_file) != receipt["scores_sha256"]:
        raise RuntimeError("Zero-shot scores changed")
    scores = {r["query_id"]: r["positive_minus_negative_mean"] for r in
              (json.loads(line) for line in scores_file.read_text().splitlines())}
    assert len(scores) == 1184
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
    held = nonhuman.iloc[test]
    x = np.array([scores[query_id] for query_id in held.query_id], dtype=np.float64)
    y = held.geolocatability_median.to_numpy(dtype=np.float64)
    result = {"model": args.model, "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
              "scores_sha256": receipt["scores_sha256"], "holdout_n": len(held),
              "prompt_score_spearman_with_qwen": round(float(spearmanr(x, y).statistic), 6),
              "qwen_low_auc": round(float(roc_auc_score(y <= 0.4, -x)), 6),
              "score_range": [round(float(x.min()), 6), round(float(x.max()), 6)],
              "fitted_parameters": False, "outcome_data_accessed": False,
              "limitation": "Qwen pseudo-label agreement, not human-ground-truth defect detection"}
    json_once(OUT / f"semantic_prompt_{args.model}_grouped_holdout_report.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
