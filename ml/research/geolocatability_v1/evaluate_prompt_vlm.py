"""Post-freeze comparison of tiny prompted VLMs with Qwen pseudo-labels."""
from __future__ import annotations

import argparse
import json

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_error, roc_auc_score

from .common import json_once, sha256
from .validate_small_encoder import V1

OUT = V1.parent / "geolocatability_fast_models_v1_20260929"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True,
                        choices=("smolvlm2_256m", "smolvlm2_500m", "lfm25_vl_450m_4bit"))
    args = parser.parse_args()
    summary = json.loads((OUT / f"prompt_vlm_{args.model}_speed32_summary.json").read_text())
    input_rows = [json.loads(line) for line in (OUT / "speed32_image_input.jsonl").read_text().splitlines()]
    if len(input_rows) != summary["n"]:
        raise RuntimeError("Speed input count changed")
    raw_dir = OUT / "prompt_vlm_raw" / args.model
    outputs = []
    for row in input_rows:
        path = raw_dir / f"{row['query_id']}.json"
        if sha256(path) != summary["raw_sha256"][row["query_id"]]:
            raise RuntimeError(f"Raw output changed: {path}")
        outputs.append(json.loads(path.read_text()))
    freeze = json.loads((V1 / "annotation_freeze.json").read_text())
    teacher_file = V1 / "geolocatability_annotations.jsonl"
    if sha256(teacher_file) != freeze["canonical_jsonl_sha256"]:
        raise RuntimeError("Qwen pseudo-label artifact changed")
    teacher = {r["query_id"]: r for r in
               (json.loads(line) for line in teacher_file.read_text().splitlines())}
    pairs = [(r["parsed_score"] / 4, teacher[r["query_id"]]["geolocatability_median"])
             for r in outputs if r["status"] == "ok" and r["parsed_score"] is not None]
    score = np.asarray([first for first, _ in pairs], dtype=np.float64)
    qwen = np.asarray([second for _, second in pairs], dtype=np.float64)
    result = {"model": args.model, "n": len(input_rows), "valid_n": len(pairs),
              "input_manifest_sha256": summary["input_manifest_sha256"],
              "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
              "teacher_mae": round(float(mean_absolute_error(qwen, score)), 6) if len(pairs) else None,
              "teacher_spearman": round(float(spearmanr(qwen, score).statistic), 6) if len(pairs) > 2 else None,
              "teacher_low_auc": round(float(roc_auc_score(qwen <= 0.4, -score)), 6)
              if len(pairs) > 2 and len(set(qwen <= 0.4)) == 2 else None,
              "speed_images_per_second": summary["images_per_second"],
              "outcome_data_accessed": False,
              "limitation": "32-image exploratory teacher agreement, not human-ground-truth accuracy"}
    json_once(OUT / f"prompt_vlm_{args.model}_teacher32_report.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
