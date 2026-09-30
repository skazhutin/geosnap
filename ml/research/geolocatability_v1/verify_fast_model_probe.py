"""Verify write-once model-probe artifacts and frozen production without localization."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from .common import json_once, sha256, verify_production
from .extract_semantic_probe import OUT

QUERY_MANIFEST = OUT.parent / "geolocatability_v1_20260928/vlm_input_manifest_1024.jsonl"
GALLERY = OUT.parent / "geographic_v8_20260928/staged/gallery.parquet"
PROMPT_MODELS = ("smolvlm2_256m", "smolvlm2_500m", "lfm25_vl_450m_4bit")
SEMANTIC_MODELS = ("mobileclip2_s0", "siglip2_base")
FORBIDDEN = {"gt_lat", "gt_lon", "lat", "lon", "baseline_correct", "failure_bucket",
             "sage_score", "candidate_rank", "localization_error"}


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def main() -> None:
    source = json.loads((OUT / "input_receipt.json").read_text())
    speed_path = OUT / "speed32_image_input.jsonl"
    holdout_path = OUT / "holdout220_image_input.jsonl"
    assert sha256(speed_path) == source["speed_manifest_sha256"]
    assert sha256(holdout_path) == source["holdout_manifest_sha256"]
    speed, holdout = read_jsonl(speed_path), read_jsonl(holdout_path)
    assert len(speed) == len({r["query_id"] for r in speed}) == 32
    assert len(holdout) == len({r["query_id"] for r in holdout}) == 220
    assert {r["query_id"] for r in speed} <= {r["query_id"] for r in holdout}
    assert not (set().union(*(row.keys() for row in speed + holdout)) & FORBIDDEN)
    all_ids = [row["query_id"] for row in read_jsonl(QUERY_MANIFEST)]
    assert len(all_ids) == len(set(all_ids)) == 1184

    prompt = {}
    for name in PROMPT_MODELS:
        summary_path = OUT / f"prompt_vlm_{name}_speed32_summary.json"
        summary = json.loads(summary_path.read_text())
        assert summary["n"] == summary["ok"] == summary["parsed_n"] == 32
        assert summary["input_manifest_sha256"] == source["speed_manifest_sha256"]
        for row in speed:
            path = OUT / "prompt_vlm_raw" / name / f"{row['query_id']}.json"
            assert sha256(path) == summary["raw_sha256"][row["query_id"]]
            answer = json.loads(path.read_text())
            assert answer["query_id"] == row["query_id"]
            assert answer["image_sha256"] == row["vlm_image_sha256"]
            assert answer["status"] == "ok"
        prompt[name] = {"summary_sha256": sha256(summary_path),
                        "images_per_second": summary["images_per_second"]}

    semantic = {}
    for name in SEMANTIC_MODELS:
        feature_path = OUT / f"query_features_{name}.npz"
        with np.load(feature_path) as array:
            ids = array["query_id"].tolist()
            values = array["feature"]
        assert ids == all_ids and np.isfinite(values).all()
        chunks = OUT / "semantic_features" / name
        for start in range(0, 1184, 32):
            stem = f"{start:04d}-{start + 31:04d}"
            receipt = json.loads((chunks / f"{stem}.json").read_text())
            assert receipt["feature_sha256"] == sha256(chunks / f"{stem}.npz")
            assert receipt["start"] == start and receipt["end_exclusive"] == start + 32
        report_path = OUT / f"{name}_grouped_holdout_report.json"
        report = json.loads(report_path.read_text())
        assert report["holdout_n"] == 220 and report["train_n"] == 828
        assert report["feature_sha256"] == sha256(feature_path)
        assert report["teacher_annotation_sha256"] == source["teacher_annotation_sha256"]
        score_path = OUT / f"semantic_prompt_scores_{name}.jsonl"
        score_receipt = json.loads((OUT / f"semantic_prompt_scores_{name}_receipt.json").read_text())
        assert sha256(score_path) == score_receipt["scores_sha256"]
        assert [r["query_id"] for r in read_jsonl(score_path)] == all_ids
        semantic[name] = {"features_sha256": sha256(feature_path),
                          "holdout_report_sha256": sha256(report_path),
                          "zero_shot_scores_sha256": sha256(score_path)}

    gallery_hash = sha256(GALLERY)
    for name, count in (("mobileclip2_s0", 256), ("siglip2_base", 2048)):
        receipt = json.loads((OUT / f"{name}_gallery_speed_{count}_b16.json").read_text())
        assert receipt["gallery_manifest_sha256"] == gallery_hash
        assert receipt["count"] == count
    result = {"production": verify_production(), "query_count": 1184,
              "gallery_manifest_sha256": gallery_hash,
              "input_receipt_sha256": sha256(OUT / "input_receipt.json"),
              "prompt_models": prompt, "semantic_models": semantic,
              "localization_outcomes_accessed": False,
              "gallery_or_production_modified": False}
    destination = OUT / "final_integrity.json"
    json_once(destination, result)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
