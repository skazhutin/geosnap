"""Freeze the validated SigLIP2-to-Qwen ridge head before gallery inference.

This uses only image features, frozen Qwen pseudo-labels, and geographic fold IDs.
It does not read GeoSnap predictions, candidate ranks, or localization outcomes.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import numpy as np
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import GroupShuffleSplit

from .common import json_once, revision, sha256, verify_production, write_once
from .extract_semantic_probe import MODELS
from .validate_small_encoder import PILOT, choose_alpha, pipeline, read_rows

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "data/evaluation/geolocatability_fast_models_v1_20260929"
OUT = ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929"
FEATURES = SOURCE / "query_features_siglip2_base.npz"
HEAD = OUT / "quality_head.npz"
RECEIPT = OUT / "quality_head_receipt.json"


def main() -> None:
    production = verify_production()
    source_report = json.loads((SOURCE / "siglip2_base_grouped_holdout_report.json").read_text())
    assert source_report["feature_sha256"] == sha256(FEATURES)
    with np.load(FEATURES) as cached:
        ids = cached["query_id"].tolist()
        vectors = cached["feature"].astype(np.float64)
    assert len(ids) == len(set(ids)) == 1184 and vectors.shape == (1184, 768)
    index = {query_id: position for position, query_id in enumerate(ids)}
    frame, freeze = read_rows()
    valid = frame[frame.annotation_status == "valid"].copy()
    sample = json.loads((PILOT / "quick20_private_sample.json").read_text())
    token_to_id = {item["token"]: item["query_id"] for item in sample["items"]}
    human = [json.loads(line) for line in
             (PILOT / "quick20_rater_reviewer1.jsonl").read_text().splitlines()]
    human_groups = set(valid.set_index("query_id").loc[
        [token_to_id[item["token"]] for item in human]].evaluation_geo_group_id)
    nonhuman = valid[~valid.evaluation_geo_group_id.isin(human_groups)].reset_index(drop=True)
    x = np.stack([vectors[index[item]] for item in nonhuman.query_id]).astype(np.float32)
    y = nonhuman.geolocatability_median.to_numpy(dtype=np.float32)
    groups = nonhuman.evaluation_geo_group_id.to_numpy()
    train, holdout = next(GroupShuffleSplit(n_splits=1, test_size=0.2,
                                            random_state=20260929).split(x, groups=groups))
    assert len(train) == 828 and len(holdout) == 220
    assert not set(groups[train]) & set(groups[holdout])
    alpha = choose_alpha(x[train], y[train], groups[train])
    assert alpha == source_report["results"][1]["ridge_alpha_inner_group_cv"]
    fitted = pipeline(alpha).fit(x[train], y[train])
    predictions = np.clip(fitted.predict(x[holdout]), 0, 1)
    mae = float(mean_absolute_error(y[holdout], predictions))
    assert abs(mae - source_report["results"][1]["mae"]) < 1e-6
    imputer = fitted.named_steps["simpleimputer"]
    scaler = fitted.named_steps["standardscaler"]
    ridge = fitted.named_steps["ridge"]
    assert np.isfinite(imputer.statistics_).all()
    assert np.isfinite(scaler.mean_).all() and np.isfinite(scaler.scale_).all()
    reconstructed = np.clip(((x[holdout] - scaler.mean_) / scaler.scale_) @ ridge.coef_
                            + ridge.intercept_, 0, 1)
    assert np.allclose(reconstructed, predictions, rtol=0, atol=2e-5)
    buffer = io.BytesIO()
    np.savez_compressed(buffer, imputer_median=imputer.statistics_.astype(np.float64),
                        scaler_mean=scaler.mean_.astype(np.float64),
                        scaler_scale=scaler.scale_.astype(np.float64),
                        ridge_coef=ridge.coef_.astype(np.float64),
                        ridge_intercept=np.asarray(ridge.intercept_, dtype=np.float64))
    if HEAD.exists():
        with np.load(HEAD) as old, np.load(io.BytesIO(buffer.getvalue())) as new:
            for name in new.files:
                assert np.array_equal(old[name], new[name])
    else:
        write_once(HEAD, buffer.getvalue())
    train_ids = nonhuman.query_id.iloc[train].tolist()
    holdout_ids = nonhuman.query_id.iloc[holdout].tolist()
    metadata = {"model": "siglip2_base", **MODELS["siglip2_base"],
                "code_revision": revision(), "source_script_sha256": sha256(Path(__file__)),
                "query_features_sha256": sha256(FEATURES),
                "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
                "head_sha256": sha256(HEAD), "alpha": alpha,
                "training_n": len(train), "holdout_n": len(holdout),
                "train_query_ids_sha256": hashlib.sha256(
                    "\n".join(train_ids).encode()).hexdigest(),
                "holdout_query_ids_sha256": hashlib.sha256(
                    "\n".join(holdout_ids).encode()).hexdigest(),
                "holdout_teacher_mae": round(mae, 6),
                "holdout_report_sha256": sha256(SOURCE / "siglip2_base_grouped_holdout_report.json"),
                "production": production, "geo_grouped": True,
                "localization_outcomes_used": False,
                "target": "Qwen three-pass median semantic geolocatability pseudo-label, not human ground truth"}
    if RECEIPT.exists():
        assert json.loads(RECEIPT.read_text())["head_sha256"] == metadata["head_sha256"]
    else:
        json_once(RECEIPT, metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
