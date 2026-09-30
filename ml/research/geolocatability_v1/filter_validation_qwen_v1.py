"""Freeze an outcome-blind Qwen-led weak-input subset, then analyze it.

The visual veto positions refer to the immutable, query-ID-sorted 119-image
contact-sheet shortlist. No location or localization result is read by freeze.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .analyze_filtered_gallery_v1 import EARTH_M, distance_m
from .common import revision, verify_production
from ml.research.geographic_v8.common import GALLERY


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/validation_qwen_quality_filter_v1_20260929"
ANNOTATIONS = ROOT / "data/evaluation/geolocatability_v1_20260928/geolocatability_annotations.parquet"
QUERY = ROOT / "data/evaluation/moscow_research_v5/prospective/development.parquet"
GALLERY_V3 = ROOT / "data/evaluation/gallery_quality_filter_v3_20260929/gallery_filtered.parquet"
BASELINE = ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz"
SECONDARY = ROOT / "data/evaluation/geographic_v8_20260928/secondary_baseline/predictions.npz"
EXPECTED_QUERY_SHA = "6bcc0b4a618682e05e15305dc76b9f02af635e73573e06ea2567d6094ad3e7d9"
EXPECTED_ANNOTATION_SHA = "165a0adafb47615502e04a022f2acf4833a48a3537199f4b0d9a622c4f4ab512"

# Determined from image pixels alone, before reading any GeoSnap outcomes.
# These six Qwen low-score frames visibly contain distinctive built structures
# (transit facility, building facades, street geometry/signs); veto exclusion.
VISUAL_FALSE_POSITIVE_POSITIONS = frozenset({5, 72, 89, 105, 112, 113})


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_once(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text() != content:
            raise RuntimeError(f"Frozen artifact differs: {path}")
    else:
        path.write_text(content)


def freeze() -> None:
    if sha(QUERY) != EXPECTED_QUERY_SHA or sha(ANNOTATIONS) != EXPECTED_ANNOTATION_SHA:
        raise RuntimeError("Original query population or frozen Qwen annotations changed")
    q = pd.read_parquet(QUERY, columns=["id", "file_sha256", "image_path", "width", "height"])
    a = pd.read_parquet(ANNOTATIONS)
    if len(q) != 1184 or q.id.nunique() != 1184 or len(a) != 1184 or a.query_id.nunique() != 1184:
        raise RuntimeError("Expected 1184 unique query IDs")
    merged = q.merge(a, left_on="id", right_on="query_id", validate="one_to_one")
    if len(merged) != 1184 or not merged.file_sha256.eq(merged.image_sha256).all():
        raise RuntimeError("Qwen annotation/image identity mismatch")
    candidates = [json.loads(line) for line in (OUT / "qwen_semantic_candidates.jsonl").read_text().splitlines()]
    if len(candidates) != 119 or len({r["query_id"] for r in candidates}) != 119:
        raise RuntimeError("Unexpected Qwen shortlist")
    for i, row in enumerate(candidates, 1):
        if row["position"] != i or row["query_id"] != sorted(r["query_id"] for r in candidates)[i - 1]:
            raise RuntimeError("Qwen shortlist order changed")
        match = merged.set_index("query_id").loc[row["query_id"]]
        if row["image_sha256"] != match.file_sha256:
            raise RuntimeError("Qwen shortlist image mismatch")
    review = []
    for row in candidates:
        position = row["position"]
        review.append({**row, "excluded": position not in VISUAL_FALSE_POSITIVE_POSITIONS,
                       "review_basis": "Qwen three-pass semantic shortlist; pixel-only veto of distinctive-scene false positives"})
    ids = {row["query_id"] for row in review if row["excluded"]}
    if len(ids) != len(candidates) - len(VISUAL_FALSE_POSITIVE_POSITIONS):
        raise RuntimeError("Visual review positions are incomplete")
    write_once(OUT / "outcome_blind_review.jsonl", "".join(json.dumps(x, sort_keys=True) + "\n" for x in review))
    write_once(OUT / "excluded_query_ids.jsonl", "".join(json.dumps({"query_id": x["query_id"],
        "image_sha256": x["image_sha256"], "qwen_reason": x["retake_reason_consensus"],
        "review_position": x["position"]}, sort_keys=True) + "\n" for x in review if x["excluded"]))
    write_once(OUT / "retained_query_ids.jsonl", "".join(json.dumps({"query_id": x.id,
        "image_sha256": x.file_sha256}, sort_keys=True) + "\n" for x in q.itertuples(index=False) if x.id not in ids))
    receipt = {
        "status": "frozen_before_outcome_join", "original_queries": 1184,
        "qwen_valid": 1162, "qwen_schema_failures_retained": 22,
        "qwen_semantic_shortlist": 119, "visually_retained_false_positives": len(VISUAL_FALSE_POSITIVE_POSITIONS),
        "excluded": len(ids),
        "retained": 1184 - len(ids), "query_manifest_sha256": sha(QUERY),
        "annotation_parquet_sha256": sha(ANNOTATIONS),
        "qwen_shortlist_sha256": sha(OUT / "qwen_semantic_candidates.jsonl"),
        "pixel_only_review_sha256": sha(OUT / "outcome_blind_review.jsonl"),
        "excluded_ids_sha256": sha(OUT / "excluded_query_ids.jsonl"),
        "retained_ids_sha256": sha(OUT / "retained_query_ids.jsonl"),
        "selection_rule": "Qwen three-pass unanimous unusable/retake with median semantic geolocatability<=0.15, OR unanimous retake with orientation>=0.8 and ORIENTATION consensus. All 119 shortlisted images reviewed without outcomes; six visibly distinctive built scenes vetoed as Qwen false positives. Generic roads and trails count as weak geolocation input.",
        "outcome_data_read": False, "code_revision": revision(),
        "production_guard_sha256": verify_production()["guard_sha256"],
    }
    write_once(OUT / "query_filter_freeze.json", json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2), flush=True)


def raw(errors: np.ndarray) -> dict:
    return {"n": len(errors), **{f"at_{m}m": int((errors <= m).sum()) for m in (25, 50, 100, 500, 1000, 5000)},
            "median_m": float(np.median(errors)), "p90_m": float(np.percentile(errors, 90)),
            "over_500m": int((errors > 500).sum())}


def spatial_oracle(qcoords: np.ndarray, gallery: pd.DataFrame) -> dict:
    tree = BallTree(np.radians(gallery[["lat", "lon"]].to_numpy(float)), metric="haversine")
    rad, _ = tree.query(np.radians(qcoords), k=1)
    return raw(rad[:, 0] * EARTH_M)


def analyze() -> None:
    receipt = json.loads((OUT / "query_filter_freeze.json").read_text())
    if (receipt["status"] != "frozen_before_outcome_join" or receipt["outcome_data_read"] or
            sha(OUT / "excluded_query_ids.jsonl") != receipt["excluded_ids_sha256"] or
            sha(OUT / "retained_query_ids.jsonl") != receipt["retained_ids_sha256"] or
            sha(ANNOTATIONS) != receipt["annotation_parquet_sha256"] or
            sha(QUERY) != receipt["query_manifest_sha256"]):
        raise RuntimeError("Query filter was not independently frozen")
    keep = {json.loads(line)["query_id"] for line in (OUT / "retained_query_ids.jsonl").read_text().splitlines()}
    q = pd.read_parquet(QUERY)
    if len(keep) != receipt["retained"] or not keep.issubset(set(q.id)):
        raise RuntimeError("Frozen query subset invalid")
    selected = q[q.id.isin(keep)].copy()
    out_parquet = OUT / "filtered_development.parquet"
    if out_parquet.exists():
        old = pd.read_parquet(out_parquet)
        if old.id.tolist() != selected.id.tolist():
            raise RuntimeError("Existing filtered development subset changed")
    else:
        selected.to_parquet(out_parquet, index=False)
    original_gallery = pd.read_parquet(GALLERY, columns=["id", "lat", "lon", "source"])
    clean_gallery = pd.read_parquet(GALLERY_V3, columns=["id", "lat", "lon", "source"])
    if len(original_gallery) != 112163 or len(clean_gallery) != 111032:
        raise RuntimeError("Gallery size changed")
    results = {"query_filter_freeze_sha256": sha(OUT / "query_filter_freeze.json"),
               "filtered_query_parquet_sha256": sha(out_parquet), "query_count": len(selected),
               "original_query_count": len(q), "excluded_query_count": 1184 - len(selected),
               "gallery_count_original": len(original_gallery), "gallery_count_clean_v3": len(clean_gallery)}
    for label, query in (("all", q), ("qwen_clean", selected)):
        coords = query[["lat", "lon"]].to_numpy(float)
        results[f"oracle_original_gallery_{label}"] = spatial_oracle(coords, original_gallery)
        results[f"oracle_clean_gallery_{label}"] = spatial_oracle(coords, clean_gallery)
        results[f"oracle_original_release_gallery_{label}"] = spatial_oracle(coords, original_gallery[original_gallery.source.isin(["mapillary", "kartaview"])])
        results[f"oracle_clean_release_gallery_{label}"] = spatial_oracle(coords, clean_gallery[clean_gallery.source.isin(["mapillary", "kartaview"])])
        for name, path, gal in (("sage_original_gallery", BASELINE, original_gallery),
                                ("sage_original_release_gallery", SECONDARY, original_gallery[original_gallery.source.isin(["mapillary", "kartaview"])])):
            with np.load(path, allow_pickle=False) as pred:
                ids = pred["query_ids"]
                if ids.tolist() != q.id.tolist():
                    raise RuntimeError("Prediction query order changed")
                mask = np.isin(ids, query.id.to_numpy())
                results[f"{name}_{label}"] = raw(pred["errors_m"][mask])
                gcoords = gal[["lat", "lon"]].to_numpy(float)
                if pred["gallery_ids"].tolist() != gal.id.tolist():
                    raise RuntimeError("Prediction gallery order changed")
                candidates = pred["indices"][mask]
                d = distance_m(coords, gcoords[candidates])
                results[f"{name}_{label}"]["recall"] = {str(k): int((d[:, :k] <= 100).any(axis=1).sum()) for k in (1, 5, 10, 20, 50, 100)}
    results["warnings"] = ["Filtered query scores are conditional, not comparable to unchanged 1184-query RAW.",
                           "Clean-gallery model RAW has not been rerun; spatial oracle alone is not a predictor.",
                           "Qwen pseudo-labels and visual review are not an independent final benchmark."]
    results["production_guard_sha256"] = verify_production()["guard_sha256"]
    write_once(OUT / "postfreeze_metrics.json", json.dumps(results, indent=2, sort_keys=True) + "\n")
    print(json.dumps(results, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["freeze", "analyze"])
    stage = parser.parse_args().stage
    freeze() if stage == "freeze" else analyze()
