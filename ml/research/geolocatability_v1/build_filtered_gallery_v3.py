"""Add manually confirmed inverted/non-street frames to research filter v2.

This is a versioned policy correction: orientation errors count as bad gallery
inputs. The previous manual review and v2 filter remain immutable evidence.
"""
from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from .build_filtered_gallery_v2 import save_once, save_receipt_once, jsonl_bytes
from .common import revision, sha256, verify_production
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
V2 = ROOT / "data/evaluation/gallery_quality_filter_v2_20260929"
AUDIT = ROOT / "data/evaluation/gallery_quality_threshold_audit_v1_20260929"
OUT = ROOT / "data/evaluation/gallery_quality_filter_v3_20260929"
BAD_LABELS = {"unusable_nonstreet", "orientation_fixable"}


def main() -> None:
    before = verify_production()
    if sha256(GALLERY) != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Frozen gallery changed")
    v2_receipt = json.loads((V2 / "filter_receipt.json").read_text())
    if (v2_receipt["gallery_manifest_sha256"] != EXPECTED_GALLERY_SHA256 or
            sha256(V2 / "excluded.jsonl") != v2_receipt["excluded_sha256"] or
            sha256(V2 / "gallery_filtered.parquet") != v2_receipt["filtered_gallery_sha256"]):
        raise RuntimeError("Version-2 filter changed")
    manual_file = AUDIT / "manual_review_20.jsonl"
    impact = json.loads((AUDIT / "threshold_impact.json").read_text())
    if sha256(manual_file) != impact["manual_review_20_sha256"]:
        raise RuntimeError("Frozen outcome-blind manual review changed")
    manual = list(map(json.loads, manual_file.open()))
    if (len(manual) != 20 or len({row["id"] for row in manual}) != 20 or
            not all(row["outcome_blind"] for row in manual)):
        raise RuntimeError("Manual review population changed")
    adjudicated = [{"id": row["id"], "original_visual_judgment": row["manual_visual_judgment"],
                    "policy_bad_input": row["manual_visual_judgment"] in BAD_LABELS,
                    "policy_reason": "orientation_is_bad_input" if row["manual_visual_judgment"] == "orientation_fixable"
                    else "non_street_image" if row["manual_visual_judgment"] == "unusable_nonstreet"
                    else "visible_scene_kept",
                    "source_file_sha256": row["file_sha256"]} for row in manual]
    new_bad = {row["id"]: row for row in manual if row["manual_visual_judgment"] in BAD_LABELS}
    if len(new_bad) != 4 or Counter(row["manual_visual_judgment"] for row in new_bad.values()) != {
            "unusable_nonstreet": 2, "orientation_fixable": 2}:
        raise RuntimeError("Expected two inverted and two non-street images")
    old = {row["id"]: row for row in map(json.loads, (V2 / "excluded.jsonl").open())}
    if len(old) != 1127 or set(old) & set(new_bad):
        raise RuntimeError("Version-2 exclusion set or manual-only set changed")

    gallery = pd.read_parquet(GALLERY)
    if len(gallery) != 112163 or gallery.id.nunique() != len(gallery):
        raise RuntimeError("Original gallery identity changed")
    excluded_ids = set(old) | set(new_bad)
    excluded = []
    for row in gallery.itertuples(index=False):
        if row.id in old:
            item = dict(old[row.id])
            if item["file_sha256"] != row.file_sha256:
                raise RuntimeError("Inherited exclusion image hash differs")
            item["v3_exclusion_basis"] = "inherited_v2"
            excluded.append(item)
        elif row.id in new_bad:
            reviewed = new_bad[row.id]
            if (reviewed["file_sha256"] != row.file_sha256 or
                    reviewed["image_path"] != row.image_path):
                raise RuntimeError("Manually reviewed image differs from original gallery")
            excluded.append({"id": row.id, "source": row.source, "image_path": row.image_path,
                             "file_sha256": row.file_sha256,
                             "siglip2_score": reviewed["siglip2_score"],
                             "manual_visual_judgment": reviewed["manual_visual_judgment"],
                             "v3_exclusion_basis": "manual_bad_input_user_policy"})
    if len(excluded) != 1131:
        raise RuntimeError("Wrong corrected exclusion count")
    keep = ~gallery.id.isin(excluded_ids)
    kept = gallery[keep].copy()
    retained_rows = np.flatnonzero(keep.to_numpy()).astype(np.int32)
    if len(kept) != 111032 or kept.id.nunique() != len(kept):
        raise RuntimeError("Wrong corrected retained gallery")
    save_once(OUT / "manual_policy_adjudication_20.jsonl", jsonl_bytes(adjudicated))
    save_once(OUT / "excluded.jsonl", jsonl_bytes(excluded))
    save_once(OUT / "included_ids.txt", ("\n".join(kept.id.tolist()) + "\n").encode())
    rows_file = OUT / "retained_original_rows.npy"
    if rows_file.exists():
        with rows_file.open("rb") as stream:
            if not np.array_equal(np.load(stream, allow_pickle=False), retained_rows):
                raise RuntimeError("Retained-row mapping changed")
    else:
        OUT.mkdir(parents=True, exist_ok=True)
        temporary = rows_file.with_suffix(f".npy.tmp.{os.getpid()}")
        with temporary.open("xb") as stream:
            np.save(stream, retained_rows, allow_pickle=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, rows_file)
    gallery_file = OUT / "gallery_filtered.parquet"
    if not gallery_file.exists():
        temporary = gallery_file.with_suffix(f".parquet.tmp.{os.getpid()}")
        kept.to_parquet(temporary, index=False)
        os.replace(temporary, gallery_file)
    if pd.read_parquet(gallery_file, columns=["id"]).id.tolist() != kept.id.tolist():
        raise RuntimeError("Corrected filtered-gallery membership changed")
    after = verify_production()
    if (before["guard_sha256"] != after["guard_sha256"] or
            sha256(GALLERY) != EXPECTED_GALLERY_SHA256):
        raise RuntimeError("Production or frozen gallery changed")
    receipt = {
        "policy": "v2_union_plus_manual_nonstreet_and_inverted_images",
        "siglip2_threshold_exclusive": 0.10,
        "original_gallery_count": 112163,
        "inherited_v2_exclusions": 1127,
        "new_manual_bad_exclusions": 4,
        "new_manual_inverted_exclusions": 2,
        "new_manual_nonstreet_exclusions": 2,
        "excluded_count": 1131,
        "filtered_gallery_count": 111032,
        "excluded_by_source": dict(Counter(row["source"] for row in excluded)),
        "original_gallery_sha256": EXPECTED_GALLERY_SHA256,
        "v2_receipt_sha256": sha256(V2 / "filter_receipt.json"),
        "manual_review_20_sha256": sha256(manual_file),
        "manual_adjudication_sha256": sha256(OUT / "manual_policy_adjudication_20.jsonl"),
        "excluded_sha256": sha256(OUT / "excluded.jsonl"),
        "included_ids_sha256": sha256(OUT / "included_ids.txt"),
        "retained_original_rows_sha256": sha256(rows_file),
        "filtered_gallery_sha256": sha256(gallery_file),
        "code_revision": revision(),
        "production_guard": after,
        "original_files_deleted": False,
        "frozen_gallery_changed": False,
        "production_changed": False,
        "research_only": True,
    }
    print(json.dumps(save_receipt_once(OUT / "filter_receipt.json", receipt),
                     ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
