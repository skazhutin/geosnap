"""Freeze an opt-in research gallery excluding only strongly corroborated image defects.

This never deletes image files or changes the fixed v8 gallery/production configuration.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path

import pandas as pd

from .common import revision, sha256, verify_production
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
SIGLIP = ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929"
EXTREME = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
OUT = ROOT / "data/evaluation/gallery_quality_filter_v1_20260929"
THRESHOLD = 0.2  # Chosen from image-only evidence before outcome inspection.


def save_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if sha256(path) != hashlib.sha256(payload).hexdigest():
            raise RuntimeError(f"Existing frozen artifact differs: {path}")
        return
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    with temporary.open("xb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def jsonl_bytes(rows: list[dict]) -> bytes:
    return "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows).encode()


def main() -> None:
    before = verify_production()
    gallery_hash = sha256(GALLERY)
    if gallery_hash != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Frozen gallery changed")
    siglip_freeze = json.loads((SIGLIP / "annotation_freeze.json").read_text())
    siglip_file = SIGLIP / "gallery_scores.jsonl"
    if (siglip_freeze["gallery_manifest_sha256"] != gallery_hash or
            siglip_freeze["gallery_scores_jsonl_sha256"] != sha256(siglip_file)):
        raise RuntimeError("SigLIP2 score artifact changed")
    extreme_integrity = json.loads((EXTREME / "final_integrity.json").read_text())
    extreme_review = json.loads((EXTREME / "final_review_receipt.json").read_text())
    decisions_file = EXTREME / "candidate_review_decisions.jsonl"
    if (extreme_integrity["gallery_manifest_sha256"] != gallery_hash or
            extreme_review["decisions_sha256"] != sha256(decisions_file)):
        raise RuntimeError("Extreme-defect review changed")
    scores = {}
    for line in siglip_file.open():
        row = json.loads(line)
        if row["status"] != "ok":
            raise RuntimeError(f"SigLIP2 gallery score failed for {row['id']}")
        scores[row["id"]] = row["geolocatability_teacher_estimate"]
    if len(scores) != 112163:
        raise RuntimeError("Wrong score population")
    decisions = [json.loads(line) for line in decisions_file.open()]
    if len(decisions) != 448 or len({row["id"] for row in decisions}) != 448:
        raise RuntimeError("Wrong candidate review population")
    to_exclude = {}
    to_review = {}
    for row in decisions:
        ident = row["id"]
        if ident not in scores:
            raise RuntimeError(f"Candidate absent from gallery scores: {ident}")
        score = scores[ident]
        if row["decision"] == "proposed_extreme_exclusion" and score < THRESHOLD:
            if (row["pass_state"] != ["valid", "valid"] or
                    not all(p["severe_technical_defect"] or p["street_view_absent"]
                            for p in (row["pass1"], row["pass2"]))):
                raise RuntimeError("Two-pass Qwen agreement contract violated")
            to_exclude[ident] = row
        else:
            to_review[ident] = row
    gallery = pd.read_parquet(GALLERY)
    if len(gallery) != 112163 or gallery.id.nunique() != len(gallery):
        raise RuntimeError("Gallery manifest identity mismatch")
    if set(gallery.id) != set(scores):
        raise RuntimeError("SigLIP2 and gallery IDs differ")
    excluded = []
    for row in gallery.itertuples(index=False):
        if row.id not in to_exclude:
            continue
        decision = to_exclude[row.id]
        excluded.append({"id": row.id, "source": row.source, "image_path": row.image_path,
                         "file_sha256": row.file_sha256, "siglip2_score": scores[row.id],
                         "pixel_flags": decision["pixel_flags"],
                         "qwen_pass1": decision["pass1"], "qwen_pass2": decision["pass2"],
                         "decision": "exclude_from_derived_research_gallery"})
    if len(excluded) != len(to_exclude):
        raise RuntimeError("Excluded identities missing from gallery")
    kept = gallery[~gallery.id.isin(to_exclude)].copy()
    if len(kept) + len(excluded) != len(gallery) or kept.id.nunique() != len(kept):
        raise RuntimeError("Filtered gallery partition invalid")
    review = [{"id": row["id"], "decision": row["decision"],
               "siglip2_score": scores[row["id"]], "pixel_flags": row["pixel_flags"],
               "qwen_pass1": row["pass1"], "qwen_pass2": row["pass2"]}
              for row in decisions if row["id"] in to_review]
    save_once(OUT / "excluded_400.jsonl", jsonl_bytes(excluded))
    save_once(OUT / "review_queue_48.jsonl", jsonl_bytes(review))
    save_once(OUT / "included_ids.txt", ("\n".join(kept.id.tolist()) + "\n").encode())
    gallery_path = OUT / "gallery_filtered.parquet"
    if not gallery_path.exists():
        temp = gallery_path.with_suffix(".parquet.tmp")
        if temp.exists():
            raise RuntimeError(f"Unfinished output exists: {temp}")
        kept.to_parquet(temp, index=False)
        os.replace(temp, gallery_path)
    check = pd.read_parquet(gallery_path, columns=["id"])
    if check.id.tolist() != kept.id.tolist():
        raise RuntimeError("Filtered gallery file differs from intended membership/order")
    after = verify_production()
    if gallery_hash != sha256(GALLERY) or before["guard_sha256"] != after["guard_sha256"]:
        raise RuntimeError("Frozen gallery or protected production file changed")
    receipt = {"policy": "pixel_extreme_candidate_and_both_qwen_extreme_and_siglip2_lt_0.2",
               "siglip2_threshold_exclusive": THRESHOLD,
               "original_gallery_count": len(gallery), "filtered_gallery_count": len(kept),
               "excluded_count": len(excluded), "review_queue_count": len(review),
               "excluded_by_source": dict(Counter(row["source"] for row in excluded)),
               "gallery_manifest_sha256": gallery_hash,
               "siglip2_scores_sha256": sha256(siglip_file),
               "extreme_decisions_sha256": sha256(decisions_file),
               "excluded_sha256": sha256(OUT / "excluded_400.jsonl"),
               "included_ids_sha256": sha256(OUT / "included_ids.txt"),
               "filtered_gallery_sha256": sha256(gallery_path),
               "review_queue_sha256": sha256(OUT / "review_queue_48.jsonl"),
               "code_revision": revision(), "production_guard": after,
               "original_files_deleted": False, "production_changed": False,
               "frozen_gallery_changed": False,
               "selection_used_localization_outcomes": False,
               "research_only": True}
    save_once(OUT / "filter_receipt.json", (json.dumps(receipt, ensure_ascii=False,
                                                        indent=2, sort_keys=True) + "\n").encode())
    print(json.dumps(receipt, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
