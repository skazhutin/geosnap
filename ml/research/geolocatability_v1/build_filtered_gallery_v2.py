"""Freeze an opt-in gallery filtered by Qwen consensus or very low SigLIP2 score.

Selection uses only frozen image annotations. Original photos, gallery and production
artifacts are never edited. The threshold is fixed before outcome analysis.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from .common import revision, sha256, verify_production
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
SIGLIP = ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929"
EXTREME = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
QWEN100 = ROOT / "data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929"
OUT = ROOT / "data/evaluation/gallery_quality_filter_v2_20260929"
SIGLIP_THRESHOLD = 0.10  # Strict lower tail, fixed from image-only review.
GALLERY_COUNT = 112163


def save_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    expected = hashlib.sha256(payload).hexdigest()
    if path.exists():
        if sha256(path) != expected:
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


def save_receipt_once(path: Path, receipt: dict) -> dict:
    if path.exists():
        existing = json.loads(path.read_text())
        previous = json.loads(json.dumps(existing))
        current = json.loads(json.dumps(receipt))
        previous["production_guard"].pop("checked_at", None)
        current["production_guard"].pop("checked_at", None)
        if previous != current:
            raise RuntimeError("Existing filter receipt differs from current inputs")
        return existing
    save_once(path, (json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode())
    return receipt


def checked_sources() -> tuple[Path, Path, Path, dict[str, float], set[str], set[str]]:
    gallery_hash = sha256(GALLERY)
    if gallery_hash != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Frozen gallery changed")

    siglip_file = SIGLIP / "gallery_scores.jsonl"
    siglip_freeze = json.loads((SIGLIP / "annotation_freeze.json").read_text())
    if (siglip_freeze["gallery_manifest_sha256"] != gallery_hash or
            siglip_freeze["gallery_scores_jsonl_sha256"] != sha256(siglip_file)):
        raise RuntimeError("Frozen SigLIP2 annotations changed")
    scores: dict[str, float] = {}
    for row in map(json.loads, siglip_file.open()):
        if row["status"] != "ok" or row["id"] in scores:
            raise RuntimeError("Invalid or duplicate SigLIP2 row")
        score = row["geolocatability_teacher_estimate"]
        if score is None or not 0 <= score <= 1:
            raise RuntimeError("Invalid SigLIP2 score")
        scores[row["id"]] = float(score)
    if len(scores) != GALLERY_COUNT:
        raise RuntimeError("Wrong SigLIP2 population")

    decisions_file = EXTREME / "candidate_review_decisions.jsonl"
    review = json.loads((EXTREME / "final_review_receipt.json").read_text())
    integrity = json.loads((EXTREME / "final_integrity.json").read_text())
    if (integrity["gallery_manifest_sha256"] != gallery_hash or
            review["decisions_sha256"] != sha256(decisions_file)):
        raise RuntimeError("Frozen Qwen extreme review changed")
    decisions = list(map(json.loads, decisions_file.open()))
    if len(decisions) != 448 or len({row["id"] for row in decisions}) != 448:
        raise RuntimeError("Wrong Qwen extreme-review population")
    qwen_extreme = set()
    for row in decisions:
        if row["decision"] != "proposed_extreme_exclusion":
            continue
        if (row["pass_state"] != ["valid", "valid"] or
                not all(p["severe_technical_defect"] or p["street_view_absent"]
                        for p in (row["pass1"], row["pass2"]))):
            raise RuntimeError("Qwen extreme-review consensus contract violated")
        qwen_extreme.add(row["id"])
    if len(qwen_extreme) != 408:
        raise RuntimeError("Wrong two-pass Qwen exclusion count")

    qwen100_file = QWEN100 / "qwen_blind_annotations.jsonl"
    qwen100_freeze = json.loads((QWEN100 / "qwen_blind_freeze.json").read_text())
    if (not qwen100_freeze["score_and_outcome_blind"] or
            not qwen100_freeze["raw_complete"] or
            qwen100_freeze["annotations_sha256"] != sha256(qwen100_file)):
        raise RuntimeError("Frozen Qwen 100-image audit changed")
    qwen100_rows = list(map(json.loads, qwen100_file.open()))
    if len(qwen100_rows) != 100 or len({row["id"] for row in qwen100_rows}) != 100:
        raise RuntimeError("Wrong Qwen audit population")
    qwen100_severe = {row["id"] for row in qwen100_rows
                      if row["qwen_valid_passes"] == 2 and row["qwen_severe_defect_votes"] == 2}
    if len(qwen100_severe) != 3:
        raise RuntimeError("Unexpected Qwen audit consensus count")
    return siglip_file, decisions_file, qwen100_file, scores, qwen_extreme, qwen100_severe


def main() -> None:
    before = verify_production()
    siglip_file, decisions_file, qwen100_file, scores, qwen_extreme, qwen100_severe = checked_sources()
    gallery = pd.read_parquet(GALLERY)
    if (len(gallery) != GALLERY_COUNT or gallery.id.nunique() != GALLERY_COUNT or
            set(gallery.id) != set(scores)):
        raise RuntimeError("Gallery and score identities differ")

    siglip_low = {ident for ident, score in scores.items() if score < SIGLIP_THRESHOLD}
    excluded_ids = siglip_low | qwen_extreme | qwen100_severe
    excluded = []
    for row in gallery.itertuples(index=False):
        if row.id not in excluded_ids:
            continue
        excluded.append({"id": row.id, "source": row.source,
                         "image_path": row.image_path, "file_sha256": row.file_sha256,
                         "siglip2_score": scores[row.id],
                         "siglip2_below_0_10": row.id in siglip_low,
                         "qwen_extreme_two_pass": row.id in qwen_extreme,
                         "qwen_audit_severe_two_pass": row.id in qwen100_severe})
    if len(excluded) != len(excluded_ids):
        raise RuntimeError("Excluded identity missing from gallery")
    keep = ~gallery.id.isin(excluded_ids)
    kept = gallery[keep].copy()
    kept_rows = np.flatnonzero(keep.to_numpy()).astype(np.int32)
    if len(kept) + len(excluded) != GALLERY_COUNT or kept.id.nunique() != len(kept):
        raise RuntimeError("Invalid gallery partition")

    save_once(OUT / "excluded.jsonl", jsonl_bytes(excluded))
    save_once(OUT / "included_ids.txt", ("\n".join(kept.id.tolist()) + "\n").encode())
    rows_path = OUT / "retained_original_rows.npy"
    if rows_path.exists():
        with rows_path.open("rb") as stream:
            if not np.array_equal(np.load(stream, allow_pickle=False), kept_rows):
                raise RuntimeError("Frozen retained-row index changed")
    else:
        OUT.mkdir(parents=True, exist_ok=True)
        temporary = rows_path.with_suffix(f".npy.tmp.{os.getpid()}")
        with temporary.open("xb") as stream:
            np.save(stream, kept_rows, allow_pickle=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, rows_path)
    gallery_path = OUT / "gallery_filtered.parquet"
    if not gallery_path.exists():
        temporary = gallery_path.with_suffix(f".parquet.tmp.{os.getpid()}")
        kept.to_parquet(temporary, index=False)
        os.replace(temporary, gallery_path)
    check = pd.read_parquet(gallery_path, columns=["id"])
    if check.id.tolist() != kept.id.tolist():
        raise RuntimeError("Filtered gallery differs from intended membership/order")

    after = verify_production()
    if (before["guard_sha256"] != after["guard_sha256"] or
            sha256(GALLERY) != EXPECTED_GALLERY_SHA256):
        raise RuntimeError("Production or frozen gallery changed")
    receipt = {
        "policy": "siglip2_score_lt_0.10_or_two_pass_qwen_extreme",
        "siglip2_threshold_exclusive": SIGLIP_THRESHOLD,
        "original_gallery_count": GALLERY_COUNT,
        "filtered_gallery_count": len(kept),
        "excluded_count": len(excluded),
        "siglip2_below_threshold": len(siglip_low),
        "qwen_extreme_two_pass": len(qwen_extreme),
        "qwen_audit_severe_two_pass": len(qwen100_severe),
        "siglip_qwen_extreme_overlap": len(siglip_low & qwen_extreme),
        "qwen_audit_only": len(qwen100_severe - siglip_low - qwen_extreme),
        "excluded_by_source": dict(Counter(row["source"] for row in excluded)),
        "gallery_manifest_sha256": EXPECTED_GALLERY_SHA256,
        "siglip2_scores_sha256": sha256(siglip_file),
        "qwen_extreme_decisions_sha256": sha256(decisions_file),
        "qwen_audit_annotations_sha256": sha256(qwen100_file),
        "excluded_sha256": sha256(OUT / "excluded.jsonl"),
        "included_ids_sha256": sha256(OUT / "included_ids.txt"),
        "retained_original_rows_sha256": sha256(rows_path),
        "filtered_gallery_sha256": sha256(gallery_path),
        "code_revision": revision(),
        "production_guard": after,
        "original_files_deleted": False,
        "production_changed": False,
        "frozen_gallery_changed": False,
        "selection_used_localization_outcomes": False,
        "research_only": True,
    }
    print(json.dumps(save_receipt_once(OUT / "filter_receipt.json", receipt),
                     ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
