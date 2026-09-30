"""Freeze all SigLIP2 gallery scores after every immutable chunk is present."""
from __future__ import annotations

import hashlib
import io
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from .common import json_once, revision, sha256, verify_production, write_once
from .scan_siglip2_gallery import CHUNKS, EXPECTED_GALLERY_SHA256, GALLERY, OUT, RECEIPT, verify_chunk

SCORES = OUT / "gallery_scores.jsonl"
PARQUET = OUT / "gallery_scores.parquet"
FREEZE = OUT / "annotation_freeze.json"


def main() -> None:
    production = verify_production()
    if sha256(GALLERY) != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Fixed gallery manifest changed")
    gallery = pd.read_parquet(GALLERY, columns=["id", "source", "image_path", "file_sha256"])
    assert len(gallery) == gallery.id.nunique() == 112163
    head_metadata = json.loads(RECEIPT.read_text())
    records = gallery.to_dict("records")
    all_scores, feature_hashes, failures = [], [], []
    chunk_size = 256
    for start in range(0, len(records), chunk_size):
        subset = records[start:start + chunk_size]
        path = CHUNKS / f"{start:06d}-{start + len(subset) - 1:06d}"
        if not path.is_dir():
            raise RuntimeError(f"Missing completed SigLIP2 chunk: {path}")
        receipt = verify_chunk(path, subset, EXPECTED_GALLERY_SHA256,
                               head_metadata["head_sha256"])
        rows = [json.loads(line) for line in (path / "scores.jsonl").read_text().splitlines()]
        assert len(rows) == len(subset)
        assert [row["id"] for row in rows] == [row["id"] for row in subset]
        with np.load(path / "features.npz") as cached:
            features = cached["feature"]
        for row, source, feature in zip(rows, subset, features, strict=True):
            assert row["source_file_sha256"] == source["file_sha256"]
            if row["status"] == "ok":
                assert np.isfinite(feature).all()
                assert 0 <= row["geolocatability_teacher_estimate"] <= 1
                assert np.isfinite(row["semantic_prompt_contrast"])
            else:
                assert row["status"] == "failed" and row["error"] is not None
                assert np.isnan(feature).all()
                failures.append({"id": row["id"], "error": row["error"]})
        assert receipt["valid"] + receipt["failed"] == len(subset)
        feature_hashes.append(receipt["features_sha256"])
        all_scores.extend(rows)
    assert len(all_scores) == len({row["id"] for row in all_scores}) == 112163
    jsonl = "".join(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n"
                    for item in all_scores).encode()
    if SCORES.exists():
        assert sha256(SCORES) == hashlib.sha256(jsonl).hexdigest()
    else:
        write_once(SCORES, jsonl)
    if not PARQUET.exists():
        buffer = io.BytesIO()
        pd.DataFrame(all_scores).to_parquet(buffer, index=False)
        write_once(PARQUET, buffer.getvalue())
    valid = [row["geolocatability_teacher_estimate"] for row in all_scores
             if row["status"] == "ok"]
    bands = Counter(min(4, int(score * 5)) for score in valid)
    receipt = {"gallery_count": len(all_scores), "valid_count": len(valid),
               "failure_count": len(failures), "failures": failures,
               "chunk_count": len(feature_hashes), "chunk_size": chunk_size,
               "gallery_manifest_sha256": EXPECTED_GALLERY_SHA256,
               "quality_head_receipt_sha256": sha256(RECEIPT),
               "gallery_scores_jsonl_sha256": sha256(SCORES),
               "gallery_scores_parquet_sha256": sha256(PARQUET),
               "feature_hashes_sha256": hashlib.sha256(
                   "\n".join(feature_hashes).encode()).hexdigest(),
               "geolocatability_bands_0_0.2_0.4_0.6_0.8_1":
                   {str(i): bands[i] for i in range(5)},
               "code_revision": revision(), "production": production,
               "gallery_modified": False, "localization_outcomes_accessed": False,
               "labels_are_qwen_teacher_imitation_not_human_ground_truth": True,
               "no_images_removed": True}
    if FREEZE.exists():
        prior = json.loads(FREEZE.read_text())
        assert prior["gallery_scores_jsonl_sha256"] == receipt["gallery_scores_jsonl_sha256"]
    else:
        json_once(FREEZE, receipt)
    print(json.dumps({key: value for key, value in receipt.items() if key != "failures"},
                     ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
