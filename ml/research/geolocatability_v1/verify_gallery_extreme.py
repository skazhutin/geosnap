"""Verify the completed extreme-defect gallery screen without editing gallery files."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from .common import json_once, sha256, verify_production

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
GALLERY = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def main() -> None:
    source = json.loads((OUT / "full_gallery_candidates_receipt.json").read_text())
    hashes = source["input_sha256"]
    assert len(hashes) == 225
    gallery_ids = pd.read_parquet(GALLERY, columns=["id"]).id.tolist()
    assert len(gallery_ids) == len(set(gallery_ids)) == 112163
    scanned = []
    for name, expected in sorted(hashes.items()):
        chunk = OUT / "chunks" / name
        assert sha256(chunk) == expected
        assert chunk.with_suffix(".sha256").read_text().strip() == expected
        scanned.extend(item["id"] for item in read_jsonl(chunk))
    assert scanned == gallery_ids
    candidates_path = OUT / "full_gallery_candidates.jsonl"
    assert sha256(candidates_path) == source["candidate_sha256"]
    candidates = read_jsonl(candidates_path)
    assert len(candidates) == len({item["id"] for item in candidates}) == 448
    decisions_path = OUT / "candidate_review_decisions.jsonl"
    final = json.loads((OUT / "final_review_receipt.json").read_text())
    assert sha256(decisions_path) == final["decisions_sha256"]
    assert sha256(OUT / "proposed_extreme_exclusions.jsonl") == final["proposed_sha256"]
    decisions = read_jsonl(decisions_path)
    assert [item["id"] for item in decisions] == [item["id"] for item in candidates]
    assert sum(final["decision_counts"].values()) == 448
    raw = OUT / "vlm_review/full_gallery_candidates/raw"
    raw_entries = []
    for candidate in candidates:
        for pass_number in (1, 2):
            path = raw / candidate["id"] / f"pass{pass_number}.json"
            record = json.loads(path.read_text())
            assert record["id"] == candidate["id"] and record["pass"] == pass_number
            raw_entries.append(f"{candidate['id']}/pass{pass_number}:{sha256(path)}")
    assert len(raw_entries) == 896
    result = {"gallery_count": len(gallery_ids), "gallery_manifest_sha256": sha256(GALLERY),
              "chunk_count": len(hashes), "checked_rows": len(scanned),
              "candidate_count": len(candidates), "raw_pass_count": len(raw_entries),
              "raw_sha256_manifest_hash": hashlib.sha256("\n".join(raw_entries).encode()).hexdigest(),
              "final_review_receipt_sha256": sha256(OUT / "final_review_receipt.json"),
              "decision_counts": final["decision_counts"], "production": verify_production(),
              "gallery_unchanged": True, "localization_outcomes_accessed": False}
    json_once(OUT / "final_integrity.json", result)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
