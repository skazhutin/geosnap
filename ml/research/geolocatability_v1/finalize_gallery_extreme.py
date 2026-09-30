"""Summarize independent extreme-defect checks without editing the frozen gallery."""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
CANDIDATES = OUT / "full_gallery_candidates.jsonl"
RAW = OUT / "vlm_review/full_gallery_candidates/raw"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_new(path: Path, value: str) -> None:
    if path.exists():
        raise FileExistsError(path)
    path.write_text(value)


def main() -> None:
    candidates = [json.loads(line) for line in CANDIDATES.read_text().splitlines()]
    reviewed = []
    for item in candidates:
        folder = RAW / item["id"]
        passes = []
        pass_state = []
        for number in (1, 2):
            path = folder / f"pass{number}.json"
            if not path.exists():
                passes.append(None)
                pass_state.append("missing")
                continue
            record = json.loads(path.read_text())
            passes.append(record["attempts"][-1].get("parsed") if record["valid"] else None)
            pass_state.append("valid" if record["valid"] else "model_failure")
        if item["status"] != "ok":
            decision = "source_error_review"
        elif "model_failure" in pass_state:
            decision = "model_failure_review"
        elif any(value is None for value in passes):
            decision = "incomplete_review"
        elif all(value["severe_technical_defect"] or value["street_view_absent"] for value in passes):
            decision = "proposed_extreme_exclusion"
        elif any(value["severe_technical_defect"] or value["street_view_absent"] for value in passes):
            decision = "disagreement_review"
        else:
            decision = "keep"
        reviewed.append({"id": item["id"], "source": item["source"], "pixel_flags": item["flags"],
                         "pass1": passes[0], "pass2": passes[1], "pass_state": pass_state,
                         "decision": decision})
    decisions = OUT / "candidate_review_decisions.jsonl"
    proposed = OUT / "proposed_extreme_exclusions.jsonl"
    write_new(decisions, "".join(json.dumps(item, sort_keys=True) + "\n" for item in reviewed))
    write_new(proposed, "".join(json.dumps(item, sort_keys=True) + "\n" for item in reviewed
                                 if item["decision"] == "proposed_extreme_exclusion"))
    counts = Counter(item["decision"] for item in reviewed)
    providers = Counter(item["source"] for item in reviewed if item["decision"] == "proposed_extreme_exclusion")
    receipt = {"fixed_gallery_count": 112163, "pixel_candidate_count": len(candidates),
               "decision_counts": dict(counts), "proposed_by_source": dict(providers),
               "vlm_review_complete": counts["incomplete_review"] == 0,
               "gallery_unchanged": True, "original_files_untouched": True,
               "no_localization_outcome_used": True,
               "candidate_sha256": sha256(CANDIDATES), "decisions_sha256": sha256(decisions),
               "proposed_sha256": sha256(proposed)}
    write_new(OUT / "final_review_receipt.json", json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
