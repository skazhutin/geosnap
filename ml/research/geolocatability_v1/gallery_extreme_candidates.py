"""Pixel-only shortlist of extreme gallery defects; never changes gallery membership."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def reasons(record: dict) -> list[str]:
    if record["status"] != "ok":
        return [record["status"]]
    f = record["features"]
    hits = []
    if f["dark_fraction"] >= 0.98 and f["p95_minus_p5_luminance"] <= 12:
        hits.append("near_black")
    if f["clipped_bright_fraction"] >= 0.98 and f["p95_minus_p5_luminance"] <= 12:
        hits.append("near_white")
    if f["p95_minus_p5_luminance"] <= 5 and f["luminance_entropy_bits"] <= 1.5:
        hits.append("nearly_flat")
    if (f["laplacian_variance"] <= 60 and f["tenengrad"] <= 800
            and f["canny_edge_density"] <= 0.015):
        hits.append("multiple_low_detail_signals")
    return hits


def run(source: str) -> None:
    if source == "probe":
        inputs = [OUT / "stratified_probe_1000.jsonl"]
        name = "pilot_candidates_1000.jsonl"
    else:
        inputs = sorted((OUT / "chunks").glob("*.jsonl"))
        if len(inputs) != 225:
            raise RuntimeError(f"Need 225 complete gallery chunks; found {len(inputs)}")
        for path in inputs:
            if sha256(path) != path.with_suffix(".sha256").read_text().strip():
                raise RuntimeError(f"Chunk checksum mismatch: {path}")
        name = "full_gallery_candidates.jsonl"
    selected = []
    ids = set()
    count = 0
    for path in inputs:
        for line in path.open():
            record = json.loads(line)
            if record["id"] in ids:
                raise RuntimeError(f"Duplicate gallery ID: {record['id']}")
            ids.add(record["id"])
            count += 1
            flags = reasons(record)
            if flags:
                selected.append({"id": record["id"], "source": record["source"],
                                 "flags": flags, "features": record.get("features"),
                                 "status": record["status"]})
    expected = 1000 if source == "probe" else 112163
    if count != expected:
        raise RuntimeError(f"Wrong population: {count} != {expected}")
    target = OUT / name
    if target.exists():
        raise FileExistsError(target)
    target.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in selected))
    receipt = {"source": source, "population": count, "candidate_count": len(selected),
               "candidate_sha256": sha256(target),
               "input_sha256": {path.name: sha256(path) for path in inputs},
               "candidate_only": True, "no_gallery_change": True,
               "no_query_outcomes_used": True}
    (OUT / name.replace(".jsonl", "_receipt.json")).write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"population": count, "candidates": len(selected), "path": str(target)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("probe", "full"), required=True)
    run(parser.parse_args().source)
