"""Strict sequence/geographic gate for the reference-only SAGE-L adaptation."""
import argparse
import json

import h3

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8 import train_sage_asymmetric as core
from ml.research.geographic_v8.common import LOCAL, frames

OUT = LOCAL / "sage_adaptation_v2"


def validate_split():
    _, g = frames()
    record = json.loads((OUT / "pairs.json").read_text())
    if digest(OUT / "pairs.json") != json.loads((OUT / "pairs_sha256.json").read_text())["sha256"]:
        raise RuntimeError("Strict pair cache changed")
    train, validation = record["train"], record["validation"][:128]
    seq = g.sequence_key.astype(str).to_numpy()
    a = {seq[i] for r in train for i in (r["anchor"], r["positive"], *r["negatives"])}
    b = {seq[i] for r in validation for i in (r["anchor"], r["positive"], *r["negatives"])}
    if a & b or len(train) < 1000 or len(validation) < 100:
        raise RuntimeError("Provider-sequence train/validation leakage or insufficient records")
    val_cells = {r["cell"] for r in validation}
    embargo = set().union(*(set(h3.grid_disk(cell, 1)) for cell in val_cells))
    for row in train:
        for index in (row["anchor"], row["positive"]):
            reference = g.iloc[index]
            if h3.latlng_to_cell(reference.lat, reference.lon, 6) in embargo:
                raise RuntimeError("Reference training anchor/positive crossed validation embargo")
    result = {"verified": True, "pairs_sha256": digest(OUT / "pairs.json"),
        "train_anchors": len(train), "validation_anchors": len(validation),
        "all_role_sequence_overlap": 0, "training_sequence_count": len(a),
        "validation_sequence_count": len(b), "validation_h3r6_cells": len(val_cells),
        "embargoed_h3r6_cells": len(embargo)}
    path = OUT / "strict_split_gate.json"
    if path.exists() and json.loads(path.read_text()) != result:
        raise RuntimeError("Strict split gate receipt changed")
    if not path.exists():
        save(path, result)
    return result


def run(smoke_only=False):
    check = validate_split()
    print("strict split gate", json.dumps(check), flush=True)
    core.OUT = OUT
    core.run(smoke_only=smoke_only)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    run(parser.parse_args().smoke)
