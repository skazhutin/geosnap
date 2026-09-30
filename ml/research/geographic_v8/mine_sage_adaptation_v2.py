"""Repair pilot reference split with sequence-disjoint roles and a geographic embargo."""
from __future__ import annotations

import hashlib
import json
import time

import faiss
import h3
import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames
from ml.research.geographic_v8.mine_sage_adaptation import full_vectors

PILOT = LOCAL / "sage_adaptation"
OUT = LOCAL / "sage_adaptation_v2"
SEED = 20260928


def role_hash(sequence):
    value = hashlib.sha256(f"sage-v2-sequence-{SEED}-{sequence}".encode()).digest()
    return "validation" if int.from_bytes(value[:4], "little") % 5 == 0 else "train"


def run():
    _, g = frames()
    original = json.loads((PILOT / "pairs.json").read_text())
    val = original["validation"][:128]
    seq = g.sequence_key.astype(str).to_numpy()
    coords = g[["lat", "lon"]].to_numpy(float)
    cells = np.asarray([h3.latlng_to_cell(lat, lon, 6) for lat, lon in coords])
    validation_cells = {r["cell"] for r in val}
    embargo = set().union(*(set(h3.grid_disk(cell, 1)) for cell in validation_cells))
    val_anchor_positive = {seq[i] for r in val for i in (r["anchor"], r["positive"])}
    train = [r for r in original["train"] if not bool(g.iloc[r["anchor"]].is_pano)
             and all(cells[i] not in embargo and seq[i] not in val_anchor_positive
                     for i in (r["anchor"], r["positive"]))]
    train_anchor_positive = {seq[i] for r in train for i in (r["anchor"], r["positive"])}
    if len(train) < 1000 or len(val) < 100 or train_anchor_positive & val_anchor_positive:
        raise RuntimeError("Strict source sequence/geographic split failed")
    role = {value: ("validation" if value in val_anchor_positive else "train")
            for value in train_anchor_positive | val_anchor_positive}
    for value in np.unique(seq):
        role.setdefault(value, role_hash(value))
    print("sequence-disjoint anchors", len(train), len(val), "embargo cells", len(embargo), flush=True)
    vectors = full_vectors(g)
    index = faiss.IndexFlatIP(8448)
    faiss.omp_set_num_threads(4)
    started = time.perf_counter()
    index.add(vectors)
    all_records = train + val
    _, matches = index.search(vectors[[r["anchor"] for r in all_records]], 512)
    output = {"train": [], "validation": []}
    radians = np.radians(coords)
    for position, (source, matched) in enumerate(zip(all_records, matches, strict=True)):
        anchor = source["anchor"]
        desired = "train" if position < len(train) else "validation"
        lat0, lon0 = radians[anchor]
        lat, lon = radians[matched].T
        a = np.sin((lat-lat0)/2)**2 + np.cos(lat0)*np.cos(lat)*np.sin((lon-lon0)/2)**2
        distance_m = 6371008.8*2*np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        acceptable = [int(j) for j, distance in zip(matched, distance_m, strict=True)
                      if distance >= 200 and seq[j] != seq[anchor] and role[seq[j]] == desired]
        if len(acceptable) < 8:
            raise RuntimeError(f"Insufficient role-isolated hard negatives for {anchor}")
        output[desired].append({"anchor": int(anchor), "positive": int(source["positive"]),
            "negatives": acceptable[:8], "cell": str(source["cell"]), "role": desired})
    train_sequences = {seq[i] for r in output["train"] for i in
                       (r["anchor"], r["positive"], *r["negatives"])}
    val_sequences = {seq[i] for r in output["validation"] for i in
                     (r["anchor"], r["positive"], *r["negatives"])}
    if train_sequences & val_sequences:
        raise RuntimeError("Reference sequence leaked across any training/validation role")
    if any(cells[i] in embargo for r in output["train"] for i in (r["anchor"], r["positive"])):
        raise RuntimeError("Training anchor/positive violates validation geographic embargo")
    OUT.mkdir(exist_ok=True)
    contract = {"source_pilot_pairs_sha256": digest(PILOT / "pairs.json"),
        "reference_gallery_sha256": digest(LOCAL / "staged/gallery.parquet"),
        "validation_anchor_count": len(val), "training_anchor_count": len(train),
        "validation_selection": "original H3r6 heldout cell hash, first128 anchors",
        "geographic_embargo": "one H3r6 ring around every validation anchor cell for train anchor and positive",
        "sequence_partition": "all anchor, positive and negative provider sequences disjoint across roles",
        "other_sequence_role": "SHA256 sequence key modulo5", "hard_negative_search": "full reference SAGE cosine top512, >=200m",
        "seed": SEED, "faiss_version": faiss.__version__, "query_GT_used": False}
    save(OUT / "mining_contract.json", contract)
    save(OUT / "pairs.json", {"contract_sha256": digest(OUT / "mining_contract.json"),
        "train": output["train"], "validation": output["validation"],
        "train_sequence_count": len(train_sequences), "validation_sequence_count": len(val_sequences),
        "cross_role_sequence_overlap": 0, "embargo_cell_count": len(embargo),
        "mining_runtime_s": time.perf_counter()-started})
    save(OUT / "pairs_sha256.json", {"sha256": digest(OUT / "pairs.json")})
    print("strict reference pairs", len(output["train"]), len(output["validation"]),
          "cross-role sequence overlap", len(train_sequences & val_sequences), flush=True)


if __name__ == "__main__":
    run()
