"""Mine reference-only spatial positives and visually hard distant negatives."""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict

import faiss
import h3
import numpy as np
from sklearn.neighbors import BallTree

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames

OUT = LOCAL / "sage_adaptation"
STAGE = LOCAL / "staged"
SEED = 20260928


def key(value):
    return hashlib.sha256(f"sage-adaptation-{SEED}-{value}".encode()).digest()


def full_vectors(g):
    pool = json.loads((STAGE / "descriptor_pool.json").read_text())["entries"]
    paths = json.loads((STAGE / "paths.json").read_text())
    assignments = defaultdict(list)
    for i, row in enumerate(g.itertuples()):
        item = pool[row.id]
        if item["image_sha256"] != row.file_sha256:
            raise RuntimeError("Reference descriptor/image identity mismatch")
        local = paths[item["path"]]["path"]
        assignments[local].append((i, item["row"]))
    matrix = np.empty((len(g), 8448), np.float32)
    for j, (filename, selected) in enumerate(assignments.items()):
        block = np.load(filename, mmap_mode="r", allow_pickle=False)
        destination, source = np.asarray(selected, dtype=int).T
        matrix[destination] = block[source]
        if j % 100 == 0:
            print("loaded staged descriptor shards", j+1, "/", len(assignments), flush=True)
    if not np.isfinite(matrix).all() or not np.allclose(np.linalg.norm(matrix, axis=1), 1, atol=1e-5):
        raise RuntimeError("Staged SAGE descriptor population invalid")
    return matrix


def run():
    q, g = frames()
    OUT.mkdir(exist_ok=True)
    manifest = OUT / "mining_contract.json"
    contract = {"gallery_sha256": digest(STAGE / "gallery.parquet"),
        "query_label_use": False, "positive": "independent provider sequence within 25m and compatible heading",
        "negative": "top128 SAGE reference-descriptor cosine, >=200m from anchor, different sequence",
        "anchors": 2304, "training_anchor_cap": 2048, "validation": "H3r6 cell hash modulo5",
        "seed": SEED, "faiss": faiss.__version__}
    if manifest.exists() and json.loads(manifest.read_text()) != contract:
        raise RuntimeError("Reference mining contract changed")
    if not manifest.exists():
        save(manifest, contract)
    coords = np.radians(g[["lat", "lon"]].to_numpy(float))
    seq = g.sequence_key.astype(str).to_numpy()
    heading = g.heading.to_numpy(float)
    cells = np.asarray([h3.latlng_to_cell(lat, lon, 6) for lat, lon in g[["lat", "lon"]].to_numpy()], dtype=str)
    nearby = BallTree(coords, metric="haversine").query_radius(coords, r=25/6371008.8)
    positive = {}
    for i, neighbors in enumerate(nearby):
        neighbors = neighbors[seq[neighbors] != seq[i]]
        if len(neighbors) == 0:
            continue
        delta = np.abs(heading[neighbors]-heading[i]) % 360
        neighbors = neighbors[(~np.isfinite(delta)) | (np.minimum(delta, 360-delta) <= 60)]
        if len(neighbors):
            positive[i] = sorted(map(int, neighbors), key=lambda j: key(g.id.iloc[j]))
    by_cell = defaultdict(list)
    for i in positive:
        by_cell[cells[i]].append(i)
    for cell in by_cell:
        by_cell[cell].sort(key=lambda i: key(g.id.iloc[i]))
    chosen = []
    for rank in range(max(map(len, by_cell.values()))):
        for cell in sorted(by_cell, key=key):
            if rank < len(by_cell[cell]):
                chosen.append(by_cell[cell][rank])
                if len(chosen) >= contract["anchors"]:
                    break
        if len(chosen) >= contract["anchors"]:
            break
    if len(chosen) < 512:
        raise RuntimeError("Too few independent-sequence positive anchors")
    print("eligible anchors", len(positive), "selected", len(chosen), flush=True)
    vectors = full_vectors(g)
    faiss.omp_set_num_threads(4)
    index = faiss.IndexFlatIP(8448)
    started = time.perf_counter()
    index.add(vectors)
    scores, matches = index.search(vectors[chosen], 128)
    print("FAISS hard-negative mining runtime", time.perf_counter()-started, flush=True)
    records = []
    for anchor, row in zip(chosen, matches, strict=True):
        lat0, lon0 = coords[anchor]
        lat, lon = coords[row].T
        a = np.sin((lat-lat0)/2)**2 + np.cos(lat0)*np.cos(lat)*np.sin((lon-lon0)/2)**2
        distance_m = 6371008.8 * 2*np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        negatives = row[(distance_m >= 200) & (seq[row] != seq[anchor])][:8]
        if len(negatives) < 8:
            raise RuntimeError(f"Hard negatives missing for anchor {anchor}")
        records.append({"anchor": int(anchor), "positive": int(positive[anchor][0]),
            "negatives": list(map(int, negatives)), "cell": cells[anchor],
            "role": "validation" if int.from_bytes(key(cells[anchor])[:2], "little") % 5 == 0 else "train"})
    train = [r for r in records if r["role"] == "train"][:2048]
    valid = [r for r in records if r["role"] == "validation"]
    if len(train) < 1000 or len(valid) < 100:
        raise RuntimeError("Reference geographic train/validation split too small")
    assert not set(r["cell"] for r in train) & set(r["cell"] for r in valid)
    save(OUT / "pairs.json", {"contract_sha256": digest(manifest), "train": train,
        "validation": valid, "eligible_anchor_count": len(positive), "selected_anchor_count": len(chosen),
        "train_geo_groups": len(set(r["cell"] for r in train)),
        "validation_geo_groups": len(set(r["cell"] for r in valid)),
        "mining_runtime_s": time.perf_counter()-started})
    save(OUT / "pairs_sha256.json", {"sha256": digest(OUT / "pairs.json")})
    print("reference pairs", len(train), len(valid), flush=True)


if __name__ == "__main__":
    run()
