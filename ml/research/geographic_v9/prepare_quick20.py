"""Draw a short blinded pilot without modifying the 200-image master sample."""
from __future__ import annotations

import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.common import OUT, record

SEED = 20260929
BUCKETS = ("correct", "ranking_failure", "retrieval_failure", "no_coverage")


def run():
    folder = OUT / "annotations"
    master_path = folder / "private_sample.json"
    master = json.loads(master_path.read_text())["items"]
    rng = np.random.default_rng(SEED)
    chosen = [master[int(i)] for bucket in BUCKETS for i in rng.choice(
        [j for j, row in enumerate(master) if row["hidden_bucket"] == bucket],
        size=5, replace=False)]
    chosen = [chosen[int(i)] for i in rng.permutation(len(chosen))]
    private = folder / "quick20_private_sample.json"
    receipt = folder / "quick20_sample_receipt.json"
    content = {"seed": SEED, "items": chosen,
               "warning": "Private outcome key; never serve this JSON to annotators."}
    details = {"sample_size": 20, "seed": SEED,
        "sampling": "5 random images without replacement per hidden bucket from the sealed 200-image sample, then random order",
        "hidden_bucket_counts": {bucket: 5 for bucket in BUCKETS},
        "master_sample_sha256": digest(master_path),
        "private_sample_sha256": None,
        "status": "awaiting_human_labels"}
    if private.exists():
        if json.loads(private.read_text()) != content:
            raise RuntimeError("Existing quick pilot sample changed")
    else:
        save(private, content)
    details["private_sample_sha256"] = digest(private)
    if receipt.exists():
        if json.loads(receipt.read_text()) != details:
            raise RuntimeError("Existing quick pilot receipt changed")
    else:
        save(receipt, details)
    record("blinded_quality_pilot_20", {"status": "prepared_awaiting_human_labels",
        "sample_seed": SEED, "sample_size": 20, "sampling": details["sampling"],
        "human_label_version": None, "metrics": None,
        "artifact_paths": [str(private), str(receipt)],
        "artifact_hashes": {"sample": digest(private), "receipt": digest(receipt)}})
    print(json.dumps({"sample_size": 20, "hidden_bucket_counts": details["hidden_bucket_counts"],
                      "private_sample_sha256": details["private_sample_sha256"]}))


if __name__ == "__main__":
    run()
