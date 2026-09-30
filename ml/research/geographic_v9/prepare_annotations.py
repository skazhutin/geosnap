"""Draw a blinded, reproducible 200-image sample from all four frozen outcome buckets."""
from __future__ import annotations

import hashlib
import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.common import OUT, V8, frames, record

SEED = 20260928
PER_BUCKET = 50
BUCKETS = ("correct", "ranking_failure", "retrieval_failure", "no_coverage")


def run():
    q, _ = frames()
    with np.load(V8 / "baseline/predictions.npz", allow_pickle=False) as baseline:
        if baseline["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("Frozen outcome/query identity mismatch")
        buckets = baseline["buckets"].copy()
    rng = np.random.default_rng(SEED)
    selected = np.concatenate([rng.choice(np.flatnonzero(buckets == name),
        size=PER_BUCKET, replace=False) for name in BUCKETS])
    selected = rng.permutation(selected)
    private = []
    for position in selected:
        row = q.iloc[int(position)]
        token = hashlib.sha256(f"geosnap-v9-blind-{SEED}-{row.id}".encode()).hexdigest()[:24]
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError(f"Query image changed: {row.id}")
        private.append({"token": token, "query_id": str(row.id),
            "query_index": int(position), "image_path": str(row.image_path),
            "image_sha256": str(row.file_sha256), "hidden_bucket": str(buckets[position]),
            "hidden_group": str(row.evaluation_geo_group_id), "source": str(row.source)})
    if len({row["token"] for row in private}) != len(private):
        raise RuntimeError("Annotation token collision")
    folder = OUT / "annotations"
    folder.mkdir(parents=True, exist_ok=True)
    private_path = folder / "private_sample.json"
    if private_path.exists():
        if json.loads(private_path.read_text())["items"] != private:
            raise RuntimeError("Existing blind sample changed")
    else:
        save(private_path, {"seed": SEED, "items": private,
            "warning": "Private outcome key. Never serve this JSON to annotators."})
    receipt = {"status": "awaiting_human_labels", "sample_size": len(private),
        "sampling": "50 uniform without replacement per hidden frozen outcome bucket, then global random order",
        "seed": SEED, "outcome_blind_ui": True, "hidden_bucket_counts": {name: PER_BUCKET for name in BUCKETS},
        "private_sample_sha256": digest(private_path),
        "baseline_predictions_sha256": digest(V8 / "baseline/predictions.npz"),
        "rater_count": 0, "human_labels": 0}
    receipt_path = folder / "sample_receipt.json"
    if receipt_path.exists():
        if json.loads(receipt_path.read_text()) != receipt:
            raise RuntimeError("Existing sample receipt changed")
    else:
        save(receipt_path, receipt)
    record("blinded_quality_sample_200", {"status": "prepared_awaiting_human_labels",
        "model_revision": None, "sample_seed": SEED, "sample_size": len(private),
        "sampling": receipt["sampling"], "human_label_version": None,
        "fitting_split": None, "evaluation_split": "blind manual labeling, no metrics yet",
        "metrics": None, "decision": "collect independent ratings from at least two raters",
        "artifact_paths": [str(private_path), str(receipt_path)],
        "artifact_hashes": {"private_sample": digest(private_path),
            "sample_receipt": digest(receipt_path)}})
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    run()
