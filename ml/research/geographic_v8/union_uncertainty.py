"""Paired geographic bootstrap for candidate-union recall deltas."""
import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames
from ml.research.metrics import paired_group_bootstrap


def run():
    q, _ = frames()
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as baseline:
        bucket = baseline["buckets"]
        ids = baseline["query_ids"].tolist()
    if ids != q.id.tolist():
        raise RuntimeError("Union/bootstrap query identity mismatch")
    base = np.isin(bucket, ["correct", "ranking_failure"])
    results = {}
    for path in sorted((LOCAL / "cross_model_union").glob("*.json")):
        if path.name == "summary.json":
            continue
        record = json.loads(path.read_text())
        if record["query_ids"] != ids:
            raise RuntimeError("Candidate union query identity mismatch")
        new = np.asarray(record["oracle100_per_query"], bool)
        results[path.stem] = {"baseline_oracle100_count": int(base.sum()),
            "union_oracle100_count": int(new.sum()),
            "gain": paired_group_bootstrap(np.where(base, 0., 101.),
                                            np.where(new, 0., 101.), q.h3_coarse.astype(str).tolist()),
            "union_sha256": digest(path)}
    save(LOCAL / "cross_model_union/uncertainty.json", results)
    print({k:v["gain"] for k,v in results.items()}, flush=True)


if __name__ == "__main__":
    run()
