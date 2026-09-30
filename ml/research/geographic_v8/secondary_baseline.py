"""Verify the fixed 44,995-reference Mapillary/KartaView comparator."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, NIGHT, QUERY, metrics


def run():
    source = NIGHT / "licensed_third"
    g = pd.read_parquet(source / "gallery.parquet")
    q = pd.read_parquet(QUERY)
    saved = json.loads((source / "context.json").read_text())
    rows = json.loads((source / "context_rows.json").read_text())
    if (len(g) != 44995 or len(q) != 1184 or set(g.source) != {"mapillary", "kartaview"}
            or rows["query_ids"] != q.id.tolist()):
        raise RuntimeError("Release-compatible comparison population changed")
    indices = np.asarray(rows["top100_gallery_rows"], dtype=np.int32)
    report, errors, _ = metrics(q, g, indices)
    np.testing.assert_allclose(errors, rows["errors_m"], rtol=0, atol=1e-7)
    if int((errors <= 100).sum()) != 380 or report["raw"]["accuracy_100m"] != saved["raw"]["accuracy_100m"]:
        raise RuntimeError("Release-compatible baseline is not 380/1184")
    out = LOCAL / "secondary_baseline"
    out.mkdir(exist_ok=True)
    np.savez(out / "predictions.npz", query_ids=q.id.to_numpy(str), gallery_ids=g.id.to_numpy(str),
             indices=indices, errors_m=errors)
    report |= {"sources": ["mapillary", "kartaview"], "gallery_count": len(g),
        "gallery_sha256": digest(source / "gallery.parquet"), "query_sha256": digest(QUERY),
        "source_rows_sha256": digest(source / "context_rows.json"),
        "score_level_reproduction": "query GPS distances recomputed from original top100; descriptors reused from primary study",
        "status": "exploratory development comparator, not production release"}
    save(out / "report.json", report)
    print(json.dumps({"raw100": report["raw"]["accuracy_100m"], "r100": report["recall_at"]["100"],
                      "all_query_errors_agree": True}))


if __name__ == "__main__":
    run()
