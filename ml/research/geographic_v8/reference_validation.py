"""Independent-reference heldout loss before and after SAGE adaptation."""
import json

import numpy as np
import torch

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.common import LOCAL, frames
from ml.research.geographic_v8.train_sage_asymmetric import evaluate_reference, load_model

OUT = LOCAL / "sage_adaptation_v3"


def run():
    torch.set_num_threads(2)
    complete = json.loads((OUT / "complete.json").read_text())
    pairs = json.loads((OUT / "pairs.json").read_text())
    if digest(OUT / "pairs.json") != complete["contract"]["pairs_sha256"]:
        raise RuntimeError("Reference validation pairs changed")
    validation = pairs["validation"][:128]
    _, g = frames()
    selected = np.unique(np.asarray([i for r in validation for i in
                                     [r["anchor"], r["positive"], *r["negatives"]]], dtype=int))
    vectors = selected_vectors(g, selected, staged_entries(g))
    lookup = {int(index): pos for pos, index in enumerate(selected)}
    model, transform = load_model(train=False)
    baseline = evaluate_reference(model, validation, g, transform, lookup, vectors)
    result = {"unadapted": baseline, "selected_epoch": complete["selected_epoch"],
        "adapted": complete["history"][complete["selected_epoch"]-1]["reference_validation"],
        "reference_heldout_only": True, "query_GT_used": False,
        "pairs_sha256": digest(OUT / "pairs.json")}
    save(OUT / "reference_validation_comparison.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    run()
