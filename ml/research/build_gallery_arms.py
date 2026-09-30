"""Finalize equal-size random/diverse gallery additions after image audit."""

from __future__ import annotations

import json

import pandas as pd

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.ingestion.schema import write_manifest
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256, write_once


def run():
    root = ROOT / "gallery_expansion"
    if (root / "arms.json").exists():
        raise ValueError("matched gallery arms already frozen")
    baseline = pd.read_parquet("data/evaluation/moscow_real_v4/gallery.parquet")
    audited = pd.read_parquet(root / "audited_union.parquet").set_index("id")
    arms = {}
    counts = {}
    for arm, filename in [("diversity", "diversity_selected"), ("random", "random_matched")]:
        planned = pd.read_parquet(root / f"{filename}.parquet")
        index = _PerceptualHashIndex(4)
        counter = 0
        for row in baseline.itertuples():
            index.add(counter, int(str(row.perceptual_hash), 16))
            counter += 1
        kept = []
        for identity in planned.id:
            if identity not in audited.index:
                continue
            row = audited.loc[identity]
            phash = int(str(row.perceptual_hash), 16)
            if index.matches(phash):
                continue
            kept.append(identity)
            index.add(counter, phash)
            counter += 1
        arms[arm] = audited.loc[kept].reset_index()
        counts[arm] = {"planned": len(planned), "available_after_independent_dedup": len(kept)}
    budget = min(len(current) for current in arms.values())
    if budget == 0:
        raise ValueError("no matched gallery additions")
    artifacts = {}
    for name, current in arms.items():
        # Frozen metadata ordering, not development outcomes, decides trimming.
        additions = current.head(budget)
        full = pd.concat([baseline, additions], ignore_index=True)
        if full.id.duplicated().any():
            raise ValueError("duplicate gallery identities")
        target = root / f"gallery_{name}.parquet"
        write_manifest(full, target)
        artifacts[target.name] = sha256(target)
    write_once(
        root / "arms.json",
        {
            "status": "equal_budget_gallery_arms_fixed_before_inference",
            "additions_per_arm": budget,
            "baseline_rows": len(baseline),
            "arm_counts": counts,
            "files": artifacts,
            "selection_used_query_coordinates_or_outcomes": False,
        },
    )
    print(json.dumps({"additions_per_arm": budget, "arm_counts": counts}, indent=2))


if __name__ == "__main__":
    run()
