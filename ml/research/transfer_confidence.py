"""Inference-only evidence before and after contextual coordinate selection."""

from __future__ import annotations

import argparse
import json
import math

from ml.localization.confidence_model import FEATURE_NAMES, PART2_5_FEATURE_NAMES
from ml.localization.geo import haversine_m
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256, write_once

EXTRA = ("rerank_same_reference", "rerank_log_move_m", "rerank_original_rank", "rerank_same_100m_location")


def transfer_features(base, contextual, original_coordinate, selected_coordinate, original_rank):
    movement = haversine_m(*original_coordinate, *selected_coordinate)
    return (
        dict(base)
        | {"context_" + name: float(contextual[name]) for name in FEATURE_NAMES}
        | {
            "rerank_same_reference": float(original_rank == 1),
            "rerank_log_move_m": math.log1p(movement),
            "rerank_original_rank": float(original_rank),
            "rerank_same_100m_location": float(movement <= 100),
        }
    )


def run(split="new"):
    original_path = ROOT / f"development/gallery_fusion_top1_{split}/union_larger_budget/fusion50_rows.json"
    contextual_path = ROOT / f"development/hybrid_top1_{split}/hybrid_context30_mix0.5_rows.json"
    stream_path = ROOT / f"development/hybrid_context_union_{split}/retrieval.json"
    original, contextual, stream = (json.loads(p.read_text()) for p in (original_path, contextual_path, stream_path))
    if original["query_ids"] != contextual["query_ids"] or original["query_ids"] != stream["query_ids"]:
        raise ValueError("confidence evidence must align with the fixed selected coordinates")
    before = stream["methods"]["fusion50"]["top100"]
    after = stream["methods"]["hybrid_context30_mix0.5"]["top100"]
    features = []
    for i in range(len(original["query_ids"])):
        rank = before[i]["ids"].index(after[i]["ids"][0]) + 1
        features.append(
            transfer_features(
                original["features"][i],
                contextual["features"][i],
                original["predictions"][i],
                contextual["predictions"][i],
                rank,
            )
        )
    write_once(
        ROOT / f"development/hybrid_transfer_features{'_historical' if split == 'historical' else ''}.json",
        dict(contextual, features=features)
        | {
            "feature_sets": {
                "base14": list(PART2_5_FEATURE_NAMES),
                "base14_transfer": list(PART2_5_FEATURE_NAMES + EXTRA),
                "combined": list(FEATURE_NAMES) + ["context_" + name for name in FEATURE_NAMES] + list(EXTRA),
            },
            "evidence_sha256": {str(p): sha256(p) for p in (original_path, contextual_path, stream_path)},
            "labels": "unchanged contextual top1 errors; no geographic query truth in features",
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["historical", "new"], default="new")
    run(parser.parse_args().split)
