"""Learned local matching of development retrievals with fixed-image caching."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import cv2
import numpy as np
import pandas as pd
import torch
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import GroupKFold
from threadpoolctl import threadpool_limits

from ml.localization.geo import haversine_m
from ml.research.metrics import paired_group_bootstrap, raw_metrics, retrieval_metrics, select_threshold
from ml.research.mode_experiment import BUNDLE, DEVELOPMENT_HASH
from ml.research.seal import sha256
from ml.retrieval.image_io import load_rgb_image

ROOT = Path("data/evaluation/moscow_research_v5/development/xfeat")
CHECKPOINT = "0f5187fd7bedd26c7fe6acc9685444493a165a35ecc087b33c2db3627f3ea10b"
CONTRACT = "xfeat-v1-max640-1024keypoints-float32-mnn082-homography3px-usac1000"


def feature(model, path):
    digest = sha256(Path(path))
    target = ROOT / "features" / f"{digest}.npz"
    if target.exists():
        with np.load(target, allow_pickle=False) as values:
            if str(values["contract"]) != CONTRACT or str(values["checkpoint"]) != CHECKPOINT:
                raise ValueError("local feature cache mismatch")
            return {k: values[k] for k in ("keypoints", "descriptors", "size")}
    image = load_rgb_image(path)
    image.thumbnail((640, 640))
    tensor = torch.from_numpy(np.asarray(image).copy()).permute(2, 0, 1).float()[None] / 255
    current = model.detectAndCompute(tensor, top_k=1024)[0]
    result = {k: current[k].detach().cpu().numpy().astype(np.float32) for k in ("keypoints", "descriptors")}
    result["size"] = np.asarray(image.size, np.float32)
    target.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(target, **result, contract=CONTRACT, checkpoint=CHECKPOINT)
    return result


def evidence(query, reference):
    a, b = query["descriptors"], reference["descriptors"]
    if len(a) < 8 or len(b) < 8:
        return [0.0] * 7
    similarities = a @ b.T
    row = similarities.argmax(axis=1)
    valid = (similarities.argmax(axis=0)[row] == np.arange(len(a))) & (similarities[np.arange(len(a)), row] > 0.82)
    left, right = query["keypoints"][valid], reference["keypoints"][row[valid]]
    if len(left) < 8:
        return [float(len(left)), 0, 0, 0, 0, 0, 0]
    cv2.setRNGSeed(20260905)
    homography, mask = cv2.findHomography(left, right, cv2.USAC_MAGSAC, 3, maxIters=1000, confidence=0.999)
    if homography is None or mask is None or not np.isfinite(homography).all():
        return [float(len(left)), 0, 0, 0, 0, 0, 0]
    mask = mask.ravel().astype(bool)
    count = int(mask.sum())
    if count < 4:
        return [float(len(left)), count, count / len(left), 0, 0, 0, 0]
    cover = []
    for points, shape in [(left[mask], query["size"]), (right[mask], reference["size"])]:
        cover.append(float(cv2.contourArea(cv2.convexHull(points)) / np.prod(shape)))
    predicted = cv2.perspectiveTransform(left[mask, None, :], homography)[:, 0, :]
    reprojection = float(np.median(np.linalg.norm(predicted - right[mask], axis=1)))
    return [
        float(len(left)),
        count,
        count / len(left),
        cover[0],
        cover[1],
        reprojection,
        float(similarities[np.arange(len(a))[valid], row[valid]][mask].mean()),
    ]


def run(top_k):
    if not 1 <= top_k <= 100:
        raise ValueError("retrieval source contains at most 100 candidates")
    if sha256(BUNDLE / "development_queries.parquet") != DEVELOPMENT_HASH:
        raise ValueError("development identity mismatch")
    ROOT.mkdir(parents=True, exist_ok=True)
    source = Path("data/models/research_v5/xfeat").resolve()
    expected_revision = "e92685f57f8318b18725c5c8c0bd28c7fe188d9a"
    if (
        subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() != expected_revision
        or subprocess.run(["git", "-C", str(source), "diff", "--quiet", "HEAD"]).returncode
    ):
        raise ValueError("XFeat source changed")
    if sha256(source / "weights/xfeat.pt") != CHECKPOINT:
        raise ValueError("official XFeat checkpoint changed")
    sys.path.insert(0, str(source))
    from modules.xfeat import XFeat

    torch.set_num_threads(2)
    cv2.setNumThreads(1)
    model = XFeat(top_k=1024)
    queries = pd.read_parquet(BUNDLE / "development_queries.parquet").set_index("id")
    gallery = pd.read_parquet(BUNDLE / "gallery.parquet").set_index("id")
    stream = json.loads((ROOT.parent / "query_views/retrieval.json").read_text())
    ids = stream["query_ids"]
    original = stream["methods"]["original"]
    groups = [str(queries.loc[i].h3_coarse) for i in ids]
    features = []
    errors = []
    started = time.monotonic()
    for i, (qid, matches) in enumerate(zip(ids, original["top100"], strict=True)):
        pair_path = ROOT / f"{qid}-k{top_k}.npz"
        if pair_path.exists():
            with np.load(pair_path, allow_pickle=False) as cache:
                if str(cache["contract"]) != CONTRACT or cache["reference_ids"].tolist() != matches["ids"][:top_k]:
                    raise ValueError("pair evidence cache changed")
                local = cache["features"]
                distances = cache["errors"]
        else:
            query = feature(model, queries.loc[qid].image_path)
            local = []
            distances = []
            with threadpool_limits(limits=2):
                for j, rid in enumerate(matches["ids"][:top_k]):
                    reference = feature(model, gallery.loc[rid].image_path)
                    pair = evidence(query, reference)
                    local.append(
                        [
                            matches["scores"][j],
                            matches["scores"][j] - matches["scores"][0],
                            1 / (j + 1),
                            np.log1p(j + 1),
                            *pair,
                        ]
                    )
                    distances.append(
                        haversine_m(
                            queries.loc[qid].lat, queries.loc[qid].lon, gallery.loc[rid].lat, gallery.loc[rid].lon
                        )
                    )
            local = np.asarray(local)
            distances = np.asarray(distances)
            np.savez(
                pair_path,
                features=local,
                errors=distances,
                contract=CONTRACT,
                reference_ids=np.array(matches["ids"][:top_k]),
            )
        features.append(local)
        errors.append(distances)
        if (i + 1) % 25 == 0:
            print("matched", i + 1, "of", len(ids), "seconds", round(time.monotonic() - started), flush=True)
    x = np.asarray(features)
    distance = np.asarray(errors)
    policies = {}
    for weight in (0.01, 0.02, 0.04, 0.08):
        # Spatial support helps disfavor repeated tiny image regions.
        geometric = np.log1p(x[:, :, 5]) * np.sqrt(np.minimum(x[:, :, 7], x[:, :, 8]))
        policies[f"similarity_plus_geometry_{weight}"] = x[:, :, 0] + weight * geometric
    for count in (8, 12, 20, 30):
        verified = (x[:, :, 5] >= count) & (np.minimum(x[:, :, 7], x[:, :, 8]) >= 0.01)
        policies[f"verified_inliers_{count}"] = np.where(verified, x[:, :, 5] + x[:, :, 0], x[:, :, 0])
    for leaves in (7, 15):
        scores = np.zeros(distance.shape)
        for train, test in GroupKFold(n_splits=5).split(x, groups=groups):
            model = HistGradientBoostingClassifier(
                max_leaf_nodes=leaves,
                max_iter=100,
                learning_rate=0.07,
                l2_regularization=5,
                min_samples_leaf=40,
                early_stopping=False,
                random_state=20260905,
            )
            with threadpool_limits(limits=2):
                model.fit(x[train].reshape(-1, x.shape[-1]), (distance[train].ravel() <= 100))
                scores[test] = model.predict_proba(x[test].reshape(-1, x.shape[-1]))[:, 1].reshape(len(test), top_k)
        policies[f"group_oof_hgb_leaf{leaves}"] = scores
    summary = {
        "kind": "historical_development_only_local_matching",
        "contract": CONTRACT,
        "checkpoint": CHECKPOINT,
        "k": top_k,
        "baseline_top1": raw_metrics(original["errors"]),
        "candidates": {},
        "query_ids": ids,
        "elapsed_seconds": time.monotonic() - started,
    }
    for name, scores in policies.items():
        order = np.argsort(-scores, axis=1, kind="stable")
        chosen = order[:, 0]
        predicted = distance[np.arange(len(ids)), chosen]
        positive_ranks = []
        for i, permutation in enumerate(order):
            positives = np.flatnonzero(distance[i, permutation] <= 100)
            positive_ranks.append(int(positives[0]) + 1 if len(positives) else original["ranks"][i])
        summary["candidates"][name] = {
            "raw": raw_metrics(predicted),
            "retrieval": retrieval_metrics(positive_ranks, gallery_size=len(gallery)),
            "paired_gain_vs_sage_top1": paired_group_bootstrap(original["errors"], predicted, groups),
            "exploratory_precision_coverage": select_threshold(predicted, scores[np.arange(len(ids)), chosen]),
            "errors": predicted.tolist(),
        }
    (ROOT / f"summary-k{top_k}.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    print(json.dumps({k: v["raw"] for k, v in summary["candidates"].items()}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-k", type=int, default=20)
    run(parser.parse_args().top_k)
