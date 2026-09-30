"""Exploratory geographically held-out KEEP/SWITCH study for v9 hypotheses."""
from __future__ import annotations

import json
import time

import h3
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import compare, metrics
from ml.research.geographic_v9.common import OUT, V8, frames, record
from ml.research.vector_evaluation import distances

HYP = OUT / "hypotheses"
SEED = 20260928
SWITCH_ERROR_COST = 5.0


def candidate_errors(query, coords, flat, offsets):
    error = np.empty(len(flat), np.float64)
    for i, row in enumerate(query.itertuples()):
        lo, hi = offsets[i:i+2]
        selected = coords[flat[lo:hi]]
        error[lo:hi] = distances(row.lat, row.lon,
            np.radians(selected[:, 0]), np.radians(selected[:, 1]))
    return error


def seal_labels():
    """Evaluation/training boundary: GT enters only after inference cache is sealed."""
    query, gallery = frames()
    contract = json.loads((HYP / "inference_contract.json").read_text())
    if contract["query_gt_used"] or digest(HYP / "features.npz") != contract["features_sha256"]:
        raise RuntimeError("Inference boundary not sealed")
    with np.load(HYP / "features.npz", allow_pickle=False) as z:
        if z["query_ids"].tolist() != query.id.tolist():
            raise RuntimeError("Query identity mismatch")
        base, challengers, offsets = z["baseline_rows"], z["challenger_rows"], z["offsets"]
        union_rows, union_offsets = z["union_rows"], z["union_offsets"]
    coords = gallery[["lat", "lon"]].to_numpy()
    base_error = candidate_errors(query, coords, base, np.arange(len(query)+1))
    challenge_error = candidate_errors(query, coords, challengers, offsets)
    union_error = candidate_errors(query, coords, union_rows, union_offsets)
    union_oracle = np.array([np.any(union_error[union_offsets[i]:union_offsets[i+1]] <= 100)
                             for i in range(len(query))])
    with np.load(V8 / "baseline/predictions.npz", allow_pickle=False) as z:
        np.testing.assert_allclose(base_error, z["errors_m"], atol=.001)
        original_bucket = z["buckets"].copy()
    if int(union_oracle.sum()) != 652:
        raise RuntimeError(f"Fixed six-model union changed: {union_oracle.sum()} vs 652")
    np.savez_compressed(HYP / "training_labels.npz", query_ids=query.id.to_numpy(str),
        baseline_errors_m=base_error, challenger_errors_m=challenge_error,
        union_oracle_100m=union_oracle, original_bucket=original_bucket)
    first = np.asarray([challenge_error[offsets[i]] for i in range(len(query))])
    reach = {}
    for top in (8, 30, 100, "all"):
        limit = None if top == "all" else top
        available = np.asarray([np.any(challenge_error[offsets[i]:offsets[i+1]][:limit] <= 100)
                                for i in range(len(query))])
        reach[str(top)] = {"correct_in_anchor_or_challenger": int(((base_error <= 100) | available).sum()),
                           "original_wrong_recoverable": int(((base_error > 100) & available).sum())}
    summary = {"candidate_union_oracle_100m": int(union_oracle.sum()),
        "representative_oracle_by_top_hypotheses": reach,
        "top_challenger_as_prediction_100m": int((first <= 100).sum()),
        "top_challenger_transitions": {
            "recoveries": int(((base_error > 100) & (first <= 100)).sum()),
            "regressions": int(((base_error <= 100) & (first > 100)).sum())},
        "original_bucket_union_retrievable": {name: int((union_oracle & (original_bucket == name)).sum())
            for name in np.unique(original_bucket)},
        "feature_sha256": contract["features_sha256"],
        "label_sha256": digest(HYP / "training_labels.npz")}
    save(HYP / "oracle_audit.json", summary)
    identifier = {"hypotheses": "hypotheses_oracle_audit_v1",
        "hypotheses_all": "hypotheses_all_oracle_audit_v2",
        "hypotheses_visual30": "hypotheses_visual30_oracle_audit_v1"}[HYP.name]
    record(identifier, {"status": "complete", "fitted": False,
        "query_gt_use": "labels and evaluation only, after sealed inference features",
        "result": summary, "artifact_paths": [str(HYP / "training_labels.npz"),
            str(HYP / "oracle_audit.json")],
        "artifact_hashes": {p.name: digest(p) for p in [HYP / "training_labels.npz", HYP / "oracle_audit.json"]}})
    print(json.dumps(summary, indent=2), flush=True)


def folds(query):
    group = query.evaluation_geo_group_id.astype(str).to_numpy()
    for fold, (possible, test) in enumerate(GroupKFold(n_splits=5).split(query, groups=group)):
        banned = set().union(*(set(h3.grid_disk(value, 1)) for value in set(group[test])))
        train = np.asarray([i for i in possible if group[i] not in banned], int)
        if len(train) < 100 or set(group[train]) & banned:
            raise RuntimeError("Geographic embargo leaves insufficient training data")
        yield fold, train, test


def frontier(probs, selected, old_good, new_good):
    """Descriptive, posthoc OOF frontier; not a threshold-selection protocol."""
    order = np.argsort(-probs, kind="stable")
    result = {}
    for limit in (0, 2, 5, 10):
        n = 0
        regressions = recoveries = 0
        for i in order:
            if probs[i] <= 0:
                break
            new_regression = bool(old_good[i] and not new_good[i])
            if regressions + new_regression > limit:
                break
            n += 1
            regressions += new_regression
            recoveries += bool(not old_good[i] and new_good[i])
        result[str(limit)] = {"switches": n, "recoveries": recoveries,
                              "regressions": regressions,
                              "final_correct": int(old_good.sum()) + recoveries - regressions}
    return result


def evaluate_oof():
    started = time.perf_counter()
    query, gallery = frames()
    with np.load(HYP / "features.npz", allow_pickle=False) as z:
        x = z["features"].copy()
        offsets = z["offsets"].copy()
        base = z["baseline_rows"].copy()
        challenger = z["challenger_rows"].copy()
        ids = z["query_ids"].tolist()
    with np.load(HYP / "training_labels.npz", allow_pickle=False) as z:
        if z["query_ids"].tolist() != ids or ids != query.id.tolist():
            raise RuntimeError("Label/feature/query mismatch")
        base_e, challenger_e = z["baseline_errors_m"], z["challenger_errors_m"]
    # 0 = neutral, 1 = net recovery, 2 = baseline regression.
    truth = np.zeros(len(x), np.int8)
    for i in range(len(query)):
        lo, hi = offsets[i:i+2]
        if base_e[i] <= 100:
            truth[lo:hi] = (challenger_e[lo:hi] > 100) * 2
        else:
            truth[lo:hi] = (challenger_e[lo:hi] <= 100) * 1
    variants = {
        "logistic": lambda: make_pipeline(StandardScaler(), LogisticRegression(
            C=.25, max_iter=600, random_state=SEED)),
        "shallow_boosted": lambda: HistGradientBoostingClassifier(max_iter=100,
            max_leaf_nodes=7, min_samples_leaf=80, l2_regularization=2.,
            learning_rate=.05, early_stopping=False, random_state=SEED),
    }
    probs = {name: np.full((len(x), 3), np.nan, np.float32) for name in variants}
    fold_receipt = []
    for fold, train, test in folds(query):
        train_rows = np.concatenate([np.arange(offsets[i], offsets[i+1]) for i in train])
        test_rows = np.concatenate([np.arange(offsets[i], offsets[i+1]) for i in test])
        weights = np.concatenate([np.full(offsets[i+1]-offsets[i],
            1/(offsets[i+1]-offsets[i]), np.float32) for i in train])
        fold_receipt.append({"fold": fold, "train_queries": len(train),
            "held_out_queries": len(test), "train_label_counts": np.bincount(truth[train_rows], minlength=3).tolist(),
            "held_out_query_ids": query.id.iloc[test].tolist(),
            "geographic_embargo": "one H3 ring"})
        for name, create in variants.items():
            model = create()
            if name == "logistic":
                model.fit(x[train_rows], truth[train_rows], logisticregression__sample_weight=weights)
            else:
                model.fit(x[train_rows], truth[train_rows], sample_weight=weights)
            p = model.predict_proba(x[test_rows])
            probs[name][test_rows] = p
        print("OOF fold", fold, "train", len(train), "test", len(test), flush=True)
    save(HYP / "folds.json", {"folds": fold_receipt, "selection": "fixed five-fold GroupKFold",
        "group": "evaluation_geo_group_id", "embargo": "one H3 ring",
        "cost_ratio_false_switch_to_recovery": SWITCH_ERROR_COST,
        "hyperparameters": {"logistic_C": .25, "boosted_max_iter": 100,
                            "boosted_max_leaf_nodes": 7}})
    coords = gallery[["lat", "lon"]].to_numpy()
    for name, p in probs.items():
        if not np.isfinite(p).all():
            raise RuntimeError(f"Incomplete OOF probabilities for {name}")
        expected_utility = p[:, 1] - SWITCH_ERROR_COST*p[:, 2]
        selected = base.copy()
        best_utility = np.zeros(len(query), np.float32)
        best_new_good = np.zeros(len(query), bool)
        for i in range(len(query)):
            lo, hi = offsets[i:i+2]
            j = lo + int(np.argmax(expected_utility[lo:hi]))
            best_utility[i] = expected_utility[j]
            best_new_good[i] = challenger_e[j] <= 100
            if expected_utility[j] > 0:
                selected[i] = challenger[j]
        output, errors, _ = metrics(query, gallery, selected[:, None])
        np.testing.assert_allclose(errors, candidate_errors(query, coords, selected,
            np.arange(len(query)+1)), atol=.001)
        comparison = compare(query, errors)
        switch = selected != base
        result = {"variant": name, "scope": "exploratory geographically embargoed OOF",
            "fitted_on_reused_development_labels": True,
            "not_independent_final_validation": True,
            "raw": output["raw"], "transitions": comparison["transitions"],
            "bucket_transitions": comparison["buckets"],
            "paired_geographic_bootstrap": comparison["paired_geographic_bootstrap"],
            "switch_count": int(switch.sum()),
            "switch_error_cost": SWITCH_ERROR_COST,
            "posthoc_oof_frontier_not_deployable_threshold": frontier(best_utility, selected,
                base_e <= 100, best_new_good),
            "runtime_s": time.perf_counter()-started,
            "feature_sha256": digest(HYP / "features.npz"),
            "folds_sha256": digest(HYP / "folds.json")}
        np.savez_compressed(HYP / f"{name}_oof.npz", query_ids=np.asarray(ids, str),
            baseline_rows=base, prediction_rows=selected, errors_m=errors,
            switch=switch, challenger_probabilities=p,
            expected_utility=best_utility)
        save(HYP / f"{name}_oof.json", result)
        suffix = {"hypotheses": "v1", "hypotheses_all": "all_v2",
                  "hypotheses_visual30": "visual30_v1"}[HYP.name]
        record(f"keep_switch_{name}_oof_{suffix}", {"status": "complete",
            "fitted": True, "fitting_split": "outer fivefold geographic OOF, one H3 ring embargo",
            "evaluation_split": "1184 reused development queries, OOF exploratory",
            "hyperparameters": {"switch_error_cost": SWITCH_ERROR_COST},
            "result": result,
            "artifact_paths": [str(HYP / f"{name}_oof.npz"), str(HYP / f"{name}_oof.json")],
            "artifact_hashes": {f: digest(HYP / f) for f in (f"{name}_oof.npz", f"{name}_oof.json")}})
        print(name, "correct", int((errors <= 100).sum()), "switches", int(switch.sum()),
              "transitions", comparison["transitions"], flush=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Evaluate all geographic hypotheses")
    parser.add_argument("--visual30", action="store_true", help="Evaluate 30 visual-context hypotheses")
    args = parser.parse_args()
    if args.all and args.visual30:
        parser.error("Choose only one candidate set")
    if args.all:
        HYP = OUT / "hypotheses_all"
    elif args.visual30:
        HYP = OUT / "hypotheses_visual30"
    if not (HYP / "training_labels.npz").exists():
        seal_labels()
    evaluate_oof()
