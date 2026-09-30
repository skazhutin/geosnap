"""Final development evidence for the common-quarantine gallery comparison."""
import json

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap, retrieval_metrics
from ml.research.night_family_uncertainty import bootstrap_differences


def categories(rows, covered):
    ranks = rows.get("positive_ranks", rows.get("positive_ranks_through100"))
    values = np.asarray([np.inf if r is None else r for r in ranks])
    return np.where(~covered, 0, np.where(values > 100, 1, np.where(values > 1, 2, 3)))


def diagnosis(before, after, before_coverage, after_coverage):
    a, b = categories(before, before_coverage), categories(after, after_coverage)
    matrix = np.zeros((4, 4), int)
    np.add.at(matrix, (a, b), 1)
    old, new = np.array(before["errors_m"]), np.array(after["errors_m"])
    return {"category_order": ["no_coverage", "retrieval_miss_top100", "wrong_top_candidate", "correct"],
        "transition_matrix_before_to_after": matrix.tolist(),
        "raw_gained": {str(m): int(((old > m) & (new <= m)).sum()) for m in (25, 50, 100)},
        "raw_lost": {str(m): int(((old <= m) & (new > m)).sum()) for m in (25, 50, 100)},
        "catastrophic_recovered": int(((old > 500) & (new <= 500)).sum()),
        "catastrophic_introduced": int(((old <= 500) & (new > 500)).sum()),
        "new_coverage_count": int((~before_coverage & after_coverage).sum()),
        "newly_covered_correct": int((~before_coverage & after_coverage & (new <= 100)).sum())}


def context_full_ranks(mean_rows, context_rows):
    """Recover the exact tail only after proving that reranking permutes the first30."""
    if mean_rows["query_ids"] != context_rows["query_ids"]:
        raise RuntimeError("context rank reconstruction query order changed")
    before = np.asarray(mean_rows["top100_gallery_rows"])
    after = np.asarray(context_rows["top100_gallery_rows"])
    if (before.shape != after.shape or before.ndim != 2 or before.shape[1] != 100
            or not np.array_equal(before[:, 30:], after[:, 30:])
            or not np.array_equal(np.sort(before[:, :30]), np.sort(after[:, :30]))):
        raise RuntimeError("context must only permute the same first30 references")
    result = []
    for old, new in zip(mean_rows["positive_ranks"], context_rows["positive_ranks_through100"], strict=True):
        if old is not None and old <= 30:
            if new is None or not 1 <= new <= 30:
                raise RuntimeError("context lost a positive inside its unchanged first30 set")
            result.append(new)
        else:
            if new != (old if old is not None and old <= 100 else None):
                raise RuntimeError("context changed a positive outside its reranked prefix")
            result.append(old)
    return result


def run():
    from ml.research import night_quarantine_audit as audit
    from ml.research.gallery_scale import fixed_development

    gc, _, night, stage, _, _, _, _ = audit.verify_audited()
    if not (stage / "expanded.done.json").exists():
        raise RuntimeError("finish the common-quarantine comparison before its final report")
    queries = fixed_development(gc)
    board = json.loads((stage / "leaderboard.json").read_text())
    records, rows, hashes = {}, {}, {}
    for key in ("G2", "first", "expanded2"):
        for arm in ("top1", "mean", "context"):
            path = stage / "evaluation" / key / f"{arm}_rows.json"
            name = f"quarantine_{key}_{arm}"
            rows[name] = json.loads(path.read_text())
            if rows[name]["query_ids"] != queries.id.tolist():
                raise RuntimeError("common-quarantine report query identities changed")
            hashes[name] = digest(path)
            records[name] = json.loads(path.with_name(f"{arm}.json").read_text())
    if records != {record["name"]: record for record in board["records"]}:
        raise RuntimeError("common-quarantine board differs from the committed comparison")
    licensed = night / "licensed_pipeline"
    if (licensed / "done.json").exists():
        for arm in ("top1", "mean", "context"):
            name = f"licensed_{arm}"
            path = licensed / f"{arm}_rows.json"
            rows[name] = json.loads(path.read_text())
            if rows[name]["query_ids"] != queries.id.tolist():
                raise RuntimeError("licensed report query identities changed")
            hashes[name] = digest(path)
            records[name] = json.loads((licensed / f"{arm}.json").read_text())
    third = night / "reference_tranche3"
    if (third / "evaluation.done.json").exists():
        for arm in ("top1", "mean", "context"):
            name = f"tranche3_{arm}"
            path = third / f"{arm}_rows.json"
            rows[name] = json.loads(path.read_text())
            if rows[name]["query_ids"] != queries.id.tolist():
                raise RuntimeError("third report query identities changed")
            hashes[name] = digest(path)
            records[name] = json.loads((third / f"{arm}.json").read_text())
    compatible_third = night / "licensed_third"
    if (compatible_third / "done.json").exists():
        for arm in ("top1", "mean", "context"):
            name = f"licensed_third_{arm}"
            path = compatible_third / f"{arm}_rows.json"
            rows[name] = json.loads(path.read_text())
            if rows[name]["query_ids"] != queries.id.tolist():
                raise RuntimeError("third source-compatible report query identities changed")
            hashes[name] = digest(path)
            records[name] = json.loads((compatible_third / f"{arm}.json").read_text())
    baseline = "quarantine_G2_top1"
    correct = np.asarray(rows[baseline]["errors_m"]) <= 100
    names = sorted(set(rows) - {baseline})
    differences = np.asarray([(np.asarray(rows[name]["errors_m"]) <= 100).astype(int) - correct.astype(int) for name in names])
    original = json.loads((night / "leaderboard.json").read_text())
    # Count all reported original alternatives as well; do not erase exploration
    # history merely because the comparison now removes one implicated sequence.
    original_count = len(original["records"]) - 1
    family_count = original_count + len(names)
    with threadpool_limits(limits=1):
        samples, clusters = bootstrap_differences(differences, queries.h3_coarse.to_numpy(), resamples=200000, seed=20260918)
    tail = .025 / family_count
    adjusted = np.quantile(samples, [tail, 1 - tail], axis=0)
    pointwise = np.quantile(samples, [.025, .975], axis=0)
    uncertainty = {"baseline": baseline, "query_count": len(queries), "clusters": clusters,
        "method": "paired spatial cluster percentile bootstrap; approximate Bonferroni tail sensitivity",
        "resamples": 200000, "seed": 20260918, "counted_alternatives": family_count,
        "original_exploration_alternatives_counted": original_count, "per_comparison_tail": tail,
        "estimates": {name: {"gain_pp": float(differences[i].mean() * 100),
            "pointwise_interval_pp": pointwise[:, i].tolist(), "approximate_family_interval_pp": adjusted[:, i].tolist()}
            for i, name in enumerate(names)},
        "limitations": ["adaptive development selection, not an independent final test",
            "approximate intervals; historical/informal choices beyond this night are not fully counted",
            "fixed predictions, no model refitting inside bootstrap; all queries remain in denominator"]}
    ordered = sorted(records.values(), key=lambda r: (-r["raw"]["accuracy_100m"], -r["raw"]["accuracy_50m"],
                     -r["raw"]["accuracy_25m"], r["raw"]["catastrophic_gt500m_rate"]))
    best = ordered[0]
    best_rows = rows[best["name"]]
    before = rows["quarantine_first_context"]
    after = rows["quarantine_expanded2_context"]
    cover = {key: np.array(rows[f"quarantine_{key}_top1"]["positive_count_100m"]) > 0 for key in ("G2", "first", "expanded2")}
    if "licensed_top1" in rows:
        cover["licensed"] = np.array(rows["licensed_top1"]["positive_count_100m"]) > 0
    if "tranche3_top1" in rows:
        cover["tranche3"] = np.array(rows["tranche3_top1"]["positive_count_100m"]) > 0
    if "licensed_third_top1" in rows:
        cover["licensed_third"] = np.array(rows["licensed_third_top1"]["positive_count_100m"]) > 0
    best_key = ("licensed_third" if best["name"].startswith("licensed_third_") else
        "tranche3" if best["name"].startswith("tranche3_") else
        "licensed" if best["name"].startswith("licensed_") else next(
        key for key in cover if best["name"].startswith(f"quarantine_{key}_"))
    )
    payload = {"scope": "common-quarantine development evidence, no final/production promotion", "selected": best,
        "records": ordered, "uncertainty": uncertainty,
        "paired_fixed_pipeline_data_gain": paired_group_bootstrap(before["errors_m"], after["errors_m"], queries.h3_coarse.tolist()),
        "second_data_diagnosis": diagnosis(before, after, cover["first"], cover["expanded2"]),
        "selected_vs_clean_G2_diagnosis": diagnosis(rows[baseline], best_rows, cover["G2"], cover[best_key]),
        "rows_sha256": hashes, "board_sha256": digest(stage / "leaderboard.json"),
        "quarantine_sha256": digest(stage / "quarantine.receipt.json"),
        "current_calibration_or_final_opened": False, "production_changed": False}
    if "tranche3_context" in rows:
        payload["third_paired_fixed_pipeline_data_gain"] = paired_group_bootstrap(
            after["errors_m"], rows["tranche3_context"]["errors_m"], queries.h3_coarse.tolist())
        payload["third_data_diagnosis"] = diagnosis(
            after, rows["tranche3_context"], cover["expanded2"], cover["tranche3"])
    if "licensed_third_context" in rows:
        payload["source_compatible_third_data_gain"] = paired_group_bootstrap(
            rows["licensed_context"]["errors_m"], rows["licensed_third_context"]["errors_m"], queries.h3_coarse.tolist())
        payload["source_compatible_third_diagnosis"] = diagnosis(rows["licensed_context"], rows["licensed_third_context"],
            cover["licensed"], cover["licensed_third"])
    full_ranks = {}
    for name, row in rows.items():
        if name.endswith("_context"):
            mean_rows = rows[name.removesuffix("_context") + "_mean"]
            ranks = context_full_ranks(mean_rows, row)
            positive_counts = mean_rows["positive_count_100m"]
        else:
            ranks, positive_counts = row["positive_ranks"], row["positive_count_100m"]
        full_ranks[name] = retrieval_metrics(ranks, gallery_size=records[name]["gallery_count"], positive_counts=positive_counts)
    payload["full_positive_rank_summaries"] = full_ranks
    payload["context_full_rank_method"] = "exact mean-score full ranks; verified first30-only context permutation replaces only ranks within30"
    chosen_gallery = pd.read_parquet(best["gallery_manifest"], columns=["id", "source"])
    chosen_indices = np.asarray(best_rows["top100_gallery_rows"], dtype=int)[:, 0]
    chosen_sources = chosen_gallery.iloc[chosen_indices].source.to_numpy()
    successful = np.asarray(best_rows["errors_m"]) <= 100
    payload["selected_reference_source_counts"] = {str(source): {
        "predictions": int((chosen_sources == source).sum()),
        "correct100": int(((chosen_sources == source) & successful).sum())} for source in np.unique(chosen_sources)}
    payload["reference_source_counts_scope"] = "posthoc prediction attribution only; no query-directed acquisition or inference feature"
    save(stage / "final_evidence.json", payload)
    print(json.dumps({"selected": best["name"], "raw": best["raw"],
        "data_gain": payload["paired_fixed_pipeline_data_gain"],
        "uncertainty": uncertainty["estimates"].get(best["name"], {"gain_pp": 0})}))
    return payload


if __name__ == "__main__":
    run()
