"""Build an evidence-linked, reproducible v8 research readout."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames
from ml.research.metrics import paired_group_bootstrap

REPORT = LOCAL / "research_report.json"
DOC = Path("docs/geographic_v8_research_report_20260928.md")

VARIANTS = [
    ("SAGE-L 322+504 context30", "same-gallery baseline", "baseline/report.json", "baseline/predictions.npz"),
    ("SAGE-L licensed-only", "release-compatible baseline", "secondary_baseline/report.json", "secondary_baseline/predictions.npz"),
    ("G3 image↔GPS", "same-gallery external pretrained", "g3/g3_full/report.json", "g3/g3_full/predictions.npz"),
    ("G3 rerank SAGE30", "same-gallery external pretrained", "g3/g3_rerank_sage30/report.json", "g3/g3_rerank_sage30/predictions.npz"),
    ("PLONK_OSV_5M direct", "direct external pretrained", "plonk/report.json", "plonk/evaluated.npz"),
    ("OSV-5M direct", "direct external pretrained", "osv5m/report.json", "osv5m/evaluated.npz"),
    ("GeoCLIP image↔GPS", "same-gallery external pretrained", "geoclip/geoclip_full/report.json", "geoclip/geoclip_full/predictions.npz"),
    ("GeoCLIP rerank SAGE30", "same-gallery external pretrained", "geoclip/geoclip_rerank_sage30/report.json", "geoclip/geoclip_rerank_sage30/predictions.npz"),
    ("SAGE-L query322 only", "same-gallery cached view", "resolution_union/322.json", "resolution_union/322.npz"),
    ("SAGE-L query504 only", "same-gallery cached view", "resolution_union/504.json", "resolution_union/504.npz"),
    ("resolution union logistic OOF", "learned OOF", "oof_union_rerank/logistic_oof.json", "oof_union_rerank/logistic_oof.npz"),
    ("resolution union boosted OOF", "learned OOF", "oof_union_rerank/shallow_boosted_oof.json", "oof_union_rerank/shallow_boosted_oof.npz"),
]


def percent(value):
    return "—" if value is None else f"{100*value:.2f}%"


def count(value):
    return "—" if value is None else f"{value:,.0f}"


def read():
    variants = VARIANTS.copy()
    complete = LOCAL / "sage_adaptation_v3/evaluation_complete.json"
    if complete.exists():
        epoch = json.loads((LOCAL / "sage_adaptation_v3/complete.json").read_text())["selected_epoch"]
        variants += [
            (f"SAGE-L reference-trained epoch{epoch}", "same-gallery reference-trained",
             f"sage_adaptation_v3/sage_asymmetric_v3_refonly_epoch{epoch}_primary/report.json",
             f"sage_adaptation_v3/sage_asymmetric_v3_refonly_epoch{epoch}_primary/predictions.npz"),
            (f"SAGE-L reference-trained licensed epoch{epoch}", "release-compatible reference-trained",
             f"sage_adaptation_v3/sage_asymmetric_v3_refonly_epoch{epoch}_licensed_secondary/report.json",
             f"sage_adaptation_v3/sage_asymmetric_v3_refonly_epoch{epoch}_licensed_secondary/predictions.npz")]
    result = []
    for label, category, report_path, predictions_path in variants:
        report_file, prediction_file = LOCAL / report_path, LOCAL / predictions_path
        if not report_file.exists() or not prediction_file.exists():
            continue
        report = json.loads(report_file.read_text())
        with np.load(prediction_file, allow_pickle=False) as pred:
            errors = pred["errors_m"].copy()
            ids = pred["query_ids"].tolist()
        if len(errors) != 1184:
            raise RuntimeError(f"Wrong query denominator: {label}")
        result.append({"variant": label, "category": category, "report_path": str(report_file),
            "report_sha256": digest(report_file), "prediction_path": str(prediction_file),
            "prediction_sha256": digest(prediction_file), "metrics": report,
            "errors_m": errors.tolist(), "query_ids": ids})
    if not result:
        raise RuntimeError("No completed research comparisons")
    baseline_ids = result[0]["query_ids"]
    if any(item["query_ids"] != baseline_ids for item in result):
        raise RuntimeError("Leaderboard query identity differs")
    return result


def run():
    rows = read()
    baseline = rows[0]
    best = max((r for r in rows if r["category"] not in
                {"release-compatible baseline", "release-compatible reference-trained"}),
               key=lambda r: (np.asarray(r["errors_m"]) <= 100).sum())
    matrix = []
    major = [r for r in rows if r["variant"] in {"SAGE-L 322+504 context30", "G3 image↔GPS",
        "GeoCLIP image↔GPS", "PLONK_OSV_5M direct", "OSV-5M direct",
        "SAGE-L query322 only", "SAGE-L query504 only"} or r["category"] == "same-gallery reference-trained"]
    for i, left in enumerate(major):
        a = np.asarray(left["errors_m"]) <= 100
        for right in major[i+1:]:
            b = np.asarray(right["errors_m"]) <= 100
            matrix.append({"left": left["variant"], "right": right["variant"],
                "both_correct": int((a & b).sum()), "only_left_correct": int((a & ~b).sum()),
                "only_right_correct": int((~a & b).sum()), "neither_correct": int((~a & ~b).sum())})
    unions = json.loads((LOCAL / "cross_model_union/summary.json").read_text())
    union_uncertainty = json.loads((LOCAL / "cross_model_union/uncertainty.json").read_text())
    adapted_union = LOCAL / "sage_adaptation_v3/candidate_union/oracle.json"
    if adapted_union.exists():
        item = json.loads(adapted_union.read_text())
        unions["sage_baseline_adapted_v3"] = item
        union_uncertainty["sage_baseline_adapted_v3"] = {
            "gain": item["paired_geographic_bootstrap"]}
    full_adapted_union = LOCAL / "sage_adaptation_v3/all_model_candidate_union.json"
    if full_adapted_union.exists():
        item = json.loads(full_adapted_union.read_text())
        unions["all_verified_plus_adapted_v3"] = item
        with np.load(LOCAL / "sage_adaptation_v3/candidate_union/oracle.npz", allow_pickle=False) as before, \
                np.load(LOCAL / "sage_adaptation_v3/all_model_candidate_union_oracle.npz",
                        allow_pickle=False) as after:
            if before["query_ids"].tolist() != after["query_ids"].tolist():
                raise RuntimeError("Candidate union query identity differs")
            baseline_oracle = before["base_oracle100"].copy()
            combined_oracle = after["oracle100"].copy()
        q, _ = frames()
        union_uncertainty["all_verified_plus_adapted_v3"] = {
            "gain": paired_group_bootstrap(np.where(baseline_oracle, 0., 200.),
                np.where(combined_oracle, 0., 200.), q.h3_coarse.tolist())}
    strongest_union_name, strongest_union = max(unions.items(), key=lambda pair: pair[1]["oracle100"])
    quality_file = LOCAL / "query_quality_audit/report.json"
    quality = None
    hard_without_low_tail = None
    if quality_file.exists():
        quality = json.loads(quality_file.read_text())
        thresholds = quality["population_decile_thresholds"]
        with np.load(LOCAL / "query_quality_audit/per_query.npz", allow_pickle=False) as measured, \
                np.load(LOCAL / "sage_adaptation_v3/all_model_candidate_union_oracle.npz",
                        allow_pickle=False) as oracle:
            if measured["query_ids"].tolist() != oracle["query_ids"].tolist():
                raise RuntimeError("Quality and candidate-union query identities differ")
            features = measured["features"]
            hard = (measured["buckets"] == "retrieval_failure") & ~oracle["oracle100"]
            low_tail = ((features[:, 0] <= thresholds["darkest_decile"]) |
                (features[:, 3] <= thresholds["lowest_contrast_decile"]) |
                (features[:, 4] <= thresholds["lowest_edge_variance_decile"]))
            hard_without_low_tail = {"hard_covered_retrieval_failures": int(hard.sum()),
                "without_any_of_three_low_tail_proxies": int((hard & ~low_tail).sum())}
    lightweight = [{k:v for k,v in r.items() if k not in ("errors_m", "query_ids")} for r in rows]
    output = {"status": "exploratory_development", "query_count": 1184,
        "baseline_raw100_count": int((np.asarray(baseline["errors_m"]) <= 100).sum()),
        "best_variant": best["variant"], "best_raw100_count": int((np.asarray(best["errors_m"]) <= 100).sum()),
        "variants": lightweight, "pairwise_correctness": matrix, "candidate_unions": unions,
        "candidate_union_uncertainty": union_uncertainty,
        "image_quality_diagnostic": {"report_path": str(quality_file),
            "report_sha256": digest(quality_file), **hard_without_low_tail} if quality else None,
        "production_modified": False, "independent_final_accuracy_claim": False}
    save(REPORT, output)
    lines = ["# GeoSnap geographic v8 research — 2026-09-28", "",
        f"The best measured RAW <=100 m result is **{output['best_raw100_count']}/1184 ({100*output['best_raw100_count']/1184:.2f}%)**: {best['variant']}. The frozen prior result is 411/1184 (34.71%). These are exploratory measurements on a heavily reused development set, not an independent final claim.",
        "", f"The original 262 no-coverage queries remain outside the fixed-gallery model ceiling. The remaining original failures comprise 309 missed by SAGE top100 retrieval and 202 with a positive candidate but wrong selection. The strongest verified top100 union ({strongest_union_name}) reaches {round(1184*strongest_union['oracle100'])}/1184 ({100*strongest_union['oracle100']:.2f}%) oracle recall and makes {strongest_union['original_309_retrieval_failures_recovered']} of the 309 retrievable; an oracle is not an actual predictor. The tested geographically embargoed OOF rerankers did not beat the frozen 411 correct predictions.",
        "", "## Comparable leaderboard", "",
        "| Variant | Scope | RAW25 | RAW50 | RAW100 | R@10 | R@50 | R@100 | Median m | p90 m | >500 m |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        raw = r["metrics"]["raw"]
        recall = r["metrics"].get("recall_at", {})
        lines.append("| " + " | ".join([r["variant"], r["category"],
            percent(raw.get("accuracy_25m")), percent(raw.get("accuracy_50m")),
            percent(raw.get("accuracy_100m")), percent(recall.get("10")),
            percent(recall.get("50")), percent(recall.get("100")),
            count(raw.get("median_error_m")), count(raw.get("p90_error_m")),
            percent(raw.get("catastrophic_gt500m_rate"))]) + " |")
    lines += ["", "### Direct-model distance bands", "",
        "| Direct model | <=25 m | <=50 m | <=100 m | <=500 m | <=1 km | <=5 km | Median m | p90 m |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        if r["category"] != "direct external pretrained":
            continue
        raw = r["metrics"]["raw"]
        lines.append("| " + " | ".join([r["variant"], *[percent(raw.get(f"accuracy_{k}m"))
            for k in (25, 50, 100, 500, 1000, 5000)], count(raw.get("median_error_m")),
            count(raw.get("p90_error_m"))]) + " |")
    lines += ["", "Release-compatible Mapillary+KartaView has 44,995 references and excludes research-only MSLS. Direct GPS models do not use the gallery for inference, so R@K is undefined. Learned OOF rows are held-out predictions by geographic fold with a one-ring embargo; they are still development evidence.",
        "", "## Failure transitions versus frozen SAGE", "",
        "| Variant | Old wrong→correct | Old correct→wrong | Old wrong→wrong | Old correct→correct | Original no coverage now correct | Original retrieval failure now correct | Original ranking failure now correct |",
        "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows[1:]:
        t, b = r["metrics"].get("transitions"), r["metrics"].get("buckets")
        if not t or not b:
            continue
        lines.append("| " + " | ".join([r["variant"],
            str(t["old_wrong_new_correct"]), str(t["old_correct_new_wrong"]),
            str(t["old_wrong_new_wrong"]), str(t["old_correct_new_correct"]),
            str(b.get("no_coverage", {}).get("new_correct", 0)),
            str(b.get("retrieval_failure", {}).get("new_correct", 0)),
            str(b.get("ranking_failure", {}).get("new_correct", 0))]) + " |")
    lines += ["", "## Candidate union oracle", "",
        "| Union | Unique/query mean | Oracle <=50 m | Oracle <=100 m | Gain vs SAGE oracle, 95% geographic CI | Recovered original 309 retrieval failures |",
        "|---|---:|---:|---:|---:|---:|"]
    for label, record in unions.items():
        ci = union_uncertainty[label]["gain"]["ci95_pp"]
        lines.append("| " + " | ".join([label, f"{record['unique_candidates_per_query']['mean']:.1f}",
            percent(record["oracle50"]), percent(record["oracle100"]),
            f"[{ci[0]:.2f}, {ci[1]:.2f}] pp",
            str(record["original_309_retrieval_failures_recovered"])]) + " |")
    lines += ["", "## Pairwise correctness overlap", "",
        "| A | B | Both correct | Only A | Only B | Neither |", "|---|---|---:|---:|---:|---:|"]
    for r in matrix:
        lines.append("| " + " | ".join([r["left"], r["right"], str(r["both_correct"]),
            str(r["only_left_correct"]), str(r["only_right_correct"]), str(r["neither_correct"])]) + " |")
    if quality:
        lines += ["", "## Image-quality diagnostic", "",
            "The query manifest has no filled human quality scores. Image width has median 2048 px; resolution alone does not identify the hard cases. The following population-decile flags are posthoc image measurements, never inference features:", "",
            "| Frozen bucket | Queries | Darkest decile | Lowest contrast decile | Lowest edge-variance decile |",
            "|---|---:|---:|---:|---:|"]
        for name in ("correct", "retrieval_failure", "ranking_failure", "no_coverage"):
            item = quality["by_frozen_failure_bucket"][name]
            flags = item["proxy_flag_rates"]
            lines.append("| " + " | ".join([name, str(item["count"]),
                percent(flags["darkest_decile"]), percent(flags["lowest_contrast_decile"]),
                percent(flags["lowest_edge_variance_decile"])]) + " |")
        lines += ["", f"Among the {hard_without_low_tail['hard_covered_retrieval_failures']} covered queries missed by every tested top100 candidate generator, {hard_without_low_tail['without_any_of_three_low_tail_proxies']} have none of these three low-tail flags. This is **not** an estimate of usable-image prevalence: camera shake, a close wall, blocked views and images without landmarks can all pass these numeric checks. A random visual inspection of ten hard misses found both apparently unusable and normal-looking images, but it was not a blinded labeled audit. Quantifying the contribution of image quality requires a separate outcome-blinded human review with categories for motion blur, obstruction/close-up, lighting, missing landmarks and clear images, preferably with two raters. Keep every query in the primary accuracy denominator and report any later quality strata separately. Full per-query proxy measurements are in `data/evaluation/geographic_v8_20260928/query_quality_audit/per_query.npz`. The fixed ten-image review sample is in `data/evaluation/geographic_v8_20260928/query_quality_audit/unrecognized10_20260929.json`."]
    lines += ["", "## Interpretation and uncertainty", "",
        "G3, PLONK_OSV_5M, OSV-5M and GeoCLIP were run with pinned official checkpoints and audited local execution changes. Their Moscow zero-shot image→GPS behavior was weak; pure G3/GeoCLIP or direct-distance reranking destroyed hundreds of SAGE-correct cases. The 322/504 query-view union recovered 24 original retrieval misses, but its OOF selection models did not improve final RAW. Strict reference-only SAGE adaptation added 10 further retrievable queries to the previous all-model union; as a final predictor it fell from 411 to 406 correct, with 24 old errors fixed and 29 old correct answers lost. Thus the remaining bottleneck is selecting the additional correct candidates without damaging existing winners. Two adaptation pilots were aborted before development scoring: the first had provider-sequence overlap; v2 let negative-role examples cross the geographic embargo. Only the all-role-disjoint v3 run is eligible for comparison. Details, model licenses and unresolved external-code gates are in [the model audit](geographic_v8_model_audit_20260928.md).",
        "", "Each important paired delta and its geographically grouped bootstrap interval is in the linked per-variant JSON report. A small delta with an interval including zero is not established improvement. Neither the historically opened final set nor new smartphone data was scored here. [The independent collection protocol](geographic_v8_independent_validation_protocol.md) and evaluator are ready for a post-freeze cohort.",
        "", "## Reproduce", "", "Run from the repository root. Large staged inputs and pinned checkpoint caches are required; the commands verify hashes and fail if an input changed.", "",
        "```sh", ".venv/bin/python -m ml.research.geographic_v8.inventory",
        ".venv/bin/python -m ml.research.geographic_v8.stage",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.baseline --recompute",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.secondary_baseline",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.g3",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_g3",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.plonk_direct",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_direct plonk",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_prior plonk",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.osv5m_direct",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_direct osv5m",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_prior osv5m",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.geoclip",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_embedding geoclip",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.resolution_union",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.oof_union_rerank",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.cross_model_union",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.union_uncertainty",
        "OMP_NUM_THREADS=4 data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.mine_sage_adaptation",
        "OMP_NUM_THREADS=4 data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.mine_sage_adaptation_v2",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.mine_sage_adaptation_v3",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.train_sage_asymmetric_v3",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.reference_validation",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_sage_adaptation",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.adapted_union",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.adapted_full_union",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.query_quality_audit",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.build_report",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.freeze_candidate",
        "data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.verify_integrity",
        "```", "", "Production baseline/hash receipts are `data/evaluation/geographic_v8_20260928/production_before.json` and `production_after.json`. The original candidate remains under `data/evaluation/moscow_night_v7/candidate_frozen.json`. Each new experiment has an immutable JSON record under `data/evaluation/geographic_v8_20260928/experiments/`; research source snapshots and hashes are under `data/evaluation/geographic_v8_20260928/source_snapshots/20260928_0957/`."]
    DOC.write_text("\n".join(lines) + "\n")
    print(DOC, "best", output["best_raw100_count"], flush=True)


if __name__ == "__main__":
    run()
