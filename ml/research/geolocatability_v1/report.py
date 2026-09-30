"""Render a source-backed report after blind annotation and outcome analysis."""
from __future__ import annotations

import json

from .common import OUT, ROOT, sha256, write_once


def pc(value: float | None) -> str:
    return "—" if value is None else f"{100*value:.1f}%"


def rho(value: float | None) -> str:
    return "—" if value is None else f"{value:+.3f}"


def band_table(rows: list[dict], subset: bool = False) -> str:
    lines = ["| Geolocatability | n | correct <=100 m | RAW <=100 m | R@100 | selection given top100+ |",
             "|---|---:|---:|---:|---:|---:|"]
    for row in rows:
        if subset:
            c, r = row["correct"], row["r100"]
            conditional = "—"
        else:
            c, r = row["baseline_correct"], row["retrieval_r100"]
            conditional = pc(row["ranking_given_positive"]["rate"])
        lines.append(f"| {row['band']} | {c['n']} | {c['positive']} | {pc(c['rate'])} | {pc(r['rate'])} | {conditional} |")
    return "\n".join(lines)


def run() -> None:
    frozen = json.loads((OUT / "annotation_freeze.json").read_text())
    receipt = json.loads((OUT / "analysis_receipt.json").read_text())
    if sha256(OUT / "geolocatability_annotations.jsonl") != frozen["canonical_jsonl_sha256"] or \
       sha256(OUT / "analysis_metrics.json") != receipt["analysis_metrics_sha256"]:
        raise RuntimeError("Frozen annotation or analysis hash changed")
    metrics = json.loads((OUT / "analysis_metrics.json").read_text())
    buckets = metrics["buckets"]
    defect_rows = []
    for defect, per_bucket in metrics["defects"].items():
        cells = [f"{pc(v['present_at_0_5']/v['n'] if v['n'] else None)} ({v['present_at_0_5']}/{v['n']})"
                 for v in (per_bucket[b] for b in ("correct", "ranking_failure", "retrieval_failure", "no_coverage"))]
        defect_rows.append("| " + defect + " | " + " | ".join(cells) + " |")
    assoc = metrics["associations"]
    classical = "\n".join(f"| {key} | {rho(vals['correct'])} | {rho(vals['r100'])} |"
                          for key, vals in assoc["classical_feature_spearman"].items())
    bucket_table = "\n".join(
        f"| {name} | {value['n']} | {value['median_geolocatability']:.2f} | "
        f"{value['low_geolocatability_le_0_4']} | {value['retake_recommended']} |"
        for name, value in buckets.items())
    low_failures = sum(buckets[name]["low_geolocatability_le_0_4"] for name in
                       ("ranking_failure", "retrieval_failure", "no_coverage"))
    high_usable_failures = sum(buckets[name]["n"] - buckets[name]["low_geolocatability_le_0_4"]
                               for name in ("ranking_failure", "retrieval_failure", "no_coverage"))
    covered_n = buckets["ranking_failure"]["n"] + buckets["retrieval_failure"]["n"]
    covered_low = (buckets["ranking_failure"]["low_geolocatability_le_0_4"] +
                   buckets["retrieval_failure"]["low_geolocatability_le_0_4"])
    correct_n = buckets["correct"]["n"]
    correct_low = buckets["correct"]["low_geolocatability_le_0_4"]
    gap = covered_low / covered_n - correct_low / correct_n if covered_n and correct_n else 0
    defect_gaps = []
    for name, per_bucket in metrics["defects"].items():
        a = per_bucket["ranking_failure"]["present_at_0_5"] + per_bucket["retrieval_failure"]["present_at_0_5"]
        b = per_bucket["correct"]["present_at_0_5"]
        defect_gaps.append((a / covered_n - b / correct_n if covered_n and correct_n else 0, name))
    defect_gaps.sort(reverse=True)
    top_defects = ", ".join(f"`{name}` ({delta:+.1%} prevalence gap)" for delta, name in defect_gaps[:3])
    residual = assoc["semantic_rank_residual_after_classical_vs_correct_spearman"]
    residual_signal = residual is not None and abs(residual) >= .05
    semantic_signal = (assoc["geolocatability_vs_correct_spearman"] is not None and
                       abs(assoc["geolocatability_vs_correct_spearman"]) >= .10)
    text = f"""# GeoSnap geolocatability v1: blind automatic annotation

**Status:** {metrics['n_valid']}/1,184 valid three-pass VLM pseudo-labels; {metrics['n_failed']} explicit failures. These labels are automatic estimates, **not human ground truth**. No image was removed from the 1,184-query RAW denominator. The VLM saw pixels only, and GeoSnap outcomes were joined only after the canonical annotation was frozen and hashed.

Explicit failure categories: `{json.dumps(metrics['failure_type_counts'], sort_keys=True)}`. Failed rows remain in the canonical 1,184-row table with null semantic fields; no image was silently skipped.

## Population and provenance

The exact development population comes from `data/evaluation/moscow_research_v5/prospective/development.parquet`: 1,184 unique IDs, all original images present, their SHA-256 values verified. It is the same population as the frozen SAGE-L 322+504 context30 result (411/1,184 correct <=100 m; 613/1,184 top-100 positive). The outcome-blind manifest contains only ID, image path, image SHA-256 and dimensions. Its SHA-256 is `{frozen['input_manifest_sha256']}`. Original queries and the 112,163-reference gallery were unchanged.

The teacher is [Qwen3.5-4B](https://huggingface.co/Qwen/Qwen3.5-4B), run locally through the [MLX 4-bit conversion](https://huggingface.co/mlx-community/Qwen3.5-4B-MLX-4bit) at commit `{frozen['model_revision']}`. Weight SHA-256: `{frozen['model_weight_sha256']}`. The model card lists Apache-2.0; this report does not infer rights to any underlying training dataset. The local implementation used MLX-VLM 0.7.3, temperature 0, max 512 output tokens, and no remote image API. The separate 1024-pixel input manifest SHA-256 is `{frozen['vlm_input_manifest_sha256']}`.

The image preprocessing displays EXIF orientation, converts to RGB, downsizes the long edge to at most 1024 px using Lanczos and writes a 4:4:4 quality-94 JPEG without EXIF. Original image hashes remain in the annotation table. Smaller VLM inputs make local inference tractable but can hide tiny text or defects visible only at full resolution. Before the 1024-pixel run, a large-image compatibility probe failed under MLX-VLM 0.4.1 and was preserved separately; none of those probe outputs entered this dataset.

The same field definitions were requested in three independent prompts with different ordering and wording. Each pass used its own image+prompt call; no pass saw another pass's output. The model returns full JSON with every named field; the parser checks exact keys, score bounds and categorical consistency. Raw responses, malformed attempts and explicit failures are stored under `annotations/raw_qwen35_full1024/<query_id>/`. Any explanation over 18 words is shortened mechanically in the normalized table, with the full original preserved. Continuous dimensions are aggregated by median, with all three values, mean, population SD and range retained. Booleans use majority vote. Retake reason uses majority where available; three-way disagreement remains null. Out-of-vocabulary free-text reasons were explicitly normalized to `OTHER` in {frozen['retake_reason_to_OTHER_normalizations']} outputs. The schema and prompts are versioned `{frozen['schema_version']}` / `{frozen['prompt_version']}`.

For {frozen['targeted_short_reason_recovered_passes']} otherwise complete passes, the original response omitted only `short_reason`. An outcome-blind, separate VLM call supplied that descriptive field; all original numeric and categorical values were preserved unchanged. Both raw calls and the assembled pass retain source references. The recovery code SHA-256 is `{frozen['targeted_short_reason_recovery_code_sha256']}` and settings SHA-256 is `{frozen['targeted_short_reason_recovery_settings_sha256']}`. These extra calls do not constitute independent fourth scoring passes.

The diagnostic technical features were reused from the outcome-free v9 image cache, computed from each original image: luminance/exposure, contrast, entropy, Laplacian variance, Tenengrad gradient energy, edge density, frequency detail, dimensions/aspect and bytes per pixel as a compression proxy. They are not themselves usability labels.

Frozen canonical table: `data/evaluation/geolocatability_v1_20260928/geolocatability_annotations.jsonl`, SHA-256 `{frozen['canonical_jsonl_sha256']}`; Parquet SHA-256 `{frozen['canonical_parquet_sha256']}`. Outcomes live only in the separate `analysis_outcome_join.parquet`, SHA-256 `{receipt['outcome_join_sha256']}`.

## Teacher distribution and agreement

Median geolocatability across valid rows: **{metrics['geolocatability_median']:.2f}**; mean {metrics['geolocatability_mean']:.2f}. The uncertainty flag is set for a geolocatability range >=0.30 or either major boolean disagreement: **{metrics['annotation_uncertain']}/{metrics['n_valid']}**. The prespecified high-consensus subset (range <=0.20 and unanimous usability/retake booleans) has **{metrics['high_consensus']}** images. Median three-pass geolocatability range: {metrics['geolocatability_range_median']:.2f}. Usability booleans disagreed on {metrics['boolean_disagreement']['usable_single_photo']} images; retake booleans on {metrics['boolean_disagreement']['retake_recommended']}; recommended-retake rows without a reason majority: {metrics['retake_reason_no_majority']}.

The score bands below were fixed before looking at accuracy. Wilson 95% intervals are stored in `analysis_metrics.json`; these are descriptive uncertainty intervals for a reused development set, not independent validation.

Three-pass median score ranges were {metrics['dimension_disagreement']['technical_quality']['median_range']:.2f} for technical quality and {metrics['dimension_disagreement']['geolocatability']['median_range']:.2f} for semantic geolocatability. The corresponding counts with range >=0.30 were {metrics['dimension_disagreement']['technical_quality']['range_ge_0_30']} and {metrics['dimension_disagreement']['geolocatability']['range_ge_0_30']}.

{band_table(metrics['bands'])}

## Outcomes and failure taxonomy

The table uses a descriptive low-geolocatability cutoff <=0.4, fixed before joining outcomes. “Retake” is the teacher's recommendation, not an observed gain from a second image.

| Frozen SAGE bucket | valid n | median geolocatability | <=0.4 | teacher recommends retake |
|---|---:|---:|---:|---:|
{bucket_table}

Across all failure buckets, {low_failures} valid images scored <=0.4 and {high_usable_failures} scored >0.4. Thus many failures, including potentially usable scenes, remain for retrieval/selection/coverage work. No-coverage cases are gallery limitations and cannot be interpreted as image-quality-caused model failures.

For each defect below, prevalence means teacher score >=0.5. `no_stable_landmarks` and `insufficient_scene_context` are inversions of the positive VLM dimensions. These cutoffs were not tuned against GeoSnap accuracy.

| Teacher-described defect | correct | ranking failure | retrieval failure | no coverage |
|---|---:|---:|---:|---:|
{chr(10).join(defect_rows)}

The **R@100 column** in the band table measures whether a <=100 m reference appears among the frozen top 100. **Selection given top100+** measures the correct rate only among those retrievable cases. This separates a poor image that fails retrieval from a usable candidate that the final ranker selects incorrectly. The primary RAW rate still uses every original image.

## High-consensus sensitivity analysis

The same fixed bands on all {metrics['high_consensus_subset']['n']} high-consensus images:

{band_table(metrics['high_consensus_subset']['bands'], subset=True)}

Spearman associations of geolocatability with RAW correctness: all valid {rho(assoc['geolocatability_vs_correct_spearman'])}; high-consensus {rho(metrics['high_consensus_subset']['semantic_vs_correct_spearman'])}. For R@100: all valid {rho(assoc['geolocatability_vs_r100_spearman'])}; high-consensus {rho(metrics['high_consensus_subset']['semantic_vs_r100_spearman'])}. Any difference may reflect sample selection and teacher noise; reporting both avoids selecting the more favorable view.

## Beyond classical technical measurements

Univariate rank correlations with the frozen outcomes are diagnostics, not new localization experiments or a trained quality model.

| Signal | Spearman with RAW correctness | Spearman with R@100 |
|---|---:|---:|
| VLM semantic geolocatability | {rho(assoc['geolocatability_vs_correct_spearman'])} | {rho(assoc['geolocatability_vs_r100_spearman'])} |
| VLM technical quality | {rho(assoc['vlm_technical_quality_vs_correct_spearman'])} | {rho(assoc['vlm_technical_quality_vs_r100_spearman'])} |
{classical}

After rank-regressing semantic geolocatability on the nine classical diagnostics **without using outcomes**, the residual's exploratory correlation with correctness is {rho(assoc['semantic_rank_residual_after_classical_vs_correct_spearman'])} and with R@100 is {rho(assoc['semantic_rank_residual_after_classical_vs_r100_spearman'])}. This is a conditional association, not proof of incremental deployable prediction. The contact sheets include sharp-looking but semantically weak scenes and technically weak but semantically rich scenes where such cases exist; they are interpretation aids only and did not alter the statistics.

At the prespecified descriptive cutoffs, {metrics['cross_cases']['high_technical_low_geolocatability']} images have VLM technical quality >=0.75 but semantic geolocatability <=0.4, and {metrics['cross_cases']['low_technical_high_geolocatability']} show the reverse (technical <=0.4, semantic >=0.6). There are {metrics['cross_cases']['high_geolocatability_retrieval_failure']} high-geolocatability retrieval failures and {metrics['cross_cases']['low_geolocatability_baseline_correct']} low-geolocatability baseline-correct images, counterexamples to any simple quality→success rule.

## Limits and next decision

This reused development set is not a fresh final test. The teacher was not calibrated against a substantial blinded human sample: the existing 11-image pilot remains separate and cannot establish validity for 1,184 pseudo-labels. The three passes share the same model and are not independent raters; their agreement can miss systematic errors. Downsizing can hide details. Different providers, time of day and gallery coverage confound the quality/outcome relationship. A retake recommendation is a hypothesis: this dataset has no matched real-phone second views to measure its causal benefit. Automatic labels do not justify deleting low-scoring images from the benchmark, selecting a retake threshold, or training a student yet.

## Five decisions

1. **Does semantic geolocatability explain a meaningful portion of failures?** Among the {covered_n} valid *covered* ranking/retrieval failures, {covered_low} ({pc(covered_low/covered_n if covered_n else None)}) score <=0.4, versus {correct_low}/{correct_n} ({pc(correct_low/correct_n if correct_n else None)}) baseline-correct images, a {gap:+.1%} descriptive prevalence gap. {"This is a meaningful association warranting further validation, not a causal explanation." if gap >= .10 else "This gap alone is too small to attribute a large share of errors to weak input."} The {covered_n-covered_low} covered failures above 0.4 remain a substantial retrieval/selection problem.
2. **Does the semantic score add beyond blur/exposure/contrast?** Its outcome-free residual correlation with correctness after nine classical diagnostics is {rho(residual)}. {"That is preliminary evidence of extra semantic information; it is not validated incremental model performance." if residual_signal else "That does not show a clear incremental semantic association on this reused set."}
3. **Which defect types stand out?** The largest covered-failure versus correct prevalence gaps at the fixed 0.5 score cutoff are {top_defects}. These are VLM-described visual properties, not verified causes.
4. **Train a small local student?** {"The pseudo-label association and self-consistency are sufficient to justify a separate, guarded student feasibility study, but only after checking the teacher against a larger blinded human sample." if semantic_signal and metrics['high_consensus'] >= .6*metrics['n_valid'] else "The current pseudo-label evidence is insufficient for student training as the next automatic step; improve human validation and teacher reliability first."} No student was trained here.
5. **Would adaptive retakes help?** Asking for a better view is product-plausible for clearly obstructed or low-context images, but no matched real-phone retakes exist here. The data cannot estimate second-photo success or choose a retake threshold.

No student model or inference policy was trained in this phase.

## Reproduction and integrity

Run from the repository root. The freeze and analysis outputs are write-once; on an existing run, verify hashes instead of rerunning those stages.

```sh
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.prepare
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.prepare_vlm_inputs
data/evaluation/geolocatability_v1_20260928/runtime/bin/python -m ml.research.geolocatability_v1.run_chunks
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.freeze
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.analyze
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.contact_sheets
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.report
```

Production guard: {receipt['production_guard']['guarded_files']} frozen files unchanged at analysis time. The v8/v9 frozen candidate and experiment artifacts were not overwritten. No localization experiment was run.
"""
    path = ROOT / "docs/geolocatability_v1_report.md"
    write_once(path, text.encode())
    print(json.dumps({"report": str(path), "sha256": sha256(path)}))


if __name__ == "__main__":
    run()
