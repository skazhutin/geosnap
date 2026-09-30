# GeoSnap geolocatability v1: blind automatic annotation

**Status:** 1162/1,184 valid three-pass VLM pseudo-labels; 22 explicit failures. These labels are automatic estimates, **not human ground truth**. No image was removed from the 1,184-query RAW denominator. The VLM saw pixels only, and GeoSnap outcomes were joined only after the canonical annotation was frozen and hashed.

Explicit failure categories: `{"schema_invalid": 22}`. Failed rows remain in the canonical 1,184-row table with null semantic fields; no image was silently skipped.

## Population and provenance

The exact development population comes from `data/evaluation/moscow_research_v5/prospective/development.parquet`: 1,184 unique IDs, all original images present, their SHA-256 values verified. It is the same population as the frozen SAGE-L 322+504 context30 result (411/1,184 correct <=100 m; 613/1,184 top-100 positive). The outcome-blind manifest contains only ID, image path, image SHA-256 and dimensions. Its SHA-256 is `998db0488b79ad1490c14797818c5039672d5db8bdbe7e43bc0ab0612353fdfc`. Original queries and the 112,163-reference gallery were unchanged.

The teacher is [Qwen3.5-4B](https://huggingface.co/Qwen/Qwen3.5-4B), run locally through the [MLX 4-bit conversion](https://huggingface.co/mlx-community/Qwen3.5-4B-MLX-4bit) at commit `32f3e8ecf65426fc3306969496342d504bfa13f3`. Weight SHA-256: `5fb9acd0246866381cf8c5c354c6db1019f6498eec4ccb4f5edcc71ffeacb2db`. The model card lists Apache-2.0; this report does not infer rights to any underlying training dataset. The local implementation used MLX-VLM 0.7.3, temperature 0, max 512 output tokens, and no remote image API. The separate 1024-pixel input manifest SHA-256 is `8e43e8db8d1948e4ec9a23a238eece6fea9c63e7a2bfe209e06b8d499c9b851c`.

The image preprocessing displays EXIF orientation, converts to RGB, downsizes the long edge to at most 1024 px using Lanczos and writes a 4:4:4 quality-94 JPEG without EXIF. Original image hashes remain in the annotation table. Smaller VLM inputs make local inference tractable but can hide tiny text or defects visible only at full resolution. Before the 1024-pixel run, a large-image compatibility probe failed under MLX-VLM 0.4.1 and was preserved separately; none of those probe outputs entered this dataset.

The same field definitions were requested in three independent prompts with different ordering and wording. Each pass used its own image+prompt call; no pass saw another pass's output. The model returns full JSON with every named field; the parser checks exact keys, score bounds and categorical consistency. Raw responses, malformed attempts and explicit failures are stored under `annotations/raw_qwen35_full1024/<query_id>/`. Any explanation over 18 words is shortened mechanically in the normalized table, with the full original preserved. Continuous dimensions are aggregated by median, with all three values, mean, population SD and range retained. Booleans use majority vote. Retake reason uses majority where available; three-way disagreement remains null. Out-of-vocabulary free-text reasons were explicitly normalized to `OTHER` in 4 outputs. The schema and prompts are versioned `geolocatability-v1.0` / `qwen35-full-json-three-paraphrases-v3.0`.

For 36 otherwise complete passes, the original response omitted only `short_reason`. An outcome-blind, separate VLM call supplied that descriptive field; all original numeric and categorical values were preserved unchanged. Both raw calls and the assembled pass retain source references. The recovery code SHA-256 is `398223922e8c3e49a8d43e910eba5a9797ee2ecad18b50c5f3deca3950440451` and settings SHA-256 is `146bda533326e4554fb49b1547fbd02496bd972afbda4d259d0653b6ec0660ec`. These extra calls do not constitute independent fourth scoring passes.

The diagnostic technical features were reused from the outcome-free v9 image cache, computed from each original image: luminance/exposure, contrast, entropy, Laplacian variance, Tenengrad gradient energy, edge density, frequency detail, dimensions/aspect and bytes per pixel as a compression proxy. They are not themselves usability labels.

Frozen canonical table: `data/evaluation/geolocatability_v1_20260928/geolocatability_annotations.jsonl`, SHA-256 `22619a028c3e7cd7e2be22420095e70ab3cc9cfb3e929cbf9d24a33ac85f0b6e`; Parquet SHA-256 `165a0adafb47615502e04a022f2acf4833a48a3537199f4b0d9a622c4f4ab512`. Outcomes live only in the separate `analysis_outcome_join.parquet`, SHA-256 `7957081b4cf5ef3a07c68dd19701df0e6b6236bddd94bb08becc5dd116c39e7d`.

## Teacher distribution and agreement

Median geolocatability across valid rows: **0.50**; mean 0.47. The uncertainty flag is set for a geolocatability range >=0.30 or either major boolean disagreement: **176/1162**. The prespecified high-consensus subset (range <=0.20 and unanimous usability/retake booleans) has **939** images. Median three-pass geolocatability range: 0.10. Usability booleans disagreed on 167 images; retake booleans on 167; recommended-retake rows without a reason majority: 13.

The score bands below were fixed before looking at accuracy. Wilson 95% intervals are stored in `analysis_metrics.json`; these are descriptive uncertainty intervals for a reused development set, not independent validation.

Three-pass median score ranges were 0.05 for technical quality and 0.10 for semantic geolocatability. The corresponding counts with range >=0.30 were 19 and 27.

| Geolocatability | n | correct <=100 m | RAW <=100 m | R@100 | selection given top100+ |
|---|---:|---:|---:|---:|---:|
| 0.0–0.2 | 111 | 12 | 10.8% | 21.6% | 50.0% |
| 0.2–0.4 | 316 | 82 | 25.9% | 43.4% | 59.9% |
| 0.4–0.6 | 355 | 134 | 37.7% | 59.2% | 63.8% |
| 0.6–0.8 | 334 | 155 | 46.4% | 61.4% | 75.6% |
| 0.8–1.0 | 46 | 19 | 41.3% | 58.7% | 70.4% |

## Outcomes and failure taxonomy

The table uses a descriptive low-geolocatability cutoff <=0.4, fixed before joining outcomes. “Retake” is the teacher's recommendation, not an observed gain from a second image.

| Frozen SAGE bucket | valid n | median geolocatability | <=0.4 | teacher recommends retake |
|---|---:|---:|---:|---:|
| correct | 402 | 0.55 | 112 | 140 |
| no_coverage | 257 | 0.35 | 157 | 163 |
| ranking_failure | 201 | 0.50 | 85 | 95 |
| retrieval_failure | 302 | 0.50 | 137 | 164 |

Across all failure buckets, 379 valid images scored <=0.4 and 381 scored >0.4. Thus many failures, including potentially usable scenes, remain for retrieval/selection/coverage work. No-coverage cases are gallery limitations and cannot be interpreted as image-quality-caused model failures.

For each defect below, prevalence means teacher score >=0.5. `no_stable_landmarks` and `insufficient_scene_context` are inversions of the positive VLM dimensions. These cutoffs were not tuned against GeoSnap accuracy.

| Teacher-described defect | correct | ranking failure | retrieval failure | no coverage |
|---|---:|---:|---:|---:|
| close_surface | 51.5% (207/402) | 62.2% (125/201) | 54.0% (163/302) | 44.4% (114/257) |
| dirty_or_glare_through_glass | 36.8% (148/402) | 35.3% (71/201) | 37.1% (112/302) | 23.7% (61/257) |
| generic_repetitive_scene | 94.0% (378/402) | 94.0% (189/201) | 93.4% (282/302) | 96.1% (247/257) |
| insufficient_scene_context | 46.5% (187/402) | 61.2% (123/201) | 64.2% (194/302) | 75.9% (195/257) |
| mostly_ground | 15.4% (62/402) | 23.4% (47/201) | 25.2% (76/302) | 27.2% (70/257) |
| mostly_sky | 5.0% (20/402) | 5.5% (11/201) | 2.3% (7/302) | 3.5% (9/257) |
| motion_blur | 40.8% (164/402) | 39.3% (79/201) | 46.7% (141/302) | 39.7% (102/257) |
| no_stable_landmarks | 45.8% (184/402) | 60.7% (122/201) | 58.9% (178/302) | 68.5% (176/257) |
| obstructed_view | 8.7% (35/402) | 7.5% (15/201) | 17.5% (53/302) | 13.2% (34/257) |
| orientation_problem | 0.5% (2/402) | 3.5% (7/201) | 4.0% (12/302) | 5.1% (13/257) |
| out_of_focus | 18.4% (74/402) | 19.4% (39/201) | 24.8% (75/302) | 21.8% (56/257) |
| overexposed | 3.5% (14/402) | 3.0% (6/201) | 4.3% (13/302) | 7.8% (20/257) |
| too_dark | 13.4% (54/402) | 16.9% (34/201) | 21.2% (64/302) | 17.9% (46/257) |
| vegetation_dominated | 11.2% (45/402) | 21.4% (43/201) | 24.5% (74/302) | 52.1% (134/257) |

The **R@100 column** in the band table measures whether a <=100 m reference appears among the frozen top 100. **Selection given top100+** measures the correct rate only among those retrievable cases. This separates a poor image that fails retrieval from a usable candidate that the final ranker selects incorrectly. The primary RAW rate still uses every original image.

## High-consensus sensitivity analysis

The same fixed bands on all 939 high-consensus images:

| Geolocatability | n | correct <=100 m | RAW <=100 m | R@100 | selection given top100+ |
|---|---:|---:|---:|---:|---:|
| 0.0–0.2 | 106 | 12 | 11.3% | 20.8% | — |
| 0.2–0.4 | 279 | 76 | 27.2% | 46.2% | — |
| 0.4–0.6 | 222 | 84 | 37.8% | 58.6% | — |
| 0.6–0.8 | 290 | 142 | 49.0% | 64.1% | — |
| 0.8–1.0 | 42 | 17 | 40.5% | 59.5% | — |

Spearman associations of geolocatability with RAW correctness: all valid +0.236; high-consensus +0.250. For R@100: all valid +0.222; high-consensus +0.241. Any difference may reflect sample selection and teacher noise; reporting both avoids selecting the more favorable view.

## Beyond classical technical measurements

Univariate rank correlations with the frozen outcomes are diagnostics, not new localization experiments or a trained quality model.

| Signal | Spearman with RAW correctness | Spearman with R@100 |
|---|---:|---:|
| VLM semantic geolocatability | +0.236 | +0.222 |
| VLM technical quality | +0.076 | +0.070 |
| canny_edge_density | -0.078 | -0.086 |
| clipped_bright_fraction | -0.023 | -0.042 |
| dark_fraction | +0.059 | +0.056 |
| frequency_high_band_ratio | -0.089 | -0.103 |
| laplacian_variance | -0.060 | -0.069 |
| luminance_entropy_bits | +0.064 | +0.060 |
| mean_luminance | -0.053 | -0.037 |
| p95_minus_p5_luminance | +0.036 | +0.036 |
| tenengrad | -0.050 | -0.033 |

After rank-regressing semantic geolocatability on the nine classical diagnostics **without using outcomes**, the residual's exploratory correlation with correctness is +0.256 and with R@100 is +0.239. This is a conditional association, not proof of incremental deployable prediction. The contact sheets include sharp-looking but semantically weak scenes and technically weak but semantically rich scenes where such cases exist; they are interpretation aids only and did not alter the statistics.

At the prespecified descriptive cutoffs, 44 images have VLM technical quality >=0.75 but semantic geolocatability <=0.4, and 1 show the reverse (technical <=0.4, semantic >=0.6). There are 92 high-geolocatability retrieval failures and 112 low-geolocatability baseline-correct images, counterexamples to any simple quality→success rule.

## Limits and next decision

This reused development set is not a fresh final test. The teacher was not calibrated against a substantial blinded human sample: the existing 11-image pilot remains separate and cannot establish validity for 1,184 pseudo-labels. The three passes share the same model and are not independent raters; their agreement can miss systematic errors. Downsizing can hide details. Different providers, time of day and gallery coverage confound the quality/outcome relationship. A retake recommendation is a hypothesis: this dataset has no matched real-phone second views to measure its causal benefit. Automatic labels do not justify deleting low-scoring images from the benchmark, selecting a retake threshold, or training a student yet.

## Five decisions

1. **Does semantic geolocatability explain a meaningful portion of failures?** Among the 503 valid *covered* ranking/retrieval failures, 222 (44.1%) score <=0.4, versus 112/402 (27.9%) baseline-correct images, a +16.3% descriptive prevalence gap. This is a meaningful association warranting further validation, not a causal explanation. The 281 covered failures above 0.4 remain a substantial retrieval/selection problem.
2. **Does the semantic score add beyond blur/exposure/contrast?** Its outcome-free residual correlation with correctness after nine classical diagnostics is +0.256. That is preliminary evidence of extra semantic information; it is not validated incremental model performance.
3. **Which defect types stand out?** The largest covered-failure versus correct prevalence gaps at the fixed 0.5 score cutoff are `insufficient_scene_context` (+16.5% prevalence gap), `no_stable_landmarks` (+13.9% prevalence gap), `vegetation_dominated` (+12.1% prevalence gap). These are VLM-described visual properties, not verified causes.
4. **Train a small local student?** The pseudo-label association and self-consistency are sufficient to justify a separate, guarded student feasibility study, but only after checking the teacher against a larger blinded human sample. No student was trained here.
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

Production guard: 85 frozen files unchanged at analysis time. The v8/v9 frozen candidate and experiment artifacts were not overwritten. No localization experiment was run.
