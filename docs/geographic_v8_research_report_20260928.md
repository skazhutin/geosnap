# GeoSnap geographic v8 research — 2026-09-28

The best measured RAW <=100 m result is **411/1184 (34.71%)**: SAGE-L 322+504 context30. The frozen prior result is 411/1184 (34.71%). These are exploratory measurements on a heavily reused development set, not an independent final claim.

The original 262 no-coverage queries remain outside the fixed-gallery model ceiling. The remaining original failures comprise 309 missed by SAGE top100 retrieval and 202 with a positive candidate but wrong selection. The strongest verified top100 union (all_verified_plus_adapted_v3) reaches 652/1184 (55.07%) oracle recall and makes 39 of the 309 retrievable; an oracle is not an actual predictor. The tested geographically embargoed OOF rerankers did not beat the frozen 411 correct predictions.

## Comparable leaderboard

| Variant | Scope | RAW25 | RAW50 | RAW100 | R@10 | R@50 | R@100 | Median m | p90 m | >500 m |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SAGE-L 322+504 context30 | same-gallery baseline | 17.15% | 26.86% | 34.71% | 43.50% | 48.73% | 51.77% | 3,803 | 30,512 | 58.36% |
| SAGE-L licensed-only | release-compatible baseline | 14.70% | 23.90% | 32.09% | 40.54% | 46.62% | 50.34% | 4,417 | 31,697 | 60.73% |
| G3 image↔GPS | same-gallery external pretrained | 0.00% | 0.00% | 0.00% | 0.08% | 0.42% | 0.68% | 22,925 | 43,259 | 99.75% |
| G3 rerank SAGE30 | same-gallery external pretrained | 1.10% | 2.03% | 3.38% | 21.96% | 48.73% | 51.77% | 15,698 | 36,923 | 93.92% |
| PLONK_OSV_5M direct | direct external pretrained | 0.00% | 0.00% | 0.00% | — | — | — | 55,833 | 1,461,720 | 100.00% |
| OSV-5M direct | direct external pretrained | 0.00% | 0.00% | 0.00% | — | — | — | 30,898 | 1,419,324 | 99.66% |
| GeoCLIP image↔GPS | same-gallery external pretrained | 0.00% | 0.00% | 0.08% | 0.17% | 0.34% | 0.51% | 19,290 | 37,818 | 99.58% |
| GeoCLIP rerank SAGE30 | same-gallery external pretrained | 0.59% | 1.52% | 3.72% | 21.88% | 48.73% | 51.77% | 15,027 | 33,873 | 93.07% |
| SAGE-L query322 only | same-gallery cached view | 16.72% | 25.76% | 33.36% | 41.81% | 47.72% | 50.93% | 5,059 | 31,126 | 60.73% |
| SAGE-L query504 only | same-gallery cached view | 16.30% | 25.84% | 33.36% | 42.31% | 48.31% | 51.18% | 4,119 | 30,986 | 59.46% |
| resolution union logistic OOF | learned OOF | 14.70% | 25.17% | 33.28% | — | — | — | 3,766 | 31,026 | 58.78% |
| resolution union boosted OOF | learned OOF | 16.64% | 26.52% | 34.38% | — | — | — | 3,767 | 30,769 | 58.36% |
| SAGE-L reference-trained epoch2 | same-gallery reference-trained | 17.23% | 26.35% | 34.29% | 41.72% | 47.55% | 50.51% | 4,963 | 31,043 | 59.63% |
| SAGE-L reference-trained licensed epoch2 | release-compatible reference-trained | 14.70% | 23.56% | 31.50% | 39.70% | 46.28% | 49.66% | 5,782 | 31,251 | 61.66% |

### Direct-model distance bands

| Direct model | <=25 m | <=50 m | <=100 m | <=500 m | <=1 km | <=5 km | Median m | p90 m |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| PLONK_OSV_5M direct | 0.00% | 0.00% | 0.00% | 0.00% | 0.17% | 2.70% | 55,833 | 1,461,720 |
| OSV-5M direct | 0.00% | 0.00% | 0.00% | 0.34% | 0.34% | 6.76% | 30,898 | 1,419,324 |

Release-compatible Mapillary+KartaView has 44,995 references and excludes research-only MSLS. Direct GPS models do not use the gallery for inference, so R@K is undefined. Learned OOF rows are held-out predictions by geographic fold with a one-ring embargo; they are still development evidence.

## Failure transitions versus frozen SAGE

| Variant | Old wrong→correct | Old correct→wrong | Old wrong→wrong | Old correct→correct | Original no coverage now correct | Original retrieval failure now correct | Original ranking failure now correct |
|---|---:|---:|---:|---:|---:|---:|---:|
| G3 image↔GPS | 0 | 411 | 773 | 0 | 0 | 0 | 0 |
| G3 rerank SAGE30 | 5 | 376 | 768 | 35 | 0 | 0 | 5 |
| PLONK_OSV_5M direct | 0 | 411 | 773 | 0 | 0 | 0 | 0 |
| OSV-5M direct | 0 | 411 | 773 | 0 | 0 | 0 | 0 |
| GeoCLIP image↔GPS | 0 | 410 | 773 | 1 | 0 | 0 | 0 |
| GeoCLIP rerank SAGE30 | 3 | 370 | 770 | 41 | 0 | 0 | 3 |
| SAGE-L query322 only | 12 | 28 | 761 | 383 | 0 | 0 | 12 |
| SAGE-L query504 only | 7 | 23 | 766 | 388 | 0 | 0 | 7 |
| resolution union logistic OOF | 14 | 31 | 759 | 380 | 0 | 0 | 14 |
| resolution union boosted OOF | 0 | 4 | 773 | 407 | 0 | 0 | 0 |
| SAGE-L reference-trained epoch2 | 24 | 29 | 749 | 382 | 0 | 0 | 24 |

## Candidate union oracle

| Union | Unique/query mean | Oracle <=50 m | Oracle <=100 m | Gain vs SAGE oracle, 95% geographic CI | Recovered original 309 retrieval failures |
|---|---:|---:|---:|---:|---:|
| sage_g3 | 199.8 | 39.78% | 52.11% | [0.00, 0.80] pp | 4 |
| sage_geoclip | 199.6 | 39.53% | 51.94% | [0.00, 0.44] pp | 2 |
| sage_g3_geoclip | 298.3 | 39.86% | 52.28% | [0.09, 1.03] pp | 6 |
| sage_both_resolutions | 143.8 | 40.46% | 53.80% | [1.18, 2.93] pp | 24 |
| all_verified_same_gallery | 341.9 | 40.79% | 54.22% | [1.46, 3.52] pp | 29 |
| sage_baseline_adapted_v3 | 147.6 | 40.54% | 53.46% | [0.95, 2.49] pp | 20 |
| all_verified_plus_adapted_v3 | 381.2 | 41.47% | 55.07% | [2.20, 4.50] pp | 39 |

## Pairwise correctness overlap

| A | B | Both correct | Only A | Only B | Neither |
|---|---|---:|---:|---:|---:|
| SAGE-L 322+504 context30 | G3 image↔GPS | 0 | 411 | 0 | 773 |
| SAGE-L 322+504 context30 | PLONK_OSV_5M direct | 0 | 411 | 0 | 773 |
| SAGE-L 322+504 context30 | OSV-5M direct | 0 | 411 | 0 | 773 |
| SAGE-L 322+504 context30 | GeoCLIP image↔GPS | 1 | 410 | 0 | 773 |
| SAGE-L 322+504 context30 | SAGE-L query322 only | 383 | 28 | 12 | 761 |
| SAGE-L 322+504 context30 | SAGE-L query504 only | 388 | 23 | 7 | 766 |
| SAGE-L 322+504 context30 | SAGE-L reference-trained epoch2 | 382 | 29 | 24 | 749 |
| G3 image↔GPS | PLONK_OSV_5M direct | 0 | 0 | 0 | 1184 |
| G3 image↔GPS | OSV-5M direct | 0 | 0 | 0 | 1184 |
| G3 image↔GPS | GeoCLIP image↔GPS | 0 | 0 | 1 | 1183 |
| G3 image↔GPS | SAGE-L query322 only | 0 | 0 | 395 | 789 |
| G3 image↔GPS | SAGE-L query504 only | 0 | 0 | 395 | 789 |
| G3 image↔GPS | SAGE-L reference-trained epoch2 | 0 | 0 | 406 | 778 |
| PLONK_OSV_5M direct | OSV-5M direct | 0 | 0 | 0 | 1184 |
| PLONK_OSV_5M direct | GeoCLIP image↔GPS | 0 | 0 | 1 | 1183 |
| PLONK_OSV_5M direct | SAGE-L query322 only | 0 | 0 | 395 | 789 |
| PLONK_OSV_5M direct | SAGE-L query504 only | 0 | 0 | 395 | 789 |
| PLONK_OSV_5M direct | SAGE-L reference-trained epoch2 | 0 | 0 | 406 | 778 |
| OSV-5M direct | GeoCLIP image↔GPS | 0 | 0 | 1 | 1183 |
| OSV-5M direct | SAGE-L query322 only | 0 | 0 | 395 | 789 |
| OSV-5M direct | SAGE-L query504 only | 0 | 0 | 395 | 789 |
| OSV-5M direct | SAGE-L reference-trained epoch2 | 0 | 0 | 406 | 778 |
| GeoCLIP image↔GPS | SAGE-L query322 only | 1 | 0 | 394 | 789 |
| GeoCLIP image↔GPS | SAGE-L query504 only | 1 | 0 | 394 | 789 |
| GeoCLIP image↔GPS | SAGE-L reference-trained epoch2 | 1 | 0 | 405 | 778 |
| SAGE-L query322 only | SAGE-L query504 only | 366 | 29 | 29 | 760 |
| SAGE-L query322 only | SAGE-L reference-trained epoch2 | 367 | 28 | 39 | 750 |
| SAGE-L query504 only | SAGE-L reference-trained epoch2 | 373 | 22 | 33 | 756 |

## Image-quality diagnostic

The query manifest has no filled human quality scores. Image width has median 2048 px; resolution alone does not identify the hard cases. The following population-decile flags are posthoc image measurements, never inference features:

| Frozen bucket | Queries | Darkest decile | Lowest contrast decile | Lowest edge-variance decile |
|---|---:|---:|---:|---:|
| correct | 411 | 8.76% | 7.79% | 8.03% |
| retrieval_failure | 309 | 11.97% | 13.27% | 11.65% |
| ranking_failure | 202 | 8.91% | 12.38% | 9.41% |
| no_coverage | 262 | 10.69% | 8.78% | 11.83% |

Among the 270 covered queries missed by every tested top100 candidate generator, 205 have none of these three low-tail flags. This is **not** an estimate of usable-image prevalence: camera shake, a close wall, blocked views and images without landmarks can all pass these numeric checks. A random visual inspection of ten hard misses found both apparently unusable and normal-looking images, but it was not a blinded labeled audit. Quantifying the contribution of image quality requires a separate outcome-blinded human review with categories for motion blur, obstruction/close-up, lighting, missing landmarks and clear images, preferably with two raters. Keep every query in the primary accuracy denominator and report any later quality strata separately. Full per-query proxy measurements are in `data/evaluation/geographic_v8_20260928/query_quality_audit/per_query.npz`. The fixed ten-image review sample is in `data/evaluation/geographic_v8_20260928/query_quality_audit/unrecognized10_20260929.json`.

## Interpretation and uncertainty

G3, PLONK_OSV_5M, OSV-5M and GeoCLIP were run with pinned official checkpoints and audited local execution changes. Their Moscow zero-shot image→GPS behavior was weak; pure G3/GeoCLIP or direct-distance reranking destroyed hundreds of SAGE-correct cases. The 322/504 query-view union recovered 24 original retrieval misses, but its OOF selection models did not improve final RAW. Strict reference-only SAGE adaptation added 10 further retrievable queries to the previous all-model union; as a final predictor it fell from 411 to 406 correct, with 24 old errors fixed and 29 old correct answers lost. Thus the remaining bottleneck is selecting the additional correct candidates without damaging existing winners. Two adaptation pilots were aborted before development scoring: the first had provider-sequence overlap; v2 let negative-role examples cross the geographic embargo. Only the all-role-disjoint v3 run is eligible for comparison. Details, model licenses and unresolved external-code gates are in [the model audit](geographic_v8_model_audit_20260928.md).

Each important paired delta and its geographically grouped bootstrap interval is in the linked per-variant JSON report. A small delta with an interval including zero is not established improvement. Neither the historically opened final set nor new smartphone data was scored here. [The independent collection protocol](geographic_v8_independent_validation_protocol.md) and evaluator are ready for a post-freeze cohort.

## Reproduce

Run from the repository root. Large staged inputs and pinned checkpoint caches are required; the commands verify hashes and fail if an input changed.

```sh
.venv/bin/python -m ml.research.geographic_v8.inventory
.venv/bin/python -m ml.research.geographic_v8.stage
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.baseline --recompute
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.secondary_baseline
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.g3
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_g3
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.plonk_direct
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_direct plonk
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_prior plonk
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.osv5m_direct
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_direct osv5m
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_prior osv5m
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.geoclip
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_embedding geoclip
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.resolution_union
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.oof_union_rerank
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.cross_model_union
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.union_uncertainty
OMP_NUM_THREADS=4 data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.mine_sage_adaptation
OMP_NUM_THREADS=4 data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.mine_sage_adaptation_v2
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.mine_sage_adaptation_v3
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.train_sage_asymmetric_v3
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.reference_validation
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.evaluate_sage_adaptation
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.adapted_union
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.adapted_full_union
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.query_quality_audit
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.build_report
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.freeze_candidate
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.verify_integrity
```

Production baseline/hash receipts are `data/evaluation/geographic_v8_20260928/production_before.json` and `production_after.json`. The original candidate remains under `data/evaluation/moscow_night_v7/candidate_frozen.json`. Each new experiment has an immutable JSON record under `data/evaluation/geographic_v8_20260928/experiments/`; research source snapshots and hashes are under `data/evaluation/geographic_v8_20260928/source_snapshots/20260928_0957/`.
