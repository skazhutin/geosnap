# GeoSnap v9 research report: input utility and safe candidate selection

**Status: completed with available data; no new predictor selected.** The strongest verified single-photo RAW <=100 m result remains frozen SAGE-L 322+504 context30, **411/1,184 (34.71%)**. All 1,184 images remain in the denominator. Production files were verified before and after the cycle; all 85 guarded files are unchanged. The current 112,163-reference gallery is unchanged; MSLS remains research-only. No quality model or adaptive policy is frozen because the user chose to stop the blind pilot after 11 images and no independent phone sessions exist.

## What the fixed candidate union actually offers

The six-source same-gallery candidate union (SAGE, query322, query504, G3, GeoCLIP, reference-adapted SAGE) retains the verified **652/1,184 = 55.07%** <=100 m candidate oracle. It adds a nearby candidate for 39 of the original 309 retrieval failures. Geographic clustering with 75 m seed radius preserves the candidates without using query GPS as a feature. A single representative coordinate per cluster reduces the all-hypothesis prediction oracle to **642/1,184**, so ten boundary/representative cases need a finer candidate decision. Restricting the challenger search to the first 8, 30 and 100 pooled-rank hypotheses makes 457, 541 and 610 queries respectively reachable by a baseline-or-challenger choice. These are *oracles*, not predictor accuracy.

## Selection results

The anchor was always the original frozen SAGE prediction. Simple pairwise tabular classifiers were trained and evaluated on five geographically grouped development folds with a one-H3-ring embargo and a five-to-one false-switch cost. This remains exploratory because these development queries have been reused. A frozen SAGE visual-context encoder scored the anchor plus 30 challenger locations without using query ground truth. Zero-shot visual selection recovered some failures but also damaged correct cases.

| Variant | RAW25 | RAW50 | RAW100 | Correct / 1,184 | Median m | p90 m | >500 m | Old wrong→correct | Old correct→wrong |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Frozen SAGE baseline | 17.15% | 26.86% | **34.71%** | **411** | 3,803 | 30,512 | 58.36% | — | — |
| 8 hypotheses, OOF logistic | 17.15% | 26.77% | 34.63% | 410 | 3,803 | 30,474 | 58.45% | 0 | 1 |
| All hypotheses, OOF logistic | 17.15% | 26.77% | 34.54% | 409 | 3,803 | 30,512 | 58.45% | 0 | 2 |
| Visual context 30, zero-shot argmax | 17.31% | 26.94% | 34.38% | 407 | 3,936 | 30,381 | 58.45% | 11 | 15 |
| Visual context 30, OOF logistic | 17.15% | 26.69% | 34.46% | 408 | 3,863 | 30,474 | 58.78% | 1 | 4 |
| Three conservative OOF boosted selectors | 17.15% | 26.86% | 34.71% | 411 | 3,803 | 30,512 | 58.36% | 0 | 0 |
| Reference-trained visual pairwise verifier, top8, 1% validation false-switch gate | 17.15% | 26.86% | 34.71% | 411 | 3,803 | 30,424 | 58.36% | 0 | 0 |
| Reference-trained visual pairwise verifier, top30, 5% validation gate | 17.15% | 26.77% | 34.63% | 410 | 3,914 | 30,474 | 58.45% | 0 | 1 |

The positive oracle is a genuine retrieval opportunity, but pooled ranks, metadata and this frozen patch-context signal did not identify enough safe switches. The boosted models preserved the baseline by never switching; that is a valid negative result, not an accuracy improvement. A separate verifier was trained **only on 1,111 reference-derived anchors** with eight hard negatives each and evaluated on 128 geographically and sequence-isolated reference anchors. Its reference pair AUC was **0.925** and patch-context pair accuracy **83.2%**, versus **70.7%** for direct cosine. However, validation-derived conservative gates made 15–32 development switches without a single old-wrong→correct transition; one top30 gate lost a correct case. The reference-pair task does not transfer to the development KEEP/SWITCH problem in this form. No v9 result justifies replacing the v8 research candidate or production.

The safe-switching frontier also failed. At zero regressions, the OOF tabular policies made at most three harmless switches and recovered zero errors. Allowing up to two regressions yielded zero recoveries for the rank/metadata policies. The visual-context OOF policy needed four regressions to recover one case. These are **posthoc descriptions of OOF ordering**, not deployable thresholds. The failure-bucket picture is therefore unchanged at the selected best: 262 uncovered, 309 retrieval misses, 202 ranking misses and 411 correct. The full candidate union changes only *availability*: 39 of the 309 retrieval misses become retrievable, while all 202 original ranking misses remain retrievable; no verified selector converts those opportunities into a net RAW gain.

## Blinded input-quality pilot

A 200-image master annotation sample was reproducibly drawn at 50 images per original hidden outcome bucket. At the user's request, a separate 20-image pilot drew five from each bucket. The annotation form hides outcomes, identities, GPS, candidate rankings and model confidence; answers are append-only. The user stopped voluntarily after **11/20**. Scores were two very weak (1), four potentially usable (2) and five good (3). Both very weak images belonged to retrieval failures, but another retrieval failure was rated good. This tiny, stopped, single-rater pilot cannot estimate population prevalence, inter-rater reliability or the share of failures caused by input quality.

The free-text notes matter: a vehicle interior was often marked only to describe capture context, not as an automatic defect. Notes also identified dirty glass, headlight glare, motion blur, an upside-down image and good but ambiguous views. The pilot memo specifies taxonomy revisions without changing the original annotations. Nine of eleven images received an “another view might help” recommendation, including baseline-correct ones; that answer cannot be treated as an abstention label.

The prespecified unsupervised technical-tail score was descriptively higher for the two human level-1 images (median **0.902**) than for the five level-3 images (median **0.732**). Eleven images cannot establish its AUROC, useful operating point or generalization. The original free-text comments and their SHA-256 remain in the sealed private pilot artifact.

All 1,184 query images now have deterministic technical features cached without query GPS or outcome labels: exposure, clipping, luminance entropy, Laplacian/gradient/frequency detail, edge density, dimensions and compression proxy. These are diagnostic signals, not validated geolocatability. The 11-image pilot is too small to train or validate a frozen-backbone classifier, an action predictor or a calibrated retake threshold. The benchmarking code requires a larger completed blind sample and geographically embargoed OOF evaluation before making such claims.

## Product and validation preparation

`adaptive_multiphoto.py` implements independently encoded photo evidence with best-photo, equal score, quality-weighted score, equal geographic consensus and quality-weighted geographic consensus alternatives. It also supports a targeted retake decision using an externally frozen quality threshold. Synthetic unit tests show the weights behave as specified; **no real phone accuracy result exists**. The prospective smartphone protocol and write-once evaluator require a frozen pipeline, new sessions, a natural initial photo, alternative directions and separate intentionally weak views. They report single-shot, adaptive and multi-photo outcomes separately.

The next real-world gate is independent human labeling and post-freeze phone collection. Neither can be replaced by the 43-panorama synthetic exercise or the repeatedly opened 1,184 development images. The user explicitly chose not to continue the current annotation pilot, so this cycle ends without a trained quality detector, a defensible retake threshold, confidence calibration or an independent smartphone result. The selected research coordinate predictor remains the already frozen v8 baseline; no new candidate freeze is warranted.

## Reproduction

Use the pinned local Python runtime and run from repository root:

```sh
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.initialize
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.prepare_annotations
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.prepare_quick20
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.annotation_server --rater reviewer1 --port 8769 --sample quick20
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.analyze_annotations --sample quick20 --raters reviewer1
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.technical_quality
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.hypotheses --all
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.safe_switch --all
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.visual_context30
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.safe_switch --visual30
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.visual_context_audit
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.reference_pairwise_verifier
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.verify_integrity
```

Experiment records are append-only under `data/evaluation/geographic_v9_20260928/experiments/`; rerunning a sealed experiment ID requires a new version rather than overwriting evidence. A clean-workspace execution can follow the commands above. For existing evidence, use the integrity verifier and artifact hashes instead of overwriting sealed outputs. The prior baseline reproduction command and full provenance remain in `docs/geographic_v8_research_report_20260928.md`. The verified v9 integrity receipt covers 85 production files, 11 key research artifacts and 19 experiment records.
