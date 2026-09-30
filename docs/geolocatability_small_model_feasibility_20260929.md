# GeoSnap lightweight geolocatability feasibility pilot — 2026-09-29

**Decision:** A local batched encoder can process the fixed 112,163-reference gallery within a six-hour compute target on this M1 Pro, based on a 2,048-image mounted-disk probe. Its current quality is adequate for exploratory ranking against the frozen Qwen teacher, but **an automatic accept/retake decision remains unvalidated**. Do not promote this pilot to production or treat its scores as human labels.

**Interpretation correction, 2026-09-29:** The annotator clarified that `would_request_another_photo=yes` sometimes meant “this image is useful, but another view could improve precision,” not “this image is unusable.” The 11 retake decisions therefore mix input rejection with an information-gathering preference. Keep the recorded counts below for provenance, but do not interpret disagreement with those decisions as missed detection of bad images or as proof that either model makes unsafe quality decisions. The 11 ordinal ratings are also too few to establish a model ranking.

## Scope and provenance

- Repository HEAD: `da059324644cd47d6318725260ba8cd98f047bd7` (pilot scripts are new local files and are separately hashed below).
- Encoder: [Meta DINOv2-S](https://github.com/facebookresearch/dinov2/blob/main/MODEL_CARD.md), 21M parameters, 384-dimensional CLS feature; [Hugging Face checkpoint](https://huggingface.co/facebook/dinov2-small) revision `ed25f3a31f01632728cabb09d1542f84ab7b0056`, safetensors SHA-256 `ae1e99fcefd534ed978cdeb8326f08030c96e28b7a81ffcbc98a857c84d14be1`. The checkpoint card states Apache-2.0; the model card names LVD-142M as training data. This does not establish rights to any individual pretraining image.
- Mac: Apple M1 Pro, 16 GB unified memory; PyTorch 2.14.0 MPS for timing.
- No query GPS, localization result, failure bucket, candidate ranking or production inference code entered feature extraction or quality fitting. The annotation teacher was the frozen 1,184-row Qwen3.5-4B artifact, SHA-256 `22619a028c3e7cd7e2be22420095e70ab3cc9cfb3e929cbf9d24a33ac85f0b6e`.
- The probe samples gallery paths at 2,048 evenly spaced manifest positions and reads original images from the mounted disk. The processor resizes to the official DINOv2 input. It does not generate the teacher's full JSON taxonomy.

## Measured speed

| Probe | Images | Measured rate | Time/image | 112,163-image projection |
|---|---:|---:|---:|---:|
| Short | 256 | 16.98/s | 0.059 s | 1 h 50 min |
| Longer mounted-disk probe | 2,048 | **10.94/s** | **0.091 s** | **2 h 51 min** |

Both rates exclude the first warm-up batch. The 2,048-image run took 186.5 s including warm-up; its average steady batch time rose from 1.389 to 1.534 s between halves, about 10% slower. Six hours requires at least 5.19 images/s, so the longer probe has roughly 2.1× throughput headroom. This is a projection, not a six-hour endurance measurement: later thermal throttling, disk interruptions, SHA verification, failures and output writes may add time. Google Colab is not needed for the tested encoder throughput.

## Outcome-blind quality validation

The 1,184 query images were encoded independently of their labels. Of 1,162 valid frozen teacher annotations, all geographic groups containing the 11 human-pilot images were excluded from pseudo-label model fitting. A separate geographically grouped split provided 828 training and 220 validation images (120 and 31 groups); ridge regularization was selected using inner grouped folds. The human 11 were held out by geographic group. The human ordinal 0–4 score is divided by four only for the displayed MAE comparison.

| Features | Teacher holdout MAE ↓ | Teacher holdout Spearman ↑ | Human 11 MAE ↓ | Human 11 Spearman ↑ |
|---|---:|---:|---:|---:|
| Deterministic technical features | 0.152 | 0.461 | 0.167 | 0.486 |
| DINOv2-S only | **0.095** | **0.798** | 0.143 | 0.633 |
| DINOv2-S + technical features | 0.095 | 0.801 | 0.145 | 0.633 |
| Original Qwen teacher | — | — | **0.091** | **0.813** |

The DINO encoder captures teacher semantic scores substantially better than brightness/blur/contrast-style diagnostics. Adding the technical features barely changes the result. The tiny human pilot is retained for provenance, but its 11 ratings are not a reliable population-level estimate or a basis for ranking the models.

### Direct DINO–Qwen comparison without human ratings

On the existing 220-image geographically grouped holdout, the DINO-only ridge head was fitted on the same 828 training images with the already selected inner-CV alpha of 1000. It was compared with the **frozen Qwen three-pass median geolocatability pseudo-label**; no human rating was used in these figures. DINO's MAE is 0.095 and Spearman correlation is 0.798. Median absolute difference is 0.077; 145/220 predictions differ by at most 0.1 and 196/220 by at most 0.2.

At the previously fixed descriptive score boundary of 0.4, Qwen marks 115/220 images low, DINO marks 90/220 low, and both mark 81 low. Relative to the Qwen pseudo-label, DINO detects 81/115 (70.4%) of low-scoring images and 81/90 (90.0%) of its low-score calls agree. It assigns a score above 0.4 to 34 Qwen-low images and a score at or below 0.4 to 9 Qwen-higher images. The threshold-independent AUROC for identifying Qwen-low images is 0.896; AUPRC is 0.915. These are **teacher-agreement metrics, not defect-detection accuracy**. The boundary was not optimized for these 220 images.

The 2,048-gallery-image DINO throughput probe measured 0.091 s/image in batches of 16. The completed detailed Qwen query annotation has a 10.47 s median per pass across 3,454 timed passes, about 115 times slower per pass under those distinct inference procedures. Qwen produces detailed visual reasons and several scores; the current DINO student predicts only an overall score and a separate teacher-retake head. The ongoing Qwen review of shortlisted extreme gallery defects uses a shorter, different prompt and is not the same task as this detailed teacher comparison.

For the separate **retake** head, DINOv2-S reached AUROC 0.846 and AUPRC 0.891 against teacher labels on the 220 grouped validation images (technical-only: 0.742 and 0.799). Against the 11 human decisions, the DINO head agreed on 6/11 and did not request another photo in 5/10 cases where the human did. The Qwen teacher also disagreed with the human on these same five decisions. After the annotator's clarification above, these are descriptive policy disagreements, **not five false keeps of unusable images**. The teacher labels cannot validate a quality-rejection or adaptive-acquisition policy by themselves.

## Recommendation

Use DINOv2-S as the leading **speed-feasible research student**, with a single semantic-score head first. It would take approximately three hours for the 112,163 images under the measured disk conditions and requires no Colab. Do not run or publish a definitive gallery-wide `usable` or `retake` label set yet: the pilot has no robust human validation sample, and the existing retake label mixes image defects with requests for additional evidence. A future annotation protocol must ask separately whether the current frame is technically defective, whether it is semantically informative, and whether another view would increase localization precision. Those are different targets. The local 3.9 GB gallery SAGE descriptor cache could be even faster, but its ability to represent blur, obstructions and semantic usefulness has not been validated.

The old 1,184-row annotation, candidate, gallery and production files were not modified. After the pilot, the 85-file production guard, three protected research artifacts and both canonical annotation hashes were verified read-only.

## Reproduce

```bash
GEOSNAP_BENCH_COUNT=2048 data/evaluation/geolocatability_v1_20260928/runtime/bin/python -m ml.research.geolocatability_v1.benchmark_small_encoder
data/evaluation/geolocatability_v1_20260928/runtime/bin/python -m ml.research.geolocatability_v1.extract_small_encoder_queries
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.validate_small_encoder
```

The checkpoint must be present in `data/evaluation/geolocatability_speedpilot_20260929/model_dinov2_small/` with the pinned SHA-256 above. Speed receipt: `data/evaluation/geolocatability_speedpilot_20260929/dinov2_small_m1pro_2048_b16.json` (SHA-256 `bc743078d8455835ec180f8e0d4c41c0c97f0e62b50f74d12b2154ba38930d9c`). Validation receipt: `data/evaluation/geolocatability_speedpilot_20260929/dinov2_small_grouped_validation.json` (SHA-256 `38f67bd7f8f22502d09c3f0c8ae5e7f3494aa65bfd34efdbaa660b9cd347919f`). Script SHA-256 values: benchmark `2163f6f22d35ca4b3ec42a6e29f6c3fb185a2a03955de041efee03c220cbb213`, extraction `5200f9acf02e37c5105b55604b3f3261c5aa04da19d2354c850b571636368f28`, validation `edd18d2f09ff19ca9bfbc7b60f55f8044a3101900fa35c655b1a91dbaad50072`.
