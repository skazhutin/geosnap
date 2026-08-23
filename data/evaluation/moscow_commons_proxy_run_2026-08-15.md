# Moscow Commons proxy run — 2026-08-15

> **Evaluation-only warning:** this is a tiny, hand-curated,
> landmark-biased Wikimedia Commons proxy. It is not street-view imagery, not a
> representative Moscow sample, not a deployable gallery, and not evidence of
> product coverage or city-wide accuracy. It cannot select a production model
> on its own.

## Content-pinned snapshot

- Dataset: `moscow-commons-landmarks-proxy-v1`
- Canonical dataset SHA-256: `39c331f6b53b439f4e40b993d58caf80f69f279a07fe1845442f2f650943b6ab`
- The tracked `moscow_commons_snapshot_ledger.json` pins every Commons source
  SHA-1, downloaded JPEG SHA-256, coordinate, and minimum attribution record.
  Unlike the generated manifest file hash, the canonical digest excludes
  `created_at` and machine-local paths.
- Four landmarks; 12 gallery pages and 8 held-out query pages; 20 JPEGs total
- Cross-split overlap: 0 page IDs, 0 Commons SHA-1s, 0 downloaded-byte
  SHA-256s, and 0 identical perceptual hashes
- Same-landmark cross-split author/time matches: 0 / 0
- Recall positive definition: a gallery geotag within 100 m of query geotag
- Localization threshold denominators: all 8 queries; abstentions are failures
- Median and p90 errors: answered queries only, as explicitly labelled
- Spatial confidence used the same default uncalibrated localizer for both
  models; no threshold was tuned on this proxy

## Primary cross-image results

| Model | R@1 | R@5 | R@10 | ≤25 m | ≤50 m | ≤100 m | Answer rate | Median error, answered | P90 error, answered | Median end-to-end |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| MegaLoc | 87.5% | 100% | 100% | 37.5% | 62.5% | 75.0% | 75.0% | 25.78 m | 52.92 m | 72.78 ms |
| DINOv2 + SALAD | 87.5% | 100% | 100% | 37.5% | 62.5% | 75.0% | 75.0% | 23.37 m | 52.92 m | 70.65 ms |

Only `status=ok` with a complete coordinate counts as answered. Diagnostic
coordinates retained on `low_confidence` results are abstentions and do not
enter accuracy or error percentiles. MegaLoc marked two queries
`out_of_coverage`; SALAD marked two `low_confidence`. One of the latter carried
a 1,912.19 m diagnostic hypothesis, but it is correctly excluded from answered
metrics and still counts as an all-query localization failure.

The proxy does not statistically distinguish the retrievers: Recall@K,
all-query threshold accuracy, answer rate, and p90 are tied. SALAD has a 2.41 m
lower answered median and a roughly 2.13 ms lower median query latency on this
run. For the repository's production default, MegaLoc remains the safer
engineering choice because its official implementation/checkpoint are MIT,
whereas the SALAD repository is GPL-3.0; this is a deployment/license decision,
not an accuracy claim. SALAD remains a useful challenger.

## Separate deterministic augmentation robustness

Nine derived variants were evaluated for each of the eight real held-out query
pages (72 derived queries total). These are not additional geographic samples
and are excluded from the primary table.

| Model | Derived R@1 | Derived R@5 | Derived R@10 | ≤25 m | ≤50 m | ≤100 m | Answer rate | Median error, answered | P90 error, answered |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| MegaLoc | 87.5% | 100% | 100% | 37.5% | 62.5% | 75.0% | 75.0% | 25.78 m | 70.00 m |
| DINOv2 + SALAD | 87.5% | 100% | 100% | 40.28% | 62.5% | 75.0% | 75.0% | 17.99 m | 70.00 m |

Variants: half-size resize, deterministic random crop, JPEG quality 45,
Gaussian blur, motion blur, low brightness, warm color temperature, modest
perspective warp, and partial occlusion. Full per-variant and per-query data is
written to the ignored local reports below.

## MegaLoc coordinate-estimator ablation

All three estimators used the same snapshot, retriever, gallery, exact FAISS
matches, clustering, and confidence settings. Retrieval and answer rate were
therefore unchanged; only the coordinate estimate inside the winning compact
spatial mode changed.

| Estimator | ≤25 m | ≤50 m | ≤100 m | Median error, answered | P90 error, answered |
|---|---:|---:|---:|---:|---:|
| Top-1 | 37.5% | 62.5% | 75.0% | 25.78 m | 52.92 m |
| Weighted medoid (current default) | 37.5% | 62.5% | 75.0% | 25.78 m | 52.92 m |
| Weighted centroid | 50.0% | 62.5% | 75.0% | 15.35 m | 41.44 m |

On the separate 72 derived robustness queries, weighted centroid also improved
≤25 m from 37.5% to 50.0%, ≤50 m from 62.5% to 65.28%, median answered error
from 25.78 m to 14.29 m, and p90 from 70.00 m to 50.26 m; answer rate and
≤100 m stayed 75%. This tiny proxy supports carrying weighted centroid into a
larger street-view ablation, but is too small and biased to tune the production
default by itself.

## Runtime and reproducibility

- Host: Apple M1 Pro MacBook Pro, 16 GB RAM, macOS 26.5.1
- Runtime: Python 3.12.12, PyTorch 2.13.0, FAISS CPU 1.15.0, NumPy 2.5.2
- Inference: Apple MPS; model weights were already cached
- Search: process-isolated normalized exact `faiss.IndexFlatIP`
- MegaLoc: official `gberton/MegaLoc` checkpoint, repository revision
  `5fe0dd697c4a70ba3e23607f6716ab3c606b16db`; load 3.04 s, gallery embedding
  0.794 s, exact index build 23.73 ms
- DINOv2 + SALAD: official SALAD v1.0.0 checkpoint, repository revision
  `6aede13a3f6c25750bf7fde10209c06cb73060bb`, DINOv2 revision
  `7764ea0f912e53c92e82eb78a2a1631e92725fc8`; load 4.59 s, gallery embedding
  0.729 s, exact index build 33.98 ms

| Model | Descriptor | Mean query descriptor | Gallery throughput | Median exact FAISS | Gallery descriptor storage |
|---|---:|---:|---:|---:|---:|
| MegaLoc | 8,448 × float32 | 73.33 ms | 15.11 images/s | 0.428 ms | 405,504 bytes |
| DINOv2 + SALAD | 8,448 × float32 | 71.56 ms | 16.46 images/s | 0.500 ms | 405,504 bytes |

The storage value is the exact 12-row descriptor matrix payload; normalized
`IndexFlatIP` stores the same 405,504 vector bytes plus small FAISS/process and
sidecar overhead. Per-query `end-to-end` here is the offline embedding + exact
search + spatial localizer path, not HTTP/API latency.

Commands:

```bash
.venv/bin/python -m ml.evaluation.commons \
  --config data/evaluation/moscow_commons_landmarks.json \
  --output-dir data/evaluation/generated/moscow_commons_proxy_v1

HF_HOME="$PWD/.cache/huggingface" .venv/bin/python \
  -m ml.evaluation.commons_benchmark \
  --manifest data/evaluation/generated/moscow_commons_proxy_v1/manifest.json \
  --output-dir data/evaluation/generated/moscow_commons_proxy_v1/reports \
  --model megaloc --device mps --batch-size 4 \
  --cache-dir .cache/torch/hub --estimator weighted_medoid \
  --report-stem megaloc_with_robustness \
  --robustness

HF_HOME="$PWD/.cache/huggingface" .venv/bin/python \
  -m ml.evaluation.commons_benchmark \
  --manifest data/evaluation/generated/moscow_commons_proxy_v1/manifest.json \
  --output-dir data/evaluation/generated/moscow_commons_proxy_v1/reports \
  --model dinov2-salad --device mps --batch-size 4 \
  --cache-dir .cache/torch/hub --estimator weighted_medoid \
  --report-stem dinov2_salad_with_robustness \
  --robustness
```

Local full reports:

- `data/evaluation/generated/moscow_commons_proxy_v1/reports/megaloc_with_robustness.json`
- `data/evaluation/generated/moscow_commons_proxy_v1/reports/megaloc_with_robustness.md`
- `data/evaluation/generated/moscow_commons_proxy_v1/reports/dinov2_salad_with_robustness.json`
- `data/evaluation/generated/moscow_commons_proxy_v1/reports/dinov2_salad_with_robustness.md`
- `data/evaluation/generated/moscow_commons_proxy_v1/reports/megaloc_estimator_top1.json`
- `data/evaluation/generated/moscow_commons_proxy_v1/reports/megaloc_estimator_weighted_centroid_with_robustness.json`
- `data/evaluation/generated/moscow_commons_proxy_v1/reports/verification_ablation_megaloc_opencv_sift.json`

The Commons manifest retains each file page, author and raw author markup,
license label and URL, attribution fields, coordinates, capture/upload time,
Commons SHA-1, downloaded SHA-256, and local attribution ledger. Commons
metadata is contributor-supplied; verify each linked file page and current
license before reuse.
