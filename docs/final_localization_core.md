# Final localization core — production handoff

Status: **frozen after the single sealed `moscow_real_v4` test** on 2026-09-03.
This is the authoritative pre-production ML document. The production decision
is Outcome B: keep the exact Part 2.5 decision policy, run it on the larger v4
gallery/index, and proceed to Part 3. No further pre-production model,
retrieval, localization, feature, gate, or threshold selection is permitted.

The correct product claim is:

> On 1,499 eligible, leakage-screened held-out Moscow street-image queries,
> GeoSnap answered 127 (8.47%); 123/127 answers (96.85%) were within 100 m.

It is not correct to claim that GeoSnap “localizes Moscow with 96.85%
accuracy”: the system abstained on 91.53% of eligible queries and the source
gallery is not city-, season-, heading-, or provider-complete.

## A. Final system

The sole production entry point is
`configs/moscow_production_frozen.json` (SHA-256
`9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`).
Its adjacent `.sha256` file is mandatory; startup and production verification
fail if the digest or any bound artifact is incompatible.

| Component | Frozen value |
|---|---|
| Retriever | SAGE ViT-B, no cross-image encoder |
| Code revision | `chenshunpeng/SAGE@c7d6241c4885526d99d6c78c158024fc2a37097c` |
| Checkpoint | `shunpeng/SAGE@2a2ea9964cdbdfd2211e7c625064a9d5e4678245/SAGE_No-Encoder_Vit-B.pth` |
| Checkpoint SHA-256 | `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e` |
| Preprocessing | official ImageNet normalization, then resize to 322 |
| Descriptor | normalized float32, 8,448 dimensions |
| Gallery | 20,487 references: 14,099 Mapillary + 6,388 KartaView |
| Index | exact normalized `faiss.IndexFlatIP`, cosine via inner product |
| Retrieval depth | K=30, single query descriptor |
| Aggregation | density-aware geographic-mode voting, rank decay, sequence-deduplicated support |
| Coordinate | weighted medoid of the winning compact mode |
| Confidence | Part 2.5 standardized L2 binary logistic score, 14 features |
| Confidence artifact | `configs/moscow_real_v3_confidence_model.json`, SHA-256 `5afee6b1a1abea5c2701bc77e05d08a00b7e0a0657985567146e1de82afe5f82` |
| Acceptance threshold | confidence score `>=0.9349250249145314` |
| Catastrophic-risk model | none in production; the v4 candidate did not generalize |
| Explicit safety gates | none beyond successful retrieval and the frozen confidence threshold |
| Local verification/reranking | disabled |
| Approximate tier | disabled |

Status semantics are deterministic:

- `ok`: at least one retrieval candidate exists and the exact Part 2.5 score
  meets the frozen threshold; coordinates and attributed matches may be shown.
- `low_confidence`: a candidate location exists, but its confidence score is
  below the frozen threshold.
- `out_of_coverage`: retrieval produced no usable reference evidence; this is
  not a synonym for ordinary low confidence.
- `approximate`: reserved by the API but disabled in this release.

No environment variable may override the retriever, index, K, aggregation,
confidence artifact, or threshold when the production frozen config is used.
SAGE load failure is a readiness failure; there is no MegaLoc, random-weight,
mock, or alternate-model fallback.

## B. Historical progression

V1 established a real Mapillary/KartaView Moscow pipeline but exposed severe
coverage and decision-layer limits. V2 enlarged the clean source pool and
compared MegaLoc, five-crop MegaLoc, and SALAD. SALAD ranked positives better,
but its product point and deployability were insufficient; the v2 Wilson
kill-switch froze threshold 1.0 and answered 0/497. That meant zero exposed
answers, not zero raw localization signal.

Part 2.5 replaced retrieval-only K selection with product evaluation. SAGE
ViT-B, K=30, density-aware geographic voting and the 14-feature logistic score
produced 92/1,195 answers on the once-opened v3 test, with 94.57% conditional
<=100 m precision but 3/92 accepted errors >500 m. SelaVPR++ was faster and
competitive but slightly worse at the calibrated product point; its official
second-stage reranking did not help. SALAD remained evaluation-only. CricaVPR
was rejected because unrelated batch neighbors changed its descriptors,
violating independent deterministic gallery/query embedding.

V4 was therefore a bounded decision-layer audit, not another acquisition or
model bake-off. It evaluated K=15/20/30/40/50, existing aggregators, expanded
features, a separate catastrophic-risk logistic model, a multinomial risk
model, and six candidate gates. Development retained SAGE K=30 and the current
density-aware aggregator. The multinomial candidate was frozen before test,
but failed to generalize; the prescribed Case 2 retained the exact Part 2.5
policy. V1, V2 and V3 artifacts and opened tests were not modified or used for
v4 fitting, feature selection, thresholds, or architecture selection.

## C. V4 final evaluation

The test was claimed once after both policies and all candidate artifacts were
frozen. Both policies consumed the same SAGE top-50 query stream in one
transaction. Receipt SHA-256:
`95060e281bd94fd6a8fa582a0267d95fd0aab171f67ad065dd9480bdaa0bd49a`.
The receipt records `completed_no_post_test_tuning`.

### Retrieval and raw localization

Retrieval is identical for both finalists.

| Metric | V4 result |
|---|---:|
| <=100 m R@1 / R@5 | 28.82% / 36.96% |
| <=100 m R@10 / R@20 | 39.89% / 42.96% |
| <=100 m R@30 / R@50 | 44.56% / 47.23% |
| <=100 m positive rank median / p75 / p90 | 78 / 1,469.5 / 6,816.4 |
| Raw <=25 / <=50 / <=100 m | 13.34% / 21.21% / 29.42% |
| Raw 100–500 m / >500 m | 81 / 977 |
| Raw median / p90 error | 7,563.11 / 25,902.95 m |

Raw performance confirms useful visual signal, but also a long, strongly
multimodal tail. In 216 test cases the correct geographic cluster was retrieved
but lost to another mode; 920 cases included a singleton false cluster.

### Product result and frozen comparison

| Metric | Exact Part 2.5 baseline — **selected** | Frozen v4 multinomial candidate |
|---|---:|---:|
| Answered / total | **127 / 1,499** | 91 / 1,499 |
| Answer rate | **8.47%** | 6.07% |
| Bootstrap 95% answer-rate CI | **7.07–9.94%** | 4.87–7.34% |
| Conditional <=25 / <=50 / <=100 m | **59.84 / 84.25 / 96.85%** | 58.24 / 83.52 / 93.41% |
| Wilson 95% for <=100 m | **92.18–98.77%** | 86.35–96.94% |
| All-query answered-and-correct <=100 m | **8.21%** | 5.67% |
| Accepted >100 m | **4** | 6 |
| Accepted 100–500 m | **1** | 1 |
| Accepted >500 m | **3** | 5 |
| >500 m / answered | **2.36%** | 5.49% |
| Accepted median / p90 / p95 | **21.29 / 62.79 / 73.47 m** | 21.47 / 66.94 / 786.68 m |

Relative to the selected baseline, the candidate removed 60 answers and added
24. It removed 58 correct answers and added 20 correct answers. It prevented
two baseline catastrophic errors but introduced four new ones; one
catastrophic error was shared. The answer-rate delta was -2.40 percentage
points, <=100 m precision delta -3.44 points, and all-query success delta -2.53
points. This is a clear generalization failure, so the test was not followed by
any threshold, feature, K, aggregation, or gate change.

The selected v4 result meets the >=8% answer-rate, >=90% conditional precision,
and <=50 m median portions of the desired region. It does **not** meet the <=1%
accepted catastrophic target: 3/127, bootstrap 95% 0–5.51%. That limitation is
frozen and must be communicated rather than hidden by another tuning cycle.

Provider evidence is imbalanced. The test contained 1,484 Mapillary and only
15 KartaView queries. The selected policy answered 125 Mapillary queries
(121/125 <=100 m; 3 >500 m) and 2 KartaView queries (2/2 <=100 m; Wilson
34.24–100%). Provider-pair results were Mapillary→Mapillary 109/111 <=100 m
with 1 catastrophic error, Mapillary→KartaView 12/14 with 2 catastrophic
errors, and KartaView→KartaView 2/2. Small KartaView and cross-provider
denominators are uncertainty, not evidence of equal performance.

## D. Development and calibration

### K and aggregation sanity check

The same SAGE retrieval stream was replayed at K=15, 20, 30, 40 and 50 through
legacy, rank-weighted, sequence-deduplicated and density-aware implemented
aggregators. K=30 density-aware remained the default because no challenger
showed a clear, substantial joint gain in raw localization, safety and useful
answer rate. On development its raw <=100 m rate was 29.69%; 129 queries had a
correct retrieved cluster that lost the mode decision. A legacy K=50 point
could answer slightly more under one fitted policy, but raw <=100 m fell to
24.10%, so it was not a defensible architecture change.

### Existing confidence audit

The Part 2.5 confidence artifact uses top similarity and margin, winning and
runner-up scores, geographic mode margin, independent sequence count, provider
diversity, winner candidate count/spread, geographic separation, local density,
best winner rank, top-1 agreement and cross-provider evidence. Its exact means,
standard deviations and coefficients are preserved in
`configs/moscow_real_v3_confidence_model.json`; the signed standardized
coefficients range from -0.507 to +1.202. The development report preserves the
full correlation/collinearity audit rather than silently deleting correlated
features.

Five-fold geographically grouped out-of-fold metrics were:

| Development architecture | Correctness ROC / PR | Correctness Brier / ECE | Catastrophic ROC / PR | Catastrophic Brier / ECE | Best preregistered tier |
|---|---:|---:|---:|---:|---|
| Existing 14-feature correctness logistic | .8890 / .8304 | .1030 / .0243 | — | — | secondary; 119/751 answers, 94.12%, 2 catastrophic |
| Expanded correctness + risk logistics | .8926 / .8256 | .1048 / .0223 | .8885 / .9155 | .1146 / .0310 | secondary; 101/751 answers, 95.05%, 2 catastrophic |
| Multinomial 3-class logistic | .8952 / .8326 | .1041 / .0196 | .8849 / .9005 | .1155 / .0287 | primary; 60/751 answers, 96.67%, 0/60 catastrophic |

The catastrophic class differed most in lower winning-mode mass, lower
geographic margin, higher runner-up score, smaller top-5/top-10 spread and
margins, lower top similarity, and less independent support. Standardized mean
differences for the leading effects were -1.32, -1.24, +1.16, -1.15 and -1.15.
The strongest fitted catastrophic-class coefficients additionally included
local density at 50 m (+0.788), winner similarity variance (-0.479), geographic
mode count (+0.435), top-10 minimum similarity (+0.413), local density at 100 m
(-0.386), top1–top5 margin (-0.388), second-mode separation (+0.323), medoid/
top1 agreement (+0.317), outside similarity variance (+0.303), and maximum
winner rank (+0.300). These are model associations, not causal safety rules.

The existing binary score did separate <=100 m from failures, but the
catastrophic-risk audit showed that a second small model could improve the
development frontier. That improvement did not survive the final holdout.

### Gate efficiency

No explicit gate survived. At the selected development point, distant-mode
ambiguity and top1/mode disagreement rejected nothing and prevented no
catastrophes. Compactness removed 12 answers, including 11 correct, and
prevented 0 catastrophes. Independent-support and sequence-dominance gates
each removed 46, including 45 correct, and prevented 0. The low-density gate
removed 5 correct answers and prevented 0. Their correct-loss-per-catastrophe
ratios are therefore undefined/infinite; stacking them would only create more
abstention.

### Threshold-only calibration

Architecture, 45-feature schema, development-fitted multinomial coefficients,
K, aggregation and gate family were frozen before calibration. Calibration
performed no fitting. The preregistered primary rule selected correctness score
`>=0.9779782812443042` and catastrophic-risk score
`<=0.017510606503924175`: 62/750 answers (8.27%), 59/62 <=100 m (95.16%;
Wilson 86.71–98.34%), 3 moderate errors and 0/62 >500 m. Accepted median/p90/
p95 were 18.06/72.39/82.53 m. Calibration’s 62 accepted examples exceeded the
minimum 40, but still left wide uncertainty; zero observed catastrophes was
never presented without its denominator.

The candidate’s correctness/risk scores remained ranking signals rather than
well-calibrated probabilities. Correctness ROC/PR/Brier/ECE changed from
.8800/.7991/.1103/.0512 on calibration to .8621/.7668/.1261/.0449 on test;
catastrophic-risk values changed from .9034/.9169/.1096/.0463 to
.8741/.9063/.1274/.0384. In the highest correctness bin, calibration observed
89.90% correctness at mean score .9769 (n=99), while test observed 90.56% at
.9713 (n=180). The low catastrophic-risk bin shifted from 3.25% observed at
mean .0139 (n=123) to 7.02% at .0186 (n=242). The selected Part 2.5 score’s
highest test bin observed 96.67% correctness at mean .9732 (n=150). Full
reliability bins and per-query out-of-fold predictions remain in the generated
reports.

## E. Failure modes

- Visually repetitive roads and streets create confident distant modes.
- The correct geographic mode can be present in top-K yet lose aggregation.
- Singleton false clusters are common, while raw error retains a very long
  tail.
- High heading gaps are weakly supported and risky: only 7 selected answers
  had >=90-degree gaps, with 2 catastrophic errors.
- Cross-provider evidence is sparse and weaker than same-provider evidence.
- Temporal/viewpoint mismatch, low local density and single-sequence support
  remain difficult, but simple hard gates were too destructive.
- `uncertainty_radius_m` remains uncalibrated and must remain null.
- The confidence output is a score, not a guaranteed probability.

## F. Supported scope and v4 data boundary

V4 reuses clean, stored Mapillary/KartaView source imagery. It performed no
citywide acquisition. Historical query frames may remain legitimate gallery
references where leakage rules permit; query eligibility, not reference
coverage, was excluded for all 2,991 unique provider sequences used by v1/v2/
v3 query splits.

| Split | Rows | Mapillary / KartaView | Provider sequences | Geographic components | H3 evaluation areas |
|---|---:|---:|---:|---:|---:|
| Gallery | 20,487 | 14,099 / 6,388 | 13,698 | — | 1,674 |
| Development | 751 | 748 / 3 | 729 | 229 | 290 |
| Calibration | 750 | 747 / 3 | 734 | 230 | 287 |
| Sealed final test | 1,499 | 1,484 / 15 | 1,474 | 447 | 574 |

All six split pairs had zero overlap in record ID, source ID, stable source ID,
path, source URL, exact file hash, provider sequence, geographic group and
pHash-near pairs. Minimum development/calibration, development/test and
calibration/test distances were 251.54, 250.97 and 250.32 m. Every query had a
gallery positive <=100 m. Bundle fingerprint:
`0eb03372570a8cd4c703aa07789065717027ba7223024d8e4361d74faa1b489f`.

These controls protect evaluation independence; they do not manufacture source
coverage. Only Mapillary and KartaView are deployable sources. Commons, MSLS
and other benchmark/training data remain excluded from the production gallery
and index. Source ID, provider, license, attribution and source/profile URLs
remain in reference metadata and `/localize` matches.

## G. Runtime

Measured on macOS arm64 with Python 3.12, PyTorch 2.13 and FAISS CPU 1.15:

| Measurement | Result |
|---|---:|
| Model load | 3,952.92 ms |
| Persisted index load | 1,456.72 ms |
| Query embedding p50 / p90 / p95 | 63.86 / 67.16 / 72.59 ms |
| Exact search p50 / p90 / p95 | 30.13 / 33.64 / 35.33 ms |
| Spatial/policy p50 / p90 / p95 | 1.12 / 1.36 / 1.39 ms |
| End-to-end p50 / p90 / p95 | 115.81 / 131.70 / 143.25 ms |
| Benchmark peak RSS | 1,877,458,944 bytes (1.75 GiB) |
| Production verification peak RSS | 1,647,394,816 bytes (1.53 GiB) |
| `index.faiss` | 692,296,749 bytes (660.23 MiB) |
| Confidence artifact | 2,143 bytes |

Policy overhead is below the 10 ms target. Service construction loads model,
FAISS and confidence once and reuses them across requests; the concurrency
semaphore protects shared inference. The model and index are not reinitialized
per request or duplicated intentionally. On macOS, run the API as its own
process because PyTorch/FAISS process isolation matters. The same locked
Python/FAISS/PyTorch path remains Linux-compatible; container optimization is
Part 3.

## H. Reproducibility and frozen evidence

Key SHA-256 values:

| Artifact | SHA-256 |
|---|---|
| Production config | `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e` |
| V4 candidate config | `96f70e0be6382365f03a9cabd2d455b7526c57d5b51c38af93b7806f92da9a79` |
| V4 test seal | `b23ee266f9dcca876ac36b08205da123bc2f45422427df6a3c689620ee6cd72d` |
| Gallery manifest | `ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da` |
| Development manifest | `17d0f339d3a34d4826a2d7cf337861a566a48b23d76fb1353919d9733fbcf64d` |
| Calibration manifest | `e2e088d5fecabc8b95c29fffcf91d0248355fafc0d0cc312a0100ab7eee4e64d` |
| Sealed test manifest | `332567068c27cd58eb7951823c3102d05cf7263abb152ffb49db4e280be0e34f` |
| Gallery descriptors | `0ffcbbe2761e6e56d56265e7125e4e6983ddebda6cb67f565d61ca4471e6f527` |
| FAISS index | `cda9739cc7269881ece33f70de9c4b6bfb027b38150bd6c228fd736cc4270ad2` |
| Index metadata | `26c8f1f87e0500d75039de5785e58b1bfd0730390b7f66a1b149abd08d07539e` |
| V4 comparison | `c2eae328a62caca12434bbfb9ad08c3bfbba70683af3c6bfa99d3062a3ca974f` |

The safe reproducibility command is:

```bash
make verify-moscow-production
```

It validates the production config digest and every bound model/index/
confidence/evaluation artifact, loads the actual SAGE model and exact index,
and runs the permanent non-test smoke set. It does **not** access or rerun the
frozen v4 test. The smoke manifest itself is marked `selection_use=forbidden`.

The smoke set covers one easy `ok`, one genuine `low_confidence`, and one
synthetic unsupported image accepted only as `low_confidence` or
`out_of_coverage`. Host API checks additionally covered JPEG recompression,
resize, brightness/contrast, small crop, EXIF present/absent, and four
concurrent requests. These are wiring/stability checks, not accuracy claims.

License evidence is recorded in `docs/licenses.md`. SAGE code and its official
model card state MIT; this remains an engineering audit, not legal advice.
DINOv2 dependencies retain Apache-2.0 terms. SALAD is evaluation-only because
its GPL-3.0 repository and checkpoint terms have not been separated clearly.
Unused evaluation checkpoints must not be included in production packaging.

Upload safety remains unchanged: GPS/EXIF coordinates are not localization
inputs; MIME/signature/decode, byte, pixel, dimension and animation limits are
fail-closed; EXIF orientation is normalized; uploads are processed in memory
and not intentionally retained. Public privacy/security hardening remains Part
3.

## I. Deployment boundary

The localization core is permanently frozen for productionization. Part 3 may
distribute the bound artifacts, slim/rebuild the container, package processes,
add reverse proxy, TLS, rate limiting, monitoring/logs, production tiles,
secrets management, CI/CD, privacy/security hardening, frontend polish and
final browser QA. It must not casually change SAGE, checkpoint/preprocessing,
gallery, descriptor/index semantics, K, aggregation, confidence artifact,
threshold, status rules, or the final evaluation claim.

The current 17.5 GB Docker image is stale and the local Docker daemon was not
available during final validation. The Dockerfile and lockfile contain the
final runtime dependencies, and Compose resolves the production config and v4
index. A clean functional rebuild and slimming belong to Part 3.
