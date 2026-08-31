# GeoSnap Moscow product-quality diagnosis

## Scope and guardrails

This is a diagnosis of the persisted, real Moscow bundle on `finalize-geosnap`,
not a model or data rewrite.  All performance analysis and every hypothetical
policy comparison in this document use the **493-query calibration split only**.
The 507-query frozen test is used only to report the already-frozen baseline and
to diagnose metadata distribution shift.  No threshold, model, index, gallery,
or production setting was changed, and no result below is a frozen-test-tuned
improvement.

The reproducible machine-readable evidence is ignored intentionally because it
contains large derived arrays:

- `data/evaluation/generated/product_quality_diagnosis/diagnosis.json` — all
  aggregate numbers in this document;
- `per_query.jsonl` — per-query gates, retrieval and localization evidence;
- `calibration_full_rank_search.json` — exact full-index ranks for the 493
  calibration queries;
- `coverage_grid.json` and `coverage_diversity_grid.png` — grid coverage and
  diversity classification;
- `runtime_probe.json` — host resource and latency measurement.

The analysis driver is
[`ml/evaluation/product_quality_diagnosis.py`](../ml/evaluation/product_quality_diagnosis.py).
It embeds the calibration images against the existing index, then searches the
immutable `IndexFlatIP` index in a separate process.  It does not regenerate
gallery embeddings or the index.  Its exact top-10 replay agrees with the
persisted threshold-0 benchmark for all 493 queries.

## 1. Verified baseline and artifact integrity

The checked revision was `32e5e103a0fd2cfb483e0b15ec8a3a7c47dd7126` on
`finalize-geosnap`, matching the expected branch and head.  The worktree was
clean before this diagnosis. `make test` passed before the analysis: Ruff,
329 Python tests (one optional-checkpoint skip), 7 frontend tests, and the
frontend build.

| Persisted artifact | Verified state |
|---|---|
| Final clean source manifest | 22,830 images: 16,504 Mapillary and 6,326 KartaView |
| Deployed gallery | 18,821 references: 16,005 Mapillary and 2,816 KartaView |
| Evaluation manifests | 493 calibration and 507 frozen-test queries; every query has a gallery reference within 100 m (max 99.92 m / 99.55 m) |
| Index / descriptors | normalized float32 `(18,821, 8,448)`; exact FAISS `IndexFlatIP`, `ntotal=18,821`; gallery order and IDs agree |
| Index hash | `102eb5d74730a…`; bundle fingerprint `a8e24a77…`; tracked manifest/AOI hashes agree |
| Input exclusions | 3,009 cleaned source rows excluded, matching the tracked audit |
| Disk before large work | 62 GiB free; no gallery or index was rebuilt |

The frozen production selection is MegaLoc, weighted medoid, threshold
`0.5548002022369389`, verification off.  Its recorded frozen result is
Recall@1/5/10 = **17.36 / 23.87 / 26.63%**, all-query <=100 m = **2.17%**,
and 11/507 accepted answers, all <=100 m.  Its status mix is 41.42%
`low_confidence` and 56.41% `out_of_coverage`.

One deployment-equivalence finding matters: the historical frozen benchmark
used top-10 retrieval, while the service/default environment uses top-20.
This is an implementation/configuration discrepancy, not evidence that either
depth is better.  The calibration-only top-20 diagnostic is reported below;
production was not changed.

## 2. Abstention decomposition (calibration, top-10)

At the frozen threshold, 50/493 (10.14%) calibration queries answer and
443/493 (89.86%) abstain.  Conditions overlap, so the first table answers
"how often does a condition participate?" rather than summing to 443.

| Condition | Queries | All calibration | Interpretation |
|---|---:|---:|---|
| Low top similarity / out of coverage | 219 | 44.42% | Stops before geographic confidence policy |
| Insufficient winning-cluster mass | 202 | 40.97% | Usually a sparse or fragmented visual hypothesis |
| Insufficient mass margin | 144 | 29.21% | Competing geographic hypotheses |
| Insufficient candidate count | 114 | 23.12% | Mainly singleton hypotheses |
| Below confidence cutoff | 218 | 44.22% | Includes the preceding hard-gated cases |
| **Confidence cutoff only** | **19** | **3.85%** | Eligible answer rejected solely by 0.5548 cutoff |

The mutually exclusive rejection combinations make the overlap explicit:

| Exact reason combination | Queries | % of abstentions |
|---|---:|---:|
| Low top similarity only | 219 | 49.44% |
| Mass + margin + candidates + cutoff | 97 | 21.90% |
| Mass + margin + cutoff | 47 | 10.61% |
| Mass + cutoff | 43 | 9.71% |
| Cutoff only | 19 | 4.29% |
| Mass + candidates + cutoff | 12 | 2.71% |
| Candidate count only | 3 | 0.68% |
| Mass + candidates | 2 | 0.45% |
| Mass only | 1 | 0.23% |

This rules out a simplistic “the cutoff is too high” explanation.  Relaxing
only the cutoff can affect 19 queries; low similarity plus the mass/candidate
gates dominate the remaining 424 abstentions.

The retrieval/rejection split is even more important.  A geographically
correct reference is present in top-10 for 174 queries.  Of those, 50 answer
and **124 (25.15% of all queries; 27.99% of abstentions) are later rejected**.
For **319 (64.71% of all queries; 72.01% of abstentions)** no correct
reference is in top-10 at all.  No query without a top-10 positive was
accepted.  Thus confidence/localization can recover only a minority of the
current abstentions without a retrieval/data change.

## 3. Retrieval oracle: the primary ceiling

The full exact FAISS search confirms that construction guarantees a <=100 m
gallery positive, but it is usually ranked far below the useful retrieval
depth: nearest <=100 m positive rank is median 155, p75 1,657, p90 7,769,
p95 11,594 (minimum 1, maximum 18,735).  This is the strongest evidence that
the issue is not absence of a nearby reference but visual matching and useful
coverage.

| Distance definition | Recall@1 | @5 | @10 | @20 | @50 |
|---|---:|---:|---:|---:|---:|
| Any reference <=25 m | 16.02% | 20.49% | 21.30% | 22.11% | 23.33% |
| Any reference <=50 m | 21.30% | 27.79% | 29.41% | 30.22% | 32.86% |
| Any reference <=100 m | 23.53% | 32.66% | 35.29% | 38.54% | 43.00% |

Therefore, with fixed retrieval depth, the generous oracle upper bound for a
perfect localizer/confidence policy is 35.29% at top-10, 38.54% at top-20, and
43.00% at top-50.  This is a ceiling on <=100 m answers, not a forecast: it
assumes every retrieved positive could be identified and localized correctly.

For the true <=100 m candidate, its similarity minus the strongest incorrect
candidate is median **-0.0536** (p75 -0.0031, p90 +0.0990).  Most correct
matches are therefore beaten visually, often by a meaningful margin.  This
points to visual/viewpoint/domain coverage and retrieval robustness before
coordinate estimation.

Temporal and viewpoint mismatch are heterogeneous even in the 212 queries
with a <=100 m positive in top-50: the selected positive's heading difference
has median 27.3° and p90 173.3°; capture-date difference has median 29.5 days,
p75 442 days, p90 788 days, and maximum 4,435 days.  This is direct evidence
for a long-tail viewpoint/temporal mismatch component, but not enough to
separate it quantitatively from provider and area effects without a balanced
next experiment.

## 4. Localization oracle: secondary, but real

Among the 174 calibration queries with a <=100 m candidate in top-10, the
geographic clustering chooses the wrong mode for **57 (32.76%)**.  Two more
choose a mode containing a correct reference but place its weighted-medoid
coordinate beyond 100 m.  Sixty-two (35.63%) are deliberately flagged as
ambiguous multi-modal retrievals (two modes separated by at least 500 m with
low mass margin).

The top-10 counterfactual does not identify the weighted medoid as the main
limiter: top-1, centroid, and weighted medoid each localize 115/493 within
100 m (66.09% of retrieval-positive queries).  Correct candidates are also
not principally being split by the current cluster radius/diameter: top-10
positive clusters are almost always a single candidate (mean 1.03, p95 1).
The common failure is instead that an incorrect mode has stronger evidence or
the positive has too little support.

Increasing depth without redesigning aggregation is not a free fix.  On
calibration, top-20 raises raw <=100 m retrieval from 174 to 190 but the
unchanged frozen policy yields 40 answers (39 correct, one >100 m); top-50
raises raw retrieval to 212 but yields 20 answers.  The wrong-mode fraction
among retrieval-positive queries rises to 40.0% at top-20 and 49.5% at top-50,
and ambiguous multimodal cases rise to 50.5% and 61.3%.  This diagnosis only
exposes the existing K=10/K=20 product mismatch; it does **not** recommend a
new production `top_k` yet.

## 5. Confidence policy and product trade-offs

`interpretable-v1-uncalibrated` ranks useful cases well enough to provide a
conservative safety filter, but it is not calibrated as a probability.  The
table compares raw coordinate-correct (<=100 m) and incorrect localizations,
before production abstention, on calibration only.  Values are medians of the
diagnostic quantities.

| Component | Correct (n=115) | Incorrect (n=378) | Reading |
|---|---:|---:|---|
| Top similarity | 0.2238 | 0.1463 | Strong discriminator |
| Winning cluster mass | 0.5266 | 0.1348 | Strong discriminator |
| Geographic mass margin | 0.0819 | 0.0045 | Strong discriminator |
| Winning candidates | 3 | 1 | Supports the hard candidate gate |
| p90 cluster spread (m) | 30.3 | 0.0 | Singleton wrong modes make compactness misleading alone |
| Query quality | 0.7779 | 0.7596 | Weak discriminator |
| Composite confidence | 0.5398 | 0.2005 | Good ranking, not probability calibration |

The scaled confidence components show the same pattern: similarity, cluster
mass, geographic margin, mode dominance, and hypothesis separation separate
correct from incorrect cases; query quality scarcely does.  Compactness is
counterintuitive in isolation because a wrong singleton is perfectly compact;
the minimum-candidate and mass gates protect against it.

Descriptive reliability confirms that the score is not probabilistically
calibrated (Brier 0.1026; ECE 0.1532):

| Confidence band | Queries | Mean score | Observed <=100 m (95% Wilson interval) |
|---|---:|---:|---:|
| [0.0, 0.2) | 193 | 0.178 | 2.6% (1.1–5.9%) |
| [0.2, 0.4) | 212 | 0.260 | 15.1% (10.9–20.5%) |
| [0.4, 0.55) | 31 | 0.476 | 74.2% (56.8–86.3%) |
| [0.55, 0.7) | 29 | 0.625 | 93.1% (78.0–98.1%) |
| [0.7, 1.0] | 28 | 0.788 | 100.0% (87.9–100.0%) |

The answer-rate/precision curve contains the present frozen operating point
and demonstrates the data limitation.  Accepted error summaries are calculated
over accepted answers, including every false confident error.

| Calibration cutoff | Answers | Answer rate | <=100 m precision (Wilson 95%) | Accepted error p50 / p95 |
|---|---:|---:|---:|---:|
| 0.00 (hard gates only) | 69 | 14.0% | 87.0% (77.0–93.0%) | 13.3 m / 2,687 m |
| 0.50 | 56 | 11.4% | 98.2% (90.6–99.7%) | 9.6 m / 54.9 m |
| **0.5548 frozen** | **50** | **10.1%** | **100.0% (92.9–100.0%)** | **8.4 m / 37.0 m** |
| 0.60 | 45 | 9.1% | 100.0% (92.1–100.0%) | 8.0 m / 32.0 m |
| 0.70 | 26 | 5.3% | 100.0% (87.1–100.0%) | 8.3 m / 31.9 m |

For analysis only, choosing a cutoff by in-sample *point* precision would
permit 13.4% answer rate at 90% (60/66 correct; 95% Wilson lower bound 81.6%),
12.6% at 95% (59/62; lower 86.7%), or 12.2% at 97.5% (59/60; lower 91.1%).
No cutoff achieves a 95% or 97.5% **Wilson lower bound** on this small sample;
the 97.5%-point option achieves only a 90% lower-bound guarantee.  The current
50/50 point result is similarly not proof of 100% future precision (lower
bound 92.9%).  This supports a prospective, stratified calibration procedure
rather than a silent threshold relaxation.

## 6. Calibration-to-frozen-test distribution shift

This section uses test **metadata only**.  The frozen outcome may explain why
the old calibration policy generalized poorly, but none of these observations
are a parameter-selection result, and they must not be revalidated on this
same frozen test.

| Characteristic | Calibration | Frozen test | Shift / implication |
|---|---:|---:|---|
| Query provider | 65.7% KartaView, 34.3% Mapillary | 34.9% KartaView, 65.1% Mapillary | Provider TVD 0.308; source mix reverses |
| Query image dimensions | 65.1% >=2500 px long side | 29.8% >=2500 px; 69.2% 1600–2499 | Size-bucket TVD 0.353 |
| Coarse 4x4 region | calibration has 54.4% in r22 | test has 25.8% r22, 25.6% r23, 13.2% r33 | Region TVD 0.359 |
| Evaluation H3 | disjoint held-out areas | disjoint held-out areas | Fine H3 TVD 1.000 by construction; coarse H3 TVD 0.628 |
| Capture month | — | — | TVD 0.450 |
| Nearest reference distance | median 25.5 m | median 31.7 m | +6.2 m for test |
| Gallery refs within 100 m | median 4, p90 13 | median 2, p90 8 | Test local density is roughly half |
| Image quality score | median 0.913 | median 0.927 | Test is not lower quality by this metric |
| Heading gap to nearest ref | median 75.1° | median 68.1° | Not worse in metadata |
| Nearest capture-date gap | median 167 d | median 175 d | Not materially worse at the median |
| Query-to-nearest-reference provider | 31.8% Map→Map; 34.7% KV→KV | 62.9% Map→Map; 8.9% KV→KV | Pairing TVD 0.311 |
| Query sequences | 180 sequences / 493 frames (p95 14.2 frames/sequence) | 342 / 507 (p95 1 frame/sequence) | Test has more independent sequence diversity |

This shift is consistent with the answer-rate drop from 10.14% calibration to
2.17% frozen test.  It especially implicates area/local-density, provider and
image-size/domain mix.  It does **not** establish their causal contribution or
justify retuning the threshold on test labels.  Historic status diagnostics
also show the test collapse is predominantly low similarity/out-of-coverage,
not merely the confidence cutoff: 286 test OOC versus 219 calibration OOC at
the same historical depth.

Source structure explains why raw image count is a poor coverage proxy.  The
clean Mapillary input has 16,504 images from 16,477 sequences (effectively one
representative per sequence), while KartaView has 6,326 images from only 45
sequences.  The deployed gallery preserves that asymmetry (16,005 Mapillary
references from 15,978 sequences; 2,816 KartaView from 22 sequences).  This is
not an ingestion bug; it is a visual/viewpoint diversity constraint that needs
to be treated explicitly in the next data experiment.

## 7. Geographic coverage and diversity

The fixed 20×20 Moscow grid has only 87/400 occupied cells; 313 are empty.
The coverage classifier deliberately does not call image count “healthy”.  A
dense healthy cell requires >=10 references, >=2 source-scoped sequences,
>=2 providers, >=2 45° heading bins, and an independent held-out query.

| Mutually exclusive grid category | Cells | Meaning |
|---|---:|---|
| Empty | 313 | No deployed reference |
| Insufficient sequence diversity | 1 | References but not enough independent captures |
| Single provider | 57 | Reference coverage without source diversity |
| Dense healthy | 29 | Meets the stronger multi-factor definition |

The non-exclusive flags add useful context: 58 cells are single-provider, one
has concentrated viewpoints, 80 cells support an independent held-out query,
and only 29 are dense healthy.  See the generated
`coverage_diversity_grid.png` and `coverage_grid.json` for every cell.  This
is a high-impact coverage bottleneck despite the 18,821-reference total:
spatial and source/viewpoint diversity, rather than bulk count, determine the
useful VPR positive rate.

## 8. Runtime and deployment baseline

The production-shaped host probe used MegaLoc, the persisted real index,
top-20, weighted medoid, the frozen threshold, and verification disabled.  It
ran 50 deterministic calibration images after loading.

| Measurement | Result |
|---|---:|
| Model and index load | 7.11 s |
| RSS after load | 1,041,696 KiB Python + 631,120 KiB FAISS worker = 1,672,816 KiB combined |
| End-to-end query latency p50 / p90 / p95 | 103.3 / 127.1 / 149.4 ms |
| Embedding p50 / p90 / p95 | 86.5 / 107.5 / 122.8 ms |
| Exact retrieval p50 / p90 / p95 | 14.1 / 15.8 / 17.5 ms |
| Image validation/preparation p50 / p90 / p95 | 31.8 / 58.6 / 59.1 ms |
| Logical index / descriptor storage | 710 MB / 712 MB |
| Reference imagery | 12.42 GB |
| Model caches (Torch / Hugging Face) | 816 MB / 1.83 GB |

The historical K=10 calibration run measured 5.00 s model load and 126 / 176
/ 186 ms end-to-end p50 / p90 / p95.  It is preserved as historical context,
not compared as a controlled performance experiment.

Docker Desktop daemon 29.5.2 and Compose 5.1.4 were available.  A clean
`docker compose up -d --build backend` built the backend and started
`geo-backend`; both `GET /health` and `GET /ready` returned success, with model,
index and metadata all `ready` (database intentionally `not_configured`).  The
Dockerfile currently produces a large 17.5 GB image because it installs the
CUDA-enabled Torch dependency set; that is a deployment-size observation, not
a productionization change.  The check is compatibility-only and did not
change the production bundle.

## 9. Ranked bottlenecks and Phase 2 direction

The evidence puts the first investment in **targeted data/viewpoint coverage
and retrieval robustness**, not a coordinate-estimator swap or a threshold
change.  The 35.29% top-10 retrieval ceiling immediately bounds any
localization/confidence-only effort; full rank shows most nearby positives are
visually outranked.  Coverage has 313 empty and 57 single-provider cells, and
the test metadata is substantially less dense and differently distributed.

Recommended Phase 2 experiments, in order:

1. Build a *small, stratified diagnostic tranche* in the empty/single-provider
   and low-density cells.  Collect or select independent sequences with both
   providers, heading diversity, and date diversity; keep a new untouched
   evaluation partition.  Measure top-10/20/50 oracle recall by cell,
   provider pairing, heading and temporal gap.  Do not rebuild all Moscow data
   before this validates the target strata.
2. Improve retrieval against that untouched, stratified split: establish a
   fixed-depth MegaLoc baseline and compare appropriate VPR/reranking or
   provider/domain-aware retrieval.  Require gains in <=100 m retrieval
   recall before changing localization policy.
3. After retrieval improves, refit and validate a confidence policy on a
   separate calibration partition with a predeclared lower-bound precision
   objective.  Include cluster support, margin, similarity and source/area
   strata; preserve abstention safety.  Resolve the K=10 benchmark versus K=20
   service contract as a separately versioned, calibration-only experiment.

Localization changes belong after those experiments.  A mode-aware aggregator
or geometric verification can be tested for the 33% of retrieved-positive
top-10 cases that select a wrong mode, but a medoid/centroid/top-1 replacement
alone has no measured upside here.

### What must not be concluded from the opened frozen test

- Do not lower or raise the production threshold based on the 507 frozen
  labels, even though its poor answer rate is diagnostically informative.
- Do not claim that K=20, K=50, a source-specific threshold, or a regional
  threshold improves production from this test; none was selected or validated
  there.
- Do not attribute the whole gap to provider mix, quality, temporal mismatch,
  or sparse cells causally.  The metadata shifts are correlated and the
  partitions are geographically disjoint by design.
- Do not report the calibration point estimates (including 50/50) as a future
  precision guarantee; the Wilson intervals show the small-sample uncertainty.

## Final priority table

| Priority | Bottleneck | Evidence | Expected upside | Cost | Recommended next experiment |
|---:|---|---|---|---|---|
| 1 | Visual retrieval plus geographic/viewpoint coverage | <=100 m Recall@10 is 35.29%; full-rank median positive rank 155; 313 empty and 57 single-provider cells | Largest; lifts the hard ceiling before policy | Medium/high, targeted collection and index experiments | Stratified low-density/empty-cell, multi-provider, heading/date-diverse tranche with untouched evaluation |
| 2 | Calibration-to-deployment shift and depth contract | Test has lower density, reversed provider/size mix; benchmark K=10 vs service K=20 | High reliability/generalization upside | Low/medium | Freeze a stratified split and version one K/deployment contract before comparative runs |
| 3 | Cluster support / multimodal wrong modes | 124 top-10 positives rejected; 57/174 retrieved-positive queries choose wrong mode | Moderate after retrieval improves | Medium | Evaluate mode-aware aggregation or verification only on a new calibration split |
| 4 | Uncalibrated confidence policy | Only 19 cutoff-only abstentions; ECE 0.153; 50 accepted samples give 92.9% Wilson lower precision | Modest answer-rate gain; important safety calibration | Low | Prospective stratified confidence calibration with predeclared Wilson-bound target |
| 5 | Coordinate estimator | Medoid, centroid and top-1 each 115/493 <=100 m at top-10 | Low alone | Low | Keep fixed while validating retrieval/data changes; revisit only with improved positive support |
