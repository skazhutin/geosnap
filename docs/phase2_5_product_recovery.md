# Phase 2.5 — recovering a useful Moscow operating point

Status: completed on 2026-09-03. The `moscow_real_v1` and `moscow_real_v2`
artifacts remain immutable historical experiments. The opened v2 final test
was used below only as historical context; no v3 choice was made from it. The
v3 candidate was frozen and hash-verified before the test was opened once, and
no post-test tuning was performed.

## 1. Why v2 became fail-closed

V2 combined three individually conservative decisions into a product-useless
policy. MegaLoc remained the production retriever, K=50 was selected by a rule
that preserved at least 90% of retrieval Recall@50, and only 16/503 calibration
queries survived the legacy non-threshold hard gates. Although all 16 were
within 100 m, their 95% Wilson lower bound was only 80.64%. The preregistered
rule required that lower endpoint to be at least 90%, so it set the confidence
threshold to 1.0. The largest observed calibration confidence was 0.9834.

The resulting 0/497 v2 final-test answer rate therefore meant that the frozen
*policy* deliberately accepted nothing. It did not mean that MegaLoc or the
localizer produced no geographically useful coordinates: on that same test,
raw MegaLoc retrieval found a <=100 m reference for 23.14% of queries by rank
10, 26.36% by rank 20, and 33.00% by rank 50. The historical result correctly
exposed an over-brittle operating rule, not zero raw localization ability.

## 2. Independent v3 development protocol

V3 reuses the clean v2 source pool and performs no new acquisition. A
deterministic component assignment created independent policy-development,
calibration, and sealed-test splits while preserving whole-sequence holdout,
source-ID and image-hash disjointness, pHash Hamming-distance protection at
threshold 4, a 250 m geographic embargo between all query splits, query
spacing, and a <=100 m gallery-positive requirement.

| Split | Rows | Mapillary / KartaView | Provider sequences | H3 areas | Resolution `<1600` / `1600–2499` / `>=2500` |
|---|---:|---:|---:|---:|---:|
| Gallery | 20,031 | 14,972 / 5,059 | 14,595 | 1,678 | 149 / 15,062 / 4,820 |
| Development | 602 | 554 / 48 | 525 | 244 | 5 / 541 / 56 |
| Calibration | 603 | 513 / 90 | 504 | 243 | 3 / 503 / 97 |
| Sealed final test | 1,195 | 1,051 / 144 | 1,011 | 499 | 5 / 1,037 / 153 |

Every pairwise split audit has zero ID, source ID, stable source ID, source
URL, image path, file SHA-256, sequence, geographic-group, and pHash-near
overlap. Minimum distances are 269.58 m for development/calibration, 250.38 m
for development/test, and 262.13 m for calibration/test. The bundle fingerprint
is `9825eaa232ae80e33fbc22c933cc2917c965a70753e76c1e5e6b7b6e15dc45b4`.
The exact protocol and immutable manifest hashes are recorded in
`configs/moscow_real_v3_experiment_protocol.json`.

The query pool also contains material variation in the factors that most often
change match difficulty. Counts below use the nearest spatial gallery positive
(not the retrieved result) to characterize each query. Heading buckets are
circular absolute differences; temporal buckets are absolute capture-time
differences; density is the number of gallery references within 100 m.

| Stratum | Development | Calibration | Sealed final test |
|---|---:|---:|---:|
| Same-provider / cross-provider positive | 526 / 76 | 513 / 90 | 1,035 / 160 |
| Heading gap 0–30 / 30–90 / 90–180 / missing | 226 / 95 / 281 / 0 | 244 / 88 / 271 / 0 | 492 / 198 / 487 / 18 |
| Temporal gap <=1 y / 1–3 y / >3 y / missing | 388 / 48 / 19 / 147 | 364 / 41 / 25 / 173 | 732 / 119 / 33 / 311 |
| Gallery density 1 / 2–5 / 6–20 / >20 | 182 / 262 / 155 / 3 | 182 / 289 / 111 / 21 | 338 / 605 / 222 / 30 |
| H3 coarse regions / H3 evaluation areas | 49 / 244 | 46 / 243 | 49 / 499 |

The future test manifest is guarded by a seal receipt. Evaluation code refuses
to open it unless the frozen configuration exists, its detached SHA-256 agrees,
all bound artifact hashes verify, and no earlier opening receipt exists.

## 3. Product objective and K-depth analysis

The v2 rule “choose the smallest K retaining at least 90% of Recall@50” was
removed. V3 evaluates K=5/10/15/20/30/50 through the same downstream policy and
selects on raw localization plus useful abstained-product behavior. Every row
reports retrieval within 50 m and 100 m, raw <=25/50/100 m localization,
catastrophic errors, geographic-mode structure, and product accuracy/answer
rate. Wilson intervals remain reported evidence rather than a kill-switch.

On development, the unchanged MegaLoc K=50 legacy medoid localizes 17.77% of
all queries within 100 m. Replacing retrieval, depth, and aggregation with the
leading SAGE K=30 density-aware policy raises raw all-query <=100 m to 30.07%,
a +12.29 point change. This passes the material-improvement gate independently
of retrieval recall. The SAGE K=20 and K=30 density-aware variants happen to
tie at 30.07% raw accuracy, while K=30 provides the highest cross-validated
answer rate at the primary precision target. K=50 is not assumed to be safer
or more accurate merely because it retrieves more positives.

The generated development grid records, for every K and aggregation, the mean
number of geographic modes, winning mass, correct-mode rank, competing-mode
separation, retrieved-but-lost positives, singleton false modes, and multimodal
cases. This makes the decision auditable without reopening images or touching
the test split.

For the selected SAGE density-aware family, raw <=100 m accuracy was 30.07% at
every evaluated depth, while useful answer coverage peaked at K=30 and then
fell at K=50. “Best answers” below is the strongest primary-feasible confidence
method for that K; K=10's handwritten point is shown, but it was not selected
over the calibrated model family.

| K | Raw <=100 m | Best answers / precision | >100 m / >500 m | Mean modes | Correct retrieved but lost | Singleton false / multimodal |
|---:|---:|---:|---:|---:|---:|---:|
| 5 | 30.07% | 10.63% / 96.88% | 2 / 0 | 4.29 | 31 | 378 / 358 |
| 10 | 30.07% | 11.30% / 98.53% | 1 / 0 | 8.41 | 51 | 360 / 127 |
| 15 | 30.07% | 16.94% / 94.12% | 6 / 1 | 12.53 | 64 | 352 / 60 |
| 20 | 30.07% | 18.27% / 94.55% | 6 / 1 | 16.67 | 77 | 347 / 35 |
| 30 | 30.07% | 19.10% / 93.04% | 8 / 1 | 24.76 | 97 | 337 / 22 |
| 50 | 30.07% | 16.78% / 93.07% | 7 / 1 | 40.80 | 109 | 325 / 10 |

At K=30 the correct cluster ranks 1/2/3/4/5 in 186/15/7/7/5 cases. The extra
depth finds more possible modes, but its value depends on whether aggregation
and confidence can separate them; this is why K=30, not maximum recall depth,
is frozen.

## 4. Geographic aggregation experiments

Four deterministic aggregators were compared:

1. the legacy compact-cluster weighted medoid;
2. similarity plus rank-decayed geographic voting;
3. sequence-deduplicated voting, where a provider-scoped source sequence
   contributes only its strongest evidence;
4. density-aware mode voting, which adds independent-sequence and provider
   support, geographic compactness, margin against the second mode, and a
   local-gallery-density correction.

All retain the weighted medoid as the coordinate estimator. The experiment is
therefore a change in which geographic mode wins and how independent evidence
is counted, not an unsupported medoid-to-centroid swap. Near-duplicate frames
from one sequence cannot accumulate the same support as independent sequences.
The selected development pipeline uses density-aware mode voting because it
substantially improves raw and abstained product performance in the dominant
multimodal failure cases.

No SIFT pipeline was resurrected. SelaVPR++'s official rerank branch was tested
as a bounded two-stage retrieval experiment: a 512-D binary branch retrieves
100 candidates and the independent 2,048-D floating branch reranks that pool
before the shared K grid and geographic aggregation. This is an official
global-descriptor reranker, not a dense local matcher; the distinction is kept
explicit.

## 5. Quantitative hard-gate audit

The legacy similarity, cluster-mass, mass-margin, and candidate-count gates
were replayed separately against the calibrated confidence score. For the SAGE
K=20 calibration probe, the results were:

| Gate | Rejected | Correct / incorrect rejected | Sole rejections | Answer-rate loss | Precision gain if used alone |
|---|---:|---:|---:|---:|---:|
| Out-of-coverage similarity | 74 | 2 / 72 | 0 | 12.27 pp | +4.10 pp |
| Minimum cluster mass | 464 | 70 / 394 | 0 | 76.95 pp | +56.48 pp |
| Minimum mass margin | 259 | 25 / 234 | 0 | 42.95 pp | +16.83 pp |
| Minimum candidates | 430 | 86 / 344 | 17 | 71.31 pp | +29.84 pp |
| Calibrated confidence threshold | 544 | 137 / 407 | 45 | 90.22 pp | +62.91 pp |

The apparent precision gains from the broad legacy gates come with enormous
answer-rate losses and largely overlap the learned confidence rejection. Their
evidence is already represented by similarity, mode margin, support count,
provider/sequence diversity, rank, spread, and density features. The selected
candidate therefore disables these legacy numeric gates and uses the calibrated
confidence policy as the product boundary. Exact-AOI, source, artifact, model,
and index compatibility checks remain safety invariants.

## 6. Confidence-model experiments

The label is `localization_error <= 100m`. Inputs are fourteen interpretable
retrieval/localization features: top similarity and top-1/top-2 margin, winning
and second mode scores, geographic margin and separation, independent sequence
and provider support, winning count and spread, local gallery density, best
support rank, top-1 agreement, and cross-provider evidence. Pixels are never
used.

The handwritten v2 score, standardized L2 logistic regression, and logistic
regression plus isotonic calibration were compared. Model selection uses
stratified five-fold group cross-validation on development geographic groups;
operating-point curves use out-of-fold scores. The final coefficients are fit
on development only. Calibration labels select only the threshold and are
cryptographically recorded as separate from training IDs.

The primary rule maximizes answer rate subject to empirical conditional <=100 m
precision >=90%, <=1% accepted >500 m errors, at least 40 answers, provider
precision >=80% and regional precision >=75% whenever an accepted stratum has
at least 20 examples. The separately labeled fallback uses an 85% precision
floor. Wilson and bootstrap intervals are reported, but a Wilson lower endpoint
below 90% no longer forces threshold 1.0.

The development-selected SAGE K=30 density-aware logistic model yields, on the
independent calibration split, 70/603 answers (11.61%; bootstrap 95% interval
9.12–14.26%) with 67/70 <=100 m (95.71%; Wilson 95% interval 88.14–98.53%),
three >100 m errors, zero >500 m errors, and accepted median/p90/p95 error
15.68/64.19/74.48 m. Its threshold is `0.9349250249145314`. KartaView is
30/30 and Mapillary 37/40 within 100 m; no regional stratum reaches the n=20
hard-veto denominator.

This produces 70 calibration answers—more than four times v2's 16 eligible
cases and above the mandatory minimum of 40, but below the aspirational target
of 100. Lower thresholds can answer more queries, but violate the predeclared
precision or catastrophic-error constraints; independence was not manufactured
with adjacent frames.

## 7. SALAD statistical audit

The v2 stratum veto was recomputed with exact denominators and paired bootstrap
uncertainty. Its headline `-10 pp` regional regression was 1/10 for MegaLoc
versus 0/10 for SALAD, with an uncertainty interval spanning roughly -30 to 0
points. No stratum with n>=30 showed a statistically credible catastrophic
regression. The earlier rejection was faithful to its preregistered literal
rule, but that rule treated small strata too aggressively.

On v3 development SALAD improves <=100 m R@20 over MegaLoc, yet its best
cross-validated primary product point answers only 47/602 (7.81%) at 43/47
within 100 m (91.49%). Higher retrieval recall does not translate into the
best product operating point. Small strata remain reported as uncertain rather
than becoming automatic vetoes.

## 8. SALAD licensing audit

The [official SALAD repository](https://github.com/serizba/salad) is GPL-3.0.
The evaluated checkpoint comes from its
[official v1.0.0 release](https://github.com/serizba/salad/releases/tag/v1.0.0),
which states no separate checkpoint terms. Dependencies were also recorded,
but their permissiveness does not remove the repository/checkpoint ambiguity.
This is not legal advice: production-service compliance implications remain
unclear, so SALAD stays evaluation-only.

Technically, SALAD's gain is concentrated in improved ranking depth rather than
a correspondingly strong confidence-separated product set. It remains useful
evidence that GeM-style global descriptors and better ranking can help, but it
is not the deployable selection.

## 9. Additional deployable retriever research

SAGE, [SelaVPR++](https://github.com/Lu-Feng/SelaVPRplusplus), and
[CricaVPR](https://github.com/Lu-Feng/CricaVPR) were audited at exact repository
commits. Code and checkpoint provenance were considered separately.

| Candidate | Revision | Checkpoint SHA-256 | Code/checkpoint conclusion |
|---|---|---|---|
| SAGE ViT-B, no cross-image encoder | `c7d6241c4885526d99d6c78c158024fc2a37097c` | `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e` | Repository and Hugging Face model card state MIT; production-eligible technical conclusion |
| SelaVPR++ base | `56bd921cbd3d53e9c5f91d0aafff147f95fb362a` | `b048490dbd1c27dee67fce6faaec7bec267d19a044c85877af94b8588e596a62` | MIT repository; official release asset has no contradictory separate terms; technical audit, not legal certainty |
| SelaVPR++ rerank | same | `da31138202b9a746916588ecd97499a56bae304e61444a50d6f34761377cdcbf` | Same conclusion; official 512-D binary -> 2,048-D float two-stage contract |
| CricaVPR | `f53e941d34a559ca8432960bc2c29ef22f940c97` | `e3102d28e07df60b9f82003e96b44feb3debe1827f7d90143180174c3ad8e046` | MIT repository and official release asset, but not deployable under GeoSnap's independent-descriptor index contract |

Each integrated SelaVPR++ model first passed one-image inference with finite,
unit-normalized output. Standard SelaVPR++ uses 2,048 dimensions. Its official
rerank artifact persists a 2,560-D pair but splits and renormalizes 512-D binary
and 2,048-D float branches for search.

CricaVPR's one-image smoke produced a finite normalized 10,752-D descriptor,
but the required batch-invariance check failed: cosine similarity for the same
image's descriptor changed to 0.9573/0.9604 when embedded alongside unrelated
images, with maximum component changes 0.01492/0.01360. This follows the
official Transformer encoder's cross-image batch behavior and makes separately
built gallery/query descriptors dependent on arbitrary batch neighbors. Full
gallery extraction was therefore rejected before spending resources. EDTformer
was not attempted because SelaVPR++ integrated cleanly, following the stated
priority rule.

## 10. Candidate comparison and operating-point curves

The following development results use the same 602 queries, gallery, K grid,
aggregators, and out-of-fold confidence procedure. The primary point is not
selected from recall alone.

| Retriever / best development product pipeline | <=100 m R@1/5/10/20/50 | Median positive rank | Raw all-query <=100 m | Answer rate / precision at >=90% | Answer rate / precision at >=85% | Accepted >100 m / >500 m |
|---|---:|---:|---:|---:|---:|---:|
| MegaLoc, K20 density-aware logistic+isotonic | 21.93/28.07/29.40/31.89/35.38% | 421 | 22.09% | 10.47% / 95.24% | 10.47% / 95.24% | 3 / 0 |
| SALAD, K10 rank-weighted logistic | 21.93/31.56/35.38/40.53/45.35% | 108 | 21.93% | 7.81% / 91.49% | 7.81% / 91.49% | 4 / 0 |
| SAGE, K30 density-aware logistic | 30.23/35.55/39.04/43.52/49.00% | 60.5 | 30.07% | 19.10% / 93.04% | 19.10% / 93.04% | 8 / 1 |
| SelaVPR++ base, K30 sequence-deduplicated logistic | 29.07/35.22/38.70/42.52/46.18% | 80.5 | 29.40% | 19.10% / 92.17% | 19.10% / 92.17% | 9 / 1 |
| SelaVPR++ official two-stage, K5 legacy medoid logistic+isotonic | 29.07/35.38/37.87/40.86/45.51% | 121 | 27.91% | 16.94% / 92.16% | 16.94% / 92.16% | 8 / 1 |

SAGE's independently selected calibration point answers 11.61% at 95.71%
conditional <=100 m precision. Standard SelaVPR++ answers 10.78% at 96.92%
(63/65), with two >100 m and zero >500 m errors. Thus standard SelaVPR++ is
slightly more precise at its discrete threshold but has lower useful coverage;
it is not automatically selected for its smaller descriptor or latency.

The official SelaVPR++ two-stage branch did not improve product selection: its
development answer rate was 16.94%, and on calibration its development-frozen
model had no feasible >=90% or >=85% point with at least 40 answers and <=1%
accepted catastrophic errors (only four answers at its perfect-precision end).
This directly tests the requested retrieve-then-rerank hypothesis and rejects
it on product evidence rather than Recall alone.

The report also includes the explicitly labeled >=85% curves. They are not
silently substituted for the primary policy, because the >=90% point is already
feasible for the leading deployable candidates.

Provider and major-region R@20 comparisons use exact denominators and paired
bootstrap 95% intervals. Counts are baseline→candidate successes/total; values
in brackets are the paired difference interval in percentage points.

| Candidate | KartaView (n=48) | Mapillary (n=554) |
|---|---:|---:|
| SALAD | 40→42, +4.17 [0.00, 10.42] | 152→202, +9.03 [5.95, 12.27] |
| SAGE | 40→42, +4.17 [0.00, 10.42] | 152→220, +12.27 [9.03, 15.52] |
| SelaVPR++ base | 40→42, +4.17 [-4.17, 12.50] | 152→214, +11.19 [8.12, 14.26] |
| SelaVPR++ rerank | 40→42, +4.17 [-4.17, 12.50] | 152→204, +9.39 [6.50, 12.27] |

| Candidate | H3 `8611aa607…` (n=32) | H3 `8611aa62f…` (n=33) | H3 `8611aa787…` (n=52) |
|---|---:|---:|---:|
| SALAD | 15→18, +9.38 [-3.13, 21.88] | 16→14, -6.06 [-18.18, 6.06] | 23→29, +11.54 [1.92, 23.08] |
| SAGE | 15→17, +6.25 [0.00, 15.63] | 16→19, +9.09 [-6.06, 24.24] | 23→30, +13.46 [3.85, 25.00] |
| SelaVPR++ base | 15→17, +6.25 [-6.25, 18.75] | 16→18, +6.06 [-9.09, 21.21] | 23→30, +13.46 [5.77, 23.08] |
| SelaVPR++ rerank | 15→15, 0.00 [-9.38, 9.38] | 16→18, +6.06 [-9.09, 21.21] | 23→28, +9.62 [1.92, 17.31] |

All four have zero hard vetoes. Small regions are classified as uncertain,
never automatic vetoes; their exact denominators and intervals remain in the
generated stratum audit reports.

## 11. Selected v3 configuration

The frozen configuration is `configs/moscow_real_v3_frozen.json`, SHA-256
`ddcfa0a66dbf0d4bbf31a292be38b07ceb6e93b60a983aaa46cd23679cafd0e2`.
It binds all selection evidence and artifacts before test opening:

- SAGE ViT-B without cross-image encoder, exact pinned repository/checkpoint;
- normalized 8,448-D descriptors and exact `faiss.IndexFlatIP`;
- single-image query descriptor, K=30, no reranker;
- density-aware geographic mode voting and weighted-medoid coordinate;
- standardized L2 logistic confidence model fit on development only;
- threshold `0.9349250249145314`, selected on calibration only;
- legacy numeric similarity/mass/margin/count gates disabled; exact AOI,
  source, leakage, artifact fingerprint, and model/index compatibility retained;
- `ok` and `low_confidence` enabled; an `approximate` tier was evaluated but
  not enabled because it did not add defensible utility.

SAGE passes the material gate on development: unchanged MegaLoc K50 legacy raw
all-query <=100 m is 17.77%, versus 30.07% for SAGE K30 density-aware, a +12.29
point paired improvement (bootstrap 95% interval +9.47 to +15.28 points).

## 12. Frozen-test results

The detached configuration hash and every bound artifact were verified before
opening the 1,195-query test once. The completed receipt is SHA-256
`186f49d60c0bb1759bf8f0e300ec457680530e180e3468df026ebbf30a36b26e`
and records `completed_no_post_test_tuning`.

| Metric | Unchanged MegaLoc v2 policy | Frozen SAGE candidate |
|---|---:|---:|
| <=100 m R@1/5/10/20/50 | 16.65/22.85/25.36/28.79/33.89% | 26.36/36.74/40.42/43.18/48.70% |
| <=50 m R@1/5/10/20/50 | 13.31/18.08/19.67/21.92/24.85% | 18.74/26.78/29.54/31.55/34.48% |
| Positive-rank median / p75 / p90 (<=100 m) | 391 / 3,221 / 9,594.2 | 61 / 1,218.5 / 6,081.4 |
| Raw all-query <=25/50/100 m | 8.45/11.38/13.05% | 11.63/19.08/26.61% |
| Raw median / p90 error | 13,824.57 / 29,119.96 m | 7,374.62 / 27,880.21 m |
| Answer rate | 0/1,195 (0%) | 92/1,195 (7.70%; bootstrap 95% 6.28–9.29%) |
| Conditional <=25/50/100 m | undefined | 67.39/82.61/94.57% |
| Conditional <=100 m Wilson 95% | undefined | 87.90–97.66% |
| Accepted >100 m / >500 m | 0 / 0 | 5 / 3 |
| Accepted median / p90 / p95 error | undefined | 18.21 / 68.09 / 90.36 m |
| `ok` / `approximate` / `low_confidence` / `out_of_coverage` | 0 / 0 / 538 / 651 | 92 / 0 / 1,103 / 0 |

The candidate is a large, honest recovery from v2's intentional zero-answer
policy and improves raw all-query <=100 m by 13.56 points on the untouched
test. It does **not** meet the preferred success target: answer rate is below
10%, and 3/92 accepted answers (3.26%) are >500 m, above the <=1% target.
Precision and median accepted error do meet their targets. These results were
not used to change the frozen system; another model/policy iteration requires a
new sealed namespace.

## 13. Runtime and storage

The one-shot final-test candidate measured 138.74/169.18/188.48 ms
p50/p90/p95 end to end. Peak process RSS was 2,308,997,120 bytes; because the
baseline and candidate ran in one isolated process this is a conservative
shared-process peak. The exact 20,031-vector FAISS payload is 676,887,552 bytes;
the complete index plus verified sidecars is 723 MiB.

Development end-to-end p50/p90/p95 was 141.74/171.73/184.91 ms for MegaLoc,
150.23/206.96/237.65 ms for SALAD, 127.88/155.61/170.11 ms for SAGE,
102.77/121.93/132.32 ms for 2,048-D SelaVPR++ base, and
111.11/131.91/143.26 ms for its 2,560-D persisted rerank pair. Their vector
payloads are 676,887,552 bytes for each 8,448-D model, 164,093,952 bytes for
SelaVPR++ base, and 205,117,440 bytes for the rerank pair. Measured development
peak RSS values were respectively 2,443,608,064; 1,321,992,192;
2,086,174,720; 2,055,356,416; and 2,239,021,056 bytes.

## 14. Remaining limitations

V3 still uses a geographically uneven source pool, exact search, and only two
supported imagery providers. Provider, viewpoint, season, lighting, weather,
and long temporal gaps remain difficult. A <=100 m result is a city-scale
visual match, not parcel-level truth. Calibration provides only 70 accepted
SAGE examples at the primary point, so its Wilson interval remains wide even
though it no longer acts as an absolute kill-switch. Regional accepted
denominators are mostly too small for hard conclusions and are labeled
uncertain. No legal opinion is offered for third-party model deployment.

The untouched test shows that the calibration constraint did not fully control
catastrophic accepted errors: zero were observed among 70 calibration answers,
but three occurred among 92 final answers. The selected artifact remains the
honest frozen result, not a claim that its current threshold is ready for an
unqualified citywide launch.

## 15. Engineering validation

`make test` passed: Ruff, 382 pytest tests (one skipped), seven frontend tests,
and the frontend production build. This includes the K contract, deterministic
aggregation, sequence-deduplicated support, confidence feature construction,
development/calibration separation, calibration-only threshold fitting, frozen
runtime, test seal, model/index compatibility, status boundaries, and
baseline/candidate reproducibility.

The real frozen SAGE index host smoke passed: `/ready` returned HTTP 200 and a
real indexed-reference multipart `/localize` returned HTTP 200, status `ok`,
confidence 0.999999953, and 30 matches. The first and final warm requests took
240.32 and 155.24 ms. The existing 17.5 GB Docker
image could not be reused because it predates the newly declared `prettytable`
dependency; startup failed closed with `ModelLoadError`. The dependency is in
`pyproject.toml`/`uv.lock`, so a clean image build would install it, but the
17.5 GB rebuild was intentionally deferred as instructed. No running test
container was left behind.

## 16. Exact Part 3 recommendation

After this evidence is frozen and verified, Part 3 should be limited to:

- deployment artifact distribution;
- container slimming;
- reverse proxy;
- TLS;
- rate limiting;
- monitoring;
- production tiles;
- privacy and security hardening;
- final browser QA;
- merge readiness.

No further model, threshold, K, aggregation, gallery, or confidence tuning may
use the opened v3 test. Any future model change requires a new sealed namespace.
