# Phase 2 — Moscow data, retrieval, and calibration core

Status: completed on 2026-09-01 on `finalize-geosnap`. The `moscow_real_v1`
bundle and reports were treated as immutable historical evidence. All v2 model,
depth, and confidence decisions below used `moscow_real_v2` calibration only;
the v2 test was run once after `configs/moscow_real_v2_frozen.json` was written
and hash-verified.

## 1. Starting diagnosis and decision boundary

The Part 1 diagnosis found that v1 MegaLoc retrieval, not merely the confidence
threshold or coordinate estimator, was the binding constraint. Within 100 m,
v1 calibration Recall@1/5/10/20/50 was
23.53/32.66/35.29/38.54/43.00%, and the median/p75/p90 first-positive rank was
155/1,657/7,769. Of 493 queries, 319 had no positive in the first ten results.
The v1 gallery contained 18,821 rows, and the source manifest had 87 occupied
fixed-grid cells but only 29 dense healthy cells.

Phase 2 therefore kept the model architecture and spatial localizer small,
targeted useful source/viewpoint coverage, measured retrieval first, and did
not reuse either the opened v1 test or its outcomes for selection.

## 2. Targeted acquisition and source availability

`ml.ingestion.moscow_acquisition_plan` combines the exact Moscow polygon,
fixed-grid density/provider/sequence/heading evidence, source availability,
and v1 calibration ranks. The persisted plan intentionally sampled six
empty-but-available cells, six single-provider cells, four low-diversity cells,
and four healthy controls before any v2 retrieval tuning.

Initial 20×20 plan inside or intersecting the exact AOI:

| Cell state | Count |
|---|---:|
| Empty, source imagery observed | 81 |
| Single provider | 54 |
| Low density/diversity | 4 |
| Dense healthy control | 29 |
| No source observed | 40 |
| Outside exact AOI | 192 |

Source discovery observed both providers in 71 cells, Mapillary only in 97,
and no supported source in 40. Availability is recorded separately from actual
gallery coverage; raw image count is never used as a diversity claim.

### Mapillary discovery versus reference expansion

The existing vector-tile stage remains a geographically balanced *sequence
discovery* mechanism. A new bounded stage expands selected sequences through
the official sequence-to-image-ID behavior, hydrates image metadata in Graph
batches, applies the exact AOI, and then selects by spatial displacement,
heading difference, image identity, and local pHash. This keeps whole-sequence
evaluation holdout independent from gallery frame density.

The implementation uses only documented behavior: the official Mapillary SDK
documents entity/sequence image access in its
[Entities API](https://mapillary.github.io/mapillary-python-sdk/docs/mapillary.config.api/mapillary.config.api.entities/)
and sequence discovery in its
[Vector Tiles API](https://mapillary.github.io/mapillary-python-sdk/docs/mapillary.config.api/mapillary.config.api.vector_tiles/).
It does not scrape a private endpoint. The run expanded 200 discovered
sequences, observed and hydrated 21,938 image IDs, selected 740 diverse frames,
made 760 bounded network attempts, and had zero failed sequences or rate-limit
events. The stage is resumable and fingerprints all inputs and diversity
parameters.

### KartaView targeting

The existing discovery/sequence expansion pipeline was reused. Frame selection
now accepts the same target plan and restricts work to planned cells and
sequences before applying 35 m spacing, 45-degree heading diversity, a
five-frame per-sequence cap, and a bounded 400-frame tranche cap. From 30,161
expanded records, 5,488 were in Moscow target cells; 301 frames from 59
sequences were selected. This increased useful independent KartaView sequence
coverage without repeatedly querying the 40 cells where no supported source
was observed.

### Diagnostic tranche and versioned union

The tranche requested 1,041 files, downloaded 861, reused 180 existing files,
and had zero failures. Exact-AOI cleaning retained 1,006 rows. Union with the
immutable v1-origin clean source removed 179 duplicate source identities and
three cross-version pHash-near duplicates, producing 23,654 source rows:
17,143 Mapillary and 6,511 KartaView, from 16,556 and 79 provider-scoped
sequences respectively.

On the same 503 v2 calibration coordinates, the 671 tranche-origin references
that remained in the v2 gallery changed the coordinate oracle as follows:

| Nearest-gallery coverage | Legacy-origin subset | Full v2 | Gain |
|---|---:|---:|---:|
| <=25 m | 35.19% | 37.97% | +2.78 pp |
| <=50 m | 65.41% | 68.59% | +3.18 pp |
| <=100 m | 95.83% | 100.00% | +4.17 pp |

Twenty-seven queries became strictly closer to a reference. At top 50,
MegaLoc retrieved at least one new reference for 394 queries, a new <=100 m
positive for 12, and a new reference as the highest-ranked positive for 12.
The tranche improved the coverage ceiling it targeted, but it did not by itself
establish a material deployable descriptor gain.

## 3. V2 coverage and evaluation protocol

After acquisition, 93 fixed-grid cells are occupied and 30 meet the dense
healthy criterion, versus 87/29 for the v1 source manifest. Of the remaining
inside/intersecting cells, 75 are empty despite observed source candidates and
40 remain unsupported with no source observed. The complete evidence is in
`data/evaluation/moscow_real_v2/coverage/coverage_plan.json`,
`coverage_grid.json`, and `coverage_diversity_grid.png`. These are supported,
weak, and unsupported areas—not an all-Moscow accuracy claim.

The split was created with seed `20260901`, representative provider/region/
resolution balancing, 20 m query spacing, a 250 m calibration/test embargo,
whole provider-sequence holdout, a <=100 m positive requirement, and pHash
Hamming threshold 4.

| Split | Rows | Mapillary / KartaView | Provider-scoped sequences | H3 areas |
|---|---:|---:|---:|---:|
| Gallery | 19,524 | 16,605 / 2,919 | 16,174 | 1,674 |
| Calibration | 503 | 248 / 255 | 254 | 275 |
| Final test | 497 | 215 / 282 | 207 | 229 |

Calibration resolution buckets (`<1600`, `1600–2499`, `>=2500` long side)
are 6/246/251; test buckets are 4/214/279. All pairwise split audits found
zero record/source ID, URL, path, file-hash, sequence, geographic-group, or
pHash-near overlap. Minimum query spacing is 20.51 m, minimum calibration/test
distance is 252.04 m, and every query has a gallery positive no farther than
99.02 m.

Bundle fingerprint and immutable split hashes:

- bundle: `78cccca3f410f318673d99f1b3dcb303b4de678c4799821e0ff575a379248862`;
- gallery: `7de1c9aa37326a388743e8accf411d71c07631dcc2d9e6ae6c83891d3e9e1e76`;
- calibration: `4e92d5e229dae00ea9a7117eb3765f23247d6c77ef82a837f24c9efe6923b5a4`;
- sealed test: `b96cb9bb39908d357c4cf24a0a366c6e1dc1f8f953323d5ce3185cc0a8a94bdf`.

`configs/moscow_real_v2_experiment_protocol.json` persisted the candidate set,
top-K rule, >=5 pp material gate, paired-bootstrap requirement, major-stratum
guard, Wilson objective, and one-test-run limit before the v2 test was opened.

## 4. Calibration retrieval experiments

All configurations used the same 19,524 gallery rows, the same ordered 503
calibration queries, normalized exact `IndexFlatIP`, weighted geographic
medoid, confidence threshold zero during selection, and full-gallery rank
diagnostics. Values in each cell are Recall@1/5/10/20/50.

| Candidate | <=25 m | <=50 m | <=100 m |
|---|---:|---:|---:|
| MegaLoc, single query (baseline) | 13.72 / 18.09 / 18.89 / 19.68 / 20.87% | 20.28 / 23.66 / 25.65 / 26.64 / 29.42% | 23.06 / 27.63 / 31.01 / 32.21 / 37.57% |
| MegaLoc, five 85% crops | 12.72 / 18.09 / 18.89 / 19.68 / 20.68% | 19.09 / 23.86 / 25.45 / 26.64 / 30.02% | 22.66 / 29.03 / 31.01 / 33.60 / 38.37% |
| DINOv2 + SALAD, single query | 12.33 / 17.10 / 17.50 / 18.49 / 21.07% | 19.09 / 24.65 / 25.84 / 28.03 / 31.41% | 23.26 / 30.82 / 33.40 / 37.57 / 42.74% |

| Candidate | <=100 m positive rank median / p75 / p90 | Median positive-minus-best-incorrect margin | End-to-end p50 / p90 / p95 | Peak benchmark RSS |
|---|---:|---:|---:|---:|
| MegaLoc single | 288 / 3,674.5 / 10,546.2 | -0.05890 | 129.57 / 168.09 / 186.80 ms | 2,393 MB |
| MegaLoc five-crop | 372 / 3,170.5 / 10,715.2 | -0.06481 | 314.25 / 378.60 / 412.27 ms | 2,335 MB |
| SALAD single | 124 / 1,244 / 5,283.8 | -0.11466 | 125.62 / 153.56 / 173.06 ms | 1,510 MB |

The exact vector payload is 659,755,008 bytes for every 8,448-dimensional
candidate. Exact index construction took 1.76 s (baseline), 1.72 s (five-crop),
and 1.40 s (SALAD). MegaLoc incrementally reused 17,518 existing descriptors
and computed 2,006, with no failures. SALAD computed all 19,524 descriptors in
about 24 minutes on MPS, also with no failures. Five-crop uses the same gallery
artifact but increases median online latency 2.43×.

The full generated JSON reports contain provider-pair, H3-region, local-density,
heading-gap, and temporal-gap slices. Important baseline examples show why a
single aggregate is insufficient: <=100 m R@20 is 39.22% for KartaView queries
and 25.00% for Mapillary; it is 12.43%, 29.30%, and 65.55% in local-density
buckets 1, 2–4, and >=5. R@20 is 42.33% for heading gap <45 degrees versus
23.98% for >=90 degrees, and 61.42% for temporal gap <30 days versus 24.61%
for >=365 days.

### Material gate and selected retrieval

SALAD gained +2.39 pp R@10 and +5.37 pp R@20. Its paired R@20 bootstrap 95%
interval is [+2.19, +8.55] pp, but two evaluated regional strata regress by
more than five points (worst -10 pp); it therefore fails the preregistered
major-stratum guard. It also remains evaluation-only because the SALAD release
does not state a separate checkpoint license and the repository is GPL-3.0;
the existing licensing assumption was not changed.

Five-crop gained 0.00 pp R@10 and +1.39 pp R@20, with R@20 bootstrap interval
[-0.60, +3.38] pp, a worst regional regression of -10 pp, and much higher
latency. It also fails the material gate. The deployable selection therefore
remains MegaLoc with a single query descriptor. No candidate is described as a
material deployable retrieval improvement.

## 5. Explicit K=10/20/50 contract and localization

The evaluation search depth was 50. Replaying its identical ordered matches
through the unchanged localizer isolates the production-depth effect:

| Production K | Recall at depth | Fraction of R@50 retained | All-query <=100 m | Answer rate | Errors >100 m at threshold 0 |
|---|---:|---:|---:|---:|---:|
| 10 | 31.01% | 82.54% | 9.74% | 10.74% | 5 |
| 20 | 32.21% | 85.71% | 6.36% | 6.56% | 1 |
| 50 | 37.57% | 100.00% | 3.18% | 3.18% | 0 |

The preregistered rule selects the smallest depth retaining at least 90% of
R@50. K=10 and K=20 do not qualify, so K=50 is frozen. The lower answer rate
is reported rather than hidden. No estimator swap was pursued: retrieval did
not produce a materially better deployable candidate, and Part 1 already found
no aggregate medoid/centroid/top-1 advantage.

## 6. Prospective confidence calibration

The predeclared objective was to maximize answer rate subject to the 95% Wilson
lower bound of conditional <=100 m precision being at least 90%. At K=50,
only 16/503 calibration queries pass all non-threshold safety gates. All 16 are
within 100 m, but the Wilson interval is [80.64%, 100%]; no threshold satisfies
the objective. On this threshold-eligible population, Brier score is 0.0801 and
10-bin ECE is 0.2654.

The report therefore marks the objective infeasible. Per the preregistered
policy, the runtime uses threshold `1.0`; the maximum observed calibration
confidence is 0.9834, so this is fail-closed. It is not presented as a 90%
precision success. A larger genuinely independent calibration set or a better
localization evidence gate is required before useful answers can be enabled.

## 7. Frozen artifact and the single v2 test

`configs/moscow_real_v2_frozen.json` was written before test and has SHA-256
`7f915c3806bcc214c928ce4bb3fb6af9467725faa420835d8acf05f5d0be0448`.
It binds the gallery hashes, MegaLoc revisions/checkpoint hash, preprocessing,
8,448-dimensional normalized descriptors, exact `IndexFlatIP`, K=50, single
query aggregation, no reranker, weighted medoid and every spatial safety gate,
verification disabled, Wilson method, threshold 1.0, and the index metadata
hash. Both benchmark and service reject conflicting runtime values.

The untouched 497-query v2 test was then run once with that artifact:

| Distance | R@1 | R@5 | R@10 | R@20 | R@50 | Positive-rank median / p75 / p90 |
|---|---:|---:|---:|---:|---:|---:|
| <=25 m | 6.44% | 8.85% | 9.66% | 10.66% | 12.07% | 661 / 4,200.5 / 11,084.3 |
| <=50 m | 11.67% | 17.30% | 18.71% | 20.52% | 24.55% | 486.5 / 3,896 / 11,736.8 |
| <=100 m | 14.89% | 21.53% | 23.14% | 26.36% | 33.00% | 417 / 3,072 / 8,634.4 |

Product result: answer rate 0/497 (0%), all-query <=25/50/100 m all 0%,
false-confident >100 m count 0, low-confidence rate 53.72%, and
out-of-coverage rate 46.28%. Conditional precision, Wilson interval, and
accepted median/p90/p95 error are undefined because there are no answers. No
post-test tuning or rerun was performed.

Test end-to-end latency p50/p90/p95 is 134.39/182.90/191.88 ms. The benchmark
process peak RSS is 2,170,896,384 bytes. The persisted `index.faiss` is
659,755,053 bytes (702 MiB including metadata sidecars), SHA-256
`2c08bc294556c29ea1eeec96d4b80abefe4b3669382c16c5fae8548274f563e5`.

## 8. Real runtime smoke

The backend loaded the frozen MegaLoc model and 19,524-vector v2 index with
macOS-safe FAISS process isolation. `/ready` returned HTTP 200 and a real
`POST /localize` of an indexed reference returned the same reference at rank
one. Its product status was the expected `low_confidence` under threshold 1.0;
request diagnostics were 99.52 ms embedding, 44.29 ms retrieval, and 204.48 ms
total. `/usr/bin/time -l` measured 2,318,712,832 bytes maximum resident set size
for the smoke process tree. This is a wiring/compatibility smoke, not accuracy
evidence.

A separate Docker-path smoke used the existing 17.5 GB
`geosnap-backend:latest` image with the current code, frozen config, and real
index mounted read-only. `/ready` returned HTTP 200, and a real multipart
`POST /localize` returned HTTP 200 with the indexed reference still at rank
one, the expected `low_confidence` status, and 50 matches. CPU diagnostics were
1,099.52 ms embedding, 122.60 ms retrieval, and 1,418.45 ms total; the
post-request container memory snapshot was 3.487 GiB. The temporary container
was stopped and removed. This validates the container runtime path, not a
fresh-image rebuild; a clean rebuild and public browser QA remain Part 3 work.

## 9. Historical v1 comparison and limitations

V1 and v2 splits differ, so the following is historical context, not a paired
improvement claim. V1 calibration <=100 m R@1/5/10/20/50 was
23.53/32.66/35.29/38.54/43.00% with median positive rank 155; the frozen v1
test answered 11/507 (2.17%) with zero >100 m errors. V2 adds 703 gallery rows,
six occupied fixed-grid cells, one healthy cell, and materially stronger source
sequence diversity, but the deployable MegaLoc candidate does not clear the
retrieval gate and the prospective confidence objective is infeasible. The
single v2 test is harder by its observed retrieval ranks and, under the
predeclared safety policy, answers nothing.

Remaining data limitations are large: 75 cells have observed source imagery
but no retained gallery reference, 40 inside/intersecting cells have no
supported source observed, cross-provider matching is much weaker than
same-sequence-domain matching, and large heading/temporal gaps remain hard.
Neither image count nor a positive coordinate oracle proves visual matchability.

## 10. Exact recommendation for Part 3

Use `configs/moscow_real_v2_frozen.json` and the v2 index for artifact loading,
security, monitoring, and operational QA, but keep public localization
fail-closed and do not claim useful citywide accuracy. Do not substitute SALAD
without checkpoint/combined-application license review. Enabling answers needs
a future v3 calibration/test protocol after additional independent,
viewpoint/season-diverse coverage or a deployable retrieval method clears the
material and major-stratum gates and the Wilson objective becomes feasible.

Part 3 should handle only deployment/artifact packaging, container
optimization, reverse proxy/TLS, rate limiting, monitoring, production tiles,
public-domain configuration, operational security, final browser QA, and merge
readiness.

## Reproduction entry points

The versioned Make targets are:

```bash
make plan-moscow-v2-acquisition
make expand-mapillary-v2
make select-kartaview-v2
make split-moscow-v2
make coverage-moscow-v2
make embed-moscow-v2 MOSCOW_V2_MODEL=megaloc MOSCOW_V2_REUSE_FROM=data/embeddings/moscow/megaloc
make benchmark-moscow-v2-calibration TORCH_DEVICE=mps
make calibrate-moscow-v2-confidence
```

`split-moscow-v2` deliberately refuses to overwrite an existing bundle. The
opened v2 test is immutable; any new tuning cycle must use a future namespace
and split rather than `--replace-existing`.

Large manifests, images, descriptors, indexes, maps, and per-query reports stay
under ignored versioned `data/.../moscow_real_v2` directories. The tracked
protocol, frozen config, pipeline code, tests, and this summary are the review
surface.
