# Отчёт о реальных данных Москвы

Актуально на 2026-09-03. Этот документ сохраняет acquisition/data evidence для
исторического `moscow_real_v2`; последний и финальный pre-production evaluation
namespace — `moscow_real_v4`. Полный Phase 2
experiment record находится в
[phase2_quality_improvement.md](phase2_quality_improvement.md), model/test
summary — в [evaluation_report.md](evaluation_report.md), v3 recovery — в
[phase2_5_product_recovery.md](phase2_5_product_recovery.md), а финальный
contract — в [final_localization_core.md](final_localization_core.md).
V1/V2/V3 сохранены как immutable historical bundles.

## Production data contract

Deployable gallery строится только из физически сохранённых Mapillary и
KartaView изображений. Exact-AOI gate использует OSM relation `102269`,
`MultiPolygon` из 10 компонентов, bounds
`36.8031012,55.1421745,37.9674277,56.0212238`, SHA-256
`33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.

MSLS, Commons и другие training/benchmark datasets исключены из production
manifest, embeddings и FAISS. Каждая retained reference сохраняет stable ID,
provider/source image ID, provider-scoped sequence, coordinates, timestamps,
heading, local image/hash, license, attribution и source/profile URLs.

## Final leakage-resistant v4 bundle

V4 использует существующий clean source pool без acquisition. Все 2 991 unique
provider sequences, использованные как v1/v2/v3 development/calibration/test
queries, исключены из новой query eligibility. Это query exclusion: historical
frames остаются допустимыми gallery references только там, где split leakage
rules это разрешают, чтобы не разрушать reference coverage.

| Split | Rows | Mapillary | KartaView | Provider sequences | Geographic components | H3 areas |
|---|---:|---:|---:|---:|---:|---:|
| Gallery | 20 487 | 14 099 | 6 388 | 13 698 | — | 1 674 |
| Development | 751 | 748 | 3 | 729 | 229 | 290 |
| Calibration | 750 | 747 | 3 | 734 | 230 | 287 |
| Sealed test | 1 499 | 1 484 | 15 | 1 474 | 447 | 574 |

Resolution buckets `<1600 / 1600–2499 / >=2500` are 1/735/15,
2/734/14 and 4/1 463/32 for development, calibration and test. Local gallery
density within 100 m has median 2 for all three query splits; test buckets
`1 / 2–5 / 6–20 / >20` contain 570/734/172/23 queries.

All six split pairs have zero overlap in record/source/stable-source ID,
image path, source URL, exact file SHA-256, provider sequence, geographic group
and pHash-near pairs. Development/calibration, development/test and calibration/
test minimum distances are 251,54/250,97/250,32 м with zero embargo violations.
Every query has a gallery positive <=100 м. KartaView query representation is
only 3/3/15, so provider-specific conclusions are explicitly uncertain.

Fingerprints:

- bundle: `0eb03372570a8cd4c703aa07789065717027ba7223024d8e4361d74faa1b489f`;
- gallery: `ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da`;
- development: `17d0f339d3a34d4826a2d7cf337861a566a48b23d76fb1353919d9733fbcf64d`;
- calibration: `e2e088d5fecabc8b95c29fffcf91d0248355fafc0d0cc312a0100ab7eee4e64d`;
- sealed test: `332567068c27cd58eb7951823c3102d05cf7263abb152ffb49db4e280be0e34f`.

The test was sealed before development and calibration work, opened exactly
once only after both finalists were frozen, and is now permanently burned.

## Targeted v2 acquisition

Acquisition plan был построен из exact AOI, empty/single-provider/low-density
ячеек, sequence/heading diversity, source availability и v1 calibration ranks.
Diagnostic tranche включил planned failures и healthy controls, а не случайный
bulk download.

| Stage | Rows / result | Notes |
|---|---:|---|
| Immutable v1-origin clean source | 22 830 | 16 504 Mapillary + 6 326 KartaView |
| Mapillary sequence expansion | 740 frames | 200 sequences; 21 938 IDs hydrated; zero failures |
| Targeted KartaView selection | 301 frames | 59 sequences; 35 m/45-degree diversity |
| Tranche download | 1 041 requested | 861 downloaded + 180 reused; zero failures |
| Tranche exact-AOI clean | 1 006 | bounded validation/quality/dedup pipeline |
| Versioned union | 23 657 | 179 repeated source identities removed before union |
| Cross-version pHash clean | 23 654 | 3 near duplicates removed |

Final v2 source manifest: 17 143 Mapillary and 6 511 KartaView rows, from
16 556 and 79 provider-scoped sequences. Multiple diverse Mapillary frames may
now come from a selected sequence; sequence discovery, gallery frame expansion,
visual deduplication, and evaluation sequence holdout are separate stages.

The Mapillary stage is resumable, fingerprints its config and inputs, uses the
official sequence image-ID behavior plus Graph metadata batching, and caps
sequences/frames. It does not retain every adjacent frame. KartaView reuses the
existing resumable sequence expansion and now accepts the same target-cell and
target-sequence plan.

## Measured source availability and coverage

| Fixed-grid state after v2 | Count |
|---|---:|
| Occupied | 93 / 400 |
| Dense healthy | 30 |
| Single provider | 60 |
| Low density/diversity | 3 |
| Empty with source imagery observed | 75 |
| Unsupported/no source observed | 40 |
| Outside exact AOI | 192 |

Observed discovery availability is both providers in 71 cells, Mapillary-only
in 97, and no supported source in 40. An available candidate is not counted as
gallery coverage until it passes download, exact AOI, validation and diversity
selection. The generated coverage artifacts are:

- `data/evaluation/moscow_real_v2/coverage/coverage_plan.json`;
- `data/evaluation/moscow_real_v2/coverage/coverage_grid.json`;
- `data/evaluation/moscow_real_v2/coverage/coverage_diversity_grid.png`.

Compared on identical v2 calibration coordinates, the full v2 gallery versus
its legacy-origin subset improves nearest-reference availability by +2,78,
+3,18 and +4,17 pp at <=25/50/100 м; 27 queries get a strictly closer
reference. This is a coordinate coverage ceiling, not VPR accuracy.

## Leakage-resistant v2 bundle

`make split-moscow-v2` uses seed `20260901`, representative provider/region/
resolution balancing, whole provider-sequence holdout, positive <=100 м,
minimum query spacing 20 м, calibration/test embargo 250 м and pHash threshold
4.

| Split | Rows | Mapillary | KartaView | Provider-scoped sequences | H3 areas |
|---|---:|---:|---:|---:|---:|
| Gallery | 19 524 | 16 605 | 2 919 | 16 174 | 1 674 |
| Calibration | 503 | 248 | 255 | 254 | 275 |
| Final test | 497 | 215 | 282 | 207 | 229 |

Calibration/test resolution buckets `<1600 / 1600–2499 / >=2500` are
6/246/251 and 4/214/279. Audit found zero pairwise overlaps in record/source
ID, URL, path, file SHA-256, provider sequence, geographic group and pHash-near
pairs. Query union has zero spacing, exact-hash or pHash violations. Minimum
query distance is 20,51 м, calibration/test distance 252,04 м, maximum nearest
positive distance 99,02 м.

Fingerprints:

- bundle: `78cccca3f410f318673d99f1b3dcb303b4de678c4799821e0ff575a379248862`;
- gallery: `7de1c9aa37326a388743e8accf411d71c07631dcc2d9e6ae6c83891d3e9e1e76`;
- calibration: `4e92d5e229dae00ea9a7117eb3765f23247d6c77ef82a837f24c9efe6923b5a4`;
- test: `b96cb9bb39908d357c4cf24a0a366c6e1dc1f8f953323d5ce3185cc0a8a94bdf`.

## Historical v2 frozen data/model artifact

Calibration did not find a deployable retrieval candidate that cleared the
material plus major-stratum gate. The v2 artifact therefore uses MegaLoc on
the improved v2 gallery, single query aggregation, exact `IndexFlatIP`, K=50,
weighted medoid and verification off. Prospective Wilson calibration was
infeasible, so `configs/moscow_real_v2_frozen.json` applies fail-closed threshold
1.0. The single v2 test answers 0/497; this is not a launch-ready data/model
result.

Local artifacts:

- `data/evaluation/generated/moscow_real_v2/` — acquisition/union/cleaning;
- `data/evaluation/moscow_real_v2/` — split, coverage and reports;
- `data/embeddings/moscow_real_v2/megaloc/` — 19 524 descriptors;
- `data/indexes/moscow_real_v2/megaloc/` — exact index and sidecars;
- `configs/moscow_real_v2_experiment_protocol.json` — pre-test protocol;
- `configs/moscow_real_v2_frozen.json` — service/benchmark runtime contract.

Large generated evidence remains ignored; tracked code, protocol, frozen
config and reports document its hashes and reproduction commands.

## Reproduction

```bash
make plan-moscow-v2-acquisition
make expand-mapillary-v2
make select-kartaview-v2
make split-moscow-v2
make coverage-moscow-v2
make embed-moscow-v2 \
  MOSCOW_V2_MODEL=megaloc \
  MOSCOW_V2_REUSE_FROM=data/embeddings/moscow/megaloc
make benchmark-moscow-v2-calibration TORCH_DEVICE=mps
make smoke-moscow-v2 TORCH_DEVICE=mps
```

V2 final test уже открыт один раз; reproduction команды не должны повторно
использовать его как held-out evidence. V1/V2/V3/V4 tests теперь permanently
opened и не предназначены для tuning.

## Immutable historical v1

V1 source cleaning produced 22 830 rows (16 504 Mapillary, 6 326 KartaView),
87 occupied cells and 29 dense healthy cells. Its bundle contains 18 821
gallery, 493 calibration and 507 test rows, fingerprint
`a8e24a77b4b4fb1a22a1a9f3198d050eca798d33565b2be8ec1aad3830e09a83`.
V1 reports and manifests were not overwritten. Because v1/v2 splits differ,
their retrieval/product numbers are historical context, not paired evidence.

## Provenance and claims not supported

API/frontend must continue to show source/license/attribution for every
reference and OpenStreetMap attribution for the map. Details are in
[licenses.md](licenses.md).

These data do not establish full-Moscow, district-complete, season-complete,
heading-complete or cross-provider-complete localization. They do not authorize
Commons/MSLS in production, make SALAD deployable, or calibrate
`uncertainty_radius_m`. The final product claim must report both 8.47% answer
rate and 96.85% conditional <=100 m precision; neither number is city-wide
accuracy.
