# Real Moscow model selection and frozen test

Актуально на 2026-08-31. Это единственный tracked summary, который описывает
выбор deployable retriever. Он использует только исходные Mapillary/KartaView
street-view изображения из exact-AOI Moscow gallery; Commons и MSLS не
участвовали в model selection, confidence calibration, production embedding
или FAISS index.

> Результат честно ограничен частичным покрытием: это не city-wide accuracy
> claim и не основание включать локализацию для любого снимка Москвы.

## Protocol and leakage boundary

- Final clean source gallery: 22 830 Mapillary/KartaView references.
- Leakage-resistant bundle: 18 821 gallery rows, 493 calibration queries and
  507 frozen held-out test queries.
- Gallery and query provider sequences are disjoint. The audit also found zero
  cross-split overlap in record ID, source image ID, source URL, resolved path,
  declared/computed SHA-256 and pHash-near duplicates (Hamming threshold 4).
- Every query has a gallery positive within 100 m. Query spacing is at least
  20 m; calibration/test use a 100 m geographic embargo.
- Both models used pinned official checkpoints, MPS, batch size 8, top-K 10,
  weighted geographic medoid and isolated normalized exact
  `faiss.IndexFlatIP`. All accuracy figures use **all queries** as the
  denominator, so abstentions count as failures.
- Candidate model selection used calibration only at threshold `0.0`. The
  test split was opened once after model, estimator and threshold were frozen.

The generated local JSON/Markdown evidence is intentionally ignored because it
contains generated paths and per-query records:
`data/evaluation/moscow_real_v1/reports/`.

## Calibration: raw retrieval and localization

| Model | R@1 / R@5 / R@10 | <=25 / <=50 / <=100 m | Answer rate | Median / P90 answered error | Query / end-to-end median |
|---|---:|---:|---:|---:|---:|
| MegaLoc | 23.53 / 32.66 / 35.29% | 9.53 / 11.36 / 12.17% | 14.00% | 13.34 / 152.67 m | 83.53 / 126.47 ms |
| DINOv2 + SALAD | 29.61 / 36.31 / 39.55% | 10.95 / 15.21 / 17.04% | 23.12% | 26.44 / 2,296.98 m | 78.33 / 120.89 ms |

SALAD has higher unthresholded recall and all-query localization on this
calibration pass, but its accepted-error tail is much worse and it produces 30
unthresholded false-confident answers beyond 100 m versus MegaLoc's 9. Raw
metrics were not used alone to select a deployment operating point.

## Safety-first confidence calibration and decision

The fixed policy first minimizes `status=ok && error > 100 m`, then maximizes
unconditional <=100 m accuracy, then answer rate, and finally prefers the
higher threshold. It never repurposes the held-out test for tuning.

| Candidate | Threshold | False-confident >100 m | Answered / total | All-query <=25 / <=50 / <=100 m |
|---|---:|---:|---:|---:|
| MegaLoc | `0.5548002022369389` | 0 | 50 / 493 | 8.52 / 9.74 / 10.14% |
| DINOv2 + SALAD | `0.7803838356776883` | 0 | 44 / 493 | 6.69 / 8.32 / 8.92% |

**Frozen deployment selection:** `RETRIEVER=megaloc`,
`COORDINATE_ESTIMATOR=weighted_medoid`,
`CONFIDENCE_THRESHOLD=0.5548002022369389`. It has the stronger useful
post-safety calibration result and the much better raw accepted-error tail;
this is not a claim that MegaLoc wins every unthresholded retrieval metric.

## Single frozen held-out test

The test used the selection above, unchanged after calibration, on 507 new
queries:

| Metric | Result |
|---|---:|
| Recall@1 / @5 / @10 | 17.36 / 23.87 / 26.63% |
| All-query localization <=25 / <=50 / <=100 m | 1.78 / 1.97 / 2.17% |
| Answer rate | 2.17% (11 / 507) |
| Conditional <=25 / <=50 / <=100 m among answers | 81.82 / 90.91 / 100.00% |
| Median / P90 error among answers | 13.45 / 33.85 m |
| `low_confidence` / `out_of_coverage` | 41.42 / 56.41% |
| False-confident answers >100 m | 0 |
| Query / end-to-end median | 76.71 / 113.36 ms |

The safety criterion holds on this frozen split, but the 2.17% answer rate is
the material product limitation. GeoSnap is therefore a partial-coverage,
abstention-first prototype rather than a launch-ready city-wide geolocator.

## Production artifact bound to the result

The deployed index is built from the **gallery split only**:

- `data/embeddings/moscow/megaloc/`: 18 821 descriptors, dimension 8 448;
- `data/indexes/moscow/megaloc/`: 18 821-vector exact `IndexFlatIP` index;
- `index.faiss` SHA-256:
  `102eb5d74730aab96ccaf6b61d4dd1ed7560de43ef0ef1e60f8aada23d3d9344`;
- vector storage: 635,999,232 bytes; embedding job completed with no residual
  checkpoint directory.

`uncertainty_radius_m` remains `null`: selecting a confidence threshold does
not calibrate a geographic uncertainty radius. The service exposes the warning
`uncertainty_not_calibrated` rather than inventing one.

## What is not evidence for this selection

The historical Commons landmark proxy and its older verification experiment
remain research-only records. They neither select this model nor establish
Moscow coverage. The current 100-query real-data geometric-verification
ablation found zero all-query accuracy gain and +1,002.40 ms median overhead,
so `VERIFICATION_ENABLED=false` remains the recorded default; details and its
all-abstention limitation are in [verification_report.md](verification_report.md).
