# Real Moscow geometric-verification ablation

Актуально на 2026-08-31. Это текущая evidence запись для решения по
`VERIFICATION_ENABLED`. Она не использует Commons proxy: обе arm работают с
одними и теми же original Mapillary/KartaView calibration queries, их query
embeddings и exact FAISS results.

## Данные и fail-closed preflight

- Gallery: 18 821 real Moscow street-view references.
- Query pool: 493 calibration queries; детерминированно выбраны 100,
  по одному из 100 H3 areas, из 72 provider sequences (64 Mapillary и 36
  KartaView query).
- Base split audit: **PASS** — ноль cross-split overlap по ID, source image ID,
  source URL, path, declared/computed SHA-256 и whole provider sequence.
- pHash preflight: **PASS** — ноль near pairs при Hamming threshold 4; каждая
  query имеет geographic gallery positive в 100 м (nearest p50 25,50 м).
- Dataset fingerprint:
  `56c3f8c15f45298e8a158538b1e177aeb33beca1d66671f6d03f6ca4f2ef3082`.

Конфигурация: MegaLoc, `weighted_medoid`, frozen safety threshold
`0.5548002022369389`, exact `top_k=10`, OpenCV SIFT verification top-10,
`geometric_weight=0.35`. Verification включён только внутри ablation; runner
не меняет production environment.

## Результат

| Метрика | Retrieval-only | + OpenCV SIFT | Delta |
|---|---:|---:|---:|
| Recall@1 | 13.00% | 12.00% | −1.00 п.п. |
| Recall@5 | 26.00% | 27.00% | +1.00 п.п. |
| Recall@10 | 34.00% | 34.00% | 0.00 п.п. |
| All-query <=25 / <=50 / <=100 м | 0 / 0 / 0% | 0 / 0 / 0% | 0 |
| Answer rate | 0.00% (0/100) | 0.00% (0/100) | 0 |
| False-confident >100 м | 0 | 0 | 0 |
| Median offline query pipeline | 143.40 ms | 1,133.68 ms | +1,002.40 ms |
| P90 offline query pipeline | 208.35 ms | 1,471.36 ms | +1,283.88 ms |

The paired median overhead is 7.16× the retrieval-only path. The decision
criteria required at least +2 percentage points all-query <=100 m gain, a
positive paired bootstrap lower bound, and <=250 ms median overhead. None of
those evidence conditions was met (the paired <=100 m gain is exactly zero;
95% bootstrap interval `[0, 0]`).

## Decision and limitation

**`keep_verification_default_off`**. No configuration was changed.

The frozen safety threshold caused all 100 geographically balanced queries in
this subset to abstain. Therefore this run cannot claim that SIFT improves or
harms *accepted-answer* precision; it can establish that it created no product
answer/accuracy gain here while adding about one second of median latency and
slightly decreasing Recall@1. The primary frozen product result remains in
[evaluation_report.md](evaluation_report.md).

The generated machine-readable report is local and ignored by Git:
`data/evaluation/moscow_real_v1/reports/megaloc_opencv_sift_k10_w0_35_moscow_verification_ablation.{json,md}`.
The older 8-query Wikimedia Commons landmark experiment is historical,
research-only proxy evidence and was not used for this decision.
