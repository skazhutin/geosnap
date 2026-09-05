# Real Moscow evaluation — final production policy

Актуально на 2026-09-03. Авторитетный current result, полный v4 decision-layer
audit и production hashes находятся в
[final_localization_core.md](final_localization_core.md). Part 2.5 model bake-off
остаётся в [phase2_5_product_recovery.md](phase2_5_product_recovery.md).
V1/V2/V3 ниже — immutable history; все четыре final tests permanently opened.

## V4 one-shot final decision

V4 использует 20 487 gallery references, 751 development, 750 calibration и
1 499 test queries. Все historical v1/v2/v3 query sequences исключены из query
eligibility. Sequence/source/path/URL/hash/pHash/geographic-group leakage между
любыми splits равен нулю, geographic embargo превышает 250 м.

Development сохранил SAGE K=30 density-aware aggregation и выбрал небольшую
multinomial risk architecture. Calibration only выбрала candidate thresholds.
Обе frozen policies были запущены на одном SAGE stream в одной sealed test
transaction. Candidate потерял utility и safety, поэтому production contract
следует prescribed Case 2: exact Part 2.5 policy на v4 gallery/index.

| V4 final-test metric | Exact Part 2.5 — **production** | Frozen multinomial candidate |
|---|---:|---:|
| <=100 м R@1/5/10/20/30/50 | 28,82 / 36,96 / 39,89 / 42,96 / 44,56 / 47,23% | identical |
| Median / p75 / p90 positive rank | 78 / 1 469,5 / 6 816,4 | identical |
| Raw <=25/50/100 м | 13,34 / 21,21 / 29,42% | identical |
| Raw median / p90 error | 7 563,11 / 25 902,95 м | identical |
| Answer rate | **127/1 499 (8,47%)** | 91/1 499 (6,07%) |
| Bootstrap 95% answer-rate CI | **7,07–9,94%** | 4,87–7,34% |
| Conditional <=25/50/100 м | **59,84 / 84,25 / 96,85%** | 58,24 / 83,52 / 93,41% |
| Wilson 95% conditional <=100 м | **92,18–98,77%** | 86,35–96,94% |
| All-query answered-and-correct <=100 м | **8,21%** | 5,67% |
| Accepted >100 / 100–500 / >500 м | **4 / 1 / 3** | 6 / 1 / 5 |
| Catastrophic accepted rate | **3/127 (2,36%)** | 5/91 (5,49%) |
| Accepted median / p90 / p95 | **21,29 / 62,79 / 73,47 м** | 21,47 / 66,94 / 786,68 м |
| End-to-end p50 / p90 / p95 | **115,81 / 131,70 / 143,25 мс** | identical retrieval stream |

Candidate removed 58 correct answers and added 20, prevented 2 baseline
catastrophes and introduced 4. No post-test tuning occurred. Production config:
`configs/moscow_production_frozen.json`, SHA-256
`9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`.
The result is useful but misses the preferred <=1% catastrophic target.

## Historical v3 frozen candidate

Новый split содержит 20 031 gallery references, 602 development, 603
calibration и 1 195 once-opened final-test queries. Между всеми query splits и
gallery подтверждены sequence/source/hash/pHash boundaries; development,
calibration и test разделены geographic embargo >=250 м.

SAGE ViT-B без cross-image encoder выбран на development/calibration: K=30,
density-aware geographic voting, weighted medoid, standardized logistic
confidence и threshold `0.9349250249145314`. Frozen config SHA-256:
`ddcfa0a66dbf0d4bbf31a292be38b07ceb6e93b60a983aaa46cd23679cafd0e2`.

| Final-test metric | Unchanged MegaLoc v2 policy | Frozen SAGE v3 |
|---|---:|---:|
| <=100 м R@10 / R@20 | 25,36 / 28,79% | 40,42 / 43,18% |
| Median positive rank | 391 | 61 |
| Raw all-query <=100 м | 13,05% | 26,61% |
| Answer rate | 0/1 195 (0%) | 92/1 195 (7,70%) |
| Conditional <=25/50/100 м | undefined | 67,39 / 82,61 / 94,57% |
| Wilson 95% conditional <=100 м | undefined | 87,90–97,66% |
| Accepted >100 м / >500 м | 0 / 0 | 5 / 3 |
| Accepted median / p90 / p95 | undefined | 18,21 / 68,09 / 90,36 м |
| End-to-end p50 / p90 / p95 | 141,31 / 171,47 / 191,86 мс | 138,74 / 169,18 / 188,48 мс |

V3 восстановил ненулевой полезный режим, но preferred target не был достигнут:
answer rate меньше 10%, а 3/92 accepted результатов (3,26%) имеют ошибку >500
м. Конфигурация не менялась после test; этот test не использовался для v4
selection.

## Historical v2 candidate

V2 не подтверждал полезную city-wide локализацию. Frozen runtime был намеренно
fail-closed: prospective Wilson objective на calibration оказался infeasible.

## V2 protocol and leakage boundary

- Deployable source manifest: 23 654 Mapillary/KartaView rows.
- Bundle: 19 524 gallery, 503 calibration, 497 final test rows.
- Provider sequence, record/source ID, URL/path, file hash, geographic group и
  pHash-near overlap между split равны нулю.
- Query spacing >=20,51 м; calibration/test distance >=252,04 м; у каждого
  query есть gallery positive <=100 м.
- Bundle fingerprint:
  `78cccca3f410f318673d99f1b3dcb303b4de678c4799821e0ff575a379248862`.
- Candidate set, >=5 pp material gate, paired bootstrap, stratum guard, K rule,
  Wilson objective и one-test limit были записаны до test.

Generated JSON/Markdown reports и per-query rows хранятся в ignored
`data/evaluation/moscow_real_v2/reports/`.

## Calibration retrieval selection

Все кандидаты использовали одинаковые gallery/query, exact normalized
`IndexFlatIP`, K=50 и weighted medoid. Таблица показывает <=100 м retrieval:

| Candidate | R@1 | R@5 | R@10 | R@20 | R@50 | Median / p75 / p90 positive rank | End-to-end p50 / p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| MegaLoc single baseline | 23,06% | 27,63% | 31,01% | 32,21% | 37,57% | 288 / 3 674,5 / 10 546,2 | 129,57 / 186,80 мс |
| MegaLoc five-crop | 22,66% | 29,03% | 31,01% | 33,60% | 38,37% | 372 / 3 170,5 / 10 715,2 | 314,25 / 412,27 мс |
| DINOv2 + SALAD | 23,26% | 30,82% | 33,40% | 37,57% | 42,74% | 124 / 1 244 / 5 283,8 | 125,62 / 173,06 мс |

SALAD дал +5,37 pp R@20 с paired-bootstrap 95% CI [+2,19, +8,55] pp,
но worst major-region regression равен -10 pp, поэтому stratum gate не пройден.
Кроме того, SALAD checkpoint остаётся evaluation-only до GPL compatibility/
checkpoint-license review. Five-crop дал только +1,39 pp R@20, CI
[-0,60, +3,38] pp, worst region -10 pp и 2,43× median latency.

**Deployable selection:** unchanged `megaloc`, single query aggregation,
`weighted_medoid`, verification disabled. Material gate не пройден; report не
называет deployable retrieval materially better.

## Historical v2 retrieval depth

| K | Recall at depth | R@50 retained | All-query <=100 м at threshold 0 | Answer rate | >100 м errors |
|---|---:|---:|---:|---:|---:|
| 10 | 31,01% | 82,54% | 9,74% | 10,74% | 5 |
| 20 | 32,21% | 85,71% | 6,36% | 6,56% | 1 |
| 50 | 37,57% | 100,00% | 3,18% | 3,18% | 0 |

Historical predeclared rule требует >=90% R@50, поэтому frozen v2 depth был
K=50.
Benchmark, confidence calibration, service и smoke читают один tracked runtime
artifact и отвергают конфликтующие overrides.

## Prospective confidence and frozen configuration

Цель: максимизировать answer rate при 95% Wilson lower bound conditional
<=100 м precision >=90%. Только 16/503 calibration rows прошли non-threshold
safety gates. Все 16 правильные, point precision 100%, но interval
[80,64%, 100%]; qualified threshold отсутствует.

Применён predeclared fail-closed threshold `1.0`, не наблюдавшийся на
calibration. Frozen artifact:

- `configs/moscow_real_v2_frozen.json`;
- SHA-256 `7f915c3806bcc214c928ce4bb3fb6af9467725faa420835d8acf05f5d0be0448`;
- MegaLoc revision `5fe0dd697c4a70ba3e23607f6716ab3c606b16db`;
- checkpoint SHA-256
  `d4f9f2bcb60018f91eb6a8e061ed054fd55654e10c2569cf13841ea986ffb4f8`;
- exact `IndexFlatIP`, 19 524×8 448 float32 vectors, K=50, single query,
  no reranker, weighted medoid, verification off, threshold 1.0.

## Single frozen v2 test

Test был запущен один раз после freeze; post-test tuning и rerun не было.

| Positive radius | R@1 | R@5 | R@10 | R@20 | R@50 | Median / p75 / p90 rank |
|---|---:|---:|---:|---:|---:|---:|
| <=25 м | 6,44% | 8,85% | 9,66% | 10,66% | 12,07% | 661 / 4 200,5 / 11 084,3 |
| <=50 м | 11,67% | 17,30% | 18,71% | 20,52% | 24,55% | 486,5 / 3 896 / 11 736,8 |
| <=100 м | 14,89% | 21,53% | 23,14% | 26,36% | 33,00% | 417 / 3 072 / 8 634,4 |

| Product metric | Result |
|---|---:|
| Answer rate | 0 / 497 (0%) |
| All-query <=25 / <=50 / <=100 м | 0 / 0 / 0% |
| Conditional <=100 м precision | undefined (no answers) |
| Wilson 95% interval | undefined (no answers) |
| False-confident >100 м | 0 |
| Accepted median / p90 / p95 | undefined |
| `low_confidence` / `out_of_coverage` | 53,72 / 46,28% |
| End-to-end p50 / p90 / p95 | 134,39 / 182,90 / 191,88 мс |
| Peak benchmark RSS | 2 170 896 384 bytes |

Это честный fail-closed результат, а не достижение 90% precision.

## Historical v2 artifact and host smoke

- index: `data/indexes/moscow_real_v2/megaloc/`, 19 524 rows;
- `index.faiss`: 659 755 053 bytes, SHA-256
  `2c08bc294556c29ea1eeec96d4b80abefe4b3669382c16c5fae8548274f563e5`;
- full index directory with sidecars: 702 MiB;
- `/ready`: HTTP 200 with model/index/metadata ready;
- real indexed-reference `POST /localize`: correct reference top-1,
  expected `low_confidence`, 204,48 мс total;
- host smoke maximum RSS: 2 318 712 832 bytes.

Docker-path smoke также прошёл на существующем `geosnap-backend:latest` с
текущими code/config/index, подключёнными read-only: `/ready` и настоящий
multipart `/localize` вернули HTTP 200, indexed reference остался top-1,
статус `low_confidence`, 50 matches; CPU diagnostics
1099,52/122,60/1418,45 мс embedding/retrieval/total. Snapshot памяти после
запроса — 3,487 GiB. Это не заменяет чистый rebuild 17,5 GB image, оставленный
для Part 3.

`uncertainty_radius_m` остаётся `null`; он не был калиброван.

## Historical immutable v1

V1 использовал 18 821 gallery rows, 493 calibration и 507 opened test queries.
Calibration <=100 м R@1/5/10/20/50 была
23,53/32,66/35,29/38,54/43,00%, median positive rank 155. Frozen test с
threshold `0.5548002022369389` ответил 11/507 (2,17%), conditional <=100 м
100%, false-confident >100 м 0, median/p90 accepted error 13,45/33,85 м.

V1 и v2 splits различаются; эти числа нельзя трактовать как paired regression
или improvement. V1 artifacts/reports не перезаписывались.
