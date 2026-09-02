# Real Moscow evaluation — current v3 candidate

Актуально на 2026-09-03. Полная Part 2.5 evidence chain, включая K grid,
aggregation/confidence experiments, SALAD/SAGE/SelaVPR++/CricaVPR audit и exact
stratum denominators, находится в
[phase2_5_product_recovery.md](phase2_5_product_recovery.md). V1 и V2 ниже
сохраняются как immutable history и не использовались для v3 tuning.

## V3 frozen candidate

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

V3 восстанавливает ненулевой полезный режим, но preferred target не достигнут:
answer rate меньше 10%, а 3/92 accepted результатов (3,26%) имеют ошибку >500
м. Конфигурация не менялась после test. Следующая итерация требует нового
sealed namespace.

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

## Production retrieval depth

| K | Recall at depth | R@50 retained | All-query <=100 м at threshold 0 | Answer rate | >100 м errors |
|---|---:|---:|---:|---:|---:|
| 10 | 31,01% | 82,54% | 9,74% | 10,74% | 5 |
| 20 | 32,21% | 85,71% | 6,36% | 6,56% | 1 |
| 50 | 37,57% | 100,00% | 3,18% | 3,18% | 0 |

Predeclared rule требует >=90% R@50, поэтому frozen production depth — K=50.
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

## V2 production artifact and host smoke

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
