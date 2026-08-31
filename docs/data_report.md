# Отчёт о реальных данных Москвы

Актуально на 2026-08-31. Этот документ описывает фактически подготовленную
двухисточниковую reference gallery и её измеренные границы. Выбор VPR-модели и
один frozen held-out test завершены по описанному ниже protocol; их компактный
evidence summary находится в [evaluation_report.md](evaluation_report.md).

## Production data contract

Deployable Moscow gallery строится только из физически сохранённых изображений
**Mapillary** и **KartaView**. Exact-AOI gate применяет сохранённую OSM
административную границу: relation `102269`, `MultiPolygon` из 10 компонентов,
bounds `36.8031012,55.1421745,37.9674277,56.0212238`, SHA-256
`33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.

MSLS, Wikimedia Commons и прочие benchmark/training datasets исключены из
production manifest, embedding и FAISS index. Они могут использоваться только
для research/evaluation и не заменяют реальную московскую street-view gallery.

## Фактический acquisition и publication

| Стадия | Всего | Mapillary | KartaView | Что означает |
|---|---:|---:|---:|---|
| Local live source manifest | 50 339 | 20 178 | 30 161 | Нормализованные source records до проверки физического файла. |
| Physical image screen | 26 954 | 20 165 | 6 789 | Локально существующие, полностью декодируемые изображения с валидными координатами. |
| Canonical exact-AOI gallery | 23 014 | 16 528 | 6 486 | Файл существует, provenance сохранён, точный OSM polygon пройден. |
| Final `manifest_clean.parquet` | 22 830 | 16 504 | 6 326 | Production-кандидат после quality/dedup/H3. |

Physical screen не зафиксировал decode или coordinate rejection среди 26 954
проверенных файлов. Exact-AOI filter исключил 3 940 строк вне polygon (3 637
Mapillary и 303 KartaView). Таким образом canonical gallery — это не bbox
approximation и не mix benchmark-данных.

Cleaning report для canonical input:

| Проверка | Удалено | Осталось |
|---|---:|---:|
| Hard validation | 0 | 23 014 |
| Quality stage | 0 | 23 014 |
| Exact/local pHash dedup | 184 | 22 830 |

Quality score остаётся консервативным аналитическим signal; отсутствие quality
rejection не означает, что каждая сцена одинаково пригодна для VPR. Dedup не
делает глобальный O(N²) обход и не отбрасывает различные городские виды только
из-за географической близости.

## Измеренное покрытие

| Метрика final gallery | Значение | Корректная интерпретация |
|---|---:|---|
| Fixed Moscow grid | 87 / 400 cells | Занято 21,75 % контрольной сетки. |
| Empty grid cells | 313 | Большая часть фиксированной сетки не имеет reference. |
| References per occupied cell, mean | 262,41 | Среднее не доказывает равномерность покрытия. |
| Unique H3 fine cells | 6 046 | Индексная spatial statistic, не административные районы. |
| Nearest-reference p50 / p90 | 28,74 / 154,17 м | Плотность среди имеющихся references, не гарантия query coverage. |

Покрытие частичное и неравномерное. Система не должна обещать локализацию
каждой московской улицы, двора, времени года, направления камеры или типа
сцены. `low_confidence` и `out_of_coverage` остаются ожидаемыми корректными
результатами за пределами фактической плотности галереи.

## Leakage-resistant real evaluation bundle

`make split-moscow` создаёт локальный, content-addressed bundle из final
Mapillary/KartaView gallery. В текущем bundle `moscow_real_v1`:

| Split | Rows | Provider sequences | H3 areas |
|---|---:|---:|---:|
| Gallery | 18 821 | 16 000 | 1 600 |
| Calibration queries | 493 | 180 | 182 |
| Final test queries | 507 | 342 | 315 |

Split использует sequence holdout, positive-distance threshold 100 м, minimum
query spacing 20 м, pHash threshold 4 и calibration/test geographic embargo
100 м. Coverage gate passed. Pairwise audit между gallery/calibration/test не
обнаружил общих IDs, source IDs, file SHA-256, sequences или pHash-near pairs;
query union не содержит exact/pHash duplicate или spacing violation. Bundle
fingerprint: `a8e24a77b4b4fb1a22a1a9f3198d050eca798d33565b2be8ec1aad3830e09a83`.

Это позволяет выбрать retriever и confidence threshold на calibration, а затем
один раз измерить замороженную конфигурацию на test. Нельзя использовать test
для model/threshold tuning. Отдельная confidence calibration сама по себе не
калибрует `uncertainty_radius_m`.

## Итоговая модель и реальный held-out результат

Calibration при `threshold=0.0` сравнила MegaLoc и DINOv2+SALAD на одних и тех
же 493 query. SALAD получил более высокий raw Recall@1/5/10
`29,61/36,31/39,55%` против `23,53/32,66/35,29%` у MegaLoc, но дал 30
unthresholded false-confident ответов >100 м против 9 и существенно худший
P90 accepted error (`2 296,98 м` против `152,67 м`). Safety-first calibration
сначала исключает такие ошибки, затем максимизирует all-query <=100 м и answer
rate. Поэтому выбрана конфигурация:

```text
RETRIEVER=megaloc
COORDINATE_ESTIMATOR=weighted_medoid
CONFIDENCE_THRESHOLD=0.5548002022369389
```

Её calibration result: 50/493 ответов, 0 false-confident >100 м и all-query
<=25/50/100 м `8,52/9,74/10,14%`. Для SALAD соответствующий zero-false-
confident threshold `0.7803838356776883` дал 44/493 и `6,69/8,32/8,92%`.

После заморозки selection единственный final test (507 query) дал Recall@1/5/10
`17,36/23,87/26,63%`, all-query <=25/50/100 м `1,78/1,97/2,17%`, 11/507
answers (2,17%), 0 false-confident >100 м и median/P90 accepted error
`13,45/33,85 м`. Это полезный safety result, но очень низкий answer rate:
система не является city-wide launch-ready локализатором. Detailed denominators
и latency — в [evaluation_report.md](evaluation_report.md).

Отдельная 100-query, 100-area, 72-sequence real calibration ablation сравнила
retrieval-only с bounded OpenCV SIFT rerank при той же frozen конфигурации.
All-query accuracy и answer rate не изменились (все выбранные query
abstained), Recall@1 снизился на 1 п.п., а median latency выросла на
1 002,40 мс. Поэтому `VERIFICATION_ENABLED=false` сохранён; подробности и
ограничение этой абляции — в [verification_report.md](verification_report.md).

## Воспроизводимость и расположение артефактов

```bash
# Воссоздать/обновить production gallery из live Mapillary + KartaView.
make prepare-moscow-gallery

# Создать защищённые от leakage gallery/calibration/test manifests.
make split-moscow

# Оценить оба retriever только на calibration split.
make benchmark-moscow-models TORCH_DEVICE=mps EMBEDDING_BATCH_SIZE=1 EVAL_TOP_K=10

# После выбора модели по calibration: калибровка, frozen test, gallery-only index.
make calibrate-moscow-confidence MOSCOW_EVAL_MODEL=<selected-model>
CONFIRM_FINAL_TEST=1 make benchmark-moscow-test \
  MOSCOW_EVAL_MODEL=<selected-model> \
  MOSCOW_CONFIDENCE_THRESHOLD=<calibrated-threshold>
make index-moscow-gallery RETRIEVER=<selected-model>
```

Основные локальные артефакты (крупные data files игнорируются Git):

- `data/raw/moscow/moscow_admin_boundary.geojson` и `.stats.json` — exact AOI;
- `data/processed/moscow/canonical_reference_gallery.parquet` — exact-AOI
  publication до final cleaning;
- `data/processed/moscow/manifest_clean.parquet` — final two-source manifest;
- `data/processed/moscow/reports/` — coverage, cleaning и source reports;
- `data/evaluation/moscow_real_v1/` — gallery/calibration/test split и audit;
- `data/embeddings/moscow/<retriever>/` и
  `data/indexes/moscow/<retriever>/` — только после embedding/index build.

Перед повторным embedding/index build нужно повторно проверить свободный диск:
exact FAISS, 8 448-dimensional descriptors, model cache и resumable chunks
требуют несколько GiB временного пространства.

## Provenance, лицензии и отображение

Каждая reference row должна сохранить `source`, `source_image_id`,
`sequence_id`, `license`, `attribution`, source/profile links и нужные поля
metadata. API возвращает display-safe attribution для match, а frontend
показывает её рядом с thumbnail; Mapillary card дополнительно показывает
официальный linked mark, карта — `© OpenStreetMap contributors`.

Детальные условия Mapillary, KartaView, OSM/tiles, Commons и model weights — в
[docs/licenses.md](licenses.md). Этот отчёт является engineering record, а не
юридической консультацией.

## Чего эти данные не доказывают

- full-city или even district-complete coverage Москвы;
- достаточную viewpoint/heading/season diversity в каждой занятой ячейке;
- заранее известную точность для нового пользовательского снимка;
- преимущество MegaLoc над SALAD для любых unthresholded retrieval metrics или
  других городов: выбор относится только к этому calibrated Moscow bundle;
- допустимость Commons, MSLS или другого benchmark-корпуса в production index;
- готовность Docker runtime на этом host: Compose configuration проверяется,
  но Docker daemon сейчас недоступен для container smoke.
