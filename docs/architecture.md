# Архитектура GeoSnap

Актуально на 2026-09-03. Этот документ описывает реализованную архитектуру и
границы данных, а не обещанную точность продукта. Production gallery имеет
реальные Mapillary/KartaView references, но измеренное покрытие остаётся
частичным. Final production selection — SAGE ViT-B, exact `IndexFlatIP`, K=30,
single query, density-aware geographic mode voting, sequence-deduplicated
support, weighted medoid, verification off и Part 2.5 logistic score threshold
`0.9349250249145314`. Результаты и ограничения — в
[final_localization_core.md](final_localization_core.md).

## Граница production данных

Единственный допустимый вход в deployable Moscow reference gallery —
физически сохранённые изображения Mapillary и KartaView. Перед cleaning
применяется exact OSM administrative AOI: relation `102269`, `MultiPolygon` из
10 компонентов, SHA-256
`33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.

V2 exact-AOI source manifest содержит 23 654 references (17 143 Mapillary,
6 511 KartaView); frozen v4 leakage-resistant gallery/index содержит 20 487
(14 099 / 6 388). В fixed 20×20 grid заняты 93 cells, только 30 dense-healthy,
поэтому `city_id=moscow` обозначает область данных, а не гарантию локализации в
любой точке города.

MSLS, Wikimedia Commons и любые другие benchmark/training datasets разрешены
только в research/evaluation workflow. Они не могут быть объединены с
canonical manifest, embedded в production descriptors или добавлены в Moscow
FAISS index.

## Принцип

GeoSnap — модульный монолит с retrieval-based локализацией. Нейросеть не
регрессирует широту и долготу напрямую: она строит глобальный визуальный
дескриптор, а координата получается только из геопривязанных записей галереи.
`moscow` — значение данных `city_id`, а не условие внутри общего алгоритма.

```mermaid
flowchart LR
    subgraph Offline["Офлайн: построение галереи"]
        Sources["Mapillary / KartaView"] --> Normalize["Нормализация и canonical manifest"]
        Normalize --> Download["Возобновляемая загрузка изображений"]
        Download --> Clean["Валидация → quality score → dedup → H3"]
        Clean --> Embed["Pinned VPR retriever (current: SAGE)"]
        Embed --> Index["L2 descriptors + FAISS IndexFlatIP + ID sidecars"]
    end

    subgraph Online["Онлайн: один запрос"]
        Upload["multipart upload"] --> Validate["MIME/signature/decode/EXIF/RGB/limits"]
        Validate --> Query["Глобальный descriptor"]
        Query --> Search["Exact top-K retrieval"]
        Search --> Verify["Опциональная top-N geometry"]
        Verify --> Cluster["Компактные географические гипотезы"]
        Cluster --> Estimate["Weighted medoid + confidence/status"]
        Estimate --> API["Typed FastAPI response"]
    end

    Index --> Search
```

## Границы модулей

| Область | Реализация | Ответственность и граница |
|---|---|---|
| Ingestion | [`ml/ingestion`](../ml/ingestion) | Сетевые API, bbox/grid, пагинация, checkpoint/retry, нормализация источников, скачивание. Не загружает ML-модель. |
| Cleaning | [`ml/cleaning`](../ml/cleaning) | Консервативная проверка файлов и координат, quality score, exact/perceptual dedup, финальный Parquet и отчёты. Не обслуживает HTTP. |
| Геообогащение | [`ml/enrichment`](../ml/enrichment) | H3-поля для анализа, бакетизации и будущего шардирования. H3 не является обязательным онлайн-префильтром. |
| Retrieval | [`ml/retrieval`](../ml/retrieval) | Общий `BaseRetriever`, официальные checkpoint-backed адаптеры, пакетные и возобновляемые embedding jobs. Ошибка загрузки весов не заменяется случайной моделью. |
| Indexing | [`ml/indexing`](../ml/indexing) | Нормализованный exact `faiss.IndexFlatIP`, явное отображение row → stable reference ID, сохранение и проверка sidecar-метаданных. На macOS FAISS может исполняться в отдельном процессе. |
| Verification | [`ml/verification`](../ml/verification) | Ограниченная top-N локальная геометрия и rerank. По умолчанию выключена; подробности и измерения — в [отчёте о verification](verification_report.md). |
| Localization | [`ml/localization`](../ml/localization) | Пространственная группировка, выбор одной моды, оценка координаты, интерпретируемая уверенность и статусы `ok`/`low_confidence`/`out_of_coverage`. Не зависит от FastAPI. |
| Backend | [`apps/backend/app`](../apps/backend/app) | HTTP trust boundary, lifecycle, типизированные схемы, ограничения upload, readiness, безопасные ошибки и thumbnail по непрозрачному ID. ML загружается один раз в lifespan. |
| Frontend | [`apps/frontend`](../apps/frontend) | Минимальный клиент: upload, состояния результата, карта, гипотезы, совпадения и атрибуция. Не содержит логики локализации. |

PostgreSQL/PostGIS остаётся опциональным хранилищем метаданных и будущих
геозапросов. Дескрипторы и FAISS-индекс намеренно не помещаются в PostgreSQL:
канонические таблицы хранятся в Parquet, изображения — в файловом слое,
дескрипторы — в NumPy, индекс и его отображения — отдельными файлами.

## Канонический reference manifest

Контракт определён в [`ml/ingestion/schema.py`](../ml/ingestion/schema.py).
`id` не зависит от позиции строки: это детерминированный UUIDv5 от
`source:source_image_id`. Все сохраняемые стадии обязаны сохранить требуемые
поля даже при нулевом результате.

| Поле | Представление | Назначение |
|---|---|---|
| `id` | непустая строка, уникальная | Стабильный внутренний reference ID. |
| `city_id` | строка | Область данных, сейчас `moscow`; позволяет добавить другие индексы. |
| `source` | строка | Источник записи; для текущей deployable gallery — `mapillary` или `kartaview`. |
| `source_image_id` | строка | Стабильный ID изображения у источника. |
| `sequence_id` | nullable string | Последовательность/трек для dedup и leakage-контроля. |
| `image_path` | строка | Локальный путь внутри offline/runtime storage; не выдаётся API. |
| `lat`, `lon` | конечные числа | Координата reference; проверяется по мировым границам, для `moscow` также по настроенному bbox. |
| `captured_at` | nullable UTC ISO-8601 string | Время съёмки после нормализации. |
| `heading` | nullable float `[0, 360)` | Направление камеры, если источник его предоставил. |
| `quality_score` | nullable float | Аналитический приоритет/выбор лучшего дубля, не доказательство пригодности сцены. |
| `license` | строка | Лицензия конкретного источника. |
| `attribution` | строка | Текст, который должен сопровождать показ reference. |
| `source_url` | строка | Ссылка на исходную запись/трек. |
| `metadata_json` | JSON-строка | Детерминированно сериализованный JSON-текст; dict и литерал `"{}"` не смешиваются. |

`download_url` — служебное поле pipeline и не часть публичного reference
контракта. После обработки добавляются `width`, `height`, `blur_score`,
`brightness`, `exposure_score`, `h3_coarse` и `h3_fine`. Значения H3 — только
индексные масштабы, не названия «район» или «улица».

## Офлайн-путь данных

1. Mapillary и KartaView пишут исходные нормализованные JSON, schema-v2
   checkpoint с config fingerprint, append-only journal и cumulative/last-run
   статистику. `MAPILLARY_ACCESS_TOKEN` читается только из окружения; без него
   loader завершается с понятной ошибкой, не блокируя KartaView orchestration.
2. Источники объединяются в один schema-validated Parquet. Downloader читает
   его Arrow-батчами, держит не более `2 × workers` pending tasks, разрешает
   только HTTPS и source-specific hosts и вручную валидирует каждый redirect.
   Повторный запуск безопасен благодаря stable ID, checkpoint и повторной
   проверке уже существующих файлов.
3. Hard validation удаляет только отсутствующие/нечитаемые файлы,
   недопустимые координаты и явно непригодные размеры/форматы.
4. Quality stage вычисляет резкость, разрешение и экспозицию. Hard blur filter
   по умолчанию равен нулю, поэтому score преимущественно служит анализу и
   выбору дубля.
5. Dedup сначала сравнивает SHA-256, затем локально индексированный pHash.
   Кандидаты ограничиваются пространством, heading, sequence и временем;
   winner выбирается quality-first. Нет глобального O(N²) сравнения и нет
   географического per-location cap, который удалял бы разные виды.
6. H3 resolution 6/9 добавляется для статистики. Финальный manifest снова
   валидируется, а отчёт строит source/coverage графики и nearest-reference
   статистику только когда она определима.
7. Embedding job загружает модель один раз, работает батчами, сохраняет
   immutable chunks/checkpoint и финализирует `descriptors.npy` через memmap без
   полной конкатенации в RAM. Input signature хеширует сами image bytes, а
   schema-v2 metadata связывает generation и SHA-256 каждого артефакта.
   Результат:
   `descriptors.npy`, `id_mapping.json`, `reference_metadata.jsonl`,
   `build_metadata.json`, `failures.jsonl`.
8. Индексатор проверяет размерность, finite/L2-инварианты и mapping, строит
   correctness-first `IndexFlatIP` и сохраняет `index.faiss` вместе с mapping,
   reference metadata и schema-v2 build metadata. Loader сверяет generation,
   размеры, counts и SHA-256 sidecars и закрывается fail-safe при drift.

## Онлайн-путь и fail-safe поведение

FastAPI lifespan создаёт один `LocalizationService`, один retriever и один
индекс на процесс. `/health` проверяет только живость процесса. `/ready`
раздельно сообщает готовность модели, индекса, reference metadata и, если это
явно включено, базы данных. `/localize` принимает ровно одно multipart-поле
`image` формата JPEG/PNG/WebP и проверяет размер тела, MIME, magic signature,
расширение, decode, decompression limits, dimensions, animation, EXIF
orientation и RGB conversion.

После exact top-K search опциональный verifier может переставить только
ограниченную голову списка. Затем локализатор:

- формирует кластеры вокруг сильных anchors и проверяет расстояние до каждого
  члена, поэтому single-link цепочка не объединяет далёкие точки;
- не смешивает разные `city_id`/`index_id` и не усредняет разные моды;
- выбирает географическую моду с rank-, sequence-, provider-, density- и
  compactness-aware evidence, затем использует weighted medoid;
- строит logistic confidence score из 14 интерпретируемых retrieval и
  localization признаков без raw pixels;
- возвращает `low_confidence` или `out_of_coverage`, когда evidence слабое;
- оставляет `uncertainty_radius_m = null`, пока нет отдельной калибровки на
  leakage-resistant московском наборе.

Production сохраняет exact Part 2.5 confidence model: fit только на v3
development с group cross-validation, threshold выбран только на независимой
v3 calibration. V4 проверил его на новой one-shot выборке. Wilson interval
остаётся reported evidence, а не kill-switch. Service загружает
`configs/moscow_production_frozen.json`, требует matching config sidecar,
проверяет confidence/gallery/embedding/index/evaluation hashes и отвергает
конфликтующие model/K/localizer/index values.
`uncertainty_radius_m` остаётся `null`.

API не возвращает абсолютные пути, download URL, токены или stack traces.
Thumbnail разрешается сервером по `reference_id` из доверенного index sidecar;
атрибуция возвращается для каждого отображаемого совпадения. Полный перечень
лицензий и ограничений находится в [лицензионном аудите](licenses.md).

## Модели и protocol выбора

MegaLoc, DINOv2+SALAD, SAGE и SelaVPR++ имеют pinned checkpoint-backed
adapters. SALAD остаётся evaluation-only из-за GPL/checkpoint uncertainty.
SelaVPR++ base и официальный two-stage reranker были проверены, но дали меньшую
calibration product utility; CricaVPR отклонён из-за batch-dependent descriptor
contract. Frozen selection — single-query SAGE ViT-B. Это не утверждение о
превосходстве в других городах. Лицензии описаны в [licenses.md](licenses.md).

Финальный selection protocol прошёл следующим порядком:

1. V4 исключает все historical v1/v2/v3 query sequences и публикует disjoint
   Mapillary/KartaView gallery/development/calibration/test manifests с
   sequence, ID/source-ID, SHA-256, pHash и >=250 м geographic embargo.
2. K, aggregation, 45-feature candidate schema and confidence architecture
   выбраны только на development с geographically grouped out-of-fold scores.
3. Coefficients, K, aggregation и gate family frozen до calibration;
   calibration только выбрала candidate numeric thresholds.
4. Exact Part 2.5 baseline и candidate frozen до единственной v4 test
   transaction. Candidate не обобщился; prescribed Case 2 сохранил exact Part
   2.5 policy без post-test tuning.
5. Model/checkpoint, gallery, confidence and index hashes связаны в единственном
   production config; permanent smoke проверяет именно этот contract.

Commons proxy и MSLS-derived benchmarks не могут выбрать production model:
первый — hand-curated landmark-biased proxy, второй — внешний benchmark/training
corpus, а оба не являются Mapillary/KartaView Moscow deployment data.

## Текущие границы готовности

- V2 source содержит 23 654, frozen v4 gallery/index — 20 487 references;
  подробности — в [финальном отчёте](final_localization_core.md).
- V4 split содержит 751 development, 750 calibration и 1 499 once-opened test
  queries. Он не означает full-city coverage: заняты 93/400 cells,
  dense-healthy только 30.
- Frozen production test answer rate равен 8,47% при 96,85% conditional <=100
  м и 8,21% all-query success, но 3/127 accepted errors >500 м не проходят
  preferred <=1% catastrophic target.
- OpenCV SIFT/LightGlue-compatible verification остаётся optional/default-off:
  real 100-query ablation дала нулевой all-query accuracy gain и median
  overhead +1 002 мс. Evidence и ограничения — в
  [verification_report.md](verification_report.md).
- `uncertainty_radius_m` нельзя считать калиброванным только из confidence
  threshold; оно остаётся `null`, пока не появится отдельная проверенная
  uncertainty calibration.
- Docker Compose configuration проверена статически; stale 17,5 GB image не
  был перестроен, а Docker daemon был недоступен. Clean functional build,
  slimming и публичная browser QA остаются в Part 3.
