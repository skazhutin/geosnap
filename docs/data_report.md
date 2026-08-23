# Отчёт о реальных данных Москвы

Актуально на 2026-08-15. Здесь отдельно приведены (1) сохраняемый в репозитории
воспроизводимый sample и (2) более широкий временный live probe. Эти результаты
подтверждают работу pipeline, но **не подтверждают покрытие Москвы и не дают
оценки точности геолокации**.

## Сохраняемый KartaView sample

Live запрос был направлен в точку `55.7558, 37.6173` с радиусом 20 м. Получена
одна уникальная запись KartaView, она нормализована, скачана, декодирована и
прошла весь путь до финального manifest, MegaLoc descriptor и FAISS index.

| Стадия | Фактический результат |
|---|---:|
| KartaView cumulative tile attempts | 2 |
| Успешные / неуспешные tile attempts | 2 / 0 |
| Cumulative source records discovered | 2 |
| Metadata normalized / duplicate / invalid | 1 / 1 / 0 |
| Unique normalized records | 1 |
| Реально сохранённые изображения | 1 |
| Последний idempotency rerun: requested / newly downloaded / skipped existing / failed | 1 / 0 / 1 / 0 |
| Basic validation: вход / удалено / выход | 1 / 0 / 1 |
| Quality stage: вход / удалено / выход | 1 / 0 / 1 |
| Dedup: exact removed / perceptual removed / выход | 0 / 0 / 1 |
| Финальный reference manifest | 1 |
| MegaLoc descriptors / failures | 1 / 0 |
| FAISS gallery size | 1 |

`images_downloaded=0` в текущем `download.stats.json` относится именно к
последнему повтору: единственный файл уже существовал и был повторно проверен,
поэтому сетевой запрос не выполнялся. Сам файл действительно был получен при
первом live запуске и остаётся в sample.

Проверенная запись:

- KartaView image ID `1312303109`, sequence `3616873`;
- координата `55.755878, 37.617322` внутри настроенного Moscow bbox;
- время `2021-01-15T10:53:42Z`, heading `290.85°`;
- JPEG `2704 × 2028`, 1 667 206 байт;
- лицензия `CC BY-SA 4.0`, атрибуция
  `© Grab and KartaView Contributors` сохранены в каждой стадии;
- стабильный internal ID `43dbf5cf-90b8-51e2-8683-dda57a4bf0f8`.

Источники фактов: [KartaView ingestion stats](../data/raw/sample/kartaview.stats.json),
[download stats](../data/raw/sample/download.stats.json),
[pipeline cleaning totals](../data/processed/sample/cleaning_report.json),
[dataset report JSON](../data/processed/sample/reports/dataset.json),
[embedding metadata](../data/embeddings/sample/megaloc/build_metadata.json) и
[index metadata](../data/indexes/sample/megaloc/index_metadata.json).

Счётчики ingestion cumulative: после обновления checkpoint schema был выполнен
ещё один live HTTP request, который вернул уже известный source ID. Текущий
checkpoint schema v2 содержит config fingerprint; финальный unique record count
остался равен одному. Реальный index/API smoke на этом кадре вернул
`low_confidence`, query embedding 93.62 ms, FAISS 2.18 ms и total 172.13 ms —
ожидаемое fail-safe поведение для singleton gallery, не accuracy metric.

## Cleaning и dedup на sample

Hard validation не обнаружила отсутствующий файл, ошибку decode, неверную
координату, слишком маленькое изображение или неподдерживаемый формат. Quality
stage не использовал жёсткое удаление по blur (`hard_blur_threshold=0.0`),
поэтому ни одно изображение не было отброшено. Полученный
`quality_score=0.998119` — внутренний составной score, а не ручная оценка сцены
и не доказательство VPR-пригодности.

Dedup использовал radius 15 м, heading threshold 45°, pHash Hamming threshold
4, temporal preserve window 7 дней и same-sequence window 180 секунд. При одной
строке сравнений и удалений не было. Поэтому sample проверяет выполнение и
сохранение схемы, но не измеряет эффективность dedup на реальном масштабе.

Подробные устойчивые отчёты: [basic cleaning](../data/processed/sample/reports/cleaning.json),
[quality](../data/processed/sample/reports/quality.json),
[dedup](../data/processed/sample/reports/dedup.json) и
[final manifest summary](../data/processed/sample/reports/final.json).

## Пространственная статистика sample

| Метрика | Значение | Корректная интерпретация |
|---|---:|---|
| Total references | 1 | Одна точка, не городская галерея. |
| Unique H3 resolution 6 cells | 1 | Индексный coarse bucket. |
| Unique H3 resolution 9 cells | 1 | Индексный fine bucket. |
| Occupied fixed-grid cells | 1 из 400 | Доля `0.0025`, то есть 0.25%. |
| References per occupied cell, mean | 1.0 | Определено только для одной занятой ячейки. |
| Nearest-reference sample count | 0 | Для единственной точки соседа нет. |
| Nearest-reference p50 / p90 | не определены | В JSON сохранено `null`; число не подставлялось. |
| Source proportions | KartaView 100%, Mapillary 0% | Относится только к этому sample. |

Графики [scatter](../data/processed/sample/reports/scatter.png),
[density](../data/processed/sample/reports/density.png),
[source comparison](../data/processed/sample/reports/sources.png) и
[preview](../data/processed/sample/reports/preview.jpg) автоматически созданы,
но визуализируют одну запись. Их нельзя интерпретировать как карту плотности
или покрытие Москвы.

## Расширенный временный KartaView probe

Отдельный bounded live probe был выполнен в нескольких московских точках.
Временные изображения и промежуточные файлы не являются устойчивыми
репозиторными артефактами; ниже зафиксированы итоговые счётчики запуска без
ссылок на временные пути.

### Metadata

- 9 HTTP tile requests, без повторов в этом probe;
- 5 успешных ответов и 4 read timeout;
- из 5 успешных: 1 непустой и 4 пустых;
- один успешный ответ был помечен как truncated по заданному bounded limit;
- 5 уникальных нормализованных записей;
- у всех 5 были и heading, и sequence ID.

### Download и финальная подготовка

- 3 изображения были успешно получены и впоследствии сохранились в финальном
  валидном manifest;
- 1 URL завершился постоянным HTTP 404;
- 1 URL завершился HTTP 502 после 2 retry;
- download observability на итоговом повторе: `network_attempts=4`,
  `retry_attempts=2`, `timeout_events=0`, `skipped_existing=3`, `failed=2`;
- финальный валидный manifest: 3 references;
- все 3 попали в одну из 400 ячеек той же fixed Moscow grid.

Четыре metadata read timeout и `timeout_events=0` не противоречат друг другу:
первое относится к запросам KartaView metadata, второе — к отдельной стадии
скачивания изображений. Probe показывает, что retry/checkpoint/error counters
работают и что выбранные точки дали крайне разреженный результат. Он не
измеряет полное доступное покрытие KartaView в Москве.

## Mapillary

| Метрика | Результат |
|---|---:|
| Live metadata records discovered | 0 |
| Live images downloaded | 0 |
| References в текущей галерее | 0 |

Причина — в окружении отсутствует `MAPILLARY_ACCESS_TOKEN`. Loader проверен на
этом пути и завершается с exit code 1 и явным сообщением о требуемой переменной,
до выполнения live API запроса. Поэтому нулевой счётчик не означает отсутствие
изображений Mapillary в Москве: источник не был измерен без credentials. Это
внешний блокер, требующий пользовательского access token; fixtures и остальной
pipeline работают независимо от него.

## Ресурсы и ограничение большого запуска

На момент отчёта машина имела Apple M1 Pro (10 CPU cores), 16 GiB RAM,
доступный PyTorch MPS и около 25 GiB свободного места на data volume; volume был
заполнен на 95%. Python 3.12.12 и PyTorch 2.13.0 использовались в локальном
окружении.

Единственный сохранённый JPEG занимает около 1.59 MiB, но по одному файлу
нельзя надёжно прогнозировать средний размер московского source. Один
8 448-мерный `float32` descriptor занимает 33 792 байта до служебных данных;
exact FAISS требует память того же порядка для векторов. С учётом 25 GiB
свободного места, model caches, исходных изображений и промежуточных manifests
неограниченный полный download был бы небезопасен. Поэтому `ingest-moscow`
дополнительно требует явного `CONFIRM_LARGE_RUN=1`, а фактический large run не
заявляется выполненным.

## Что эти данные доказывают и чего не доказывают

Доказано фактическим запуском:

- текущий KartaView API может вернуть нормализуемую московскую запись;
- реальный файл скачивается, декодируется и сохраняет ID/coordinate/time/
  heading/sequence/license/attribution;
- повторный download пропускает уже валидный файл;
- schema проходит cleaning, quality, dedup, H3, final manifest, embedding и
  FAISS build;
- сетевые и permanent failures учитываются отдельно.

Не доказано:

- покрытие районов или дорог Москвы каким-либо источником;
- достаточная плотность, heading/viewpoint/season diversity;
- nearest-reference distribution на масштабе города;
- retrieval/localization accuracy на street-view held-out queries;
- пригодность текущего sample как production gallery;
- преимущество одного retriever или coordinate estimator на уличных данных.

Следующий корректный data milestone — после добавления Mapillary token и
повторной оценки диска выполнить ограниченный, возобновляемый двухисточниковый
Moscow acquisition, построить coverage report, а затем отделить query от
gallery по sequence/time и только после этого считать продуктовые метрики.
