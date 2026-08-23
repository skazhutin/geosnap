# GeoSnap — визуальная геолокация по фотографии

GeoSnap — работающий retrieval-based MVP для локализации городской фотографии
по геопривязанной reference-галерее. Система строит descriptor, выполняет exact
FAISS search, выбирает одну компактную географическую моду и либо возвращает
координату, либо честно воздерживается (`low_confidence` /
`out_of_coverage`). GPS/EXIF координаты query не используются.

Текущий production scope — Москва, но сохранённая street-view галерея содержит
только один реальный KartaView sample. Это подтверждает связность pipeline, а
не покрытие города или продуктовую точность. Полный исходный SoT задачи
сохранён без сокращений в [docs/project_sot.md](docs/project_sot.md), исходный
SHA-256 — в [docs/project_sot.sha256](docs/project_sot.sha256).

## Что реализовано

- resumable Mapillary/KartaView ingestion с pagination, retry/backoff,
  checkpoint fingerprint, append-only journal и cumulative statistics;
- bounded downloader: HTTPS/source allowlists, проверка каждого redirect,
  ограничения bytes/pixels/dimensions, полное decode и atomic replace;
- единый Parquet schema со stable UUID, source/license/attribution, quality,
  H3 и строгой финальной валидацией;
- conservative cleaning, quality score, SHA-256 + локальный pHash dedup без
  полного O(N²) сравнения и без удаления разных ракурсов только по близости;
- официальные pinned MegaLoc и DINOv2+SALAD adapters, CPU/MPS, batched и
  resumable embedding job;
- normalized exact `faiss.IndexFlatIP` со stable ID mapping, generation ID и
  SHA-256 sidecars; fail-closed load и process isolation на macOS;
- spatial hypotheses, weighted medoid/centroid/top-1, интерпретируемый
  uncalibrated confidence и nullable uncertainty;
- optional bounded OpenCV SIFT / LightGlue-compatible verification interface;
  verification выключена по измеренному результату;
- FastAPI `/health`, `/ready`, `/localize`, безопасный thumbnail endpoint,
  typed errors, request ID, structured logs, lifecycle и concurrency limit;
- React/TypeScript UI с drag/drop, preview, всеми статусами, MapLibre/OSM,
  uncertainty circle, hypotheses, matches и кликабельной атрибуцией;
- unit, regression, integration, API, frontend, real-model/evaluation smoke и
  один канонический `make test`.

## Быстрый запуск

Требуются Python 3.12, [uv](https://docs.astral.sh/uv/), Node.js/npm и около
нескольких GiB для model cache. На Apple Silicon поддерживается MPS.

```bash
cp .env.example .env
make setup
make test
```

`.env` не отслеживается Git. Для sample Mapillary token не нужен.

### Реальный минимальный sample → FAISS → smoke

```bash
make ingest-sample
make prepare-data PROFILE=sample
make embed PROFILE=sample RETRIEVER=megaloc TORCH_DEVICE=auto
make build-index PROFILE=sample RETRIEVER=megaloc
make smoke PROFILE=sample RETRIEVER=megaloc TORCH_DEVICE=auto
```

Артефакты модели и индекса разделены по retriever:
`data/embeddings/sample/megaloc/` и `data/indexes/sample/megaloc/`.
Однокадровый sample намеренно даёт `low_confidence`; smoke проверяет реальный
model/index/API wiring, но не является held-out accuracy test.

### API и frontend

В двух терминалах:

```bash
make api PROFILE=sample RETRIEVER=megaloc TORCH_DEVICE=auto
make frontend
```

- UI: <http://localhost:5173>
- liveness: <http://localhost:8000/health>
- readiness: <http://localhost:8000/ready>
- OpenAPI: <http://localhost:8000/docs>

`/health` отвечает за живость процесса. `/ready` возвращает 200 только когда
модель, FAISS и reference metadata готовы (и DB, если явно включён её check).
Пример реального запроса:

```bash
curl -sS -X POST http://localhost:8000/localize \
  -H 'Accept: application/json' \
  -F 'image=@path/to/street.jpg'
```

Поддерживаются JPEG/PNG/WebP. Upload проходит MIME/signature/decode,
decompression, dimensions, animation, EXIF orientation и RGB проверки.
Абсолютные пути, download URL и секреты API не выдаются клиенту.

## Данные Москвы

Полный bounded/resumable запуск требует явного подтверждения из-за объёма:

```bash
CONFIRM_LARGE_RUN=1 make ingest-moscow
make prepare-data PROFILE=moscow
make embed PROFILE=moscow RETRIEVER=megaloc TORCH_DEVICE=auto
make build-index PROFILE=moscow RETRIEVER=megaloc
```

KartaView и Mapillary запускаются независимо: отказ одного источника не мешает
использовать валидный результат другого. Для Mapillary задайте секрет только в
окружении/`.env`:

```bash
MAPILLARY_ACCESS_TOKEN=... CONFIRM_LARGE_RUN=1 make ingest-moscow
```

Без token Mapillary явно пропускается. Пустой merged manifest блокирует download.
Перед большим запуском проверьте диск/RAM: на тестовой машине было около 25 GiB
свободно при 95% заполнении volume, поэтому полный Moscow download не запускался.
Подробности и реальные счётчики: [docs/data_report.md](docs/data_report.md).

## Evaluation

Tracked content ledger фиксирует 20 реальных Commons JPEG (12 gallery / 8
held-out query, четыре московских landmarks) с canonical SHA-256
`39c331f6b53b439f4e40b993d58caf80f69f279a07fe1845442f2f650943b6ab`.

```bash
make eval EVAL_MODEL=megaloc TORCH_DEVICE=auto EMBEDDING_BATCH_SIZE=4
make eval EVAL_MODEL=dinov2-salad TORCH_DEVICE=auto EMBEDDING_BATCH_SIZE=4
```

Recall-positive — gallery geotag в пределах 100 м. Abstention считается
ошибкой в ≤25/50/100 м; median/p90 считаются только по `status=ok`.

| Model | R@1 / R@5 / R@10 | ≤25 / ≤50 / ≤100 м | Answer rate | Median / p90 | Median offline query |
|---|---:|---:|---:|---:|---:|
| MegaLoc | 87.5 / 100 / 100% | 37.5 / 62.5 / 75% | 75% | 25.78 / 52.92 м | 72.78 ms |
| DINOv2 + SALAD | 87.5 / 100 / 100% | 37.5 / 62.5 / 75% | 75% | 23.37 / 52.92 м | 70.65 ms |

Это tiny landmark-biased, non-street-view proxy — не city-wide accuracy и не
основание калибровать confidence. MegaLoc остаётся default не из-за выигрыша
точности (метрики связаны), а как более простой для deployment MIT-вариант;
SALAD repository — GPL-3.0 challenger. Weighted centroid на proxy лучше
weighted medoid, но default не меняется до независимого street-view ablation.

OpenCV SIFT verification сохранила Recall, но снизила all-query accuracy
`37.5/62.5/75% → 12.5/12.5/12.5%`, answer rate `75% → 12.5%` и увеличила
median offline query `88.64 → 719.42 ms`. Поэтому
`VERIFICATION_ENABLED=false`.

Полные отчёты:

- [docs/evaluation_report.md](docs/evaluation_report.md)
- [data/evaluation/moscow_commons_proxy_run_2026-08-15.md](data/evaluation/moscow_commons_proxy_run_2026-08-15.md)
- [docs/verification_report.md](docs/verification_report.md)

## Docker Compose

Сначала должен существовать совместимый index в
`data/indexes/<PROFILE>/<RETRIEVER>/`. Затем:

```bash
docker compose --env-file .env config --quiet
docker compose --env-file .env up --build backend frontend
```

PostgreSQL/PostGIS опционален для локального filesystem/index path:

```bash
docker compose --env-file .env --profile database up --build
```

Порты привязаны к loopback. Контейнер без model/index не притворяется готовым:
`/health` остаётся liveness, `/ready` вернёт 503. Compose-конфигурация проверена
локально; полноценный container runtime smoke на текущем host не выполнялся,
потому что Docker daemon недоступен.

## Архитектура и структура

```text
Mapillary / KartaView
  → canonical manifest → bounded download → clean / quality / dedup / H3
  → pinned VPR embeddings → exact FAISS + verified sidecars
  → upload validation → retrieval → optional top-N verification
  → compact spatial modes → coordinate/confidence/status → API → map UI
```

```text
apps/backend/        FastAPI trust boundary и service lifecycle
apps/frontend/       React UI и MapLibre map
ml/ingestion/        source clients, schema, checkpoints, downloader
ml/cleaning/         hard validation, quality, dedup, reports
ml/enrichment/       H3 fields
ml/retrieval/        MegaLoc/SALAD adapters и embedding job
ml/indexing/         FAISS persistence/search/process worker
ml/localization/     clustering, estimators, confidence/status
ml/verification/     bounded local-feature reranking
ml/evaluation/       metrics, Commons snapshot, robustness, ablations
data/evaluation/     tracked proxy config/ledger/readable run report
docs/                architecture, data, evaluation, verification, licenses
```

PostgreSQL/PostGIS остаётся опциональным metadata layer; изображения,
descriptors и FAISS не помещаются в БД. Подробный контракт и lifecycle:
[docs/architecture.md](docs/architecture.md).

## Основные переменные окружения

Полный безопасный шаблон — [.env.example](.env.example). Главные настройки:

- `MAPILLARY_ACCESS_TOKEN`, `DATA_ROOT`, `GEOSNAP_MODEL_CACHE`, `HF_HOME`;
- `RETRIEVER`, `TORCH_DEVICE`, `GEOSNAP_INDEX_DIR`, `EMBEDDING_BATCH_SIZE`;
- `RETRIEVAL_TOP_K`, `VERIFICATION_ENABLED`, `VERIFY_TOP_K`;
- `CONFIDENCE_THRESHOLD`, `OOC_SIMILARITY_THRESHOLD`, cluster/mass margins;
- upload byte/pixel/dimension limits и `LOCALIZATION_CONCURRENCY`;
- `CORS_ORIGINS`, optional `DATABASE_URL`/`READINESS_CHECK_DATABASE`.

## Ограничения и лицензии

- сохранённая deployable gallery: KartaView 1, Mapillary 0, одна из 400
  контрольных ячеек; это pipeline sample, не продуктовая база;
- Mapillary acquisition заблокирован отсутствующим access token;
- расширенный KartaView probe был sparse/нестабилен: 9 tile attempts, 5 HTTP
  successes, 4 timeout, 3 финально пригодных изображения;
- confidence/uncertainty не откалиброваны на leakage-resistant street-view;
- Commons benchmark мал и смещён к landmarks; full Moscow ingestion не выполнен;
- OSM standard tiles подходят только для лёгкого demo, не high-volume сервиса;
- LightGlue опционален: протестированная установка конфликтовала с закреплённым
  OpenCV 4, а SIFT-ablation не оправдала включение verification.

Каждый reference сохраняет license/attribution/source link; UI показывает их
рядом с thumbnail и отдельно показывает OSM attribution. Актуальный аудит:
[docs/licenses.md](docs/licenses.md). Исходное состояние репозитория и неверные
предыдущие предположения зафиксированы в [docs/audit.md](docs/audit.md).
