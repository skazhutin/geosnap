# GeoSnap — визуальная геолокация по фотографии

GeoSnap — retrieval-based MVP: фотография пользователя проходит безопасную
предобработку, превращается в VPR-descriptor, ищется exact FAISS-поиском по
геопривязанной галерее, а координата получается из компактной географической
гипотезы. GPS/EXIF координаты query не используются. Когда evidence слабое,
API возвращает `low_confidence` или `out_of_coverage`, а не выдуманную точку.

Текущий data scope — Москва, но это **не** заявление о поддержке всей Москвы.
Deployable reference gallery содержит только физически сохранённые изображения
Mapillary и KartaView внутри точной административной границы Москвы. MSLS,
Wikimedia Commons и другие benchmark-наборы не допускаются в production
gallery; они имеют только исследовательскую/evaluation роль.

## Состояние реальной московской галереи

Граница — OSM relation `102269`, валидный `MultiPolygon` из 10 компонентов,
SHA-256 `33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.
Из 26 954 физически валидных локальных изображений exact-AOI gate оставил
23 014 canonical references: 16 528 Mapillary и 6 486 KartaView. После
quality/dedup финальный manifest содержит 22 830 references:

| Стадия | Всего | Mapillary | KartaView |
|---|---:|---:|---:|
| Canonical exact-AOI gallery | 23 014 | 16 528 | 6 486 |
| Final gallery после cleaning/quality/dedup | 22 830 | 16 504 | 6 326 |

Hard validation и quality stage не удалили строки; локальный exact/pHash dedup
удалил 184. В fixed 20×20 Moscow grid заняты 87 из 400 ячеек (21,75 %), а
nearest-reference p50/p90 равны 28,74/154,17 м. Это измеренная частичная
поддерживаемая область, а не плотное или полногородское покрытие. Полные
счётчики, границы, provenance и ограничения — в
[docs/data_report.md](docs/data_report.md).

## Что реализовано

- resumable Mapillary/KartaView ingestion с pagination, retry/backoff,
  checkpoint fingerprint, append-only journal и cumulative statistics;
- bounded downloader: HTTPS/source allowlists, redirect validation, byte/pixel
  limits, полное decode и atomic replace;
- единый Parquet schema со stable UUID, source/license/attribution, quality,
  H3 и строгой финальной валидацией;
- conservative cleaning, SHA-256 + локальный pHash dedup без глобального
  O(N²) сравнения;
- pinned MegaLoc и DINOv2+SALAD adapters, batched/resumable embedding и
  normalized exact `faiss.IndexFlatIP` со stable ID mapping и SHA-256 sidecars;
- spatial modes, weighted medoid/centroid/top-1, confidence/status и nullable
  uncertainty; confidence threshold выбирается только по real calibration;
- FastAPI `/health`, `/ready`, `/localize`, безопасный thumbnail endpoint,
  typed errors, request ID, structured logs и concurrency limit;
- React/TypeScript UI с upload, map, hypotheses, matches и кликабельной
  source attribution;
- unit/regression/integration/API/frontend tests и один `make test`.

## Быстрый запуск sample

Требуются Python 3.12, [uv](https://docs.astral.sh/uv/), Node.js/npm и несколько
GiB для model cache. На Apple Silicon доступен MPS.

```bash
cp .env.example .env
make setup
make test
```

`.env` игнорируется Git. Не записывайте token в shell history, документацию или
коммиты. Небольшой KartaView sample не является production gallery: он служит
только дешёвым pipeline/API smoke.

```bash
make ingest-sample
make prepare-data PROFILE=sample
make embed PROFILE=sample RETRIEVER=megaloc TORCH_DEVICE=auto
make build-index PROFILE=sample RETRIEVER=megaloc
make smoke PROFILE=sample RETRIEVER=megaloc TORCH_DEVICE=auto
```

## Реальный Moscow run: воспроизводимый порядок

Большой acquisition выполняется только после проверки места на диске и памяти.
Он resumable; token передаётся только через окружение или локальный `.env`.

```bash
# Нужен только при новом/обновлённом acquisition.
CONFIRM_LARGE_RUN=1 make ingest-moscow

# Публикация deployable Mapillary+KartaView gallery и leakage-resistant split.
make prepare-moscow-gallery
make split-moscow

# Оба кандидата оцениваются только на calibration split. Target принудительно
# использует confidence threshold 0.0, чтобы calibration не была скрыта
# старым порогом abstention.
make benchmark-moscow-models TORCH_DEVICE=mps EMBEDDING_BATCH_SIZE=1 EVAL_TOP_K=10
```

После выбора retriever по **calibration** reports (не по Commons proxy) создайте
confidence-calibration artifact, заморозьте retriever/estimator/threshold и
лишь затем единственный раз откройте held-out test split:

```bash
make calibrate-moscow-confidence \
  MOSCOW_EVAL_MODEL=<selected-model> \
  MOSCOW_CALIBRATION_BENCHMARK_JSON=data/evaluation/moscow_real_v1/reports/<selected_model_with_underscores>_moscow_real_calibration.json

CONFIRM_FINAL_TEST=1 make benchmark-moscow-test \
  MOSCOW_EVAL_MODEL=<selected-model> \
  MOSCOW_CONFIDENCE_THRESHOLD=<calibrated-threshold> \
  TORCH_DEVICE=mps EMBEDDING_BATCH_SIZE=1 EVAL_TOP_K=10

# Production index строится только из gallery split, не из calibration/test.
make index-moscow-gallery RETRIEVER=<selected-model> \
  TORCH_DEVICE=mps EMBEDDING_BATCH_SIZE=1
```

`data/evaluation/moscow_real_v1/` — локальный generated bundle. В текущем
запуске он содержит 18 821 gallery rows, 493 calibration queries и 507 final
held-out test queries; audit проверяет отсутствие межsplit совпадений по ID,
source ID, file SHA-256, sequence и pHash-near дубликатам. Calibration выбрала
`megaloc` + `weighted_medoid` +
`CONFIDENCE_THRESHOLD=0.5548002022369389`: на единственном frozen test ответ
выдан в 11 из 507 случаев (2,17%), при этом 0 выданных ответов ошиблись более
чем на 100 м. Полная таблица и честные ограничения — в
[docs/evaluation_report.md](docs/evaluation_report.md).

## API и frontend

После `index-moscow-gallery` запускайте backend с выбранным retriever и явным
калиброванным порогом — сервис не читает calibration report автоматически:

```bash
make api PROFILE=moscow CITY_ID=moscow RETRIEVER=megaloc \
  CONFIDENCE_THRESHOLD=0.5548002022369389 TORCH_DEVICE=mps
```

Во втором терминале:

```bash
make frontend
```

- UI: <http://localhost:5173>
- liveness: <http://localhost:8000/health>
- readiness: <http://localhost:8000/ready>
- OpenAPI: <http://localhost:8000/docs>

`/health` означает только живой процесс. `/ready` должен вернуть 200 до
реального `/localize`: он требует готовые модель, FAISS и reference metadata.
Пример запроса:

```bash
curl -sS -X POST http://localhost:8000/localize \
  -H 'Accept: application/json' \
  -F 'image=@path/to/street.jpg'
```

Для host smoke с настоящей held-out фотографией используйте, например:

```bash
make smoke PROFILE=moscow RETRIEVER=megaloc \
  SMOKE_EXTRA_ARGS='--query-image /absolute/path/to/heldout.jpg --expected-status ok'
```

Поддерживаются JPEG/PNG/WebP. Upload проходит MIME/signature/decode,
decompression, dimensions, animation, EXIF orientation и RGB проверки.
Абсолютные пути, download URL и секреты API не выдаются клиенту.

## Выполненная host-проверка

На 2026-08-31 host runtime был проверен с готовым real MegaLoc Moscow index:

- `/health` и `/ready` вернули healthy/ready состояние;
- настоящий held-out Mapillary JPEG прошёл через `POST /localize` со статусом
  `ok`, реальной FAISS галереей и атрибуцией Mapillary;
- frontend upload проверен на desktop и mobile: карта, geographic hypotheses,
  source/license/author links и thumbnails пришли из живого API, без mock;
- другой реальный held-out JPEG отрисовал явный `low_confidence` abstention,
  а не выдуманную точку; browser console не показал errors.

Это host verification, не отменяющая низкий answer rate frozen test и ограничение
покрытия. Docker daemon на этой машине недоступен, поэтому container-runtime
smoke ещё должен быть выполнен на host с работающим daemon.

## Evaluation scope

Real Moscow evaluation разделяет provider sequences между gallery и query,
требует gallery positive в 100 м, применяет pHash leakage checks, минимум 20 м
между query и 100 м calibration/test geographic embargo. Это рабочая база для
выбора модели, оценки abstention и калибровки confidence; одна только
confidence calibration не делает `uncertainty_radius_m` калиброванным.

Wikimedia Commons proxy и любые MSLS-derived artifacts остаются
research/evaluation-only. Commons — маленький hand-curated landmark-biased
набор, а MSLS — benchmark/training data; ни один из них не является московской
street-view production gallery, не участвует в `prepare-moscow-gallery` или
`index-moscow-gallery` и не обосновывает city-wide product claim. Реальный
выбор модели и frozen test описаны в
[docs/evaluation_report.md](docs/evaluation_report.md); геометрическая
verification ablation — в [docs/verification_report.md](docs/verification_report.md).

## Docker Compose

Сначала должен существовать совместимый host-built index в
`data/indexes/moscow/<selected-model>/`. Проверка конфигурации:

```bash
make compose-config
docker compose --env-file .env config --quiet
```

Полный container runtime smoke на этом host пока не выполнен: Docker CLI и
Compose config доступны, но Docker daemon недоступен. Поэтому это ограничение
не скрывается под `/health` или формальной compose-проверкой; перед deployment
нужно выполнить `/ready` и real `/localize` на машине с запущенным daemon.

PostgreSQL/PostGIS остаётся опциональным metadata layer:

```bash
docker compose --env-file .env --profile database up --build
```

## Архитектура, атрибуция и ограничения

```text
Mapillary / KartaView
  → canonical exact-AOI manifest → bounded download → clean / quality / dedup / H3
  → VPR embeddings → exact FAISS + verified sidecars
  → upload validation → retrieval → optional top-N verification
  → compact spatial modes → coordinate/confidence/status → API → map UI
```

PostgreSQL/PostGIS не хранит изображения, descriptors или FAISS. Полный
контракт, lifecycle и fail-safe поведение — в
[docs/architecture.md](docs/architecture.md). Атрибуция, source links,
Mapillary/KartaView условия, OSM/tiles и model licenses — в
[docs/licenses.md](docs/licenses.md). UI показывает source/license/attribution
для каждой отображаемой reference thumbnail и `© OpenStreetMap contributors`
для карты.

Ограничения, которые нельзя маскировать:

- 87 из 400 контрольных ячеек заняты, поэтому многие районы/виды/сезоны/ракурсы
  не поддержаны;
- модель и confidence threshold должны быть выбраны по real calibration, а
  final test нельзя использовать для tuning;
- verification остаётся optional/default-off: real 100-query Moscow ablation
  не дала accuracy/answer-rate gain и добавила median +1 002 мс;
- OSM standard tiles подходят для лёгкого demo, не для high-volume public
  deployment;
- Docker runtime на текущей машине ещё не проверен из-за недоступного daemon.

## Canonical SoT integrity

Полная спецификация хранится без сокращений в
[docs/project_sot.md](docs/project_sot.md), completion directive — в
[docs/completion_directive.md](docs/completion_directive.md). Сохранённые файлы
проверены по SHA-256 sidecars:

- `project_sot.md`: `009852dbe06d74f8d50ee34714f221f8396265551692b057e9fb1b6726db1c0c`;
- `completion_directive.md`: `238406917412ce5c3ad09f1165b86e5bbefa92a0613dc055aebca5696d3642f1`.

Это подтверждает целостность сохранённых копий. Оригинальное attachment больше
не доступно для прямого byte-for-byte comparison, поэтому GeoSnap не заявляет,
что exact identity с исходным attachment была повторно проверена.
