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

Актуальный candidate — versioned `moscow_real_v2`. Граница по-прежнему OSM
relation `102269`, валидный `MultiPolygon` из 10 компонентов, SHA-256
`33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.
Targeted acquisition и cross-version dedup дали 23 654 exact-AOI source rows;
leakage-resistant production gallery содержит только Mapillary/KartaView:

| Артефакт v2 | Всего | Mapillary | KartaView |
|---|---:|---:|---:|
| Clean source manifest | 23 654 | 17 143 | 6 511 |
| Gallery split / FAISS index | 19 524 | 16 605 | 2 919 |

В fixed 20×20 grid заняты 93 ячейки, 30 удовлетворяют dense-healthy критерию;
75 внутри/на границе AOI остаются пустыми при наличии source candidates, а в
40 source data не обнаружены. Это измеренная частичная поддерживаемая область,
не заявление о полногородском покрытии. Полная Phase 2 evidence chain — в
[docs/phase2_quality_improvement.md](docs/phase2_quality_improvement.md),
данные и исторический v1 — в [docs/data_report.md](docs/data_report.md).

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
make plan-moscow-v2-acquisition
make expand-mapillary-v2
make select-kartaview-v2
make split-moscow-v2
make coverage-moscow-v2
make embed-moscow-v2 \
  MOSCOW_V2_MODEL=megaloc \
  MOSCOW_V2_REUSE_FROM=data/embeddings/moscow/megaloc \
  TORCH_DEVICE=mps
make benchmark-moscow-v2-calibration TORCH_DEVICE=mps
```

Протокол `configs/moscow_real_v2_experiment_protocol.json` был записан до
экспериментов, а `configs/moscow_real_v2_frozen.json` — до единственного v2
test run. Test уже открыт и **не должен запускаться повторно**. Calibration не
нашла deployable candidate, прошедший material/stratum gate: SALAD дал
+5,37 п.п. R@20, но имеет региональные регрессии и остаётся evaluation-only;
MegaLoc five-crop дал только +1,39 п.п. R@20 при 2,43× median latency.

Явный production contract: MegaLoc, exact normalized `IndexFlatIP`, single
query descriptor, K=50, weighted medoid, verification off. Wilson calibration
не смогла доказать нижнюю 95% границу precision >=90%: 16/16 calibration
answers дают только 80,64% lower bound. Поэтому frozen threshold равен `1.0`
и fail-closed v2 test ответил 0/497. Это ограничение, а не safety success.
Подробные calibration/test таблицы — в
[docs/evaluation_report.md](docs/evaluation_report.md).

## API и frontend

V2 backend запускается только через frozen runtime contract; конфликтующий
`RETRIEVER`, K, estimator, threshold или index metadata приводит к fail-fast:

```bash
make api GEOSNAP_RUNTIME_CONFIG=configs/moscow_real_v2_frozen.json \
  TORCH_DEVICE=mps
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

Для host smoke с реальным v2 index:

```bash
make smoke-moscow-v2 TORCH_DEVICE=mps
```

Поддерживаются JPEG/PNG/WebP. Upload проходит MIME/signature/decode,
decompression, dimensions, animation, EXIF orientation и RGB проверки.
Абсолютные пути, download URL и секреты API не выдаются клиенту.

## Выполненная host-проверка

На 2026-09-01 host runtime проверен с frozen v2 MegaLoc index:

- `/ready` вернул HTTP 200 после загрузки модели, 19 524-vector FAISS и metadata;
- реальный `POST /localize` на indexed reference вернул тот же reference в
  top-1 и ожидаемый `low_confidence` при fail-closed threshold `1.0`;
- embedding/retrieval/total заняли 99,52/44,29/204,48 мс;
- maximum resident set size smoke process tree — 2 318 712 832 bytes;
- persisted `index.faiss` — 659 755 053 bytes, SHA-256
  `2c08bc294556c29ea1eeec96d4b80abefe4b3669382c16c5fae8548274f563e5`.

Это wiring/artifact smoke, не accuracy measurement. Историческая 2026-08-31
frontend-проверка v1 сохраняется в git history; новая публичная browser QA
относится к Part 3. Отдельный container smoke также подтверждён ниже.

## Evaluation scope

V2 real Moscow evaluation разделяет provider sequences между gallery и query,
требует gallery positive в 100 м, применяет pHash leakage checks, минимум 20 м
между query и 250 м calibration/test geographic embargo. Audit подтвердил ноль
пересечений, но measured retrieval и Wilson limitation не позволяют включить
полезный public answer mode. Одна confidence calibration также не делает
`uncertainty_radius_m` калиброванным.

Wikimedia Commons proxy и любые MSLS-derived artifacts остаются
research/evaluation-only. Commons — маленький hand-curated landmark-biased
набор, а MSLS — benchmark/training data; ни один из них не является московской
street-view production gallery, не участвует в `prepare-moscow-gallery` или
`index-moscow-gallery` и не обосновывает city-wide product claim. Реальный
выбор модели и single frozen test описаны в
[docs/phase2_quality_improvement.md](docs/phase2_quality_improvement.md); геометрическая
verification ablation — в [docs/verification_report.md](docs/verification_report.md).

## Docker Compose

Сначала должен существовать совместимый host-built v2 index в
`data/indexes/moscow_real_v2/megaloc/`. Проверка конфигурации:

```bash
make compose-config
docker compose --env-file .env config --quiet
```

Docker daemon на этом host доступен. Временный backend container на базе
существующего `geosnap-backend:latest`, с текущими code/config/index,
подключёнными read-only, успешно прошёл `/ready` (HTTP 200) и настоящий
multipart `POST /localize` (HTTP 200): indexed reference остался top-1,
статус ожидаемо `low_confidence`, 50 matches, diagnostics
1099,52/122,60/1418,45 мс embedding/retrieval/total на CPU. Snapshot памяти
container после запроса — 3,487 GiB. Чистый rebuild 17,5 GB image намеренно
отложен до Part 3; этот smoke подтверждает container runtime path, но не
reproducibility свежесобранного image и не accuracy.

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

- 93 из 400 контрольных ячеек заняты и только 30 dense-healthy, поэтому многие
  районы/виды/сезоны/ракурсы не поддержаны;
- v2 final test уже использован один раз и не может применяться для tuning или
  повторного frozen claim; следующая итерация требует v3 split;
- Wilson objective на v2 calibration infeasible; threshold `1.0` намеренно
  делает текущий public candidate fail-closed;
- verification остаётся optional/default-off: real 100-query Moscow ablation
  не дала accuracy/answer-rate gain и добавила median +1 002 мс;
- OSM standard tiles подходят для лёгкого demo, не для high-volume public
  deployment;
- Container runtime path проверен с текущими read-only code/config/index, но
  чистый rebuild 17,5 GB image и публичная browser QA остаются в Part 3.

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
