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

Финальный production contract — `configs/moscow_production_frozen.json`: точная
Part 2.5 policy на leakage-safe v4 gallery/index. V4 переиспользует чистый v2
source pool без нового acquisition. Граница по-прежнему OSM
relation `102269`, валидный `MultiPolygon` из 10 компонентов, SHA-256
`33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.
Targeted acquisition и cross-version dedup дали 23 654 exact-AOI source rows;
leakage-resistant production gallery содержит только Mapillary/KartaView:

| Артефакт | Всего | Mapillary | KartaView |
|---|---:|---:|---:|
| Clean source manifest | 23 654 | 17 143 | 6 511 |
| Frozen v4 gallery / SAGE FAISS index | 20 487 | 14 099 | 6 388 |

В fixed 20×20 grid заняты 93 ячейки, 30 удовлетворяют dense-healthy критерию;
75 внутри/на границе AOI остаются пустыми при наличии source candidates, а в
40 source data не обнаружены. Это измеренная частичная поддерживаемая область,
не заявление о полногородском покрытии. Полная Phase 2 evidence chain — в
[docs/phase2_quality_improvement.md](docs/phase2_quality_improvement.md),
восстановление product operating point — в
[docs/phase2_5_product_recovery.md](docs/phase2_5_product_recovery.md), а
авторитетный финальный ML contract и v4 test — в
[docs/final_localization_core.md](docs/final_localization_core.md).

## Что реализовано

- resumable Mapillary/KartaView ingestion с pagination, retry/backoff,
  checkpoint fingerprint, append-only journal и cumulative statistics;
- bounded downloader: HTTPS/source allowlists, redirect validation, byte/pixel
  limits, полное decode и atomic replace;
- единый Parquet schema со stable UUID, source/license/attribution, quality,
  H3 и строгой финальной валидацией;
- conservative cleaning, SHA-256 + локальный pHash dedup без глобального
  O(N²) сравнения;
- pinned MegaLoc, DINOv2+SALAD, SAGE и SelaVPR++ adapters,
  batched/resumable embedding и normalized exact `faiss.IndexFlatIP` со stable
  ID mapping и SHA-256 sidecars;
- rank/sequence/density-aware spatial modes, weighted medoid и интерпретируемый
  logistic confidence score; модель fit только на development, threshold
  выбран только по независимой real calibration;
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

## Реальный Moscow run: исторический v2 acquisition

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

Исторический протокол `configs/moscow_real_v2_experiment_protocol.json` был записан до
экспериментов, а `configs/moscow_real_v2_frozen.json` — до единственного v2
test run. Test уже открыт и **не должен запускаться повторно**. Calibration не
нашла deployable candidate, прошедший material/stratum gate: SALAD дал
+5,37 п.п. R@20, но имеет региональные регрессии и остаётся evaluation-only;
MegaLoc five-crop дал только +1,39 п.п. R@20 при 2,43× median latency.

Исторический v2 contract: MegaLoc, exact normalized `IndexFlatIP`, single
query descriptor, K=50, weighted medoid, verification off. Wilson calibration
не смогла доказать нижнюю 95% границу precision >=90%: 16/16 calibration
answers дают только 80,64% lower bound. Поэтому frozen threshold равен `1.0`
и fail-closed v2 test ответил 0/497. Это ограничение, а не safety success.
Подробные таблицы v2 и v3 — в
[docs/evaluation_report.md](docs/evaluation_report.md).

## Финальный production operating point

Production использует SAGE ViT-B без cross-image encoder, exact K=30,
density-aware географическое голосование, sequence-deduplicated support и
weighted medoid. Exact Part 2.5 logistic confidence threshold
`0.9349250249145314` сохранён после того, как отдельный v4 catastrophic-risk
candidate не обобщился. Frozen contract:

- `configs/moscow_production_frozen.json`, SHA-256
  `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`;
- 20 487 gallery references, 751 development, 750 calibration и 1 499 test;
- единственный v4 test run: 127/1 499 answers (8,47%), 96,85% conditional
  <=100 м, 8,21% all-query answered-and-correct, 4 ошибки >100 м и 3 >500 м;
- accepted median/p90/p95 21,29/62,79/73,47 м; Wilson 95% <=100 м
  92,18–98,77%.

Это полезный, но ограниченный operating point: accepted >500 м rate 2,36% не
достигает желаемого <=1%. Localization core теперь закрыт для pre-production
tuning; оставшаяся работа — deployment-only Part 3.

## API и frontend

Backend запускается только через frozen runtime contract; конфликтующий
`RETRIEVER`, K, estimator, threshold или index metadata приводит к fail-fast:

```bash
make api GEOSNAP_RUNTIME_CONFIG=configs/moscow_production_frozen.json \
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

Для host smoke с реальным production index сначала дождитесь `/ready`, затем отправьте
реальное изображение через показанный выше `curl`; отдельный процесс API важен
на macOS из-за изоляции FAISS/PyTorch:

```bash
curl -sS http://localhost:8000/ready
curl -sS -X POST http://localhost:8000/localize \
  -F 'image=@path/to/street.jpg'
```

Поддерживаются JPEG/PNG/WebP. Upload проходит MIME/signature/decode,
decompression, dimensions, animation, EXIF orientation и RGB проверки.
Абсолютные пути, download URL и секреты API не выдаются клиенту.

## Выполненная host-проверка

На 2026-09-03 host runtime проверен с frozen v4 SAGE index:

- `/ready` вернул HTTP 200 после загрузки модели, 20 487-vector FAISS и metadata;
- реальный indexed-reference multipart `POST /localize` вернул HTTP 200,
  status `ok`, finite coordinates и 30 attributed matches;
- end-to-end benchmark p50/p90/p95 — 115,81/131,70/143,25 мс;
- persisted `index.faiss` — 692 296 749 bytes, SHA-256
  `cda9739cc7269881ece33f70de9c4b6bfb027b38150bd6c228fd736cc4270ad2`.

Это wiring/artifact smoke, не accuracy measurement. Публичная browser QA
относится к Part 3.

## Evaluation scope

V4 real Moscow evaluation разделяет provider sequences между gallery и тремя
query splits, исключает все historical v1/v2/v3 query sequences, требует
gallery positive в 100 м и применяет source/hash/pHash
leakage checks и 250 м geographic embargo между development, calibration и
test. Audit подтвердил ноль пересечений. V1/V2/V3 остаются immutable history;
их открытые tests не использовались для v4 tuning. `uncertainty_radius_m`
остаётся некалиброванным.

Wikimedia Commons proxy и любые MSLS-derived artifacts остаются
research/evaluation-only. Commons — маленький hand-curated landmark-biased
набор, а MSLS — benchmark/training data; ни один из них не является московской
street-view production gallery, не участвует в `prepare-moscow-gallery` или
`index-moscow-gallery` и не обосновывает city-wide product claim. Реальный
финальный single frozen v4 test описан в
[docs/final_localization_core.md](docs/final_localization_core.md); геометрическая
verification ablation — в [docs/verification_report.md](docs/verification_report.md).

## Docker Compose

Сначала должен существовать совместимый host-built v4 index в
`data/indexes/moscow_real_v4/sage-vitb/`. Проверка конфигурации и runtime:

```bash
make compose-config
docker compose --env-file .env config --quiet
make verify-moscow-production
```

Существующий `geosnap-backend:latest` размером около 17,5 GB устарел, а Docker
daemon во время финальной проверки был недоступен. Финальные зависимости
зафиксированы в `pyproject.toml`/`uv.lock`, Dockerfile копирует production
contract, а Compose статически его разрешает. Clean build, functional smoke и
slimming относятся к Part 3.

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
- v1/v2/v3/v4 final tests уже использованы по одному разу и не могут
  применяться для tuning или повторного frozen claim;
- production policy отвечает 8,47% eligible v4 queries при 96,85% conditional
  <=100 м, но 3/127 accepted errors >500 м означают, что preferred safety
  target не достигнут;
- verification остаётся optional/default-off: real 100-query Moscow ablation
  не дала accuracy/answer-rate gain и добавила median +1 002 мс;
- OSM standard tiles подходят для лёгкого demo, не для high-volume public
  deployment;
- stale container image не проверялся с финальным runtime; clean build,
  slimming и публичная browser QA остаются в Part 3.

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
