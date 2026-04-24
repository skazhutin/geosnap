# GeoPhoto — визуальная геолокация по фотографии

GeoPhoto — это веб-сервис, который определяет предполагаемое местоположение пользователя по фотографии городской среды без использования GPS.

Пользователь загружает фото, система ищет наиболее похожие geotagged изображения в reference-базе Москвы, после чего возвращает:
- точку на карте;
- top-3 наиболее похожих reference images;
- оценку уверенности в результате.

## Зачем нужен проект

В городской среде GPS может работать неточно или быть недоступным: во дворах, среди плотной застройки, в новых жилых комплексах, подземных переходах и других сложных локациях.

Идея проекта — использовать не спутниковый сигнал, а визуальную информацию с фотографии, чтобы помочь пользователю определить, где он находится.

## Что делает сервис

На вход:
- фотография городской среды.

На выход:
- предполагаемое местоположение на карте;
- 3 наиболее похожих изображения из reference-базы;
- confidence score;
- краткое объяснение результата.

## Как это работает

Продукт построен как retrieval-based visual geolocation system.

Пайплайн:
1. Пользователь загружает изображение.
2. Сервис выполняет preprocessing и проверку качества.
3. Для изображения строится embedding.
4. По embedding выполняется поиск похожих изображений в reference-базе Москвы.
5. По найденным кандидатам вычисляется итоговая точка.
6. Результат отображается на интерактивной карте.

## Архитектура

Основные компоненты:

- **Frontend** — интерфейс загрузки изображения и отображения результата.
- **Backend API** — принимает запросы, запускает inference pipeline, возвращает результат.
- **Reference database** — geotagged изображения Москвы и их метаданные.
- **Vector index** — индекс embeddings для быстрого nearest-neighbor поиска.
- **Localization pipeline** — preprocessing, retrieval, оценка координат и confidence.

## Стек

- **Frontend:** React, MapLibre
- **Backend:** FastAPI
- **Database:** PostgreSQL + PostGIS
- **Vector search:** FAISS
- **Geospatial indexing:** H3
- **ML / Retrieval:** DINOv2 + SALAD
- **Reference data:** Mapillary, KartaView

## Структура проекта

```text
apps/
  backend/        # API и серверная логика
  frontend/       # веб-интерфейс

ml/
  ingestion/      # загрузка и нормализация reference-данных
  cleaning/       # очистка и фильтрация данных
  embeddings/     # построение embeddings
  index/          # создание и использование FAISS индекса
  evaluation/     # метрики и тестирование

data/
  manifests/      # manifests и служебные таблицы
  samples/        # примеры данных
  evaluation/     # тестовые выборки

infra/
  db/             # инициализация БД
  scripts/        # инфраструктурные скрипты
```

## Запуск проекта

### 1) Подготовка переменных окружения

Создайте `.env` в корне проекта (или отредактируйте существующий):

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=geo
POSTGRES_PORT=5432
DATABASE_URL=postgresql+psycopg2://postgres:postgres@db:5432/geo
```

### 2) Запуск в Docker Compose

Из корня репозитория:

```bash
docker compose up --build
```

Сервисы будут доступны:

- Backend API: `http://localhost:8000`
- Backend health: `http://localhost:8000/health`
- Frontend: `http://localhost:3000`

Остановка:

```bash
docker compose down
```

### 3) Быстрая проверка старта backend + db

Для автоматической проверки используйте скрипт:

```bash
infra/scripts/verify_backend_start.sh
```

Скрипт:
- поднимает `db` и `backend`;
- ждёт успешный ответ `GET /health`;
- печатает health-ответ;
- по завершении останавливает и удаляет только контейнеры `db` и `backend`, не затрагивая тома и другие сервисы Compose-проекта.
## Локальный запуск проверок CI

### Тесты / базовые проверки runtime

```bash
cd apps/frontend
npm ci
npm run build

cd ../..
python -m pip install -r apps/backend/requirements.txt
PYTHONPATH=apps/backend python -c "from app.main import app; assert app.title == 'GeoSnap API'"
```

### Smoke check запуска проекта

```bash
docker compose up -d --build
timeout 120 bash -c 'until curl -fsS http://127.0.0.1:8000/health | grep -q "\"status\":\"ok\""; do sleep 2; done'
docker compose down -v
```

## Обязательный CI check перед merge

1. Откройте `Settings` → `Branches` → `Branch protection rules`.
2. Создайте/измените правило для default branch (`main`).
3. Включите `Require status checks to pass before merging`.
4. Выберите check `CI / test-and-smoke`.

## Этап 2: Ingestion данных (Mapillary + KartaView)

Ниже — минимальная инструкция, как запустить ingestion по Москве и получить единый `manifest.parquet`.

### 1) Подготовить окружение

```bash
python -m pip install -r apps/backend/requirements.txt
python -m pip install requests pillow pandas pyarrow
```

Для Mapillary задайте токен:

```bash
export MAPILLARY_ACCESS_TOKEN=YOUR_TOKEN
```

### 2) Загрузить metadata по тайлам Москвы

Mapillary (только metadata + URL):

```bash
python -m ml.ingestion.mapillary_loader --output-json data/raw/mapillary_raw.json --request-pause-sec 0.25 --request-retries 5 --backoff-sec 1.5 --max-pages-per-tile 200
```

KartaView (только metadata + URL):

```bash
python -m ml.ingestion.kartaview_loader --output-json data/raw/kartaview_raw.json --request-pause-sec 0.25 --request-retries 5 --backoff-sec 1.5 --max-pages-per-tile 200
```

> Загрузчики не скачивают изображения: они сохраняют только metadata + `image_url`, проходят пагинацию API и ограничены `--max-pages-per-tile` для защиты от бесконечных циклов.

### 3) Объединить источники в единый manifest

```bash
python -m ml.ingestion.merge_sources --mapillary-json data/raw/mapillary_raw.json --kartaview-json data/raw/kartaview_raw.json --output-manifest data/raw/manifest.parquet --dedup-radius-m 7 --max-per-cluster 2
```

`merge_sources` добавляет в manifest поле `download_url` и выполняет spatial deduplication между источниками (по умолчанию радиус 7 м, максимум 2 фото на кластер).

### 4) Скачать изображения из manifest

```bash
python -m ml.ingestion.download_images --manifest data/raw/manifest.parquet --errors-log data/raw/download_errors.log --retries 3
```

### 5) Проверить качество датасета

```bash
python -m ml.ingestion.validate_dataset --manifest data/raw/manifest.parquet --report data/raw/validation_report.json
```

### 6) Sanity preview (20 случайных изображений)

```bash
python -m ml.ingestion.preview --manifest data/raw/manifest.parquet --count 20 --output-image data/raw/preview.jpg
```

В результате вы получаете:

- изображения в `data/raw/images/mapillary/` и `data/raw/images/kartaview/`;
- единый `data/raw/manifest.parquet`;
- отчёт качества `data/raw/validation_report.json`;
- визуальный превью-лист `data/raw/preview.jpg`.

## Этап 3: Cleaning + H3 + manifest для retrieval

Минимальный прогон пайплайна:

```bash
python -m ml.cleaning.clean_images --manifest data/raw/manifest.parquet --output data/processed/manifest_step1.parquet
python -m ml.cleaning.quality_filter --manifest data/processed/manifest_step1.parquet --output data/processed/manifest_step2.parquet --min-width 224 --min-height 224
python -m ml.cleaning.deduplicate --manifest data/processed/manifest_step2.parquet --output data/processed/manifest_step3.parquet --dedup-radius-m 15 --max-per-geo-point 5
python -m ml.enrichment.h3_assign --manifest data/processed/manifest_step3.parquet --output data/processed/manifest_step4.parquet --coarse-resolution 6 --fine-resolution 9
python -m ml.cleaning.build_final_manifest --manifest data/processed/manifest_step4.parquet --output data/processed/manifest_clean.parquet
python -m ml.cleaning.check_dataset --manifest data/processed/manifest_clean.parquet
```

Ключевые выходы:
- `data/processed/manifest_clean.parquet`
- отчёты в `data/processed/reports/`
- sanity-визуализации (`dataset_scatter.png`, `dataset_preview_20.jpg`).
