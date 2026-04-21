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
