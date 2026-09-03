FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN python -m pip install --no-cache-dir uv==0.11.19 \
    && uv sync --locked --no-dev --no-install-project

ENV PATH="/app/.venv/bin:${PATH}"

COPY apps/backend/app ./app
COPY ml ./ml
COPY configs/moscow_production_frozen.json ./configs/moscow_production_frozen.json
COPY configs/moscow_production_frozen.sha256 ./configs/moscow_production_frozen.sha256
COPY configs/moscow_production_smoke_set.json ./configs/moscow_production_smoke_set.json
COPY configs/moscow_real_v3_confidence_model.json ./configs/moscow_real_v3_confidence_model.json

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
