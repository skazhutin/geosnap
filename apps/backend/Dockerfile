FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy

RUN python -m pip install --no-cache-dir uv==0.11.19
WORKDIR /build
COPY apps/backend/requirements-prod.lock ./requirements-prod.lock
RUN uv venv /opt/venv \
    && uv pip install --python /opt/venv/bin/python --require-hashes -r requirements-prod.lock

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=4 \
    MKL_NUM_THREADS=4

RUN apt-get update \
    && apt-get install --no-install-recommends --yes libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system geosnap \
    && useradd --system --gid geosnap --home-dir /app --shell /usr/sbin/nologin geosnap

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY apps/backend/app ./app
COPY ml ./ml
COPY configs/moscow_production_frozen.json ./configs/moscow_production_frozen.json
COPY configs/moscow_production_frozen.sha256 ./configs/moscow_production_frozen.sha256
COPY configs/moscow_production_smoke_set.json ./configs/moscow_production_smoke_set.json
COPY configs/moscow_real_v3_confidence_model.json ./configs/moscow_real_v3_confidence_model.json
COPY configs/production_artifacts.json ./configs/production_artifacts.json

USER geosnap
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log", "--proxy-headers", "--forwarded-allow-ips=*"]
