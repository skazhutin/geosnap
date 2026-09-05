FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy

RUN python -m pip install --no-cache-dir uv==0.11.19
WORKDIR /build
COPY apps/telegram_bot/requirements.lock ./requirements.lock
RUN uv venv /opt/venv \
    && uv pip install --python /opt/venv/bin/python --require-hashes -r requirements.lock

FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system geosnap \
    && useradd --system --gid geosnap --home-dir /app --shell /usr/sbin/nologin geosnap

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY apps/telegram_bot ./apps/telegram_bot

USER geosnap
CMD ["python", "-m", "apps.telegram_bot"]
