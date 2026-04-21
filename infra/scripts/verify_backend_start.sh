#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required but was not found" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required but was not found in PATH" >&2
  exit 1
fi

cleanup() {
  docker compose stop backend db >/dev/null 2>&1 || true
  docker compose rm -f backend db >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[1/4] Building and starting db + backend"
docker compose up -d --build db backend

echo "[2/4] Waiting for backend health endpoint"
for i in {1..30}; do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then
    echo "Backend is healthy"
    break
  fi
  sleep 2
  if [[ "$i" -eq 30 ]]; then
    echo "Backend health check failed" >&2
    docker compose logs backend db >&2 || true
    exit 1
  fi
done

echo "[3/4] Health payload"
curl -fsS http://localhost:8000/health

echo
echo "[4/4] Done"
