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

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to validate the readiness payload" >&2
  exit 1
fi

cleanup() {
  docker compose stop backend db >/dev/null 2>&1 || true
  docker compose rm -f backend db >/dev/null 2>&1 || true
}
trap cleanup EXIT

ready_url="http://localhost:8000/ready"

backend_is_ready() {
  curl -fsS --connect-timeout 2 --max-time 5 "$ready_url" |
    python3 -c '
import json
import sys

payload = json.load(sys.stdin)
if payload.get("ready") is not True:
    raise SystemExit(1)
json.dump(payload, sys.stdout, sort_keys=True)
sys.stdout.write("\n")
'
}

echo "[1/4] Building and starting db + backend"
docker compose up -d --build db backend

echo "[2/4] Waiting for backend readiness endpoint"
for i in {1..30}; do
  if backend_is_ready >/dev/null 2>&1; then
    echo "Backend is ready"
    break
  fi
  sleep 2
  if [[ "$i" -eq 30 ]]; then
    echo "Backend readiness check failed" >&2
    docker compose logs backend db >&2 || true
    exit 1
  fi
done

echo "[3/4] Readiness payload"
backend_is_ready

echo
echo "[4/4] Done"
