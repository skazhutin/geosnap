#!/usr/bin/env python3
"""Verify a running GeoSnap production Compose deployment end to end."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
FROZEN_CONFIG = ROOT / "configs/moscow_production_frozen.json"
FROZEN_SHA256 = "9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e"
COMPOSE = ("docker", "compose", "--env-file", ".env", "-f", "docker-compose.prod.yml")
_ASSET = re.compile(r'(?:src|href)="(/assets/[^"]+)"')


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run(*args: str, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        args,
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _compose(*args: str) -> str:
    return _run(*COMPOSE, *args)


def _request(url: str) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, headers={"X-Request-ID": "production-verifier"})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return (
                response.status,
                {key.lower(): value for key, value in response.headers.items()},
                response.read(),
            )
    except urllib.error.HTTPError as exc:
        return exc.code, {key.lower(): value for key, value in exc.headers.items()}, exc.read()


def _json_get(url: str) -> tuple[int, dict[str, str], dict[str, Any]]:
    status, headers, body = _request(url)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{url} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{url} did not return a JSON object")
    return status, headers, payload


_CONTAINER_HTTP_SMOKE = r"""
import json, os, pathlib, uuid, urllib.request
path=pathlib.Path(os.environ['GEOSNAP_ARTIFACT_DIR'])/'smoke/ffcd4860-f8ec-5a76-ab11-91e4f291f916.jpg'
data=open(path,'rb').read()
boundary='geosnap-'+uuid.uuid4().hex
body=(f'--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="smoke.jpg"\r\nContent-Type: image/jpeg\r\n\r\n'.encode()+data+f'\r\n--{boundary}--\r\n'.encode())
request=urllib.request.Request('http://127.0.0.1:8000/localize', data=body, method='POST', headers={'Content-Type':f'multipart/form-data; boundary={boundary}','X-Request-ID':'production-real-smoke'})
with urllib.request.urlopen(request,timeout=120) as response:
    payload=json.load(response)
assert response.status == 200
assert payload['status'] == 'ok'
assert len(payload['matches']) == 30
assert all(payload['prediction'][key] == payload['prediction'][key] for key in ('lat','lon'))
assert all(match.get('attribution') for match in payload['matches'])
print(json.dumps({'status':payload['status'],'match_count':len(payload['matches']),'request_id':payload['request_id'],'timings_ms':payload['diagnostics']}))
"""


def run(base_url: str) -> dict[str, Any]:
    if _sha256(FROZEN_CONFIG) != FROZEN_SHA256:
        raise RuntimeError("frozen production configuration hash changed")
    _compose("config", "--quiet")

    artifact_report = json.loads(
        _compose(
            "exec",
            "-T",
            "backend",
            "python",
            "-m",
            "ml.artifacts.verify_production_artifacts",
            "--manifest",
            "/app/configs/production_artifacts.json",
            "--include-optional",
        )
    )
    http_smoke = json.loads(
        _compose("exec", "-T", "backend", "python", "-c", _CONTAINER_HTTP_SMOKE)
    )

    health_status, health_headers, health = _json_get(f"{base_url}/api/health")
    ready_status, ready_headers, ready = _json_get(f"{base_url}/api/ready")
    if health_status != 200 or health.get("status") != "ok":
        raise RuntimeError("production liveness check failed")
    if ready_status != 200 or ready.get("ready") is not True:
        raise RuntimeError("production readiness check failed")
    if ready.get("production_config_sha256") != FROZEN_SHA256[:12]:
        raise RuntimeError("readiness returned an unexpected frozen-config identity")
    identity = ready.get("runtime")
    expected_identity = {
        "retriever": "sage-vitb",
        "source_revision": "c7d6241c4885526d99d6c78c158024fc2a37097c",
        "checkpoint_revision": "2a2ea9964cdbdfd2211e7c625064a9d5e4678245",
        "checkpoint_sha256": "8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e",
        "index_id": "moscow-real-v4-sage-vitb",
        "faiss_index_type": "IndexFlatIP",
        "gallery_count": 20487,
        "descriptor_dimension": 8448,
        "top_k": 30,
        "query_aggregation": "single",
        "geographic_aggregation": "density_aware_mode_vote",
        "coordinate_estimator": "weighted_medoid",
        "confidence_feature_count": 14,
        "confidence_threshold": 0.9349250249145314,
        "reranking_enabled": False,
        "approximate_tier_enabled": False,
    }
    if identity != expected_identity:
        raise RuntimeError("readiness returned an unexpected frozen runtime identity")
    if health_headers.get("x-request-id") != "production-verifier":
        raise RuntimeError("request ID was not propagated")

    root_status, root_headers, html = _request(f"{base_url}/")
    if root_status != 200 or b'id="root"' not in html:
        raise RuntimeError("production frontend did not load")
    csp = root_headers.get("content-security-policy", "")
    if "frame-ancestors 'none'" not in csp:
        raise RuntimeError("production security headers are incomplete")
    asset_paths = sorted(set(_ASSET.findall(html.decode("utf-8"))))
    if not asset_paths:
        raise RuntimeError("production frontend contains no built assets")
    assets = b""
    for asset_path in asset_paths:
        status, headers, body = _request(f"{base_url}{asset_path}")
        if status != 200 or "immutable" not in headers.get("cache-control", ""):
            raise RuntimeError(f"frontend asset failed cache/load validation: {asset_path}")
        assets += body
    if b"localhost:8000" in assets or b"localhost:3000" in assets:
        raise RuntimeError("localhost API URL leaked into the production frontend")

    metrics_status, _, _ = _request(f"{base_url}/api/metrics")
    if metrics_status != 404:
        raise RuntimeError("metrics are exposed by the public proxy")
    metrics = _compose(
        "exec",
        "-T",
        "backend",
        "python",
        "-c",
        "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/metrics').read().decode())",
    )
    if "geosnap_backend_ready" not in metrics or "geosnap_localization_requests_total" not in metrics:
        raise RuntimeError("required operational metrics are absent")

    bot_env = os.environ.copy()
    bot_env["TELEGRAM_VALIDATE_ONLY"] = "true"
    bot_env.setdefault("TELEGRAM_BOT_TOKEN", "")
    _run(
        *COMPOSE,
        "run",
        "--rm",
        "-e",
        "TELEGRAM_VALIDATE_ONLY=true",
        "telegram-bot",
        "python",
        "-m",
        "apps.telegram_bot",
        "--validate-config",
        env=bot_env,
    )

    return {
        "status": "ok",
        "frozen_config_sha256": FROZEN_SHA256,
        "artifact_manifest": artifact_report,
        "model_identity": identity,
        "readiness": ready,
        "real_http_localize": http_smoke,
        "frontend_assets": asset_paths,
        "security_headers": True,
        "metrics_internal_only": True,
        "telegram_configuration": "valid_without_network_contact",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("GEOSNAP_PUBLIC_URL", "http://localhost:8080").rstrip("/"),
    )
    args = parser.parse_args()
    print(json.dumps(run(args.base_url), indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"production verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
