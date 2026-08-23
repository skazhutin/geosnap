"""Exercise the production model, index, and HTTP contract on one indexed image.

This is a wiring smoke test, not a held-out accuracy measurement: querying an
indexed reference deliberately verifies artifact/model compatibility and the
API path without making a localization-quality claim.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _first_reference(index_dir: Path) -> tuple[str, Path]:
    sidecar = index_dir / "reference_metadata.jsonl"
    if not sidecar.is_file():
        raise RuntimeError(f"missing index sidecar: {sidecar}")
    with sidecar.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            metadata = row.get("metadata") or {}
            raw_path = metadata.get("image_path")
            if raw_path and Path(str(raw_path)).is_file():
                return str(row["reference_id"]), Path(str(raw_path))
    raise RuntimeError("the index has no reference with an available local image")


def _index_configuration(index_dir: Path) -> tuple[str, str, str]:
    metadata = _read_json(index_dir / "index_metadata.json")
    retriever = (metadata.get("retriever") or {}).get("model_name")
    mapping = {"megaloc": "megaloc", "dinov2-salad": "dinov2-salad"}
    if retriever not in mapping:
        raise RuntimeError(f"index declares unsupported retriever {retriever!r}")
    city_id = metadata.get("city_id")
    index_id = metadata.get("index_id")
    if not city_id or not index_id:
        raise RuntimeError("index metadata is missing city_id/index_id")
    return mapping[retriever], str(city_id), str(index_id)


def run(
    index_dir: Path,
    *,
    query_image: Path | None = None,
    expected_status: str | None = None,
) -> dict[str, Any]:
    reference_id, indexed_image_path = _first_reference(index_dir)
    image_path = query_image or indexed_image_path
    if not image_path.is_file():
        raise RuntimeError(f"query image is missing: {image_path}")
    retriever, city_id, index_id = _index_configuration(index_dir)
    repository_root = Path(__file__).resolve().parents[2]
    backend_root = repository_root / "apps" / "backend"
    for import_root in (repository_root, backend_root):
        if str(import_root) not in sys.path:
            sys.path.insert(0, str(import_root))
    os.environ["GEOSNAP_INDEX_DIR"] = str(index_dir)
    os.environ["RETRIEVER"] = retriever
    os.environ["CITY_ID"] = city_id
    os.environ["INDEX_ID"] = index_id
    os.environ.setdefault(
        "GEOSNAP_LOCALIZATION_FACTORY",
        "ml.localization.service:create_localization_service",
    )

    # Environment must be fixed before importing Settings/app modules.
    from app.config import Settings
    from app.main import create_app
    from fastapi.testclient import TestClient

    media_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
    app = create_app(settings=Settings())
    with TestClient(app) as client:
        readiness = client.get("/ready")
        if readiness.status_code != 200:
            raise RuntimeError(f"service is not ready: {readiness.text}")
        with image_path.open("rb") as handle:
            response = client.post(
                "/localize",
                files={"image": (image_path.name, handle, media_type)},
                headers={"X-Request-ID": "real-production-smoke"},
            )
    if response.status_code != 200:
        raise RuntimeError(f"localize returned HTTP {response.status_code}: {response.text}")
    payload = response.json()
    allowed_statuses = (
        {expected_status}
        if expected_status is not None
        else ({"ok", "low_confidence"} if query_image is None else {"ok", "low_confidence", "out_of_coverage"})
    )
    if payload.get("status") not in allowed_statuses:
        raise RuntimeError(f"smoke returned {payload.get('status')!r}; expected {sorted(allowed_statuses)}")
    matches = payload.get("matches") or []
    if query_image is None and (not matches or matches[0].get("reference_id") != reference_id):
        raise RuntimeError("top-1 ID does not match the queried index row")
    return {
        "status": payload["status"],
        "query_kind": "indexed_reference" if query_image is None else "external_image",
        "query_reference_id": reference_id if query_image is None else None,
        "top1_reference_id": matches[0]["reference_id"] if matches else None,
        "retriever": payload.get("diagnostics", {}).get("retriever"),
        "embedding_ms": payload.get("diagnostics", {}).get("embedding_ms"),
        "retrieval_ms": payload.get("diagnostics", {}).get("retrieval_ms"),
        "verification_ms": payload.get("diagnostics", {}).get("verification_ms"),
        "total_ms": payload.get("diagnostics", {}).get("total_ms"),
        "note": "indexed-reference wiring smoke; not a held-out accuracy metric",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a real production localization smoke")
    parser.add_argument("--index-dir", type=Path, required=True)
    parser.add_argument("--query-image", type=Path)
    parser.add_argument(
        "--expected-status",
        choices=("ok", "low_confidence", "out_of_coverage"),
    )
    args = parser.parse_args()
    try:
        result = run(
            args.index_dir,
            query_image=args.query_image,
            expected_status=args.expected_status,
        )
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
