"""Immutable v9 input contracts and append-only research receipts."""
from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime

import pandas as pd

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.geographic_v8.common import FROZEN, QUERY

V8 = WORKSPACE / "data/evaluation/geographic_v8_20260928"
OUT = WORKSPACE / "data/evaluation/geographic_v9_20260928"
SOURCE = WORKSPACE / "ml/research/geographic_v9"
GALLERY = V8 / "staged/gallery.parquet"


def initialize():
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "experiments").mkdir(exist_ok=True)


def production_guard(stage):
    if stage not in {"before", "after"}:
        raise ValueError("Production stage must be before or after")
    initialize()
    original = V8 / "production_before.json"
    expected = json.loads(original.read_text())["files"]
    actual = {name: digest(WORKSPACE / name) for name in expected}
    changed = [name for name, sha in expected.items() if actual[name] != sha]
    if changed:
        raise RuntimeError(f"Frozen production file changed: {changed}")
    result = {"stage": stage, "verified": True, "files": len(actual),
        "source_receipt_sha256": digest(original), "hashes": actual,
        "verified_at": datetime.now(UTC).isoformat()}
    path = OUT / f"production_{stage}.json"
    if path.exists():
        old = json.loads(path.read_text())
        if old["hashes"] != actual:
            raise RuntimeError("Production guard changed within v9 cycle")
        return old
    save(path, result)
    return result


def frames():
    frozen = json.loads(FROZEN.read_text())
    if digest(GALLERY) != frozen["candidate"]["gallery_sha256"]:
        raise RuntimeError("Staged fixed gallery differs from v8 frozen manifest")
    if digest(QUERY) != frozen["query_sha256"]:
        raise RuntimeError("Development query population differs from v8 frozen manifest")
    q, g = pd.read_parquet(QUERY), pd.read_parquet(GALLERY)
    if len(q) != 1184 or len(g) != 112163:
        raise RuntimeError("Wrong frozen population")
    if q.id.duplicated().any() or g.id.duplicated().any():
        raise RuntimeError("Duplicate query/reference identity")
    return q, g


def record(identifier, fields):
    initialize()
    path = OUT / "experiments" / f"{identifier}.json"
    if path.exists():
        raise FileExistsError(f"Immutable experiment record already exists: {path}")
    result = {"id": identifier, "date": datetime.now(UTC).isoformat(),
        "code_revision": subprocess.check_output(["git", "rev-parse", "HEAD"],
            cwd=WORKSPACE, text=True).strip(),
        "source_hashes": {p.name: digest(p) for p in sorted(SOURCE.glob("*.py"))},
        "query_manifest_sha256": digest(QUERY), "gallery_manifest_sha256": digest(GALLERY),
        "production_guard_before_sha256": digest(OUT / "production_before.json"), **fields}
    save(path, result)
    return result
