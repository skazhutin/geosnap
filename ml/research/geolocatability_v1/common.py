"""Write-once paths and validation shared by the annotation stages."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/geolocatability_v1_20260928"
QUERY = ROOT / "data/evaluation/moscow_research_v5/prospective/development.parquet"
TECHNICAL = ROOT / "data/evaluation/geographic_v9_20260928/technical_quality/image_features.npz"
PRODUCTION_GUARD = ROOT / "data/evaluation/geographic_v8_20260928/production_before.json"
SCHEMA_VERSION = "geolocatability-v1.0"
PROMPT_VERSION = "qwen35-full-json-three-paraphrases-v3.0"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_once(path: Path, data: bytes) -> None:
    """Publish complete bytes atomically, refusing to replace any evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".pending-", delete=False) as f:
        tmp = Path(f.name)
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.link(tmp, path)  # atomic creation; fails if another run already wrote this path
    finally:
        tmp.unlink(missing_ok=True)


def json_once(path: Path, value: object) -> None:
    write_once(path, (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode())


def now() -> str:
    return datetime.now(UTC).isoformat()


def revision() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def verify_production() -> dict:
    expected = json.loads(PRODUCTION_GUARD.read_text())["files"]
    changed = [name for name, old in expected.items() if sha256(ROOT / name) != old]
    if changed:
        raise RuntimeError(f"Frozen production changed: {changed}")
    return {"verified": True, "guarded_files": len(expected), "guard_sha256": sha256(PRODUCTION_GUARD), "checked_at": now()}
