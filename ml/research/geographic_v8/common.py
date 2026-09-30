"""Research contracts, inference-only inputs, and append-only experiment records."""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.vector_evaluation import distances

LOCAL = WORKSPACE / "data/evaluation/geographic_v8_20260928"
FROZEN = WORKSPACE / "data/evaluation/moscow_night_v7/candidate_frozen.json"
NIGHT = Path(json.loads(FROZEN.read_text())["candidate"]["gallery_manifest"]).parent.parent
ROOT = NIGHT.parent / "geographic_v8_20260928"
GALLERY = NIGHT / "reference_tranche3/gallery.parquet"
QUERY = WORKSPACE / "data/evaluation/moscow_research_v5/prospective/development.parquet"


def initialize():
    LOCAL.mkdir(parents=True, exist_ok=True)
    ROOT.mkdir(parents=True, exist_ok=True)


def guard(stage):
    initialize()
    old = json.loads((WORKSPACE / "data/evaluation/moscow_research_v5/baseline_snapshot.json").read_text())["files"]
    # Include public API/runtime code as well as the historical 42-file guard.
    extra = [p for directory in ("apps/backend/app", "apps/telegram_bot", "configs")
             for p in (WORKSPACE / directory).rglob("*")
             if p.is_file() and p.suffix in {".py", ".json", ".sha256"}]
    expected = dict(old)
    before = LOCAL / "production_before.json"
    if before.exists():
        expected = json.loads(before.read_text())["files"]
    actual = {p: digest(WORKSPACE / p) for p in expected}
    changed = [p for p in expected if actual[p] != expected[p]]
    if changed:
        raise RuntimeError(f"Frozen production mismatch: {changed}")
    if not before.exists():
        actual.update({str(p.relative_to(WORKSPACE)): digest(p) for p in extra})
    result = {"stage": stage, "verified": True, "historical_files": len(old), "files": actual,
              "date": datetime.now(timezone.utc).isoformat()}
    save(LOCAL / f"production_{stage}.json", result)
    return {"verified": True, "files": len(actual), "historical_files": len(old)}


def frames():
    frozen = json.loads(FROZEN.read_text())
    if digest(GALLERY) != frozen["candidate"]["gallery_sha256"] or digest(QUERY) != frozen["query_sha256"]:
        raise RuntimeError("Frozen gallery/query manifest changed")
    q, g = pd.read_parquet(QUERY), pd.read_parquet(GALLERY)
    if len(q) != 1184 or len(g) != 112163 or q.id.duplicated().any() or g.id.duplicated().any():
        raise RuntimeError("Wrong research population")
    return q, g


def image_inputs():
    """Only these columns may cross the inference boundary; no query GPS or H3."""
    return pd.read_parquet(QUERY, columns=["id", "image_path", "file_sha256"])


def topk(scores, k=100):
    # Full stable sort preserves manifest-order ties, including duplicate GPS.
    return np.argsort(-scores, axis=1, kind="stable")[:, :k]


def metrics(q, g, indices=None, gps=None):
    if indices is not None:
        if indices.shape[0] != len(q) or not np.issubdtype(indices.dtype, np.integer):
            raise ValueError("Candidate identities must align with every query")
        if (indices < 0).any() or (indices >= len(g)).any():
            raise ValueError("Invalid gallery row")
        coords = g[["lat", "lon"]].to_numpy()[indices]
    else:
        coords = np.asarray(gps, dtype=float)[:, None, :]
    if coords.shape[0] != len(q) or not np.isfinite(coords).all():
        raise ValueError("One finite prediction per query required")
    d = np.stack([distances(row.lat, row.lon, np.radians(coords[i, :, 0]), np.radians(coords[i, :, 1]))
                  for i, row in enumerate(q.itertuples())])
    e = d[:, 0]
    raw = raw_metrics(e) | {f"accuracy_{r}m": float((e <= r).mean()) for r in (500, 1000, 5000)}
    recall = {str(k): float((d[:, :k] <= 100).any(axis=1).mean()) for k in (1, 5, 10, 20, 50, 100)
              if indices is not None and k <= indices.shape[1]}
    return {"raw": raw, "recall_at": recall}, e, d


def registry(identifier, record):
    """Each ID has one immutable record; old evidence is never replaced."""
    initialize()
    path = LOCAL / "experiments" / f"{identifier}.json"
    record = {"id": identifier, "date": datetime.now(timezone.utc).isoformat(),
              "code_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=WORKSPACE, text=True).strip(),
              "code_hashes": {p.name: digest(p) for p in Path(__file__).parent.glob("*.py")},
              "gallery": {"path": str(GALLERY), "sha256": digest(GALLERY), "count": 112163,
                          "msls": "research-only"},
              "query_set": {"path": str(QUERY), "sha256": digest(QUERY), "count": 1184},
              "evaluation_split": "reused exploratory development; not independent final",
              "fitted": bool(record.get("fitted_parameters", False)), "fitting_split": None, **record}
    if path.exists():
        raise FileExistsError(f"Experiment already recorded: {path}")
    save(path, record)
    return record


def compare(q, errors, distances_to_candidates=None):
    base = np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False)
    old = base["errors_m"] <= 100
    correct = errors <= 100
    buckets = base["buckets"]
    answer = {"transitions": {
        "old_wrong_new_correct": int((~old & correct).sum()),
        "old_correct_new_wrong": int((old & ~correct).sum()),
        "old_wrong_new_wrong": int((~old & ~correct).sum()),
        "old_correct_new_correct": int((old & correct).sum())},
        "buckets": {name: {"count": int((buckets == name).sum()),
                             "new_correct": int(((buckets == name) & correct).sum())}
                    for name in np.unique(buckets)},
        "paired_geographic_bootstrap": paired_group_bootstrap(base["errors_m"], errors, q.h3_coarse.tolist())}
    if distances_to_candidates is not None:
        for name, item in answer["buckets"].items():
            item["retrievable_100m"] = int(((buckets == name) & (distances_to_candidates <= 100).any(axis=1)).sum())
    return answer
