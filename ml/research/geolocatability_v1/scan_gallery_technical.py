"""Resumable, read-only technical diagnostics for the fixed research gallery.

This records pixels-derived measurements only. It never changes gallery membership
or assigns a semantic geolocatability/retake label.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ml.research.geographic_v9.technical_quality import FEATURES, measure

ROOT = Path(__file__).resolve().parents[3]
GALLERY = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"
OUT = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
CHUNK = 500


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            result.update(block)
    return result.hexdigest()


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_atomic(path: Path, payload: bytes) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    with temporary.open("xb") as target:
        target.write(payload)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)


def main(limit: int | None) -> None:
    gallery_sha = digest(GALLERY)
    frozen = json.loads((ROOT / "data/evaluation/moscow_night_v7/candidate_frozen.json").read_text())
    assert gallery_sha == frozen["candidate"]["gallery_sha256"]
    frame = pd.read_parquet(GALLERY, columns=["id", "source", "image_path", "file_sha256"])
    assert len(frame) == frame.id.nunique() == 112163
    OUT.mkdir(parents=True, exist_ok=True)
    chunks = OUT / "chunks"
    chunks.mkdir(exist_ok=True)
    contract = {"gallery_sha256": gallery_sha, "gallery_count": len(frame),
                "code_sha256": digest(Path(__file__)), "features": FEATURES,
                "chunk_size": CHUNK, "pixel_only": True,
                "no_query_gt_or_localization_outcomes": True,
                "decisions_or_deletions": False}
    contract_path = OUT / "contract.json"
    if contract_path.exists():
        assert json.loads(contract_path.read_text()) == contract
    else:
        write_atomic(contract_path, (json.dumps(contract, indent=2) + "\n").encode())

    completed = 0
    started = time.monotonic()
    for start in range(0, len(frame), CHUNK):
        if limit is not None and start >= limit:
            break
        end = min(start + CHUNK, len(frame))
        target = chunks / f"{start:06d}-{end-1:06d}.jsonl"
        checksum = target.with_suffix(".sha256")
        expected_ids = frame.id.iloc[start:end].tolist()
        if target.exists() and checksum.exists():
            assert digest(target) == checksum.read_text().strip()
            stored = [json.loads(line)["id"] for line in target.read_text().splitlines()]
            assert stored == expected_ids
            completed += len(stored)
            continue
        if target.exists() or checksum.exists():
            raise RuntimeError(f"Incomplete chunk receipt: {target}")
        records = []
        missing_streak = 0
        for row in frame.iloc[start:end].itertuples(index=False):
            path = Path(row.image_path)
            if not path.is_file():
                missing_streak += 1
                if missing_streak >= 5:
                    raise RuntimeError("Five consecutive gallery images unavailable; stop before recording an unmounted disk as image defects")
                records.append({"id": row.id, "source": row.source, "image_path": row.image_path,
                                "manifest_image_sha256": row.file_sha256,
                                "status": "source_unavailable", "features": None})
                continue
            missing_streak = 0
            try:
                values = measure(path)
                record = {"id": row.id, "source": row.source, "image_path": row.image_path,
                          "manifest_image_sha256": row.file_sha256, "status": "ok",
                          "features": dict(zip(FEATURES, map(float, values), strict=True))}
            except (OSError, ValueError, RuntimeError) as error:
                record = {"id": row.id, "source": row.source, "image_path": row.image_path,
                          "manifest_image_sha256": row.file_sha256,
                          "status": "decode_or_measure_error", "error": str(error)[:300], "features": None}
            records.append(record)
        payload = ("\n".join(json.dumps(record, ensure_ascii=False, sort_keys=True)
                             for record in records) + "\n").encode()
        write_atomic(target, payload)
        write_atomic(checksum, (digest(target) + "\n").encode())
        completed += len(records)
        elapsed = time.monotonic() - started
        print(json.dumps({"at": utc(), "complete": completed, "total": len(frame),
                          "last_chunk": [start, end], "elapsed_s": round(elapsed, 1),
                          "recent_images_per_s": round(len(records) / max(elapsed, 1), 2) if completed == len(records) else None}),
              flush=True)
    print(json.dumps({"status": "limit_reached" if limit is not None else "scan_complete",
                      "complete": completed, "total": len(frame), "at": utc()}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    main(parser.parse_args().limit)
