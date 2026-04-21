"""Shared ingestion utilities."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def download_file(
    url: str,
    destination: Path,
    *,
    timeout_sec: int = 30,
    retries: int = 3,
    backoff_sec: float = 1.5,
    session: Any | None = None,
) -> bool:
    """Download URL to destination with retry and skip-if-exists behavior."""
    if retries < 1:
        raise ValueError("retries must be >= 1")
    if destination.exists() and destination.stat().st_size >= 10_000:
        return True

    ensure_parent(destination)
    created_session = False
    if session is None:
        import requests

        client = requests.Session()
        created_session = True
    else:
        client = session

    try:
        for attempt in range(1, retries + 1):
            tmp_path = destination.with_suffix(destination.suffix + ".part")
            try:
                with client.get(url, stream=True, timeout=timeout_sec) as response:
                    response.raise_for_status()
                    with tmp_path.open("wb") as fp:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                fp.write(chunk)
                os.replace(tmp_path, destination)
                return True
            except Exception as exc:  # noqa: BLE001
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                logger.warning("download failed (%s/%s): %s -> %s", attempt, retries, url, exc)
                if attempt == retries:
                    if destination.exists() and destination.stat().st_size < 10_000:
                        destination.unlink(missing_ok=True)
                    return False
                time.sleep(backoff_sec * attempt)
        return False
    finally:
        if created_session:
            client.close()
