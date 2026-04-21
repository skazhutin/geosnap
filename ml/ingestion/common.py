"""Shared ingestion utilities."""

from __future__ import annotations

import json
import logging
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
    if destination.exists() and destination.stat().st_size > 0:
        return True

    ensure_parent(destination)
    if session is None:
        import requests

        client = requests.Session()
    else:
        client = session

    for attempt in range(1, retries + 1):
        try:
            with client.get(url, stream=True, timeout=timeout_sec) as response:
                response.raise_for_status()
                with destination.open("wb") as fp:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            fp.write(chunk)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("download failed (%s/%s): %s -> %s", attempt, retries, url, exc)
            if attempt == retries:
                return False
            time.sleep(backoff_sec * attempt)
    return False
