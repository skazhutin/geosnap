"""Bounded, restart-safe image downloader for a canonical manifest."""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import requests

from ml.ingestion.common import (
    DEFAULT_MAX_DOWNLOAD_BYTES,
    DEFAULT_MAX_IMAGE_DIMENSION,
    DEFAULT_MAX_IMAGE_PIXELS,
    DownloadResult,
    download_file_detailed,
    redact_url,
    write_json,
)
from ml.ingestion.schema import (
    CANONICAL_COLUMNS,
    ManifestSchemaError,
    coerce_manifest_schema,
    validate_manifest_schema,
)

logger = logging.getLogger(__name__)

SOURCE_DOWNLOAD_HOST_SUFFIXES: dict[str, frozenset[str]] = {
    "kartaview": frozenset({"kartaview.org", "openstreetcam.org"}),
    "mapillary": frozenset({"mapillary.com", "fbcdn.net", "fbsbx.com"}),
}


def _iter_manifest_records(
    manifest_path: Path,
    *,
    batch_size: int = 2_048,
):
    """Yield validated Parquet rows without materializing the full gallery."""

    import pyarrow.parquet as parquet

    source = parquet.ParquetFile(manifest_path)
    missing = [column for column in CANONICAL_COLUMNS if column not in source.schema_arrow.names]
    if missing:
        raise ManifestSchemaError("; ".join(f"missing_column:{column}" for column in missing))
    for batch in source.iter_batches(batch_size=batch_size):
        frame = coerce_manifest_schema(batch.to_pandas())
        errors = validate_manifest_schema(frame, allow_empty=True)
        if errors:
            raise ManifestSchemaError("; ".join(errors))
        yield from frame.to_dict(orient="records")


class _SessionPool:
    def __init__(self) -> None:
        self.local = threading.local()
        self.sessions: list[requests.Session] = []
        self.lock = threading.Lock()

    def get(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            self.local.session = session
            with self.lock:
                self.sessions.append(session)
        return session

    def close(self) -> None:
        for session in self.sessions:
            session.close()


def _download_one(
    row: dict[str, Any],
    *,
    sessions: _SessionPool,
    retries: int,
    timeout_sec: float,
    backoff_sec: float,
    min_valid_size_bytes: int,
    min_width: int,
    min_height: int,
    max_download_bytes: int,
    max_image_pixels: int,
    max_image_dimension: int,
) -> tuple[str, str, DownloadResult]:
    reference_id = str(row.get("id") or "")
    url = str(row.get("download_url") or "")
    if not url or url.lower() in {"nan", "none", "<na>"}:
        return reference_id, url, DownloadResult("failed", reason="missing_download_url")
    path = Path(str(row.get("image_path") or ""))
    if not str(path) or str(path) == ".":
        return reference_id, url, DownloadResult("failed", reason="missing_image_path")
    source = str(row.get("source") or "").strip().lower()
    allowed_host_suffixes = SOURCE_DOWNLOAD_HOST_SUFFIXES.get(source)
    if allowed_host_suffixes is None:
        return reference_id, url, DownloadResult("failed", reason="unsupported_source")
    result = download_file_detailed(
        url,
        path,
        retries=retries,
        timeout_sec=timeout_sec,
        backoff_sec=backoff_sec,
        session=sessions.get(),
        min_valid_size_bytes=min_valid_size_bytes,
        min_width=min_width,
        min_height=min_height,
        max_download_bytes=max_download_bytes,
        max_image_pixels=max_image_pixels,
        max_image_dimension=max_image_dimension,
        allowed_host_suffixes=allowed_host_suffixes,
    )
    return reference_id, url, result


def run(
    manifest_path: Path,
    errors_log: Path,
    retries: int,
    min_valid_size_bytes: int,
    *,
    workers: int = 8,
    timeout_sec: float = 30.0,
    backoff_sec: float = 1.0,
    min_width: int = 64,
    min_height: int = 64,
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    max_image_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
    max_image_dimension: int = DEFAULT_MAX_IMAGE_DIMENSION,
    stats_path: Path | None = None,
) -> dict[str, int | float | str]:
    if retries < 1:
        raise ValueError("retries must be >= 1")
    if min_valid_size_bytes < 0:
        raise ValueError("min_valid_size_bytes must be >= 0")
    if not 1 <= workers <= 64:
        raise ValueError("workers must be between 1 and 64")
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be > 0")
    if backoff_sec < 0:
        raise ValueError("backoff_sec must be >= 0")
    if min_width < 1 or min_height < 1:
        raise ValueError("min_width and min_height must be >= 1")
    if max_download_bytes < 1:
        raise ValueError("max_download_bytes must be >= 1")
    if max_image_pixels < 1 or max_image_dimension < 1:
        raise ValueError("max_image_pixels and max_image_dimension must be >= 1")

    started_at = time.monotonic()
    # Validate the file schema up front, then stream bounded Arrow batches.
    import pyarrow.parquet as parquet

    record_count = int(parquet.ParquetFile(manifest_path).metadata.num_rows)
    records = _iter_manifest_records(manifest_path)
    sessions = _SessionPool()
    counts = {"downloaded": 0, "skipped": 0, "failed": 0}
    network_attempts = 0
    retry_attempts = 0
    timeout_events = 0
    failures: list[dict[str, str]] = []
    max_pending_downloads = min(record_count, workers * 2)

    def submit_one(
        executor: ThreadPoolExecutor,
        row: dict[str, Any],
    ) -> Future[tuple[str, str, DownloadResult]]:
        return executor.submit(
            _download_one,
            row,
            sessions=sessions,
            retries=retries,
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec,
            min_valid_size_bytes=min_valid_size_bytes,
            min_width=min_width,
            min_height=min_height,
            max_download_bytes=max_download_bytes,
            max_image_pixels=max_image_pixels,
            max_image_dimension=max_image_dimension,
        )

    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="image-download") as executor:
            rows = iter(records)
            pending: set[Future[tuple[str, str, DownloadResult]]] = set()
            for _ in range(max_pending_downloads):
                pending.add(submit_one(executor, next(rows)))

            completed = 0
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    completed += 1
                    try:
                        reference_id, url, result = future.result()
                    except Exception as exc:  # noqa: BLE001 - one image must not abort the job
                        reference_id, url = "unknown", ""
                        result = DownloadResult("failed", reason=f"worker_error:{type(exc).__name__}")
                    counts[result.status] += 1
                    network_attempts += result.network_attempts
                    retry_attempts += result.retry_attempts
                    timeout_events += result.timeout_events
                    if not result.success:
                        failures.append(
                            {
                                "reference_id": reference_id,
                                "reason": result.reason or "download_failed",
                                "url": redact_url(url),
                            }
                        )
                    if completed % 100 == 0 or completed == record_count:
                        logger.info(
                            "download_progress total=%s downloaded=%s skipped=%s failed=%s",
                            completed,
                            counts["downloaded"],
                            counts["skipped"],
                            counts["failed"],
                        )
                    try:
                        row = next(rows)
                    except StopIteration:
                        continue
                    pending.add(submit_one(executor, row))
    finally:
        sessions.close()

    errors_log.parent.mkdir(parents=True, exist_ok=True)
    write_json(errors_log, failures)
    elapsed_seconds = time.monotonic() - started_at
    stats: dict[str, int | float | str] = {
        "images_requested": record_count,
        "images_downloaded": counts["downloaded"],
        "already_existing_images_skipped": counts["skipped"],
        "failed_downloads": counts["failed"],
        "network_attempts": network_attempts,
        "retry_attempts": retry_attempts,
        "timeout_events": timeout_events,
        "elapsed_seconds": round(elapsed_seconds, 6),
        "images_per_second": round(record_count / elapsed_seconds, 6) if elapsed_seconds > 0 else 0.0,
        "workers": workers,
        "max_pending_downloads": max_pending_downloads,
        "max_download_bytes": max_download_bytes,
        "max_image_pixels": max_image_pixels,
        "max_image_dimension": max_image_dimension,
        "errors_log": str(errors_log),
    }
    write_json(stats_path or manifest_path.with_suffix(".download.stats.json"), stats)
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and validate images listed in a canonical manifest")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--errors-log", default="data/raw/download_errors.json")
    parser.add_argument("--stats")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--backoff-sec", type=float, default=1.0)
    parser.add_argument("--min-valid-size-bytes", type=int, default=10_000)
    parser.add_argument("--min-width", type=int, default=64)
    parser.add_argument("--min-height", type=int, default=64)
    parser.add_argument("--max-download-bytes", type=int, default=DEFAULT_MAX_DOWNLOAD_BYTES)
    parser.add_argument("--max-image-pixels", type=int, default=DEFAULT_MAX_IMAGE_PIXELS)
    parser.add_argument("--max-image-dimension", type=int, default=DEFAULT_MAX_IMAGE_DIMENSION)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    run(
        manifest_path=Path(args.manifest),
        errors_log=Path(args.errors_log),
        retries=args.retries,
        min_valid_size_bytes=args.min_valid_size_bytes,
        workers=args.workers,
        timeout_sec=args.timeout_sec,
        backoff_sec=args.backoff_sec,
        min_width=args.min_width,
        min_height=args.min_height,
        max_download_bytes=args.max_download_bytes,
        max_image_pixels=args.max_image_pixels,
        max_image_dimension=args.max_image_dimension,
        stats_path=Path(args.stats) if args.stats else None,
    )


if __name__ == "__main__":
    main()
