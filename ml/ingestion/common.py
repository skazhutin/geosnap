"""Shared ingestion utilities with atomic persistence and safe retries."""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import tempfile
import threading
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

logger = logging.getLogger(__name__)

DEFAULT_MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024
DEFAULT_MAX_IMAGE_PIXELS = 40_000_000
DEFAULT_MAX_IMAGE_DIMENSION = 12_000
DEFAULT_MAX_REDIRECTS = 5

URL_IN_TEXT_RE = re.compile(r"https?://[^\s\"'<>]+", flags=re.IGNORECASE)
SENSITIVE_PARAMETER_RE = re.compile(r"(?i)\b(access_token|api_key|apikey|token|signature|sig)=([^&\s]+)")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def write_json(path: Path, payload: Any) -> None:
    """Atomically write JSON so an interrupted job cannot corrupt a checkpoint."""
    ensure_parent(path)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def redact_url(url: str) -> str:
    """Drop credentials, query parameters and fragments from a URL for logs."""
    try:
        split = urlsplit(str(url))
        port = split.port
    except ValueError:
        return "<invalid-url>"
    host = split.hostname or ""
    if port:
        host = f"{host}:{port}"
    return urlunsplit((split.scheme, host, split.path, "", ""))


def sanitize_error_message(value: Any, *, max_length: int = 200) -> str:
    """Make an exception safe for logs/checkpoints without losing its cause."""
    if max_length < 1:
        raise ValueError("max_length must be >= 1")
    message = str(value).splitlines()[0]
    message = URL_IN_TEXT_RE.sub(lambda match: redact_url(match.group(0)), message)
    message = SENSITIVE_PARAMETER_RE.sub(lambda match: f"{match.group(1)}=<redacted>", message)
    return message[:max_length]


def retry_delay_seconds(
    response: Any | None, *, attempt: int, backoff_sec: float, max_backoff_sec: float = 60.0
) -> float:
    """Exponential delay, honoring Retry-After seconds or HTTP dates."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if backoff_sec < 0 or max_backoff_sec < 0:
        raise ValueError("backoff values must be >= 0")
    delay = backoff_sec * (2 ** (attempt - 1))
    headers = getattr(response, "headers", {}) if response is not None else {}
    retry_after = headers.get("Retry-After") if hasattr(headers, "get") else None
    if retry_after:
        try:
            delay = max(delay, float(retry_after))
        except (TypeError, ValueError):
            try:
                parsed = parsedate_to_datetime(str(retry_after))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                delay = max(delay, (parsed - datetime.now(UTC)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return float(min(max(delay, 0.0), max_backoff_sec))


def is_timeout_error(value: Any) -> bool:
    description = f"{type(value).__name__}: {value}".lower()
    return "timeout" in description or "timed out" in description


@dataclass
class RetryMetrics:
    network_attempts: int = 0
    retry_attempts: int = 0
    timeout_events: int = 0
    retryable_http_events: int = 0
    rate_limit_events: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "network_attempts": self.network_attempts,
            "retry_attempts": self.retry_attempts,
            "timeout_events": self.timeout_events,
            "retryable_http_events": self.retryable_http_events,
            "rate_limit_events": self.rate_limit_events,
        }


class RequestRateLimiter:
    """Serialize network attempts and enforce one shared minimum interval.

    The limiter is deliberately invoked by ``request_with_retry`` for every
    attempt, so retries cannot accidentally bypass a public API rate limit.
    """

    def __init__(
        self,
        minimum_interval_sec: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if minimum_interval_sec < 0:
            raise ValueError("minimum_interval_sec must be >= 0")
        self.minimum_interval_sec = float(minimum_interval_sec)
        self._clock = clock
        self._sleeper = sleeper
        self._next_attempt_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            delay = max(0.0, self._next_attempt_at - now)
            if delay:
                self._sleeper(delay)
                now = self._clock()
            self._next_attempt_at = now + self.minimum_interval_sec


def request_with_retry(
    session: Any,
    *,
    url: str,
    params: dict[str, Any] | None = None,
    timeout_sec: float = 30.0,
    retries: int = 3,
    backoff_sec: float = 1.0,
    retry_statuses: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504}),
    metrics: RetryMetrics | None = None,
    before_request: Callable[[], None] | None = None,
) -> Any:
    if retries < 1:
        raise ValueError("retries must be >= 1")
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be > 0")
    if backoff_sec < 0:
        raise ValueError("backoff_sec must be >= 0")

    for attempt in range(1, retries + 1):
        if before_request is not None:
            before_request()
        if metrics is not None:
            metrics.network_attempts += 1
        try:
            response = session.get(url, params=params or {}, timeout=timeout_sec)
        except Exception as exc:
            if metrics is not None and is_timeout_error(exc):
                metrics.timeout_events += 1
            if attempt == retries:
                raise
            if metrics is not None:
                metrics.retry_attempts += 1
            delay = retry_delay_seconds(None, attempt=attempt, backoff_sec=backoff_sec)
            logger.warning(
                "request_retry attempt=%s/%s delay_sec=%.2f url=%s",
                attempt,
                retries,
                delay,
                redact_url(url),
            )
            time.sleep(delay)
            continue

        if response.status_code not in retry_statuses:
            try:
                response.raise_for_status()
            except Exception:
                if hasattr(response, "close"):
                    response.close()
                raise
            return response
        if metrics is not None:
            metrics.retryable_http_events += 1
            if response.status_code == 429:
                metrics.rate_limit_events += 1
        if attempt == retries:
            try:
                response.raise_for_status()
            finally:
                if hasattr(response, "close"):
                    response.close()

        if metrics is not None:
            metrics.retry_attempts += 1
        delay = retry_delay_seconds(response, attempt=attempt, backoff_sec=backoff_sec)
        if hasattr(response, "close"):
            response.close()
        logger.warning(
            "request_retry status=%s attempt=%s/%s delay_sec=%.2f url=%s",
            getattr(response, "status_code", None),
            attempt,
            retries,
            delay,
            redact_url(url),
        )
        time.sleep(delay)
    raise RuntimeError("request retry loop exited without response")


@dataclass(frozen=True)
class ImageValidation:
    valid: bool
    reason: str | None = None
    width: int | None = None
    height: int | None = None
    image_format: str | None = None


@dataclass(frozen=True)
class DownloadResult:
    status: str  # downloaded | skipped | failed
    reason: str | None = None
    width: int | None = None
    height: int | None = None
    bytes_written: int = 0
    network_attempts: int = 0
    retry_attempts: int = 0
    timeout_events: int = 0

    @property
    def success(self) -> bool:
        return self.status in {"downloaded", "skipped"}


def validate_image_file(
    path: Path,
    *,
    min_valid_size_bytes: int = 1,
    min_width: int = 1,
    min_height: int = 1,
    max_image_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
    max_image_dimension: int = DEFAULT_MAX_IMAGE_DIMENSION,
    max_file_size_bytes: int | None = None,
) -> ImageValidation:
    if min_valid_size_bytes < 0:
        raise ValueError("min_valid_size_bytes must be >= 0")
    if min_width < 1 or min_height < 1:
        raise ValueError("min_width and min_height must be >= 1")
    if max_image_pixels < 1 or max_image_dimension < 1:
        raise ValueError("max_image_pixels and max_image_dimension must be >= 1")
    if max_file_size_bytes is not None and max_file_size_bytes < 1:
        raise ValueError("max_file_size_bytes must be >= 1 when provided")
    if not path.exists() or not path.is_file():
        return ImageValidation(False, "missing_file")
    try:
        file_size = path.stat().st_size
        if file_size < min_valid_size_bytes:
            return ImageValidation(False, "too_small_file")
        if max_file_size_bytes is not None and file_size > max_file_size_bytes:
            return ImageValidation(False, "file_too_large")
    except OSError:
        return ImageValidation(False, "stat_failed")
    try:
        from PIL import Image, UnidentifiedImageError

        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(path) as image:
                width, height = image.size
                image_format = str(image.format or "").upper()
                if width <= 0 or height <= 0:
                    return ImageValidation(False, "invalid_dimensions", width, height, image_format)
                if width > max_image_dimension or height > max_image_dimension:
                    return ImageValidation(False, "too_large_dimensions", width, height, image_format)
                if width * height > max_image_pixels:
                    return ImageValidation(False, "too_many_pixels", width, height, image_format)
                if getattr(image, "n_frames", 1) != 1:
                    return ImageValidation(False, "animated_image", width, height, image_format)
                image.load()
        if width < min_width or height < min_height:
            return ImageValidation(False, "too_small_dimensions", width, height, image_format)
        if image_format not in {"JPEG", "PNG", "WEBP"}:
            return ImageValidation(False, "unsupported_image_format", width, height, image_format)
        return ImageValidation(True, width=width, height=height, image_format=image_format)
    except (
        OSError,
        ValueError,
        UnidentifiedImageError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ):
        return ImageValidation(False, "decode_failed")


class PermanentDownloadError(RuntimeError):
    """A response or decoded image that cannot become valid by retrying."""


def validate_download_url(
    url: str,
    *,
    allowed_host_suffixes: frozenset[str] | None = None,
) -> str:
    """Reject URLs that could turn a data manifest into an SSRF primitive."""
    try:
        parsed = urlsplit(str(url))
        hostname = (parsed.hostname or "").rstrip(".").lower()
        port = parsed.port
    except ValueError as exc:
        raise PermanentDownloadError("unsafe_download_url:invalid") from exc
    if parsed.scheme.lower() != "https" or not hostname:
        raise PermanentDownloadError("unsafe_download_url:https_required")
    if parsed.username is not None or parsed.password is not None:
        raise PermanentDownloadError("unsafe_download_url:credentials")
    if port not in (None, 443):
        raise PermanentDownloadError("unsafe_download_url:port")

    try:
        address = ipaddress.ip_address(hostname.strip("[]"))
    except ValueError:
        address = None
    if address is not None and not address.is_global:
        raise PermanentDownloadError("unsafe_download_url:non_public_address")

    if allowed_host_suffixes is not None:
        normalized_suffixes = {suffix.rstrip(".").lower() for suffix in allowed_host_suffixes}
        if not any(hostname == suffix or hostname.endswith(f".{suffix}") for suffix in normalized_suffixes):
            raise PermanentDownloadError("unsafe_download_url:host_not_allowed")
    return str(url)


def download_file_detailed(
    url: str,
    destination: Path,
    *,
    timeout_sec: float = 30.0,
    retries: int = 3,
    backoff_sec: float = 1.0,
    session: Any | None = None,
    min_valid_size_bytes: int = 10_000,
    min_width: int = 1,
    min_height: int = 1,
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    max_image_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
    max_image_dimension: int = DEFAULT_MAX_IMAGE_DIMENSION,
    allowed_host_suffixes: frozenset[str] | None = None,
    max_redirects: int = DEFAULT_MAX_REDIRECTS,
) -> DownloadResult:
    """Download and fully decode an image before atomically replacing destination."""
    if retries < 1:
        raise ValueError("retries must be >= 1")
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be > 0")
    if backoff_sec < 0:
        raise ValueError("backoff_sec must be >= 0")
    if max_download_bytes < 1:
        raise ValueError("max_download_bytes must be >= 1")
    if max_image_pixels < 1 or max_image_dimension < 1:
        raise ValueError("max_image_pixels and max_image_dimension must be >= 1")
    if max_redirects < 0:
        raise ValueError("max_redirects must be >= 0")

    try:
        validate_download_url(url, allowed_host_suffixes=allowed_host_suffixes)
    except PermanentDownloadError as exc:
        return DownloadResult("failed", reason=str(exc))

    existing = validate_image_file(
        destination,
        min_valid_size_bytes=min_valid_size_bytes,
        min_width=min_width,
        min_height=min_height,
        max_image_pixels=max_image_pixels,
        max_image_dimension=max_image_dimension,
        max_file_size_bytes=max_download_bytes,
    )
    if existing.valid:
        return DownloadResult("skipped", width=existing.width, height=existing.height)

    ensure_parent(destination)
    created_session = False
    if session is None:
        import requests

        client = requests.Session()
        created_session = True
    else:
        client = session

    last_reason = "download_failed"
    retry_attempts = 0
    timeout_events = 0
    network_attempts = 0
    try:
        for attempt in range(1, retries + 1):
            response = None
            temp_path: Path | None = None
            try:
                request_url = url
                for redirect_count in range(max_redirects + 1):
                    validate_download_url(request_url, allowed_host_suffixes=allowed_host_suffixes)
                    network_attempts += 1
                    response = client.get(
                        request_url,
                        stream=True,
                        timeout=timeout_sec,
                        allow_redirects=False,
                    )
                    if response.status_code not in {301, 302, 303, 307, 308}:
                        break
                    location = str(response.headers.get("Location", "")).strip()
                    response.close()
                    response = None
                    if not location:
                        raise PermanentDownloadError("unsafe_redirect:missing_location")
                    if redirect_count >= max_redirects:
                        raise PermanentDownloadError("unsafe_redirect:too_many")
                    request_url = urljoin(request_url, location)
                    validate_download_url(request_url, allowed_host_suffixes=allowed_host_suffixes)
                if response is None:
                    raise PermanentDownloadError("unsafe_redirect:invalid_response")
                if response.status_code in {408, 425, 429, 500, 502, 503, 504}:
                    if attempt == retries:
                        response.raise_for_status()
                    raise RuntimeError(f"retryable_http_{response.status_code}")
                response.raise_for_status()
                content_type = str(response.headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()
                if not content_type.startswith("image/"):
                    raise PermanentDownloadError(f"invalid_content_type:{content_type or 'missing'}")
                content_length = response.headers.get("Content-Length")
                if content_length not in (None, ""):
                    try:
                        declared_bytes = int(content_length)
                    except (TypeError, ValueError) as exc:
                        raise PermanentDownloadError("invalid_content_length") from exc
                    if declared_bytes < 0:
                        raise PermanentDownloadError("invalid_content_length")
                    if declared_bytes > max_download_bytes:
                        raise PermanentDownloadError("download_too_large:content_length")

                fd, name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".part", dir=destination.parent)
                os.close(fd)
                temp_path = Path(name)
                streamed_bytes = 0
                with temp_path.open("wb") as fp:
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            streamed_bytes += len(chunk)
                            if streamed_bytes > max_download_bytes:
                                raise PermanentDownloadError("download_too_large:stream")
                            fp.write(chunk)
                    fp.flush()
                    os.fsync(fp.fileno())

                validation = validate_image_file(
                    temp_path,
                    min_valid_size_bytes=min_valid_size_bytes,
                    min_width=min_width,
                    min_height=min_height,
                    max_image_pixels=max_image_pixels,
                    max_image_dimension=max_image_dimension,
                )
                if not validation.valid:
                    raise PermanentDownloadError(validation.reason or "invalid_image")
                bytes_written = temp_path.stat().st_size
                os.replace(temp_path, destination)
                temp_path = None
                return DownloadResult(
                    "downloaded",
                    width=validation.width,
                    height=validation.height,
                    bytes_written=bytes_written,
                    network_attempts=network_attempts,
                    retry_attempts=retry_attempts,
                    timeout_events=timeout_events,
                )
            except Exception as exc:  # noqa: BLE001 - the failure reason is returned to the caller
                last_reason = sanitize_error_message(exc) or type(exc).__name__
                if is_timeout_error(exc):
                    timeout_events += 1
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
                if isinstance(exc, PermanentDownloadError):
                    break
                status_code = getattr(response, "status_code", None)
                if (
                    status_code is not None
                    and status_code >= 400
                    and status_code
                    not in {
                        408,
                        425,
                        429,
                        500,
                        502,
                        503,
                        504,
                    }
                ):
                    break
                if attempt == retries:
                    break
                retry_attempts += 1
                delay = retry_delay_seconds(response, attempt=attempt, backoff_sec=backoff_sec)
                logger.warning(
                    "image_download_retry attempt=%s/%s delay_sec=%.2f url=%s reason=%s",
                    attempt,
                    retries,
                    delay,
                    redact_url(url),
                    last_reason,
                )
                time.sleep(delay)
            finally:
                if response is not None and hasattr(response, "close"):
                    response.close()
        return DownloadResult(
            "failed",
            reason=last_reason,
            network_attempts=network_attempts,
            retry_attempts=retry_attempts,
            timeout_events=timeout_events,
        )
    finally:
        if created_session:
            client.close()


def download_file(
    url: str,
    destination: Path,
    *,
    timeout_sec: int = 30,
    retries: int = 3,
    backoff_sec: float = 1.5,
    session: Any | None = None,
    min_valid_size_bytes: int = 10_000,
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    max_image_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
    max_image_dimension: int = DEFAULT_MAX_IMAGE_DIMENSION,
    allowed_host_suffixes: frozenset[str] | None = None,
    max_redirects: int = DEFAULT_MAX_REDIRECTS,
) -> bool:
    """Compatibility wrapper returning only success/failure."""
    return download_file_detailed(
        url,
        destination,
        timeout_sec=timeout_sec,
        retries=retries,
        backoff_sec=backoff_sec,
        session=session,
        min_valid_size_bytes=min_valid_size_bytes,
        max_download_bytes=max_download_bytes,
        max_image_pixels=max_image_pixels,
        max_image_dimension=max_image_dimension,
        allowed_host_suffixes=allowed_host_suffixes,
        max_redirects=max_redirects,
    ).success
