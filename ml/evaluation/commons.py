"""Reproducible, evaluation-only Wikimedia Commons Moscow proxy acquisition.

The source is deliberately separate from production ingestion. Wikimedia
Commons landmark photos are not street-view coverage and must never be used to
claim representative Moscow performance.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import io
import json
import math
import os
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import imagehash
import requests
from PIL import Image, UnidentifiedImageError

from ml.localization.geo import haversine_m

COMMONS_API_URL = "https://commons.wikimedia.org/w/api.php"
DEFAULT_USER_AGENT = "GeoSnap-Moscow-evaluation/0.1 (https://github.com/skazhutin/geosnap; evaluation-only research)"
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
SUPPORTED_LICENSE_PREFIXES = ("CC BY", "CC0")
SNAPSHOT_LEDGER_SCHEMA_VERSION = 1
SNAPSHOT_PIN_FIELDS = frozenset(
    {
        "page_id",
        "commons_sha1",
        "downloaded_sha256",
        "lat",
        "lon",
        "page_url",
        "author",
        "attribution",
        "license_short_name",
        "license_url",
    }
)


class CommonsAcquisitionError(RuntimeError):
    """The Commons snapshot could not be acquired without violating policy."""


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def strip_html(value: str | None) -> str | None:
    """Convert Commons extmetadata HTML to a stable plain-text companion."""

    if value is None:
        return None
    parser = _TextExtractor()
    parser.feed(value)
    text = html.unescape(" ".join(parser.parts))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _metadata_value(metadata: Mapping[str, Any], key: str) -> str | None:
    entry = metadata.get(key)
    if not isinstance(entry, Mapping):
        return None
    value = entry.get("value")
    return None if value is None else str(value)


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@dataclass(frozen=True, slots=True)
class LandmarkSpec:
    landmark_id: str
    name: str
    anchor_lat: float
    anchor_lon: float
    max_distance_m: float
    page_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class SnapshotPin:
    """Content and provenance expected for one immutable evaluation input."""

    page_id: int
    commons_sha1: str
    downloaded_sha256: str
    lat: float
    lon: float
    page_url: str
    author: str
    attribution: str
    license_short_name: str
    license_url: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SnapshotLedger:
    """Validated tracked pins for a reproducible Commons snapshot."""

    path: Path
    file_sha256: str
    canonical_dataset_sha256: str
    pins_by_page_id: Mapping[int, SnapshotPin]


@dataclass(frozen=True, slots=True)
class CommonsImageRecord:
    record_id: str
    split: str
    landmark_id: str
    landmark_name: str
    page_id: int
    title: str
    page_url: str
    original_url: str
    download_url: str
    local_path: str
    lat: float
    lon: float
    coordinate_source: str
    distance_to_anchor_m: float
    author: str
    author_html: str
    attribution: str
    attribution_html: str
    license_short_name: str
    license_url: str
    attribution_required: bool | None
    captured_at: str | None
    uploaded_at: str | None
    commons_sha1: str
    downloaded_sha256: str
    perceptual_hash: str
    mime: str
    original_width: int
    original_height: int
    downloaded_width: int
    downloaded_height: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CommonsClient:
    """Small polite HTTP client with explicit retry and rate bounds."""

    def __init__(
        self,
        *,
        api_url: str = COMMONS_API_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout_seconds: float = 30.0,
        max_attempts: int = 4,
        backoff_seconds: float = 0.75,
        minimum_interval_seconds: float = 0.1,
        session: requests.Session | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if backoff_seconds < 0 or minimum_interval_seconds < 0:
            raise ValueError("retry/rate durations cannot be negative")
        if not user_agent.strip():
            raise ValueError("a descriptive User-Agent is required")
        self.api_url = api_url
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.minimum_interval_seconds = minimum_interval_seconds
        self.session = session or requests.Session()
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        remaining = self.minimum_interval_seconds - (time.monotonic() - self._last_request_at)
        if remaining > 0:
            time.sleep(remaining)

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        last_error: BaseException | None = None
        for attempt in range(self.max_attempts):
            self._throttle()
            try:
                response = self.session.get(
                    url,
                    headers={"User-Agent": self.user_agent},
                    timeout=self.timeout_seconds,
                    **kwargs,
                )
                self._last_request_at = time.monotonic()
            except requests.RequestException as exc:
                last_error = exc
                server_delay = 0.0
            else:
                if response.status_code not in RETRYABLE_HTTP_STATUSES:
                    try:
                        response.raise_for_status()
                    except requests.RequestException as exc:
                        raise CommonsAcquisitionError(f"non-retryable HTTP {response.status_code} from {url}") from exc
                    return response
                last_error = CommonsAcquisitionError(f"retryable HTTP {response.status_code} from {url}")
                retry_after = response.headers.get("Retry-After")
                try:
                    server_delay = min(float(retry_after), 30.0) if retry_after else 0.0
                except ValueError:
                    server_delay = 0.0
            if attempt + 1 < self.max_attempts:
                time.sleep(max(server_delay, self.backoff_seconds * (2**attempt)))
        raise CommonsAcquisitionError(
            f"request failed after {self.max_attempts} attempts: {url}: {last_error}"
        ) from last_error

    def fetch_pages(self, page_ids: Sequence[int], *, thumbnail_width: int) -> list[dict[str, Any]]:
        if not page_ids:
            return []
        if thumbnail_width < 128 or thumbnail_width > 4096:
            raise ValueError("thumbnail_width must be in [128, 4096]")
        pages: list[dict[str, Any]] = []
        # The Action API permits up to 50 page IDs for ordinary clients. Keep
        # the request bounded even if the checked-in seed grows later.
        for start in range(0, len(page_ids), 50):
            chunk = page_ids[start : start + 50]
            response = self._get(
                self.api_url,
                params={
                    "action": "query",
                    "format": "json",
                    "formatversion": "2",
                    "pageids": "|".join(str(value) for value in chunk),
                    "prop": "imageinfo|coordinates",
                    "coprimary": "all",
                    "iiprop": "url|extmetadata|timestamp|mime|size|sha1",
                    "iiextmetadatafilter": (
                        "GPSLatitude|GPSLongitude|Artist|Attribution|"
                        "AttributionRequired|LicenseShortName|LicenseUrl|DateTimeOriginal"
                    ),
                    "iiurlwidth": str(thumbnail_width),
                    "maxlag": "5",
                },
            )
            try:
                payload = response.json()
            except requests.JSONDecodeError as exc:
                raise CommonsAcquisitionError("Commons API returned invalid JSON") from exc
            if payload.get("error"):
                raise CommonsAcquisitionError(f"Commons API error: {payload['error']}")
            returned = payload.get("query", {}).get("pages", [])
            if not isinstance(returned, list):
                raise CommonsAcquisitionError("Commons API response lacks query.pages")
            pages.extend(dict(page) for page in returned)
        return pages

    def download_jpeg(
        self,
        url: str,
        destination: Path,
        *,
        max_bytes: int = 25_000_000,
        reuse_existing: bool = True,
        expected_sha256: str | None = None,
    ) -> tuple[str, str, int, int]:
        """Download and verify a bounded JPEG, returning hashes and dimensions.

        A cached file is reusable only when its bytes match ``expected_sha256``.
        A mismatched cache is refreshed atomically; mismatched network bytes are
        rejected before they can replace the cache.
        """

        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if expected_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise ValueError("expected_sha256 must be a lowercase SHA-256 hex digest")
        reused_cache = False
        if reuse_existing and destination.is_file():
            if destination.stat().st_size <= max_bytes:
                cached_payload = destination.read_bytes()
                cached_sha256 = hashlib.sha256(cached_payload).hexdigest()
                if expected_sha256 is None or cached_sha256 == expected_sha256:
                    payload = cached_payload
                    reused_cache = True
        if not reused_cache:
            response = self._get(url, stream=True)
            try:
                advertised = response.headers.get("Content-Length")
                if advertised:
                    try:
                        advertised_bytes = int(advertised)
                    except ValueError:
                        advertised_bytes = 0
                    if advertised_bytes > max_bytes:
                        raise CommonsAcquisitionError(f"download exceeds {max_bytes} bytes by Content-Length: {url}")
                buffer = io.BytesIO()
                for chunk in response.iter_content(chunk_size=128 * 1024):
                    if not chunk:
                        continue
                    if buffer.tell() + len(chunk) > max_bytes:
                        raise CommonsAcquisitionError(f"download exceeded {max_bytes} bytes while streaming: {url}")
                    buffer.write(chunk)
                payload = buffer.getvalue()
                if not payload:
                    raise CommonsAcquisitionError(f"empty image response: {url}")
            finally:
                response.close()

        downloaded_sha256 = hashlib.sha256(payload).hexdigest()
        if expected_sha256 is not None and downloaded_sha256 != expected_sha256:
            origin = "cached" if reused_cache else "downloaded"
            raise CommonsAcquisitionError(
                f"{origin} JPEG SHA-256 drift for {url}: expected {expected_sha256}, got {downloaded_sha256}"
            )

        try:
            with Image.open(io.BytesIO(payload)) as opened:
                opened.verify()
            with Image.open(io.BytesIO(payload)) as opened:
                if opened.format != "JPEG":
                    raise CommonsAcquisitionError(f"decoded media is {opened.format}, expected JPEG: {url}")
                rgb = opened.convert("RGB")
                width, height = rgb.size
                perceptual_hash = str(imagehash.phash(rgb))
        except (UnidentifiedImageError, OSError) as exc:
            raise CommonsAcquisitionError(f"invalid JPEG payload: {url}") from exc
        if min(width, height) < 128:
            raise CommonsAcquisitionError(f"downloaded image is too small ({width}x{height})")

        destination.parent.mkdir(parents=True, exist_ok=True)
        if not reused_cache:
            temporary = destination.with_name(destination.name + ".tmp")
            with temporary.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        return downloaded_sha256, perceptual_hash, width, height


def load_landmark_config(path: str | Path) -> tuple[dict[str, Any], list[LandmarkSpec]]:
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    if config.get("purpose") != "evaluation_only_tiny_landmark_biased_proxy":
        raise CommonsAcquisitionError("config must declare its evaluation-only proxy purpose")
    raw_landmarks = config.get("landmarks")
    if not isinstance(raw_landmarks, list) or len(raw_landmarks) < 2:
        raise CommonsAcquisitionError("config requires at least two distinct landmarks")
    landmarks = [
        LandmarkSpec(
            landmark_id=str(row["landmark_id"]),
            name=str(row["name"]),
            anchor_lat=float(row["anchor_lat"]),
            anchor_lon=float(row["anchor_lon"]),
            max_distance_m=float(row["max_distance_m"]),
            page_ids=tuple(int(value) for value in row["page_ids"]),
        )
        for row in raw_landmarks
    ]
    all_page_ids = [page_id for landmark in landmarks for page_id in landmark.page_ids]
    if len(all_page_ids) != len(set(all_page_ids)):
        raise CommonsAcquisitionError("page IDs must be globally unique across landmarks")
    gallery_count = int(config.get("gallery_images_per_landmark", 0))
    query_count = int(config.get("query_images_per_landmark", 0))
    if gallery_count < 1 or query_count < 1:
        raise CommonsAcquisitionError("gallery/query counts must both be positive")
    for landmark in landmarks:
        if len(landmark.page_ids) != gallery_count + query_count:
            raise CommonsAcquisitionError(
                f"{landmark.landmark_id} has {len(landmark.page_ids)} page IDs; expected {gallery_count + query_count}"
            )
        if landmark.max_distance_m <= 0:
            raise CommonsAcquisitionError("landmark max_distance_m must be positive")
    return config, landmarks


def _canonical_snapshot_payload(
    config: Mapping[str, Any],
    landmarks: Sequence[LandmarkSpec],
    pins_by_page_id: Mapping[int, SnapshotPin],
) -> dict[str, Any]:
    gallery_count = int(config["gallery_images_per_landmark"])
    images: list[dict[str, Any]] = []
    for landmark in landmarks:
        for index, page_id in enumerate(landmark.page_ids):
            images.append(
                {
                    "record_id": f"commons:{page_id}",
                    "split": "gallery" if index < gallery_count else "query",
                    "landmark_id": landmark.landmark_id,
                    **pins_by_page_id[page_id].to_dict(),
                }
            )
    return {
        "schema_version": SNAPSHOT_LEDGER_SCHEMA_VERSION,
        "dataset_id": str(config["dataset_id"]),
        "purpose": str(config["purpose"]),
        "source": "Wikimedia Commons",
        "thumbnail_width_px": int(config["thumbnail_width_px"]),
        "gallery_images_per_landmark": gallery_count,
        "query_images_per_landmark": int(config["query_images_per_landmark"]),
        "landmarks": [asdict(landmark) for landmark in landmarks],
        "images": images,
    }


def canonical_snapshot_sha256(
    config: Mapping[str, Any],
    landmarks: Sequence[LandmarkSpec],
    pins_by_page_id: Mapping[int, SnapshotPin],
) -> str:
    """Hash stable dataset identity, excluding acquisition time and local paths."""

    payload = _canonical_snapshot_payload(config, landmarks, pins_by_page_id)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_snapshot_ledger(
    config_path: str | Path,
    config: Mapping[str, Any],
    landmarks: Sequence[LandmarkSpec],
) -> SnapshotLedger:
    """Load tracked snapshot pins and reject incomplete or self-inconsistent ledgers."""

    config_path = Path(config_path)
    raw_ledger_path = config.get("snapshot_ledger")
    if not isinstance(raw_ledger_path, str) or not raw_ledger_path.strip():
        raise CommonsAcquisitionError("config must declare a tracked snapshot_ledger")
    ledger_path = Path(raw_ledger_path)
    if not ledger_path.is_absolute():
        ledger_path = config_path.parent / ledger_path
    try:
        ledger_bytes = ledger_path.read_bytes()
        ledger = json.loads(ledger_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise CommonsAcquisitionError(f"cannot read snapshot ledger {ledger_path}: {exc}") from exc
    if not isinstance(ledger, dict):
        raise CommonsAcquisitionError("snapshot ledger must be a JSON object")
    expected_top_level = {
        "schema_version",
        "dataset_id",
        "thumbnail_width_px",
        "canonical_dataset_sha256",
        "images",
    }
    if set(ledger) != expected_top_level:
        raise CommonsAcquisitionError(f"snapshot ledger fields must be exactly {sorted(expected_top_level)}")
    if ledger.get("schema_version") != SNAPSHOT_LEDGER_SCHEMA_VERSION:
        raise CommonsAcquisitionError("unsupported snapshot ledger schema_version")
    if ledger.get("dataset_id") != config.get("dataset_id"):
        raise CommonsAcquisitionError("snapshot ledger dataset_id does not match config")
    if ledger.get("thumbnail_width_px") != config.get("thumbnail_width_px"):
        raise CommonsAcquisitionError("snapshot ledger thumbnail width does not match config")

    rows = ledger.get("images")
    if not isinstance(rows, list) or not rows:
        raise CommonsAcquisitionError("snapshot ledger images must be a non-empty list")
    pins: list[SnapshotPin] = []
    for raw in rows:
        if not isinstance(raw, dict) or set(raw) != SNAPSHOT_PIN_FIELDS:
            raise CommonsAcquisitionError(f"every snapshot pin must contain exactly {sorted(SNAPSHOT_PIN_FIELDS)}")
        page_id = raw.get("page_id")
        if type(page_id) is not int or page_id <= 0:
            raise CommonsAcquisitionError("snapshot pin page_id must be a positive integer")
        commons_sha1 = raw.get("commons_sha1")
        downloaded_sha256 = raw.get("downloaded_sha256")
        if not isinstance(commons_sha1, str) or not re.fullmatch(r"[0-9a-f]{40}", commons_sha1):
            raise CommonsAcquisitionError(f"snapshot pin {page_id} has invalid Commons SHA-1")
        if not isinstance(downloaded_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", downloaded_sha256):
            raise CommonsAcquisitionError(f"snapshot pin {page_id} has invalid downloaded SHA-256")
        try:
            lat = float(raw["lat"])
            lon = float(raw["lon"])
        except (TypeError, ValueError) as exc:
            raise CommonsAcquisitionError(f"snapshot pin {page_id} has invalid coordinates") from exc
        if not math.isfinite(lat) or not math.isfinite(lon) or not -90 <= lat <= 90 or not -180 <= lon <= 180:
            raise CommonsAcquisitionError(f"snapshot pin {page_id} has invalid coordinates")
        text_values: dict[str, str] = {}
        for field_name in (
            "page_url",
            "author",
            "attribution",
            "license_short_name",
            "license_url",
        ):
            value = raw.get(field_name)
            if not isinstance(value, str) or not value.strip():
                raise CommonsAcquisitionError(f"snapshot pin {page_id} lacks {field_name}")
            text_values[field_name] = value.strip()
        if not text_values["page_url"].startswith("https://commons.wikimedia.org/"):
            raise CommonsAcquisitionError(f"snapshot pin {page_id} has non-Commons page_url")
        if not text_values["license_url"].startswith("https://"):
            raise CommonsAcquisitionError(f"snapshot pin {page_id} has non-HTTPS license_url")
        pins.append(
            SnapshotPin(
                page_id=page_id,
                commons_sha1=commons_sha1,
                downloaded_sha256=downloaded_sha256,
                lat=lat,
                lon=lon,
                **text_values,
            )
        )

    expected_page_ids = [page_id for landmark in landmarks for page_id in landmark.page_ids]
    actual_page_ids = [pin.page_id for pin in pins]
    if actual_page_ids != expected_page_ids:
        raise CommonsAcquisitionError("snapshot ledger page IDs/order must exactly match the landmark config")
    if len({pin.commons_sha1 for pin in pins}) != len(pins):
        raise CommonsAcquisitionError("snapshot ledger contains duplicate Commons SHA-1 values")
    if len({pin.downloaded_sha256 for pin in pins}) != len(pins):
        raise CommonsAcquisitionError("snapshot ledger contains duplicate downloaded SHA-256 values")
    pins_by_page_id = {pin.page_id: pin for pin in pins}
    declared_digest = ledger.get("canonical_dataset_sha256")
    if not isinstance(declared_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", declared_digest):
        raise CommonsAcquisitionError("snapshot ledger has invalid canonical_dataset_sha256")
    actual_digest = canonical_snapshot_sha256(config, landmarks, pins_by_page_id)
    if actual_digest != declared_digest:
        raise CommonsAcquisitionError("snapshot ledger canonical digest mismatch; update pins and digest together")
    return SnapshotLedger(
        path=ledger_path,
        file_sha256=hashlib.sha256(ledger_bytes).hexdigest(),
        canonical_dataset_sha256=actual_digest,
        pins_by_page_id=pins_by_page_id,
    )


def _coordinates(page: Mapping[str, Any], metadata: Mapping[str, Any]) -> tuple[float, float, str]:
    lat_raw = _metadata_value(metadata, "GPSLatitude")
    lon_raw = _metadata_value(metadata, "GPSLongitude")
    if lat_raw is not None and lon_raw is not None:
        return float(lat_raw), float(lon_raw), "imageinfo.extmetadata.GPSLatitude/GPSLongitude"
    coordinates = page.get("coordinates")
    if isinstance(coordinates, list) and coordinates:
        primary = next(
            (value for value in coordinates if isinstance(value, Mapping) and value.get("primary")),
            coordinates[0],
        )
        return float(primary["lat"]), float(primary["lon"]), "page.coordinates"
    raise CommonsAcquisitionError(f"page {page.get('pageid')} lacks usable coordinates")


def _required_text(value: str | None, *, field: str, page_id: int) -> str:
    if value is None or not value.strip():
        raise CommonsAcquisitionError(f"page {page_id} lacks required {field}")
    return value.strip()


def _assert_page_matches_pin(
    pin: SnapshotPin,
    *,
    commons_sha1: str,
    lat: float,
    lon: float,
    page_url: str,
    author: str,
    attribution: str,
    license_short_name: str,
    license_url: str,
) -> None:
    observed_text = {
        "commons_sha1": commons_sha1.lower(),
        "page_url": page_url,
        "author": author,
        "attribution": attribution,
        "license_short_name": license_short_name,
        "license_url": license_url,
    }
    expected = pin.to_dict()
    for field_name, observed in observed_text.items():
        if observed != expected[field_name]:
            raise CommonsAcquisitionError(
                f"Commons page {pin.page_id} {field_name} drift: expected {expected[field_name]!r}, got {observed!r}"
            )
    if not math.isclose(lat, pin.lat, rel_tol=0.0, abs_tol=1e-7):
        raise CommonsAcquisitionError(f"Commons page {pin.page_id} latitude drift: expected {pin.lat}, got {lat}")
    if not math.isclose(lon, pin.lon, rel_tol=0.0, abs_tol=1e-7):
        raise CommonsAcquisitionError(f"Commons page {pin.page_id} longitude drift: expected {pin.lon}, got {lon}")


def _parse_page(
    page: Mapping[str, Any],
    *,
    landmark: LandmarkSpec,
    split: str,
    pin: SnapshotPin,
    output_dir: Path,
    client: CommonsClient,
    reuse_existing: bool,
) -> CommonsImageRecord:
    page_id = int(page.get("pageid", 0))
    if page.get("missing") is not None or page_id <= 0:
        raise CommonsAcquisitionError(f"Commons page {page_id} is missing")
    if page_id != pin.page_id:
        raise CommonsAcquisitionError(f"Commons response page {page_id} does not match snapshot pin {pin.page_id}")
    imageinfo_values = page.get("imageinfo")
    if not isinstance(imageinfo_values, list) or len(imageinfo_values) != 1:
        raise CommonsAcquisitionError(f"page {page_id} lacks exactly one imageinfo record")
    imageinfo = imageinfo_values[0]
    if imageinfo.get("mime") != "image/jpeg":
        raise CommonsAcquisitionError(f"page {page_id} has unsupported MIME {imageinfo.get('mime')!r}")
    metadata = imageinfo.get("extmetadata") or {}
    if not isinstance(metadata, Mapping):
        raise CommonsAcquisitionError(f"page {page_id} has malformed extmetadata")
    lat, lon, coordinate_source = _coordinates(page, metadata)
    distance = haversine_m(landmark.anchor_lat, landmark.anchor_lon, lat, lon)
    if distance > landmark.max_distance_m:
        raise CommonsAcquisitionError(
            f"page {page_id} is {distance:.1f}m from {landmark.landmark_id}, outside {landmark.max_distance_m:.1f}m"
        )

    license_name = _required_text(
        strip_html(_metadata_value(metadata, "LicenseShortName")),
        field="LicenseShortName",
        page_id=page_id,
    )
    if not license_name.startswith(SUPPORTED_LICENSE_PREFIXES):
        raise CommonsAcquisitionError(f"page {page_id} uses unsupported/unclear license {license_name!r}")
    license_url = _required_text(_metadata_value(metadata, "LicenseUrl"), field="LicenseUrl", page_id=page_id)
    author_html = _required_text(_metadata_value(metadata, "Artist"), field="Artist", page_id=page_id)
    author = _required_text(strip_html(author_html), field="plain-text Artist", page_id=page_id)
    attribution_html = _metadata_value(metadata, "Attribution") or author_html
    attribution = _required_text(strip_html(attribution_html), field="Attribution/Artist", page_id=page_id)
    attribution_required_raw = _metadata_value(metadata, "AttributionRequired")
    attribution_required = (
        None if attribution_required_raw is None else attribution_required_raw.strip().lower() == "true"
    )

    original_url = _required_text(imageinfo.get("url"), field="original URL", page_id=page_id)
    download_url = _required_text(imageinfo.get("thumburl") or original_url, field="download URL", page_id=page_id)
    page_url = _required_text(imageinfo.get("descriptionurl"), field="description URL", page_id=page_id)
    commons_sha1 = _required_text(imageinfo.get("sha1"), field="Commons SHA-1", page_id=page_id).lower()
    _assert_page_matches_pin(
        pin,
        commons_sha1=commons_sha1,
        lat=lat,
        lon=lon,
        page_url=page_url,
        author=author,
        attribution=attribution,
        license_short_name=license_name,
        license_url=license_url,
    )
    destination = output_dir / "images" / f"{page_id}.jpg"
    downloaded_sha256, perceptual_hash, downloaded_width, downloaded_height = client.download_jpeg(
        download_url,
        destination,
        reuse_existing=reuse_existing,
        expected_sha256=pin.downloaded_sha256,
    )
    if downloaded_sha256 != pin.downloaded_sha256:
        raise CommonsAcquisitionError(
            f"Commons page {page_id} downloaded SHA-256 drift: "
            f"expected {pin.downloaded_sha256}, got {downloaded_sha256}"
        )
    return CommonsImageRecord(
        record_id=f"commons:{page_id}",
        split=split,
        landmark_id=landmark.landmark_id,
        landmark_name=landmark.name,
        page_id=page_id,
        title=str(page.get("title", "")),
        page_url=page_url,
        original_url=original_url,
        download_url=download_url,
        local_path=str(destination.relative_to(output_dir)),
        lat=lat,
        lon=lon,
        coordinate_source=coordinate_source,
        distance_to_anchor_m=distance,
        author=author,
        author_html=author_html,
        attribution=attribution,
        attribution_html=attribution_html,
        license_short_name=license_name,
        license_url=license_url,
        attribution_required=attribution_required,
        captured_at=strip_html(_metadata_value(metadata, "DateTimeOriginal")),
        uploaded_at=(str(imageinfo["timestamp"]) if imageinfo.get("timestamp") else None),
        commons_sha1=commons_sha1,
        downloaded_sha256=downloaded_sha256,
        perceptual_hash=perceptual_hash,
        mime="image/jpeg",
        original_width=int(imageinfo["width"]),
        original_height=int(imageinfo["height"]),
        downloaded_width=downloaded_width,
        downloaded_height=downloaded_height,
    )


def audit_split(records: Sequence[CommonsImageRecord | Mapping[str, Any]]) -> dict[str, Any]:
    def value(row: CommonsImageRecord | Mapping[str, Any], key: str) -> Any:
        return getattr(row, key) if isinstance(row, CommonsImageRecord) else row.get(key)

    gallery = [row for row in records if value(row, "split") == "gallery"]
    queries = [row for row in records if value(row, "split") == "query"]
    if not gallery or not queries:
        raise CommonsAcquisitionError("split must contain both gallery and query records")
    if len(gallery) + len(queries) != len(records):
        raise CommonsAcquisitionError("every record split must be exactly 'gallery' or 'query'")
    exact_fields = ("page_id", "commons_sha1", "downloaded_sha256")
    for field in exact_fields:
        values = [value(row, field) for row in records]
        if any(item is None or item == "" for item in values):
            raise CommonsAcquisitionError(f"every record requires non-empty {field}")
        if len(values) != len(set(values)):
            raise CommonsAcquisitionError(f"duplicate {field} inside evaluation split")
    overlaps = {
        field: sorted(set(value(row, field) for row in gallery) & set(value(row, field) for row in queries))
        for field in exact_fields
    }
    if any(overlaps.values()):
        raise CommonsAcquisitionError(f"exact gallery/query leakage detected: {overlaps}")

    same_author_pairs = 0
    same_time_pairs = 0
    same_phash_pairs = 0
    for query in queries:
        peers = [row for row in gallery if value(row, "landmark_id") == value(query, "landmark_id")]
        if not peers:
            raise CommonsAcquisitionError(f"query landmark {value(query, 'landmark_id')!r} has no gallery reference")
        query_author = str(value(query, "author") or "").casefold()
        query_time = str(value(query, "captured_at") or "")
        for gallery_row in peers:
            if query_author and query_author == str(value(gallery_row, "author") or "").casefold():
                same_author_pairs += 1
            if query_time and query_time == str(value(gallery_row, "captured_at") or ""):
                same_time_pairs += 1
            if value(query, "perceptual_hash") == value(gallery_row, "perceptual_hash"):
                same_phash_pairs += 1
    if same_phash_pairs:
        raise CommonsAcquisitionError(f"gallery/query perceptual hashes are identical in {same_phash_pairs} pairs")
    return {
        "gallery_count": len(gallery),
        "query_count": len(queries),
        "landmark_count": len({value(row, "landmark_id") for row in records}),
        "exact_cross_split_overlap": overlaps,
        "same_landmark_gallery_query_same_author_pair_count": same_author_pairs,
        "same_landmark_gallery_query_same_capture_time_pair_count": same_time_pairs,
        "same_landmark_gallery_query_identical_perceptual_hash_pair_count": same_phash_pairs,
        "preference": "query pages should differ from same-landmark gallery by author and time",
    }


def _attribution_markdown(records: Iterable[CommonsImageRecord]) -> str:
    lines = [
        "# Wikimedia Commons image attribution",
        "",
        "These images are used only for the tiny landmark-biased evaluation proxy. ",
        "Follow each linked file page and license for current reuse requirements.",
        "",
    ]
    for record in records:
        lines.extend(
            [
                f"## {record.title}",
                "",
                f"- File page: <{record.page_url}>",
                f"- Author/credit: {record.attribution}",
                f"- License: [{record.license_short_name}]({record.license_url})",
                f"- Split / landmark: `{record.split}` / `{record.landmark_id}`",
                "- Downloaded by GeoSnap at 1280 px maximum width; no content edits.",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def acquire_commons_proxy(
    config_path: str | Path,
    output_dir: str | Path,
    *,
    client: CommonsClient | None = None,
    reuse_existing: bool = False,
) -> Path:
    """Fetch the content-pinned Commons snapshot or fail closed on any drift."""

    config, landmarks = load_landmark_config(config_path)
    ledger = load_snapshot_ledger(config_path, config, landmarks)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    active_client = client or CommonsClient(api_url=str(config.get("api_url", COMMONS_API_URL)))
    all_page_ids = [page_id for landmark in landmarks for page_id in landmark.page_ids]
    pages = active_client.fetch_pages(all_page_ids, thumbnail_width=int(config.get("thumbnail_width_px", 1280)))
    pages_by_id = {int(page["pageid"]): page for page in pages if page.get("pageid")}
    missing = sorted(set(all_page_ids) - set(pages_by_id))
    if missing:
        raise CommonsAcquisitionError(f"Commons API did not return pinned pages: {missing}")

    gallery_count = int(config["gallery_images_per_landmark"])
    records: list[CommonsImageRecord] = []
    for landmark in landmarks:
        for index, page_id in enumerate(landmark.page_ids):
            split = "gallery" if index < gallery_count else "query"
            records.append(
                _parse_page(
                    pages_by_id[page_id],
                    landmark=landmark,
                    split=split,
                    pin=ledger.pins_by_page_id[page_id],
                    output_dir=output_dir,
                    client=active_client,
                    reuse_existing=reuse_existing,
                )
            )
    # Commons original hashes must also be unique within each side so a copied
    # page cannot silently inflate gallery or query counts.
    for field in ("page_id", "commons_sha1", "downloaded_sha256"):
        values = [getattr(record, field) for record in records]
        if len(values) != len(set(values)):
            raise CommonsAcquisitionError(f"duplicate {field} inside acquired snapshot")
    split_audit = audit_split(records)
    manifest = {
        "schema_version": 1,
        "dataset_id": config["dataset_id"],
        "purpose": config["purpose"],
        "warning": (
            "Tiny hand-curated landmark-biased Wikimedia Commons proxy; not street-view, "
            "not representative of Moscow, and not evidence of production coverage."
        ),
        "created_at": datetime.now(UTC).isoformat(),
        "canonical_dataset_sha256": ledger.canonical_dataset_sha256,
        "source": {
            "name": "Wikimedia Commons",
            "api_url": active_client.api_url,
            "reuse_guidance_url": ("https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia"),
            "config_path": str(Path(config_path)),
            "snapshot_ledger_path": str(ledger.path),
            "snapshot_ledger_sha256": ledger.file_sha256,
            "thumbnail_width_px": int(config.get("thumbnail_width_px", 1280)),
        },
        "split_policy": {
            "assignment": ("per landmark, first pinned page IDs are gallery and remaining IDs are query"),
            "gallery_images_per_landmark": gallery_count,
            "query_images_per_landmark": int(config["query_images_per_landmark"]),
            "exact_image_overlap_forbidden": True,
            "different_author_and_capture_time_preferred": True,
        },
        "split_audit": split_audit,
        "landmarks": [asdict(landmark) for landmark in landmarks],
        "images": [record.to_dict() for record in records],
    }
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    manifest_path = output_dir / "manifest.json"
    _atomic_text(manifest_path, manifest_text)
    _atomic_text(output_dir / "ATTRIBUTION.md", _attribution_markdown(records))
    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Acquire the evaluation-only Moscow Wikimedia Commons proxy")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="reuse already-downloaded bytes after local validation instead of refreshing",
    )
    args = parser.parse_args()
    manifest_path = acquire_commons_proxy(
        args.config,
        args.output_dir,
        client=CommonsClient(user_agent=args.user_agent),
        reuse_existing=args.reuse_existing,
    )
    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    print(
        json.dumps(
            {
                "manifest": str(manifest_path),
                "dataset_id": manifest["dataset_id"],
                "canonical_dataset_sha256": manifest["canonical_dataset_sha256"],
                "warning": manifest["warning"],
                "split_audit": manifest["split_audit"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
