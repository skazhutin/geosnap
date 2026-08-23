from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from typing import Any

import imagehash
import pytest
import requests
from PIL import Image, ImageDraw

from ml.evaluation.commons import (
    CommonsAcquisitionError,
    CommonsClient,
    SnapshotPin,
    acquire_commons_proxy,
    audit_split,
    canonical_snapshot_sha256,
    load_landmark_config,
    load_snapshot_ledger,
    strip_html,
)


def _page(page_id: int, *, lat: float, lon: float, author: str) -> dict[str, Any]:
    return {
        "pageid": page_id,
        "title": f"File:fixture-{page_id}.jpg",
        "imageinfo": [
            {
                "timestamp": "2024-01-02T03:04:05Z",
                "size": 1000,
                "width": 640,
                "height": 480,
                "url": f"https://upload.wikimedia.org/original/{page_id}.jpg",
                "thumburl": f"https://upload.wikimedia.org/thumb/{page_id}.jpg",
                "descriptionurl": f"https://commons.wikimedia.org/wiki/File:fixture-{page_id}.jpg",
                "sha1": f"{page_id:040x}",
                "mime": "image/jpeg",
                "extmetadata": {
                    "GPSLatitude": {"value": str(lat)},
                    "GPSLongitude": {"value": str(lon)},
                    "Artist": {"value": f"<a>{author}</a>"},
                    "Attribution": {"value": f"<b>{author} credit</b>"},
                    "AttributionRequired": {"value": "true"},
                    "LicenseShortName": {"value": "CC BY-SA 4.0"},
                    "LicenseUrl": {"value": "https://creativecommons.org/licenses/by-sa/4.0"},
                    "DateTimeOriginal": {"value": f"2020-01-{page_id:02d}"},
                },
            }
        ],
    }


class _FakeCommonsClient:
    api_url = "https://commons.wikimedia.org/w/api.php"

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.download_calls = 0

    def fetch_pages(self, page_ids: list[int], *, thumbnail_width: int) -> list[dict[str, Any]]:
        assert thumbnail_width == 512
        assert set(page_ids) == {1, 2, 3, 4}
        return self.pages

    def download_jpeg(
        self,
        url: str,
        destination: Path,
        *,
        max_bytes: int = 25_000_000,
        reuse_existing: bool = True,
        expected_sha256: str | None = None,
    ) -> tuple[str, str, int, int]:
        del max_bytes, reuse_existing
        self.download_calls += 1
        page_id = int(Path(url).stem)
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = _fixture_image_payload(page_id)
        destination.write_bytes(payload)
        with Image.open(io.BytesIO(payload)) as opened:
            phash = str(imagehash.phash(opened.convert("RGB")))
        sha256 = hashlib.sha256(payload).hexdigest()
        assert expected_sha256 is None or sha256 == expected_sha256
        return sha256, phash, 320, 240


def _fixture_image_payload(page_id: int) -> bytes:
    image = Image.new("RGB", (320, 240), (page_id * 40, page_id * 20, 30))
    draw = ImageDraw.Draw(image)
    left = 15 + page_id * 45
    draw.rectangle((left, 20, left + 35, 220), fill=(255, 255, 255))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=91)
    return buffer.getvalue()


def _write_fixture_config_and_ledger(tmp_path: Path, pages: list[dict[str, Any]]) -> Path:
    config = {
        "schema_version": 1,
        "dataset_id": "fixture",
        "purpose": "evaluation_only_tiny_landmark_biased_proxy",
        "api_url": "https://commons.wikimedia.org/w/api.php",
        "snapshot_ledger": "snapshot-ledger.json",
        "thumbnail_width_px": 512,
        "gallery_images_per_landmark": 1,
        "query_images_per_landmark": 1,
        "landmarks": [
            {
                "landmark_id": "a",
                "name": "A",
                "anchor_lat": 55.75,
                "anchor_lon": 37.61,
                "max_distance_m": 100,
                "page_ids": [1, 2],
            },
            {
                "landmark_id": "b",
                "name": "B",
                "anchor_lat": 55.80,
                "anchor_lon": 37.70,
                "max_distance_m": 100,
                "page_ids": [3, 4],
            },
        ],
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    _, landmarks = load_landmark_config(config_path)
    pins: list[SnapshotPin] = []
    for page in pages:
        page_id = int(page["pageid"])
        imageinfo = page["imageinfo"][0]
        metadata = imageinfo["extmetadata"]
        pins.append(
            SnapshotPin(
                page_id=page_id,
                commons_sha1=str(imageinfo["sha1"]),
                downloaded_sha256=hashlib.sha256(_fixture_image_payload(page_id)).hexdigest(),
                lat=float(metadata["GPSLatitude"]["value"]),
                lon=float(metadata["GPSLongitude"]["value"]),
                page_url=str(imageinfo["descriptionurl"]),
                author=str(strip_html(metadata["Artist"]["value"])),
                attribution=str(strip_html(metadata["Attribution"]["value"])),
                license_short_name=str(metadata["LicenseShortName"]["value"]),
                license_url=str(metadata["LicenseUrl"]["value"]),
            )
        )
    pins_by_page_id = {pin.page_id: pin for pin in pins}
    ledger = {
        "schema_version": 1,
        "dataset_id": config["dataset_id"],
        "thumbnail_width_px": config["thumbnail_width_px"],
        "canonical_dataset_sha256": canonical_snapshot_sha256(config, landmarks, pins_by_page_id),
        "images": [pin.to_dict() for pin in pins],
    }
    (tmp_path / "snapshot-ledger.json").write_text(json.dumps(ledger, ensure_ascii=False), encoding="utf-8")
    return config_path


def test_strip_html_keeps_human_credit_text() -> None:
    assert strip_html('<a href="x">Alice &amp; Bob</a> from <b>Moscow</b>') == ("Alice & Bob from Moscow")


def test_checked_in_config_and_ledger_are_complete_without_generated_files() -> None:
    config_path = Path(__file__).parents[3] / "data" / "evaluation" / "moscow_commons_landmarks.json"
    config, landmarks = load_landmark_config(config_path)
    ledger = load_snapshot_ledger(config_path, config, landmarks)
    assert config["purpose"] == "evaluation_only_tiny_landmark_biased_proxy"
    assert len(landmarks) == 4
    assert len({page for landmark in landmarks for page in landmark.page_ids}) == 20
    assert len(ledger.pins_by_page_id) == 20
    assert ledger.path.name == "moscow_commons_snapshot_ledger.json"
    assert len(ledger.canonical_dataset_sha256) == 64


def test_acquire_writes_attributed_snapshot_and_cross_page_split(tmp_path: Path) -> None:
    pages = [
        _page(1, lat=55.75, lon=37.61, author="Gallery A"),
        _page(2, lat=55.75, lon=37.61, author="Query A"),
        _page(3, lat=55.80, lon=37.70, author="Gallery B"),
        _page(4, lat=55.80, lon=37.70, author="Query B"),
    ]
    config_path = _write_fixture_config_and_ledger(tmp_path, pages)
    manifest_path = acquire_commons_proxy(
        config_path,
        tmp_path / "snapshot",
        client=_FakeCommonsClient(pages),  # type: ignore[arg-type]
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["split_audit"]["gallery_count"] == 2
    assert manifest["split_audit"]["query_count"] == 2
    assert manifest["split_audit"]["same_landmark_gallery_query_same_author_pair_count"] == 0
    assert all(row["page_url"].startswith("https://commons.wikimedia.org/") for row in manifest["images"])
    assert all(row["license_short_name"] == "CC BY-SA 4.0" for row in manifest["images"])
    assert "Query A credit" in (manifest_path.parent / "ATTRIBUTION.md").read_text()
    config, landmarks = load_landmark_config(config_path)
    ledger = load_snapshot_ledger(config_path, config, landmarks)
    assert manifest["canonical_dataset_sha256"] == ledger.canonical_dataset_sha256

    second_path = acquire_commons_proxy(
        config_path,
        tmp_path / "snapshot-second",
        client=_FakeCommonsClient(pages),  # type: ignore[arg-type]
    )
    second = json.loads(second_path.read_text(encoding="utf-8"))
    assert second["canonical_dataset_sha256"] == manifest["canonical_dataset_sha256"]
    manifest.pop("created_at")
    second.pop("created_at")
    assert second == manifest


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("commons_sha1", "f" * 40),
        ("attribution", "Changed credit"),
        ("lat", "55.7505"),
    ],
)
def test_acquisition_fails_closed_when_current_commons_metadata_drifts(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    pages = [
        _page(1, lat=55.75, lon=37.61, author="Gallery A"),
        _page(2, lat=55.75, lon=37.61, author="Query A"),
        _page(3, lat=55.80, lon=37.70, author="Gallery B"),
        _page(4, lat=55.80, lon=37.70, author="Query B"),
    ]
    config_path = _write_fixture_config_and_ledger(tmp_path, pages)
    imageinfo = pages[0]["imageinfo"][0]
    if field == "commons_sha1":
        imageinfo["sha1"] = replacement
    elif field == "attribution":
        imageinfo["extmetadata"]["Attribution"]["value"] = replacement
    else:
        imageinfo["extmetadata"]["GPSLatitude"]["value"] = replacement
    client = _FakeCommonsClient(pages)
    with pytest.raises(CommonsAcquisitionError, match="drift"):
        acquire_commons_proxy(config_path, tmp_path / "drifted", client=client)  # type: ignore[arg-type]
    assert client.download_calls == 0


def test_snapshot_ledger_rejects_a_pin_changed_without_digest_update(tmp_path: Path) -> None:
    pages = [
        _page(1, lat=55.75, lon=37.61, author="Gallery A"),
        _page(2, lat=55.75, lon=37.61, author="Query A"),
        _page(3, lat=55.80, lon=37.70, author="Gallery B"),
        _page(4, lat=55.80, lon=37.70, author="Query B"),
    ]
    config_path = _write_fixture_config_and_ledger(tmp_path, pages)
    ledger_path = tmp_path / "snapshot-ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger["images"][0]["attribution"] = "Unreviewed replacement"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    config, landmarks = load_landmark_config(config_path)
    with pytest.raises(CommonsAcquisitionError, match="canonical digest mismatch"):
        load_snapshot_ledger(config_path, config, landmarks)


def test_audit_rejects_exact_cross_split_hash_leakage() -> None:
    rows = [
        {
            "split": "gallery",
            "page_id": 1,
            "commons_sha1": "same",
            "downloaded_sha256": "gallery",
            "perceptual_hash": "a",
            "landmark_id": "x",
            "author": "one",
            "captured_at": "2020",
        },
        {
            "split": "query",
            "page_id": 2,
            "commons_sha1": "same",
            "downloaded_sha256": "query",
            "perceptual_hash": "b",
            "landmark_id": "x",
            "author": "two",
            "captured_at": "2021",
        },
    ]
    with pytest.raises(CommonsAcquisitionError, match="duplicate commons_sha1|exact gallery"):
        audit_split(rows)


def test_http_client_has_bounded_attempt_validation() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        CommonsClient(max_attempts=0)


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class _FakeSession:
    def __init__(self, statuses: list[int]) -> None:
        self.statuses = statuses
        self.calls = 0

    def get(self, *args: Any, **kwargs: Any) -> _FakeResponse:
        del args, kwargs
        status = self.statuses[min(self.calls, len(self.statuses) - 1)]
        self.calls += 1
        return _FakeResponse(status)


class _PayloadResponse(_FakeResponse):
    def __init__(self, payload: bytes) -> None:
        super().__init__(200)
        self.payload = payload
        self.headers = {"Content-Length": str(len(payload))}
        self.closed = False

    def iter_content(self, *, chunk_size: int):  # type: ignore[no-untyped-def]
        yield from (self.payload[start : start + chunk_size] for start in range(0, len(self.payload), chunk_size))

    def close(self) -> None:
        self.closed = True


class _PayloadSession:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls = 0
        self.responses: list[_PayloadResponse] = []

    def get(self, *args: Any, **kwargs: Any) -> _PayloadResponse:
        del args, kwargs
        self.calls += 1
        response = _PayloadResponse(self.payload)
        self.responses.append(response)
        return response


def test_http_client_retries_only_retryable_statuses_with_a_hard_bound() -> None:
    retry_session = _FakeSession([503, 200])
    client = CommonsClient(
        session=retry_session,  # type: ignore[arg-type]
        max_attempts=3,
        backoff_seconds=0,
        minimum_interval_seconds=0,
    )
    assert client._get("https://example.invalid").status_code == 200
    assert retry_session.calls == 2

    nonretry_session = _FakeSession([404, 200])
    client = CommonsClient(
        session=nonretry_session,  # type: ignore[arg-type]
        max_attempts=3,
        backoff_seconds=0,
        minimum_interval_seconds=0,
    )
    with pytest.raises(CommonsAcquisitionError, match="non-retryable HTTP 404"):
        client._get("https://example.invalid")
    assert nonretry_session.calls == 1


def test_content_addressed_cache_reuses_only_matching_bytes_and_refreshes_drift(
    tmp_path: Path,
) -> None:
    expected_payload = _fixture_image_payload(1)
    expected_sha256 = hashlib.sha256(expected_payload).hexdigest()
    destination = tmp_path / "cached.jpg"
    destination.write_bytes(expected_payload)
    session = _PayloadSession(expected_payload)
    client = CommonsClient(
        session=session,  # type: ignore[arg-type]
        minimum_interval_seconds=0,
    )

    cached_sha256, _, _, _ = client.download_jpeg(
        "https://upload.wikimedia.org/expected.jpg",
        destination,
        reuse_existing=True,
        expected_sha256=expected_sha256,
    )
    assert cached_sha256 == expected_sha256
    assert session.calls == 0

    stale_payload = _fixture_image_payload(2)
    destination.write_bytes(stale_payload)
    refreshed_sha256, _, _, _ = client.download_jpeg(
        "https://upload.wikimedia.org/expected.jpg",
        destination,
        reuse_existing=True,
        expected_sha256=expected_sha256,
    )
    assert refreshed_sha256 == expected_sha256
    assert destination.read_bytes() == expected_payload
    assert session.calls == 1
    assert session.responses[0].closed is True
