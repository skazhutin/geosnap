from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from app.config import Settings
from app.main import create_app
from app.schemas import ApiStatus, LocalizeResponse, ReadyResponse
from app.services.localization import (
    ServiceDiagnostics,
    ServiceHypothesis,
    ServiceMatch,
    ServicePrediction,
    ServiceReadiness,
    ServiceResult,
)
from PIL import Image

from ml.indexing import FaissExactIndex
from ml.localization.service import LocalizationService as MLService
from ml.retrieval.testing import DeterministicFixtureRetriever


@dataclass
class Response:
    status_code: int
    headers: dict[str, str]
    content: bytes

    def json(self) -> dict[str, Any]:
        return json.loads(self.content)


class ASGITestClient:
    """Small dependency-free ASGI client used because httpx is not a runtime dep."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.runner = asyncio.Runner()
        self.lifespan = app.router.lifespan_context(app)

    def __enter__(self) -> ASGITestClient:
        self.runner.run(self.lifespan.__aenter__())
        return self

    def __exit__(self, *args: Any) -> None:
        self.runner.run(self.lifespan.__aexit__(*args))
        self.runner.close()

    def get(self, path: str, headers: dict[str, str] | None = None) -> Response:
        return self.request("GET", path, headers=headers)

    def post(
        self,
        path: str,
        *,
        body: bytes,
        headers: dict[str, str] | None = None,
    ) -> Response:
        return self.request("POST", path, body=body, headers=headers)

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> Response:
        return self.runner.run(self._request(method, path, body=body, headers=headers or {}))

    async def _request(
        self,
        method: str,
        path: str,
        *,
        body: bytes,
        headers: dict[str, str],
    ) -> Response:
        sent_request = False
        messages: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            nonlocal sent_request
            if not sent_request:
                sent_request = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            messages.append(message)

        encoded_headers = [(key.lower().encode("latin-1"), value.encode("latin-1")) for key, value in headers.items()]
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "root_path": "",
            "headers": encoded_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
        await self.app(scope, receive, send)
        start = next(message for message in messages if message["type"] == "http.response.start")
        content = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
        response_headers = {key.decode("latin-1"): value.decode("latin-1") for key, value in start["headers"]}
        return Response(start["status"], response_headers, content)


class FakeService:
    def __init__(
        self,
        result: ServiceResult | dict[str, Any] | None = None,
        readiness: ServiceReadiness | None = None,
    ) -> None:
        self.result = result or success_result()
        self.state = readiness or ServiceReadiness(True, True, True)
        self.load_calls = 0
        self.localize_calls = 0
        self.last_query = None
        self.thumbnail = None

    def load(self) -> None:
        self.load_calls += 1

    def readiness(self) -> ServiceReadiness:
        return self.state

    def localize(self, query):
        self.localize_calls += 1
        self.last_query = query
        return self.result

    def get_thumbnail(self, reference_id: str):
        if reference_id != "mapillary/id 1":
            return None
        return self.thumbnail


def make_settings(**overrides: Any) -> Settings:
    values = {
        "database_url": "postgresql+psycopg2://unused:unused@invalid/unused",
        "readiness_check_database": False,
        "localization_service_factory": "unused:unused",
        "max_upload_bytes": 1024 * 1024,
        "max_image_pixels": 2_000_000,
        "max_image_dimension": 2000,
        "multipart_overhead_bytes": 32 * 1024,
        "localization_concurrency": 1,
        "cors_origins": ("http://localhost:3000",),
    }
    values.update(overrides)
    return Settings(**values)


def artifact_manifest(path: Path, *, payload: bytes = b"valid") -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "manifest_id": "startup-failure-test",
                "version": "1",
                "artifacts": [
                    {
                        "artifact_id": "required-fixture",
                        "version": "1",
                        "source_url": "https://example.invalid/fixture",
                        "source_type": "file",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                        "size_bytes": len(payload),
                        "destination": "fixture.bin",
                        "required": True,
                        "provenance": "test",
                        "description": "test",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def success_result() -> ServiceResult:
    return ServiceResult(
        status="ok",
        prediction=ServicePrediction(
            lat=55.751244,
            lon=37.618423,
            confidence=0.91,
            uncertainty_radius_m=85.0,
        ),
        hypotheses=(ServiceHypothesis(lat=55.751, lon=37.618, score=0.91),),
        matches=(
            ServiceMatch(
                reference_id="mapillary/id 1",
                source="mapillary",
                lat=55.751,
                lon=37.618,
                retrieval_score=0.88,
                verification_score=0.75,
                thumbnail_available=True,
                attribution="Mapillary contributor",
                license="CC BY-SA 4.0",
                source_url="https://www.mapillary.com/app/?pKey=1",
                license_url="https://creativecommons.org/licenses/by-sa/4.0/",
                contributor_url="https://www.mapillary.com/app/user/contributor",
            ),
        ),
        diagnostics=ServiceDiagnostics(
            retriever="fake-vpr",
            embedding_ms=2.0,
            retrieval_ms=1.0,
            verification_ms=3.0,
            query_ms=6.0,
        ),
    )


def encoded_image(
    format_name: str = "JPEG",
    size: tuple[int, int] = (96, 64),
    color: tuple[int, int, int] = (70, 120, 180),
) -> bytes:
    image = Image.new("RGB", size, color)
    output = io.BytesIO()
    image.save(output, format=format_name)
    return output.getvalue()


def multipart(
    payload: bytes,
    *,
    content_type: str = "image/jpeg",
    filename: str = "query.jpg",
    field_name: str = "image",
) -> tuple[bytes, dict[str, str]]:
    boundary = "geosnap-test-boundary"
    body = (
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("ascii")
        + payload
        + f"\r\n--{boundary}--\r\n".encode("ascii")
    )
    return body, {
        "content-type": f"multipart/form-data; boundary={boundary}",
        "content-length": str(len(body)),
    }


def test_health_is_process_only_and_preserves_safe_request_id() -> None:
    fake = FakeService()

    def exploding_database_check() -> bool:
        raise AssertionError("health must not access the database")

    app = create_app(
        settings=make_settings(),
        service_factory=lambda: fake,
        database_checker=exploding_database_check,
    )
    with ASGITestClient(app) as client:
        response = client.get("/health", {"x-request-id": "test-request-1"})

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "request_id": "test-request-1"}
    assert response.headers["x-request-id"] == "test-request-1"
    assert fake.load_calls == 1


def test_cors_preflight_allows_only_configured_frontend_origin() -> None:
    app = create_app(settings=make_settings(), service_factory=FakeService)
    with ASGITestClient(app) as client:
        allowed = client.request(
            "OPTIONS",
            "/localize",
            headers={
                "origin": "http://localhost:3000",
                "access-control-request-method": "POST",
                "access-control-request-headers": "content-type",
            },
        )
        denied = client.request(
            "OPTIONS",
            "/localize",
            headers={
                "origin": "https://untrusted.example",
                "access-control-request-method": "POST",
            },
        )

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "http://localhost:3000"
    assert denied.status_code == 400
    assert "access-control-allow-origin" not in denied.headers


def test_production_disables_interactive_api_documentation() -> None:
    app = create_app(
        settings=make_settings(environment="production", artifact_manifest="required-at-runtime.json"),
        service_factory=FakeService,
    )

    assert app.docs_url is None
    assert app.redoc_url is None
    assert app.openapi_url is None


def test_health_survives_localization_startup_failure() -> None:
    def broken_factory():
        raise RuntimeError("missing model at /private/model.bin")

    app = create_app(settings=make_settings(), service_factory=broken_factory)
    with ASGITestClient(app) as client:
        health_response = client.get("/health")
        ready_response = client.get("/ready")

    assert health_response.status_code == 200
    assert health_response.json()["status"] == "ok"
    assert ready_response.status_code == 503
    assert ready_response.json()["status"] == "model_not_ready"
    assert "/private/model.bin" not in ready_response.content.decode()


@pytest.mark.parametrize("condition", ["missing", "corrupt"])
def test_production_artifact_failure_keeps_process_alive_but_unready(
    condition: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "manifest.json"
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    artifact_manifest(manifest)
    if condition == "corrupt":
        (artifacts / "fixture.bin").write_bytes(b"bad!!")
    monkeypatch.setenv("GEOSNAP_ARTIFACT_DIR", str(artifacts))
    factory_called = False

    def factory() -> FakeService:
        nonlocal factory_called
        factory_called = True
        return FakeService()

    app = create_app(
        settings=make_settings(environment="production", artifact_manifest=str(manifest)),
        service_factory=factory,
    )
    with ASGITestClient(app) as client:
        health_response = client.get("/health")
        ready_response = client.get("/ready")

    assert health_response.status_code == 200
    assert ready_response.status_code == 503
    assert ready_response.json()["ready"] is False
    assert factory_called is False


def test_ready_reports_all_required_components() -> None:
    app = create_app(settings=make_settings(), service_factory=FakeService)
    with ASGITestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    parsed = ReadyResponse.model_validate(response.json())
    assert parsed.ready is True
    assert parsed.status is ApiStatus.OK
    assert parsed.components.model.value == "ready"
    assert parsed.components.index.value == "ready"
    assert parsed.components.metadata.value == "ready"
    assert parsed.components.database.value == "not_configured"


def test_ready_optionally_checks_database_without_exposing_details() -> None:
    app = create_app(
        settings=make_settings(readiness_check_database=True),
        service_factory=FakeService,
        database_checker=lambda: False,
    )
    with ASGITestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "internal_error"
    assert payload["components"]["database"] == "not_ready"
    assert "postgres" not in response.content.decode().lower()


def test_localize_success_is_typed_and_uses_safe_thumbnail_route() -> None:
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 200
    parsed = LocalizeResponse.model_validate(response.json())
    assert parsed.status is ApiStatus.OK
    assert parsed.prediction is not None
    assert parsed.prediction.lat == 55.751244
    assert parsed.matches[0].thumbnail_url == "/thumbnails/mapillary%2Fid%201"
    assert parsed.matches[0].source_url == "https://www.mapillary.com/app/?pKey=1"
    assert parsed.matches[0].license == "CC BY-SA 4.0"
    assert parsed.matches[0].contributor_url == "https://www.mapillary.com/app/user/contributor"
    assert parsed.diagnostics.width == 96
    assert parsed.diagnostics.height == 64
    assert parsed.diagnostics.brightness is not None
    assert parsed.diagnostics.exposure is not None
    assert parsed.diagnostics.retriever == "fake-vpr"
    assert fake.localize_calls == 1
    assert fake.last_query.image.mode == "RGB"


def test_thumbnail_route_serves_bounded_jpeg_without_storage_path() -> None:
    fake = FakeService()
    fake.thumbnail = Image.new("RGB", (32, 24), (20, 80, 140))
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    with ASGITestClient(app) as client:
        response = client.get("/thumbnails/mapillary/id 1")
        missing = client.get("/thumbnails/unknown")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/jpeg"
    assert response.headers["cache-control"] == "public, max-age=86400"
    assert response.content.startswith(b"\xff\xd8\xff")
    assert missing.status_code == 404


def test_localize_corrupt_image() -> None:
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(b"\xff\xd8\xffthis-is-not-a-jpeg")
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 422
    assert response.json()["status"] == "invalid_image"
    assert fake.localize_calls == 0


def test_localize_corrupt_file_without_an_image_signature() -> None:
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(b"this-is-not-an-image")
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 422
    assert response.json()["status"] == "invalid_image"
    assert fake.localize_calls == 0


def test_localize_rejects_empty_image_field() -> None:
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(b"")
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 422
    assert response.json()["status"] == "invalid_image"
    assert fake.localize_calls == 0


@pytest.mark.parametrize(
    ("format_name", "content_type", "filename"),
    (("PNG", "image/png", "query.png"), ("WEBP", "image/webp", "query.webp")),
)
def test_localize_accepts_supported_png_and_webp(
    format_name: str,
    content_type: str,
    filename: str,
) -> None:
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(
        encoded_image(format_name),
        content_type=content_type,
        filename=filename,
    )
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 200
    assert fake.localize_calls == 1


def test_localize_unsupported_format() -> None:
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(
        b"GIF89a-not-supported",
        content_type="image/gif",
        filename="query.gif",
    )
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 415
    assert response.json()["status"] == "unsupported_format"


def test_localize_rejects_oversized_upload_before_decode() -> None:
    fake = FakeService()
    app = create_app(
        settings=make_settings(max_upload_bytes=128, multipart_overhead_bytes=512),
        service_factory=lambda: fake,
    )
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 413
    assert response.json()["status"] == "image_too_large"
    assert fake.localize_calls == 0


def test_localize_rejects_excessive_decoded_dimensions() -> None:
    fake = FakeService()
    app = create_app(
        settings=make_settings(max_image_dimension=2000, max_image_pixels=2_000_000),
        service_factory=lambda: fake,
    )
    body, headers = multipart(encoded_image(size=(2001, 64)))
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 413
    assert response.json()["status"] == "image_too_large"
    assert fake.localize_calls == 0


def test_localize_low_confidence_is_honest_product_response() -> None:
    fake = FakeService(
        ServiceResult(
            status="low_confidence",
            diagnostics=ServiceDiagnostics(retriever="fake-vpr"),
            message="No geographically coherent candidate was found.",
        )
    )
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    parsed = LocalizeResponse.model_validate(response.json())
    assert response.status_code == 200
    assert parsed.status is ApiStatus.LOW_CONFIDENCE
    assert parsed.prediction is None
    assert parsed.message == "The image could not be localized with sufficient confidence."


def test_localize_out_of_coverage_is_an_explicit_product_response() -> None:
    fake = FakeService(ServiceResult(status="out_of_coverage"))
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    parsed = LocalizeResponse.model_validate(response.json())
    assert response.status_code == 200
    assert parsed.status is ApiStatus.OUT_OF_COVERAGE
    assert parsed.prediction is None


def test_exif_orientation_is_applied_before_localization() -> None:
    image = Image.new("RGB", (80, 40), (70, 120, 180))
    exif = Image.Exif()
    exif[274] = 6
    output = io.BytesIO()
    image.save(output, format="JPEG", exif=exif)
    body, headers = multipart(output.getvalue())
    fake = FakeService()
    app = create_app(settings=make_settings(), service_factory=lambda: fake)

    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 200
    assert fake.last_query.image.size == (40, 80)
    assert response.json()["diagnostics"]["width"] == 40
    assert response.json()["diagnostics"]["height"] == 80


def test_exposure_quality_does_not_reward_blown_white_or_black() -> None:
    diagnostics = []
    for color in (0, 128, 255):
        body, headers = multipart(encoded_image(color=(color, color, color)))
        fake = FakeService()
        app = create_app(settings=make_settings(), service_factory=lambda fake=fake: fake)
        with ASGITestClient(app) as client:
            response = client.post("/localize", body=body, headers=headers)
        assert response.status_code == 200
        diagnostics.append(response.json()["diagnostics"])

    black, midtone, white = diagnostics
    assert black["brightness"] < midtone["brightness"] < white["brightness"]
    assert midtone["exposure"] > black["exposure"]
    assert midtone["exposure"] > white["exposure"]


def test_model_not_ready_is_explicit() -> None:
    fake = FakeService(readiness=ServiceReadiness(False, False, False))
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 503
    assert response.json()["status"] == "model_not_ready"
    assert fake.localize_calls == 0


def test_missing_index_returns_explicit_503() -> None:
    fake = FakeService(readiness=ServiceReadiness(True, False, False))
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        ready = client.get("/ready")
        localized = client.post("/localize", body=body, headers=headers)

    assert ready.status_code == 503
    assert ready.json()["status"] == "index_not_ready"
    assert localized.status_code == 503
    assert localized.json()["status"] == "index_not_ready"
    assert fake.localize_calls == 0


def test_mapping_adapter_drops_raw_paths_and_external_thumbnail_urls() -> None:
    fake = FakeService(
        {
            "status": "ok",
            "prediction": {"lat": 55.75, "lon": 37.61, "confidence": 0.8},
            "matches": [
                {
                    "reference_id": "ref-1",
                    "source": "kartaview",
                    "lat": 55.75,
                    "lon": 37.61,
                    "retrieval_score": 0.7,
                    "attribution": "KartaView contributor",
                    "license": "CC BY-SA 4.0",
                    "source_url": "https://kartaview.org/details/1/1/track-info",
                    "image_path": "/private/data/secret.jpg",
                    "thumbnail_url": "https://example.test/image?token=secret",
                }
            ],
        }
    )
    app = create_app(settings=make_settings(), service_factory=lambda: fake)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 200
    serialized = response.content.decode()
    assert "/private/" not in serialized
    assert "token=" not in serialized
    assert response.json()["matches"][0]["thumbnail_url"] is None


def test_internal_failure_has_safe_structured_diagnostic_log(caplog) -> None:
    class BrokenService(FakeService):
        def localize(self, query):
            raise RuntimeError("secret /absolute/path token=abc")

    app = create_app(settings=make_settings(), service_factory=BrokenService)
    body, headers = multipart(encoded_image())
    with caplog.at_level(logging.ERROR, logger="geosnap.api"):
        with ASGITestClient(app) as client:
            response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 500
    payload = response.json()
    assert payload["status"] == "internal_error"
    assert "secret" not in response.content.decode()
    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert '"event":"localization_failed"' in messages
    assert '"error_type":"RuntimeError"' in messages
    assert '"stage":"inference"' in messages
    assert "secret" not in messages


def test_upload_decode_is_inside_concurrency_bound(monkeypatch) -> None:
    active = 0
    maximum_active = 0

    async def instrumented_read(_request, _settings):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        from app.services.image_validation import UploadedPart

        return UploadedPart(encoded_image(), "image/jpeg", "query.jpg")

    monkeypatch.setattr("app.api.localize.read_image_part", instrumented_read)
    app = create_app(
        settings=make_settings(localization_concurrency=1),
        service_factory=FakeService,
    )
    with ASGITestClient(app) as client:

        async def send_concurrently():
            return await asyncio.gather(
                *(client._request("POST", "/localize", body=b"", headers={}) for _ in range(3))
            )

        responses = client.runner.run(send_concurrently())

    assert all(response.status_code == 200 for response in responses)
    assert maximum_active == 1


def test_rate_limit_returns_429_and_retry_after() -> None:
    app = create_app(
        settings=make_settings(
            localization_rate_per_minute=0.01,
            localization_rate_burst=1,
        ),
        service_factory=FakeService,
    )
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        first = client.post("/localize", body=body, headers=headers)
        second = client.post("/localize", body=body, headers=headers)

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json()["status"] == "rate_limited"
    assert int(second.headers["retry-after"]) >= 1


def test_full_localization_queue_fails_closed(monkeypatch) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_read(_request, _settings):
        from app.services.image_validation import UploadedPart

        entered.set()
        await release.wait()
        return UploadedPart(encoded_image(), "image/jpeg", "query.jpg")

    monkeypatch.setattr("app.api.localize.read_image_part", blocked_read)
    app = create_app(
        settings=make_settings(localization_concurrency=1, localization_queue_limit=0),
        service_factory=FakeService,
    )
    with ASGITestClient(app) as client:
        async def exercise():
            first_task = asyncio.create_task(client._request("POST", "/localize", body=b"", headers={}))
            await entered.wait()
            overloaded = await client._request("POST", "/localize", body=b"", headers={})
            release.set()
            first = await first_task
            return first, overloaded

        first, overloaded = client.runner.run(exercise())

    assert first.status_code == 200
    assert overloaded.status_code == 503
    assert overloaded.json()["status"] == "service_overloaded"


def test_metrics_are_prometheus_compatible_and_do_not_label_request_ids() -> None:
    app = create_app(settings=make_settings(), service_factory=FakeService)
    body, headers = multipart(encoded_image())
    with ASGITestClient(app) as client:
        localized = client.post(
            "/localize",
            body=body,
            headers=headers | {"x-request-id": "unique-request-id"},
        )
        metrics = client.get("/metrics")

    assert localized.status_code == 200
    assert metrics.status_code == 200
    text = metrics.content.decode()
    assert "geosnap_localization_requests_total" in text
    assert "geosnap_embedding_duration_seconds" in text
    assert "unique-request-id" not in text


def test_invalid_boolean_environment_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("READINESS_CHECK_DATABASE", "treu")
    with pytest.raises(ValueError, match="READINESS_CHECK_DATABASE"):
        Settings()


def test_api_runs_real_fixture_embedding_index_localization_and_thumbnail(tmp_path) -> None:
    """Exercise the full API composition with deterministic, non-production weights."""

    gallery_image = tmp_path / "red-reference.jpg"
    red = Image.new("RGB", (96, 64), (235, 15, 15))
    blue = Image.new("RGB", (96, 64), (15, 15, 235))
    red.save(gallery_image)
    builder = DeterministicFixtureRetriever(allow_test_only=True).load()
    descriptors = builder.embed_batch([red, blue])
    index_dir = tmp_path / "index"
    FaissExactIndex.build(
        descriptors,
        ["red-reference", "blue-reference"],
        reference_metadata=[
            {
                "lat": 55.751,
                "lon": 37.618,
                "city_id": "moscow",
                "source": "kartaview",
                "attribution": "© Grab and KartaView Contributors",
                "license": "CC BY-SA 4.0",
                "source_url": "https://kartaview.org/details/1/1/track-info",
                "image_path": str(gallery_image),
            },
            {
                "lat": 55.851,
                "lon": 37.718,
                "city_id": "moscow",
                "source": "mapillary",
                "attribution": "Fixture Mapillary contributor, CC BY-SA",
                "license": "CC BY-SA 4.0",
                "source_url": "https://www.mapillary.com/app/?pKey=2",
            },
        ],
        retriever_metadata=builder.metadata.to_dict(),
        city_id="moscow",
        index_id="api-fixture",
    ).save(index_dir)

    def service_factory():
        return MLService(
            DeterministicFixtureRetriever(allow_test_only=True),
            index_dir,
            top_k=2,
            process_isolate_faiss=False,
        )

    app = create_app(settings=make_settings(), service_factory=service_factory)
    body, headers = multipart(encoded_image(size=(96, 64)))
    # encoded_image uses a blue-ish color, so the blue reference must win.
    with ASGITestClient(app) as client:
        response = client.post("/localize", body=body, headers=headers)

    assert response.status_code == 200
    parsed = LocalizeResponse.model_validate(response.json())
    assert parsed.status in {ApiStatus.OK, ApiStatus.LOW_CONFIDENCE}
    assert parsed.matches[0].reference_id == "blue-reference"
    assert parsed.prediction is not None
    assert parsed.prediction.lat == 55.851
