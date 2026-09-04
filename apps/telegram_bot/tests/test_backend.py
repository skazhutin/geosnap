from __future__ import annotations

import httpx
import pytest

from apps.telegram_bot.backend import (
    BackendClient,
    BackendRateLimited,
    BackendTimeout,
    InvalidBackendResponse,
)


@pytest.mark.asyncio
async def test_backend_client_forwards_multipart_image_and_request_id() -> None:
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["content_type"] = request.headers["content-type"]
        seen["request_id"] = request.headers["x-request-id"]
        seen["body"] = await request.aread()
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "prediction": {"lat": 55.75, "lon": 37.61, "confidence": 0.95},
                "matches": [],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await BackendClient("http://backend:8000", 5.0, http).localize(
            b"\xff\xd8\xffpayload", request_id="bot-request"
        )
    assert result["status"] == "ok"
    assert seen["content_type"].startswith("multipart/form-data; boundary=")
    assert seen["request_id"] == "bot-request"
    assert b"payload" in seen["body"]


@pytest.mark.asyncio
async def test_backend_client_maps_429() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(429, json={"status": "rate_limited"}))
    ) as http:
        with pytest.raises(BackendRateLimited):
            await BackendClient("http://backend:8000", 5.0, http).localize(b"image", request_id="r")


@pytest.mark.asyncio
async def test_backend_client_maps_timeout() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(BackendTimeout):
            await BackendClient("http://backend:8000", 5.0, http).localize(b"image", request_id="r")


@pytest.mark.asyncio
async def test_backend_client_rejects_malformed_response() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text="not json"))
    ) as http:
        with pytest.raises(InvalidBackendResponse):
            await BackendClient("http://backend:8000", 5.0, http).localize(b"image", request_id="r")
