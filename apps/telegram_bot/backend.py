from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import httpx

_PRODUCT_STATUSES = {"ok", "low_confidence", "out_of_coverage"}


class BackendFailure(RuntimeError):
    category = "backend_error"


class BackendUnavailable(BackendFailure):
    category = "backend_unavailable"


class BackendTimeout(BackendFailure):
    category = "backend_timeout"


class BackendRateLimited(BackendFailure):
    category = "backend_rate_limited"


class InvalidBackendResponse(BackendFailure):
    category = "malformed_backend_response"


class BackendRejectedImage(BackendFailure):
    category = "invalid_image"


class BackendImageTooLarge(BackendFailure):
    category = "oversized_image"


@dataclass(frozen=True, slots=True)
class BackendClient:
    base_url: str
    timeout_seconds: float
    client: httpx.AsyncClient | None = None

    async def localize(self, image: bytes, *, request_id: str) -> dict[str, Any]:
        owns_client = self.client is None
        client = self.client or httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_seconds),
            follow_redirects=False,
        )
        try:
            try:
                response = await client.post(
                    f"{self.base_url}/localize",
                    files={"image": ("telegram.jpg", image, "image/jpeg")},
                    headers={"X-Request-ID": request_id, "Accept": "application/json"},
                )
            except httpx.TimeoutException as exc:
                raise BackendTimeout("backend request timed out") from exc
            except httpx.HTTPError as exc:
                raise BackendUnavailable("backend request failed") from exc
        finally:
            if owns_client:
                await client.aclose()
        if response.status_code == 429:
            raise BackendRateLimited("backend rate limit")
        if response.status_code == 413:
            raise BackendImageTooLarge("backend upload limit")
        if response.status_code in {400, 415, 422}:
            raise BackendRejectedImage("backend rejected image")
        if response.status_code in {502, 503, 504}:
            raise BackendUnavailable("backend is not ready")
        if response.status_code >= 500:
            raise BackendUnavailable("backend internal error")
        try:
            payload = response.json()
        except ValueError as exc:
            raise InvalidBackendResponse("backend did not return JSON") from exc
        return validate_product_response(payload)


def _finite_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def validate_product_response(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("status") not in _PRODUCT_STATUSES:
        raise InvalidBackendResponse("unknown backend status")
    status = str(payload["status"])
    prediction = payload.get("prediction")
    if prediction is not None:
        if not isinstance(prediction, dict):
            raise InvalidBackendResponse("response has an invalid prediction")
        lat = prediction.get("lat")
        lon = prediction.get("lon")
        confidence = prediction.get("confidence")
        if not (
            _finite_number(lat)
            and _finite_number(lon)
            and _finite_number(confidence)
            and -90 <= lat <= 90
            and -180 <= lon <= 180
            and 0 <= confidence <= 1
        ):
            raise InvalidBackendResponse("response has an invalid prediction")
    elif status == "ok":
        raise InvalidBackendResponse("ok response has no prediction")
    matches = payload.get("matches", [])
    if not isinstance(matches, list):
        raise InvalidBackendResponse("matches must be a list")
    return payload
