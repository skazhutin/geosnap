from __future__ import annotations

import asyncio
import hashlib
import logging
from time import perf_counter

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.api.health import get_localization_service
from app.api.localize import _call, error_response, public_result
from app.capacity import CapacityUnavailable
from app.schemas.api import ApiStatus, MultiPhotoResponse
from app.services.errors import (
    IndexNotReadyError,
    InvalidImageError,
    LocalizationTimeoutError,
    ModelNotReadyError,
    PublicAPIError,
    ServiceOverloadedError,
)
from app.services.image_validation import prepare_image, read_image_parts
from app.services.localization import coerce_readiness
from app.services.multi_photo import combine_views

router = APIRouter(tags=["localization"])
logger = logging.getLogger("geosnap.api")


async def _process(uploads, service, settings, request_id: str) -> MultiPhotoResponse:
    results = []
    seen = set()
    started = perf_counter()
    for position, upload in enumerate(uploads, 1):
        try:
            prepared = await run_in_threadpool(prepare_image, upload, settings)
        except PublicAPIError as exc:
            raise type(exc)(f"Image {position}: {exc.public_message}") from exc
        try:
            # Decoded-pixel identity also catches the same image with changed EXIF.
            fingerprint = await run_in_threadpool(
                lambda image=prepared.image: hashlib.sha256(str(image.size).encode() + image.tobytes()).digest()
            )
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            result = await _call(service.localize, prepared)
            results.append(public_result(result, prepared, request_id, (perf_counter() - started) * 1000))
        finally:
            prepared.image.close()
    return combine_views(results, submitted=len(uploads), request_id=request_id)


@router.post(
    "/localize/multi",
    response_model=MultiPhotoResponse,
    openapi_extra={"requestBody": {"required": True, "content": {"multipart/form-data": {"schema": {
        "type": "object", "required": ["images"], "properties": {"images": {
            "type": "array", "minItems": 1, "maxItems": 10, "items": {"type": "string", "format": "binary"},
        }},
    }}}}},
)
async def localize_multi(request: Request) -> MultiPhotoResponse | JSONResponse:
    settings = request.app.state.settings
    service = get_localization_service(request)
    started = perf_counter()
    try:
        readiness = coerce_readiness(await _call(service.readiness))
        if not readiness.model_loaded:
            raise ModelNotReadyError()
        if not readiness.index_loaded or not readiness.metadata_available:
            raise IndexNotReadyError()
        try:
            lease = await request.app.state.localization_capacity.acquire()
        except CapacityUnavailable as exc:
            raise ServiceOverloadedError() from exc
        async with lease:
            try:
                uploads = await asyncio.wait_for(
                    read_image_parts(request, settings, field_name="images", max_parts=10, total_limit=settings.max_batch_upload_bytes),
                    timeout=settings.upload_timeout_seconds,
                )
            except TimeoutError as exc:
                raise InvalidImageError("The upload timed out.") from exc
            task = asyncio.create_task(_process(uploads, service, settings, request.state.request_id))
            try:
                result = await asyncio.wait_for(asyncio.shield(task), timeout=settings.batch_timeout_seconds)
            except TimeoutError as exc:
                lease.defer_until(task)
                raise LocalizationTimeoutError() from exc
            except asyncio.CancelledError:
                lease.defer_until(task)
                raise
        result.diagnostics.total_ms = (perf_counter() - started) * 1000
        request.app.state.metrics.localization_requests.labels(status=result.status.value).inc()
        logger.info("multi-photo localization complete", extra={"event": "multi_photo_complete", "request_id": request.state.request_id})
        return result
    except PublicAPIError as exc:
        request.app.state.metrics.localization_requests.labels(status=exc.status.value).inc()
        return error_response(request, exc.status, exc.public_message, exc.http_status)
    except Exception as exc:
        logger.error("multi-photo localization failed", extra={"event": "multi_photo_failed", "error_category": type(exc).__name__, "request_id": request.state.request_id})
        return error_response(request, ApiStatus.INTERNAL_ERROR, "Localization failed unexpectedly.", 500)
