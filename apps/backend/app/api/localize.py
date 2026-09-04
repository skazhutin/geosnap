from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from time import perf_counter
from typing import Any

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.api.health import get_localization_service
from app.capacity import CapacityUnavailable
from app.schemas import ApiStatus, Diagnostics, LocalizeResponse
from app.services.errors import (
    IndexNotReadyError,
    InvalidImageError,
    LocalizationTimeoutError,
    ModelNotReadyError,
    PublicAPIError,
    ServiceOverloadedError,
)
from app.services.image_validation import prepare_image, read_image_part
from app.services.localization import (
    LocalizationService,
    coerce_readiness,
    coerce_result,
    safe_thumbnail_url,
)

router = APIRouter(tags=["localization"])
logger = logging.getLogger("geosnap.api")

_PRODUCT_RESULT_STATUSES = {
    ApiStatus.OK,
    ApiStatus.LOW_CONFIDENCE,
    ApiStatus.OUT_OF_COVERAGE,
}
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+-]{0,127}$")
_SAFE_WARNING = re.compile(r"^[a-z0-9][a-z0-9_:-]{0,63}$")
_PUBLIC_MESSAGES = {
    ApiStatus.LOW_CONFIDENCE: "The image could not be localized with sufficient confidence.",
    ApiStatus.OUT_OF_COVERAGE: (
        "The scene is not sufficiently represented by the current Moscow reference coverage."
    ),
}


async def _call(function: Any, *args: Any) -> Any:
    if inspect.iscoroutinefunction(function):
        return await function(*args)
    result = await run_in_threadpool(function, *args)
    if inspect.isawaitable(result):
        return await result
    return result


def error_response(
    request: Request,
    status: ApiStatus,
    message: str,
    http_status: int,
    *,
    diagnostics: Diagnostics | None = None,
) -> JSONResponse:
    payload = LocalizeResponse(
        status=status,
        diagnostics=diagnostics or Diagnostics(),
        message=message,
        request_id=request.state.request_id,
    )
    return JSONResponse(status_code=http_status, content=payload.model_dump(mode="json"))


@router.post(
    "/localize",
    response_model=LocalizeResponse,
    responses={
        413: {"model": LocalizeResponse, "description": "Upload is too large."},
        415: {"model": LocalizeResponse, "description": "Unsupported image format."},
        422: {"model": LocalizeResponse, "description": "Invalid image."},
        500: {"model": LocalizeResponse, "description": "Safe internal error."},
        503: {"model": LocalizeResponse, "description": "Model or index is not ready."},
    },
)
async def localize(
    request: Request,
) -> LocalizeResponse | JSONResponse:
    service: LocalizationService = get_localization_service(request)
    started = perf_counter()
    stage = "readiness"
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
            # Bound buffering and decode as well as model inference. Otherwise
            # concurrent maximum-size bodies can exhaust memory before reaching
            # the inference-only semaphore.
            stage = "upload"
            try:
                upload = await asyncio.wait_for(
                    read_image_part(request, request.app.state.settings),
                    timeout=request.app.state.settings.upload_timeout_seconds,
                )
            except TimeoutError as exc:
                raise InvalidImageError("The upload timed out.") from exc
            stage = "preprocess"
            prepared = await run_in_threadpool(prepare_image, upload, request.app.state.settings)
            stage = "inference"
            inference_task = asyncio.create_task(_call(service.localize, prepared))
            try:
                raw_result = await asyncio.wait_for(
                    asyncio.shield(inference_task),
                    timeout=request.app.state.settings.localization_timeout_seconds,
                )
            except TimeoutError as exc:
                lease.defer_until(inference_task)
                raise LocalizationTimeoutError() from exc
        stage = "response_contract"
        result = coerce_result(raw_result)
        try:
            status = ApiStatus(result.status)
        except ValueError as exc:
            raise RuntimeError("localization service returned an unknown status") from exc
        if status not in _PRODUCT_RESULT_STATUSES:
            raise RuntimeError("localization service returned a non-product status")

        diagnostics = Diagnostics(
            width=prepared.diagnostics.width,
            height=prepared.diagnostics.height,
            sharpness=prepared.diagnostics.sharpness,
            brightness=prepared.diagnostics.brightness,
            exposure=prepared.diagnostics.exposure,
            preprocess_ms=prepared.diagnostics.preprocess_ms,
            retriever=(
                result.diagnostics.retriever
                if result.diagnostics.retriever and _SAFE_LABEL.fullmatch(result.diagnostics.retriever)
                else None
            ),
            embedding_ms=result.diagnostics.embedding_ms,
            retrieval_ms=result.diagnostics.retrieval_ms,
            verification_ms=result.diagnostics.verification_ms,
            query_ms=result.diagnostics.query_ms,
            policy_ms=result.diagnostics.policy_ms,
            total_ms=(perf_counter() - started) * 1000.0,
            warnings=[warning for warning in result.diagnostics.warnings if _SAFE_WARNING.fullmatch(warning)],
        )
        payload = LocalizeResponse.model_validate(
            {
                "status": status,
                "prediction": result.prediction,
                "hypotheses": list(result.hypotheses),
                "matches": [
                    {
                        "reference_id": match.reference_id,
                        "source": match.source,
                        "lat": match.lat,
                        "lon": match.lon,
                        "retrieval_score": match.retrieval_score,
                        "verification_score": match.verification_score,
                        "thumbnail_url": safe_thumbnail_url(match),
                        "attribution": match.attribution,
                        "license": match.license,
                        "source_url": match.source_url,
                        "license_url": match.license_url,
                        "contributor_url": match.contributor_url,
                    }
                    for match in result.matches
                ],
                "diagnostics": diagnostics,
                # Do not expose arbitrary adapter exception/details. Product
                # statuses have fixed user-facing copy at this trust boundary.
                "message": _PUBLIC_MESSAGES.get(status),
                "request_id": request.state.request_id,
            },
            from_attributes=True,
        )
        logger.info(
            json.dumps(
                {
                    "event": "localization_complete",
                    "request_id": request.state.request_id,
                    "status": payload.status.value,
                    "preprocess_ms": round(prepared.diagnostics.preprocess_ms, 3),
                    "embedding_ms": result.diagnostics.embedding_ms,
                    "retrieval_ms": result.diagnostics.retrieval_ms,
                    "verification_ms": result.diagnostics.verification_ms,
                    "query_ms": result.diagnostics.query_ms,
                    "policy_ms": result.diagnostics.policy_ms,
                    "total_ms": round(payload.diagnostics.total_ms or 0.0, 3),
                },
                separators=(",", ":"),
            ),
            extra={
                "event": "localization_complete",
                "request_id": request.state.request_id,
                "localization_status": payload.status.value,
                "preprocess_ms": round(prepared.diagnostics.preprocess_ms, 3),
                "embedding_ms": result.diagnostics.embedding_ms,
                "retrieval_ms": result.diagnostics.retrieval_ms,
                "policy_ms": result.diagnostics.policy_ms,
                "duration_ms": round(payload.diagnostics.total_ms or 0.0, 3),
            },
        )
        metrics = request.app.state.metrics
        metrics.localization_requests.labels(status=payload.status.value).inc()
        if result.diagnostics.embedding_ms is not None:
            metrics.embedding_latency.observe(result.diagnostics.embedding_ms / 1000.0)
        if result.diagnostics.retrieval_ms is not None:
            metrics.retrieval_latency.observe(result.diagnostics.retrieval_ms / 1000.0)
        if result.diagnostics.policy_ms is not None:
            metrics.policy_latency.observe(result.diagnostics.policy_ms / 1000.0)
        return payload
    except PublicAPIError as exc:
        request.app.state.metrics.localization_requests.labels(status=exc.status.value).inc()
        if exc.status in {
            ApiStatus.INVALID_IMAGE,
            ApiStatus.UNSUPPORTED_FORMAT,
            ApiStatus.IMAGE_TOO_LARGE,
        }:
            request.app.state.metrics.invalid_uploads.labels(category=exc.status.value).inc()
        return error_response(
            request,
            exc.status,
            exc.public_message,
            exc.http_status,
            diagnostics=Diagnostics(total_ms=(perf_counter() - started) * 1000.0),
        )
    except (ValidationError, TypeError, KeyError, ValueError) as exc:
        # Invalid ML adapter output is an implementation fault, not client input.
        logger.error(
            json.dumps(
                {
                    "event": "localization_failed",
                    "request_id": request.state.request_id,
                    "error_type": type(exc).__name__,
                    "stage": stage,
                },
                separators=(",", ":"),
            )
        )
        return error_response(
            request,
            ApiStatus.INTERNAL_ERROR,
            "Localization failed unexpectedly.",
            500,
            diagnostics=Diagnostics(total_ms=(perf_counter() - started) * 1000.0),
        )
    except Exception as exc:
        logger.error(
            json.dumps(
                {
                    "event": "localization_failed",
                    "request_id": request.state.request_id,
                    "error_type": type(exc).__name__,
                    "stage": stage,
                },
                separators=(",", ":"),
            )
        )
        return error_response(
            request,
            ApiStatus.INTERNAL_ERROR,
            "Localization failed unexpectedly.",
            500,
            diagnostics=Diagnostics(total_ms=(perf_counter() - started) * 1000.0),
        )
