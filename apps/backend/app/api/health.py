from __future__ import annotations

import inspect
from typing import Any

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.schemas import ApiStatus, HealthResponse, ReadyResponse
from app.schemas.api import ComponentStatus, ReadyComponents
from app.services.localization import (
    LocalizationService,
    ServiceReadiness,
    coerce_readiness,
)

router = APIRouter(tags=["service"])


def get_localization_service(request: Request) -> LocalizationService:
    return request.app.state.localization_service


async def _call(function: Any, *args: Any) -> Any:
    if inspect.iscoroutinefunction(function):
        return await function(*args)
    result = await run_in_threadpool(function, *args)
    if inspect.isawaitable(result):
        return await result
    return result


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """Liveness only: no database, model, or index access."""

    return HealthResponse(status=ApiStatus.OK, request_id=request.state.request_id)


@router.get(
    "/ready",
    response_model=ReadyResponse,
    responses={503: {"model": ReadyResponse, "description": "A required component is not ready."}},
)
async def ready(
    request: Request,
) -> ReadyResponse | JSONResponse:
    service: LocalizationService = get_localization_service(request)
    try:
        service_state = coerce_readiness(await _call(service.readiness))
    except Exception:
        service_state = ServiceReadiness(False, False, False)

    database_status = ComponentStatus.NOT_CONFIGURED
    database_ready = True
    if request.app.state.settings.readiness_check_database:
        try:
            database_ready = bool(await _call(request.app.state.database_checker))
        except Exception:
            database_ready = False
        database_status = ComponentStatus.READY if database_ready else ComponentStatus.NOT_READY

    if not service_state.model_loaded:
        status = ApiStatus.MODEL_NOT_READY
    elif not service_state.index_loaded or not service_state.metadata_available:
        status = ApiStatus.INDEX_NOT_READY
    elif not database_ready:
        status = ApiStatus.INTERNAL_ERROR
    else:
        status = ApiStatus.OK

    response = ReadyResponse(
        status=status,
        ready=status is ApiStatus.OK,
        components=ReadyComponents(
            model=(ComponentStatus.READY if service_state.model_loaded else ComponentStatus.NOT_READY),
            index=(ComponentStatus.READY if service_state.index_loaded else ComponentStatus.NOT_READY),
            metadata=(ComponentStatus.READY if service_state.metadata_available else ComponentStatus.NOT_READY),
            database=database_status,
        ),
        request_id=request.state.request_id,
    )
    if response.ready:
        return response
    return JSONResponse(status_code=503, content=response.model_dump(mode="json"))
