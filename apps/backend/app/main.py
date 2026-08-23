from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from time import perf_counter
from typing import Any

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.health import router as health_router
from app.api.localize import router as localize_router
from app.api.thumbnails import router as thumbnails_router
from app.config import Settings
from app.config import settings as default_settings
from app.db.session import check_database
from app.schemas import ApiStatus, Diagnostics, LocalizeResponse
from app.services.localization import (
    UnavailableLocalizationService,
    load_configured_service,
)

logger = logging.getLogger("geosnap.api")
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


async def _call_lifecycle_hook(service: Any, name: str) -> None:
    hook = getattr(service, name, None)
    if hook is None:
        return
    if inspect.iscoroutinefunction(hook):
        await hook()
        return
    result = await run_in_threadpool(hook)
    if inspect.isawaitable(result):
        await result


def _request_id(request: Request) -> str:
    supplied = request.headers.get("x-request-id", "")
    if _SAFE_REQUEST_ID.fullmatch(supplied):
        return supplied
    return str(uuid.uuid4())


def create_app(
    *,
    settings: Settings | None = None,
    service_factory: Callable[[], Any] | None = None,
    database_checker: Callable[[], bool] | None = None,
) -> FastAPI:
    app_settings = settings or default_settings

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        service: Any
        try:
            factory = service_factory or (lambda: load_configured_service(app_settings.localization_service_factory))
            service = factory()
            if inspect.isawaitable(service):
                service = await service
            await _call_lifecycle_hook(service, "load")
        except Exception as exc:
            logger.warning(
                json.dumps(
                    {
                        "event": "localization_service_unavailable",
                        "error_type": type(exc).__name__,
                    }
                )
            )
            service = UnavailableLocalizationService()

        application.state.localization_service = service
        application.state.settings = app_settings
        application.state.database_checker = database_checker or (lambda: check_database(app_settings.database_url))
        application.state.localization_semaphore = asyncio.Semaphore(app_settings.localization_concurrency)
        try:
            yield
        finally:
            try:
                await _call_lifecycle_hook(service, "close")
            except Exception as exc:
                logger.warning(
                    json.dumps(
                        {
                            "event": "localization_service_close_failed",
                            "error_type": type(exc).__name__,
                        }
                    )
                )

    application = FastAPI(title="GeoSnap API", version="1.0.0", lifespan=lifespan)
    if app_settings.cors_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(app_settings.cors_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["Content-Type", "X-Request-ID"],
            expose_headers=["X-Request-ID"],
            max_age=600,
        )

    @application.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = _request_id(request)
        started = perf_counter()
        response_status = 500
        try:
            response = await call_next(request)
            response_status = response.status_code
        except Exception as exc:
            logger.error(
                json.dumps(
                    {
                        "event": "request_unhandled_exception",
                        "request_id": request.state.request_id,
                        "error_type": type(exc).__name__,
                        "stage": "request_middleware",
                    },
                    separators=(",", ":"),
                )
            )
            response = JSONResponse(
                status_code=500,
                content=LocalizeResponse(
                    status=ApiStatus.INTERNAL_ERROR,
                    diagnostics=Diagnostics(),
                    message="The server could not complete the request.",
                    request_id=request.state.request_id,
                ).model_dump(mode="json"),
            )
        elapsed_ms = (perf_counter() - started) * 1000.0
        response.headers["X-Request-ID"] = request.state.request_id
        logger.info(
            json.dumps(
                {
                    "event": "request_complete",
                    "request_id": request.state.request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response_status,
                    "total_ms": round(elapsed_ms, 3),
                },
                separators=(",", ":"),
            )
        )
        return response

    application.include_router(health_router)
    application.include_router(localize_router)
    application.include_router(thumbnails_router)
    return application


app = create_app()
