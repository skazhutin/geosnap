from __future__ import annotations

import inspect
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
from app.capacity import LocalizationCapacity
from app.config import Settings
from app.config import settings as default_settings
from app.observability import Metrics, configure_logging
from app.rate_limit import TokenBucketLimiter, client_ip
from app.schemas import ApiStatus, Diagnostics, LocalizeResponse
from app.services.localization import (
    UnavailableLocalizationService,
    coerce_readiness,
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


def _endpoint_label(path: str) -> str:
    if path.startswith("/thumbnails/"):
        return "/thumbnails/{reference_id}"
    if path in {"/health", "/ready", "/metrics", "/localize"}:
        return path
    return "other"


def _database_check(database_url: str) -> bool:
    from app.db.session import check_database

    return check_database(database_url)


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
        if app_settings.environment == "production":
            configure_logging(app_settings.log_level)
        try:
            if app_settings.artifact_manifest:
                from ml.artifacts import validate_production_artifacts

                validate_production_artifacts(app_settings.artifact_manifest)
            factory = service_factory or (lambda: load_configured_service(app_settings.localization_service_factory))
            service = factory()
            if inspect.isawaitable(service):
                service = await service
            await _call_lifecycle_hook(service, "load")
        except Exception as exc:
            logger.exception(
                "localization service unavailable",
                extra={
                    "event": "localization_service_unavailable",
                    "error_category": type(exc).__name__,
                },
            )
            service = UnavailableLocalizationService()

        application.state.localization_service = service
        application.state.settings = app_settings
        application.state.database_checker = database_checker or (
            lambda: _database_check(app_settings.database_url)
        )
        initial_readiness = coerce_readiness(await _readiness(service))
        application.state.metrics.backend_ready.set(
            1.0
            if (
                initial_readiness.model_loaded
                and initial_readiness.index_loaded
                and initial_readiness.metadata_available
            )
            else 0.0
        )
        model_load_ms = getattr(service, "model_load_ms", None)
        index_load_ms = getattr(service, "index_load_ms", None)
        if model_load_ms is not None:
            application.state.metrics.model_load.set(float(model_load_ms) / 1000.0)
        if index_load_ms is not None:
            application.state.metrics.index_load.set(float(index_load_ms) / 1000.0)
        if not isinstance(service, UnavailableLocalizationService):
            index = getattr(service, "index", None)
            logger.info(
                "production localization runtime ready",
                extra={
                    "event": "runtime_ready",
                    "model": getattr(getattr(service, "retriever", None), "model_name", None),
                    "device": getattr(getattr(service, "retriever", None), "device", None),
                    "index_id": getattr(service, "expected_index_id", None),
                    "gallery_count": getattr(index, "size", None),
                    "top_k": getattr(service, "top_k", None),
                    "config_sha256": str(getattr(service, "production_config_sha256", ""))[:12],
                    "checkpoint_sha256": "8cfed7d4e8bb",
                },
            )
        try:
            yield
        finally:
            try:
                await _call_lifecycle_hook(service, "close")
            except Exception as exc:
                logger.exception(
                    "localization service close failed",
                    extra={
                        "event": "localization_service_close_failed",
                        "error_category": type(exc).__name__,
                    },
                )

    production = app_settings.environment == "production"
    application = FastAPI(
        title="GeoSnap API",
        version="1.0.0",
        lifespan=lifespan,
        debug=False,
        docs_url=None if production else "/docs",
        redoc_url=None if production else "/redoc",
        openapi_url=None if production else "/openapi.json",
    )
    application.state.metrics = Metrics()
    application.state.localization_capacity = LocalizationCapacity(
        concurrency=app_settings.localization_concurrency,
        queue_limit=app_settings.localization_queue_limit,
        queue_timeout_seconds=app_settings.localization_queue_timeout_seconds,
    )
    application.state.rate_limiter = TokenBucketLimiter(
        rate_per_minute=app_settings.localization_rate_per_minute,
        burst=app_settings.localization_rate_burst,
    )
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
        endpoint = _endpoint_label(request.url.path)
        try:
            if request.method == "POST" and request.url.path == "/localize":
                key = client_ip(request, trust_forwarded_for=app_settings.trust_forwarded_for)
                allowed, retry_after = await application.state.rate_limiter.allow(key)
                if not allowed:
                    application.state.metrics.rate_limited.inc()
                    application.state.metrics.localization_requests.labels(status="rate_limited").inc()
                    payload = LocalizeResponse(
                        status=ApiStatus.RATE_LIMITED,
                        diagnostics=Diagnostics(),
                        message="Too many localization requests. Retry later.",
                        request_id=request.state.request_id,
                    )
                    response = JSONResponse(
                        status_code=429,
                        content=payload.model_dump(mode="json"),
                        headers={"Retry-After": str(retry_after)},
                    )
                else:
                    response = await call_next(request)
            else:
                response = await call_next(request)
            response_status = response.status_code
        except Exception as exc:
            logger.exception(
                "unhandled request exception",
                extra={
                    "event": "request_unhandled_exception",
                    "request_id": request.state.request_id,
                    "error_category": type(exc).__name__,
                    "endpoint": endpoint,
                },
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
        response.headers.setdefault("Cache-Control", "no-store" if endpoint == "/localize" else "no-cache")
        application.state.metrics.http_requests.labels(
            method=request.method,
            endpoint=endpoint,
            status=str(response_status),
        ).inc()
        application.state.metrics.http_latency.labels(
            method=request.method,
            endpoint=endpoint,
        ).observe(elapsed_ms / 1000.0)
        if response_status >= 500:
            application.state.metrics.server_errors.inc()
        logger.info(
            "request complete",
            extra={
                "event": "request_complete",
                "request_id": request.state.request_id,
                "method": request.method,
                "endpoint": endpoint,
                "http_status": response_status,
                "duration_ms": round(elapsed_ms, 3),
            },
        )
        return response

    application.include_router(health_router)
    application.include_router(localize_router)
    application.include_router(thumbnails_router)
    return application


app = create_app()


async def _readiness(service: Any) -> Any:
    try:
        return await _call_lifecycle_readiness(service)
    except Exception:
        return {"model_loaded": False, "index_loaded": False, "metadata_available": False}


async def _call_lifecycle_readiness(service: Any) -> Any:
    readiness = service.readiness
    if inspect.iscoroutinefunction(readiness):
        return await readiness()
    return await run_in_threadpool(readiness)
