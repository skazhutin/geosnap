from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

from app.services.image_validation import PreparedImage


@dataclass(frozen=True, slots=True)
class ServiceReadiness:
    model_loaded: bool
    index_loaded: bool
    metadata_available: bool


@dataclass(frozen=True, slots=True)
class ServicePrediction:
    lat: float
    lon: float
    confidence: float
    uncertainty_radius_m: float | None = None


@dataclass(frozen=True, slots=True)
class ServiceHypothesis:
    lat: float
    lon: float
    score: float
    uncertainty_radius_m: float | None = None


@dataclass(frozen=True, slots=True)
class ServiceMatch:
    reference_id: str
    source: str
    lat: float
    lon: float
    retrieval_score: float
    attribution: str
    license: str
    source_url: str
    license_url: str | None = None
    contributor_url: str | None = None
    verification_score: float | None = None
    thumbnail_available: bool = False


@dataclass(frozen=True, slots=True)
class ServiceDiagnostics:
    retriever: str | None = None
    embedding_ms: float | None = None
    retrieval_ms: float | None = None
    verification_ms: float | None = None
    query_ms: float | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ServiceResult:
    status: str
    prediction: ServicePrediction | None = None
    hypotheses: tuple[ServiceHypothesis, ...] = ()
    matches: tuple[ServiceMatch, ...] = ()
    diagnostics: ServiceDiagnostics = field(default_factory=ServiceDiagnostics)
    message: str | None = None


@runtime_checkable
class LocalizationService(Protocol):
    """Adapter contract between FastAPI and the long-lived ML composition.

    A configured factory is called once during application lifespan. ``load`` and
    ``close`` are optional; ``readiness`` and ``localize`` may be sync or async.
    Implementations may return the dataclasses above or equivalent mappings.
    """

    def readiness(self) -> ServiceReadiness | Mapping[str, Any]: ...

    def localize(self, query: PreparedImage) -> ServiceResult | Mapping[str, Any]: ...


class UnavailableLocalizationService:
    def readiness(self) -> ServiceReadiness:
        return ServiceReadiness(False, False, False)

    def localize(self, query: PreparedImage) -> ServiceResult:
        raise RuntimeError("localization service is unavailable")


def load_configured_service(factory_path: str) -> Any:
    """Import a no-argument ML adapter factory without importing ML at module load."""

    if ":" not in factory_path:
        raise ValueError("localization factory must use module:callable syntax")
    module_name, attribute_name = factory_path.split(":", 1)
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute_name)
    return factory()


def coerce_readiness(value: ServiceReadiness | Mapping[str, Any] | Any) -> ServiceReadiness:
    if isinstance(value, ServiceReadiness):
        return value
    if isinstance(value, Mapping):
        return ServiceReadiness(
            model_loaded=_as_ready_bool(value.get("model_loaded", value.get("model", False))),
            index_loaded=_as_ready_bool(value.get("index_loaded", value.get("index", False))),
            metadata_available=_as_ready_bool(value.get("metadata_available", value.get("metadata", False))),
        )
    return ServiceReadiness(
        model_loaded=_as_ready_bool(getattr(value, "model_loaded", False)),
        index_loaded=_as_ready_bool(getattr(value, "index_loaded", False)),
        metadata_available=_as_ready_bool(getattr(value, "metadata_available", False)),
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    fields = getattr(value, "__dataclass_fields__", None)
    if fields:
        return {name: getattr(value, name) for name in fields}
    raise TypeError("localization result must be a dataclass or mapping")


def coerce_result(value: ServiceResult | Mapping[str, Any] | Any) -> ServiceResult:
    if isinstance(value, ServiceResult):
        return value
    raw = _as_mapping(value)
    prediction_raw = raw.get("prediction")
    prediction = None
    if prediction_raw is not None:
        item = _as_mapping(prediction_raw)
        prediction = ServicePrediction(
            lat=float(item["lat"]),
            lon=float(item["lon"]),
            confidence=float(item["confidence"]),
            uncertainty_radius_m=_optional_float(item.get("uncertainty_radius_m")),
        )

    hypotheses = tuple(
        ServiceHypothesis(
            lat=float(item_map["lat"]),
            lon=float(item_map["lon"]),
            score=float(item_map["score"]),
            uncertainty_radius_m=_optional_float(item_map.get("uncertainty_radius_m")),
        )
        for item_map in (_as_mapping(item) for item in raw.get("hypotheses", ()))
    )
    matches = tuple(_coerce_match(item) for item in raw.get("matches", ()))
    diagnostics_raw = raw.get("diagnostics") or {}
    diagnostics_map = _as_mapping(diagnostics_raw)
    diagnostics = ServiceDiagnostics(
        retriever=_optional_string(diagnostics_map.get("retriever")),
        embedding_ms=_optional_float(diagnostics_map.get("embedding_ms")),
        retrieval_ms=_optional_float(diagnostics_map.get("retrieval_ms")),
        verification_ms=_optional_float(diagnostics_map.get("verification_ms")),
        query_ms=_optional_float(diagnostics_map.get("query_ms")),
        warnings=tuple(str(item) for item in diagnostics_map.get("warnings", ())),
    )
    return ServiceResult(
        status=_status_string(raw.get("status", "internal_error")),
        prediction=prediction,
        hypotheses=hypotheses,
        matches=matches,
        diagnostics=diagnostics,
        message=_optional_string(raw.get("message")),
    )


def _coerce_match(value: Any) -> ServiceMatch:
    item = _as_mapping(value)
    # Deliberately ignore image_path/raw thumbnail_url. The only public form is
    # an opaque same-origin route generated from a stable reference identifier.
    thumbnail_available = bool(item.get("thumbnail_available", False))
    return ServiceMatch(
        reference_id=str(item["reference_id"]),
        source=str(item["source"]),
        lat=float(item["lat"]),
        lon=float(item["lon"]),
        retrieval_score=float(item["retrieval_score"]),
        verification_score=_optional_float(item.get("verification_score")),
        attribution=str(item["attribution"]),
        license=str(item["license"]),
        source_url=str(item["source_url"]),
        license_url=_optional_string(item.get("license_url")),
        contributor_url=_optional_string(item.get("contributor_url")),
        thumbnail_available=thumbnail_available,
    )


def safe_thumbnail_url(match: ServiceMatch) -> str | None:
    if not match.thumbnail_available:
        return None
    return "/thumbnails/" + quote(match.reference_id, safe="")


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _optional_string(value: Any) -> str | None:
    return None if value is None else str(value)


def _status_string(value: Any) -> str:
    enum_value = getattr(value, "value", value)
    return str(enum_value)


def _as_ready_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "ready", "loaded", "available"}
    return bool(value)
