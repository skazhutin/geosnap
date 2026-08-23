from __future__ import annotations

from enum import StrEnum
from math import isfinite
from typing import Annotated, Literal
from urllib.parse import parse_qsl, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]
UnitScore = Annotated[float, Field(ge=0.0, le=1.0)]
Milliseconds = Annotated[float, Field(ge=0.0)]


class ApiStatus(StrEnum):
    OK = "ok"
    INVALID_IMAGE = "invalid_image"
    UNSUPPORTED_FORMAT = "unsupported_format"
    IMAGE_TOO_LARGE = "image_too_large"
    MODEL_NOT_READY = "model_not_ready"
    INDEX_NOT_READY = "index_not_ready"
    LOW_CONFIDENCE = "low_confidence"
    OUT_OF_COVERAGE = "out_of_coverage"
    INTERNAL_ERROR = "internal_error"


class ComponentStatus(StrEnum):
    READY = "ready"
    NOT_READY = "not_ready"
    NOT_CONFIGURED = "not_configured"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class HealthResponse(StrictModel):
    status: Literal[ApiStatus.OK] = ApiStatus.OK
    request_id: str


class ReadyComponents(StrictModel):
    model: ComponentStatus
    index: ComponentStatus
    metadata: ComponentStatus
    database: ComponentStatus = ComponentStatus.NOT_CONFIGURED


class ReadyResponse(StrictModel):
    status: ApiStatus
    ready: bool
    components: ReadyComponents
    request_id: str


class Prediction(StrictModel):
    lat: Latitude
    lon: Longitude
    confidence: UnitScore
    uncertainty_radius_m: float | None = Field(default=None, gt=0.0)


class Hypothesis(StrictModel):
    lat: Latitude
    lon: Longitude
    score: UnitScore
    uncertainty_radius_m: float | None = Field(default=None, gt=0.0)


class Match(StrictModel):
    reference_id: str = Field(min_length=1, max_length=256)
    source: str = Field(min_length=1, max_length=64)
    lat: Latitude
    lon: Longitude
    retrieval_score: float
    verification_score: float | None = None
    thumbnail_url: str | None = Field(default=None, max_length=1024)
    attribution: str = Field(min_length=1, max_length=2048)
    license: str = Field(min_length=1, max_length=256)
    source_url: str = Field(min_length=1, max_length=2048)
    license_url: str | None = Field(default=None, max_length=2048)
    contributor_url: str | None = Field(default=None, max_length=2048)

    @field_validator("reference_id")
    @classmethod
    def reference_id_is_not_a_storage_path(cls, value: str) -> str:
        if (
            value.startswith(("/", "\\"))
            or "\\" in value
            or ".." in value
            or any(ord(character) < 32 for character in value)
        ):
            raise ValueError("reference_id must be an opaque identifier")
        return value

    @field_validator("source")
    @classmethod
    def source_is_a_safe_label(cls, value: str) -> str:
        if not value.replace("-", "").replace("_", "").isalnum():
            raise ValueError("source must be a safe label")
        return value

    @field_validator("attribution")
    @classmethod
    def attribution_contains_no_sensitive_details(cls, value: str) -> str:
        lowered = value.lower()
        forbidden = (
            "access_token",
            "token=",
            "password=",
            "postgresql://",
            "postgresql+",
            "file://",
            "traceback (most recent call last)",
        )
        if any(marker in lowered for marker in forbidden):
            raise ValueError("attribution contains unsafe content")
        return value

    @field_validator("source_url", "license_url", "contributor_url")
    @classmethod
    def attribution_urls_are_safe_https(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("attribution URL must be a public HTTPS URL")
        sensitive = {"access_token", "token", "api_key", "apikey", "signature", "sig"}
        if any(key.lower() in sensitive for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
            raise ValueError("attribution URL contains a sensitive query parameter")
        return value

    @model_validator(mode="after")
    def attribution_urls_match_the_declared_source(self) -> Match:
        host = (urlsplit(self.source_url).hostname or "").lower()
        allowed_source_hosts = {
            "mapillary": {"mapillary.com", "www.mapillary.com"},
            "kartaview": {"kartaview.org", "www.kartaview.org"},
        }
        allowed = allowed_source_hosts.get(self.source.lower())
        if allowed is None or host not in allowed:
            raise ValueError("source_url host does not match source")
        if self.contributor_url is not None:
            contributor_host = (urlsplit(self.contributor_url).hostname or "").lower()
            if self.source.lower() != "mapillary" or contributor_host not in allowed_source_hosts["mapillary"]:
                raise ValueError("contributor_url host does not match source")
        if self.license_url is not None:
            license_host = (urlsplit(self.license_url).hostname or "").lower()
            if license_host not in {"creativecommons.org", "www.creativecommons.org"}:
                raise ValueError("license_url host is not approved")
        return self

    @field_validator("retrieval_score", "verification_score")
    @classmethod
    def scores_must_be_finite(cls, value: float | None) -> float | None:
        if value is not None and not isfinite(value):
            raise ValueError("score must be finite")
        return value

    @field_validator("thumbnail_url")
    @classmethod
    def thumbnail_must_be_an_internal_route(cls, value: str | None) -> str | None:
        # Reference storage paths and third-party signed URLs must never cross the
        # API boundary. A future thumbnail endpoint can resolve this opaque route.
        if value is None:
            return None
        if not value.startswith("/thumbnails/") or value.startswith("//"):
            raise ValueError("thumbnail_url must be an internal thumbnail route")
        if "?" in value or "#" in value or ".." in value or "\\" in value:
            raise ValueError("unsafe thumbnail_url")
        return value


class Diagnostics(StrictModel):
    width: int | None = Field(default=None, gt=0)
    height: int | None = Field(default=None, gt=0)
    sharpness: UnitScore | None = None
    brightness: UnitScore | None = None
    exposure: UnitScore | None = None
    retriever: str | None = Field(default=None, max_length=128)
    preprocess_ms: Milliseconds | None = None
    embedding_ms: Milliseconds | None = None
    retrieval_ms: Milliseconds | None = None
    verification_ms: Milliseconds | None = None
    query_ms: Milliseconds | None = None
    total_ms: Milliseconds | None = None
    warnings: list[str] = Field(default_factory=list, max_length=20)

    @field_validator("warnings")
    @classmethod
    def bounded_warnings(cls, values: list[str]) -> list[str]:
        return [str(value)[:160] for value in values]


class LocalizeResponse(StrictModel):
    status: ApiStatus
    prediction: Prediction | None = None
    hypotheses: list[Hypothesis] = Field(default_factory=list, max_length=50)
    matches: list[Match] = Field(default_factory=list, max_length=100)
    diagnostics: Diagnostics = Field(default_factory=Diagnostics)
    message: str | None = Field(default=None, max_length=300)
    request_id: str

    @model_validator(mode="after")
    def successful_result_has_prediction(self) -> LocalizeResponse:
        if self.status is ApiStatus.OK and self.prediction is None:
            raise ValueError("an ok response requires a prediction")
        return self
