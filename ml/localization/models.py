"""Typed values passed through localization independently of HTTP models."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from .geo import validate_coordinate


class LocalizationStatus(StrEnum):
    OK = "ok"
    LOW_CONFIDENCE = "low_confidence"
    OUT_OF_COVERAGE = "out_of_coverage"


@dataclass(frozen=True, slots=True)
class Candidate:
    reference_id: str
    lat: float
    lon: float
    retrieval_score: float
    rank: int
    localization_score: float | None = None
    verification_score: float | None = None
    city_id: str | None = None
    index_id: str | None = None
    source: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.reference_id:
            raise ValueError("candidate reference_id cannot be empty")
        validate_coordinate(self.lat, self.lon)
        if not math.isfinite(self.retrieval_score):
            raise ValueError("candidate retrieval_score must be finite")
        if self.rank < 1:
            raise ValueError("candidate rank must be >= 1")
        if self.verification_score is not None and not math.isfinite(self.verification_score):
            raise ValueError("verification_score must be finite when present")
        if self.localization_score is not None and not math.isfinite(self.localization_score):
            raise ValueError("localization_score must be finite when present")

    @classmethod
    def from_retrieval_result(cls, result: Any) -> Candidate:
        metadata = dict(getattr(result, "metadata", {}) or {})
        lat = getattr(result, "lat", None)
        lon = getattr(result, "lon", None)
        if lat is None:
            lat = metadata.get("lat")
        if lon is None:
            lon = metadata.get("lon")
        if lat is None or lon is None:
            raise ValueError(f"retrieval result {result.reference_id!r} lacks lat/lon metadata")
        verification = getattr(result, "verification_score", None)
        if verification is None:
            verification = metadata.get("verification_score")
        retrieval_score = getattr(result, "score", None)
        if retrieval_score is None:
            retrieval_score = result.retrieval_score
        return cls(
            reference_id=str(result.reference_id),
            lat=float(lat),
            lon=float(lon),
            retrieval_score=float(retrieval_score),
            rank=int(result.rank),
            localization_score=(
                None
                if metadata.get("localization_score") is None
                else float(metadata["localization_score"])
            ),
            verification_score=None if verification is None else float(verification),
            city_id=_optional_string(metadata.get("city_id", getattr(result, "city_id", None))),
            index_id=_optional_string(metadata.get("index_id")),
            source=_optional_string(metadata.get("source", getattr(result, "source", None))),
            metadata=metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _optional_string(value: Any) -> str | None:
    return None if value is None else str(value)


@dataclass(frozen=True, slots=True)
class LocationHypothesis:
    rank: int
    lat: float
    lon: float
    score: float
    mass_fraction: float
    compactness_m: float
    reference_ids: tuple[str, ...]
    city_id: str | None = None
    index_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LocalizationResult:
    status: LocalizationStatus
    lat: float | None
    lon: float | None
    confidence: float
    uncertainty_radius_m: float | None
    hypotheses: tuple[LocationHypothesis, ...]
    matches: tuple[Candidate, ...]
    diagnostics: Mapping[str, Any]
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "prediction": (
                None
                if self.lat is None or self.lon is None
                else {
                    "lat": self.lat,
                    "lon": self.lon,
                    "confidence": self.confidence,
                    "uncertainty_radius_m": self.uncertainty_radius_m,
                }
            ),
            "hypotheses": [hypothesis.to_dict() for hypothesis in self.hypotheses],
            "matches": [match.to_dict() for match in self.matches],
            "diagnostics": dict(self.diagnostics),
            "reasons": list(self.reasons),
        }
