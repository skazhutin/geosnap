"""Typed inputs and outputs for bounded local geometric verification."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

type ImageInput = str | Path | Image.Image | np.ndarray


class VerificationError(RuntimeError):
    """Base error raised by geometric verification."""


class VerificationInputError(VerificationError):
    """An image or candidate supplied to verification is invalid."""


class OptionalVerificationDependencyError(VerificationError):
    """A requested optional verification backend is not installed."""


@dataclass(frozen=True, slots=True)
class VerificationCandidate:
    """One retrieved reference eligible for bounded second-stage reranking."""

    reference_id: str
    image: ImageInput
    retrieval_score: float
    original_rank: int
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.reference_id:
            raise ValueError("reference_id cannot be empty")
        if not math.isfinite(self.retrieval_score):
            raise ValueError("retrieval_score must be finite")
        if self.original_rank < 1:
            raise ValueError("original_rank must be >= 1")


@dataclass(frozen=True, slots=True)
class GeometricEvidence:
    """Auditable feature-match and RANSAC signals for one image pair.

    ``raw_match_count`` is the number of descriptor-neighbour pairs considered.
    ``tentative_match_count`` is the count left after descriptor filtering (and,
    for the OpenCV backend, a mutual consistency check). The selected inliers
    come from whichever robust model has the strongest support.
    """

    backend: str
    query_keypoint_count: int
    reference_keypoint_count: int
    raw_match_count: int
    tentative_match_count: int
    homography_inlier_count: int
    fundamental_inlier_count: int
    inlier_count: int
    inlier_ratio: float
    normalized_score: float
    selected_model: str | None
    latency_ms: float
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        counts = (
            self.query_keypoint_count,
            self.reference_keypoint_count,
            self.raw_match_count,
            self.tentative_match_count,
            self.homography_inlier_count,
            self.fundamental_inlier_count,
            self.inlier_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("verification counts cannot be negative")
        if not 0.0 <= self.inlier_ratio <= 1.0:
            raise ValueError("inlier_ratio must be in [0, 1]")
        if not 0.0 <= self.normalized_score <= 1.0:
            raise ValueError("normalized_score must be in [0, 1]")
        if not math.isfinite(self.latency_ms) or self.latency_ms < 0:
            raise ValueError("latency_ms must be finite and non-negative")
        if self.inlier_count > self.tentative_match_count:
            raise ValueError("inlier_count cannot exceed tentative_match_count")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class VerifiedCandidate:
    """Candidate after optional local verification and reranking."""

    reference_id: str
    retrieval_score: float
    original_rank: int
    final_rank: int
    rerank_score: float
    verification_score: float | None
    evidence: GeometricEvidence | None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence"] = None if self.evidence is None else self.evidence.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class RerankResult:
    """Complete result, including whether verification actually ran."""

    enabled: bool
    backend: str | None
    verified_count: int
    total_latency_ms: float
    candidates: tuple[VerifiedCandidate, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "backend": self.backend,
            "verified_count": self.verified_count,
            "total_latency_ms": self.total_latency_ms,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }
