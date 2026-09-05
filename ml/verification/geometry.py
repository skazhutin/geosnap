"""Shared robust two-view geometry estimation."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass

import numpy as np

from .models import GeometricEvidence, OptionalVerificationDependencyError


@dataclass(frozen=True, slots=True)
class RansacConfig:
    homography_reprojection_threshold_px: float = 4.0
    fundamental_reprojection_threshold_px: float = 2.0
    confidence: float = 0.999
    max_iterations: int = 10_000
    minimum_scoring_inlier_count: int = 8
    full_score_inlier_count: int = 30

    def __post_init__(self) -> None:
        if self.homography_reprojection_threshold_px <= 0:
            raise ValueError("homography threshold must be positive")
        if self.fundamental_reprojection_threshold_px <= 0:
            raise ValueError("fundamental threshold must be positive")
        if not 0 < self.confidence < 1:
            raise ValueError("RANSAC confidence must be in (0, 1)")
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be >= 1")
        if self.minimum_scoring_inlier_count < 0:
            raise ValueError("minimum_scoring_inlier_count cannot be negative")
        if self.full_score_inlier_count <= self.minimum_scoring_inlier_count:
            raise ValueError(
                "full_score_inlier_count must exceed minimum_scoring_inlier_count"
            )


def _mask_count(mask: np.ndarray | None, match_count: int) -> int:
    if mask is None:
        return 0
    values = np.asarray(mask).reshape(-1)
    if len(values) != match_count:
        return 0
    return int(np.count_nonzero(values))


def _failure_evidence(
    *,
    backend: str,
    query_keypoints: int,
    reference_keypoints: int,
    raw_matches: int,
    tentative_matches: int,
    latency_ms: float,
    reason: str,
) -> GeometricEvidence:
    return GeometricEvidence(
        backend=backend,
        query_keypoint_count=query_keypoints,
        reference_keypoint_count=reference_keypoints,
        raw_match_count=raw_matches,
        tentative_match_count=tentative_matches,
        homography_inlier_count=0,
        fundamental_inlier_count=0,
        inlier_count=0,
        inlier_ratio=0.0,
        normalized_score=0.0,
        selected_model=None,
        latency_ms=latency_ms,
        failure_reason=reason,
    )


def normalize_geometric_score(
    inlier_count: int,
    tentative_match_count: int,
    *,
    config: RansacConfig | None = None,
) -> float:
    """Map robust support to ``[0, 1]`` without rewarding minimal fits."""

    settings = config or RansacConfig()
    if inlier_count < 0 or tentative_match_count < 0:
        raise ValueError("match counts cannot be negative")
    if inlier_count > tentative_match_count:
        raise ValueError("inlier_count cannot exceed tentative_match_count")
    if tentative_match_count == 0:
        return 0.0
    inlier_ratio = inlier_count / tentative_match_count
    support = max(
        0.0,
        min(
            1.0,
            (inlier_count - settings.minimum_scoring_inlier_count)
            / (
                settings.full_score_inlier_count
                - settings.minimum_scoring_inlier_count
            ),
        ),
    )
    return max(0.0, min(1.0, inlier_ratio * support))


def estimate_geometry(
    query_points: np.ndarray,
    reference_points: np.ndarray,
    *,
    backend: str,
    query_keypoint_count: int,
    reference_keypoint_count: int,
    raw_match_count: int,
    latency_ms: float,
    config: RansacConfig | None = None,
) -> GeometricEvidence:
    """Estimate homography and fundamental-matrix support with RANSAC.

    The normalized score is intentionally interpretable rather than learned:
    selected-model inlier ratio multiplied by linearly saturated inlier support.
    Minimal four-point homographies and eight-point fundamental matrices can be
    reported as raw evidence but receive zero score until support exceeds the
    configured minimum, avoiding a perfect score from a minimally determined fit.
    """

    settings = config or RansacConfig()
    ransac_started = time.perf_counter()

    def total_latency_ms() -> float:
        return latency_ms + (time.perf_counter() - ransac_started) * 1_000

    points0 = np.asarray(query_points, dtype=np.float32).reshape(-1, 2)
    points1 = np.asarray(reference_points, dtype=np.float32).reshape(-1, 2)
    if points0.shape != points1.shape:
        raise ValueError("query and reference point arrays must have matching shapes")
    tentative_count = len(points0)
    if tentative_count < 4:
        return _failure_evidence(
            backend=backend,
            query_keypoints=query_keypoint_count,
            reference_keypoints=reference_keypoint_count,
            raw_matches=raw_match_count,
            tentative_matches=tentative_count,
            latency_ms=total_latency_ms(),
            reason="fewer_than_4_tentative_matches",
        )

    try:
        import cv2
    except ImportError as exc:  # pragma: no cover - project dependency in normal installs
        raise OptionalVerificationDependencyError(
            "geometric RANSAC requires opencv-python-headless"
        ) from exc

    homography_inliers = 0
    fundamental_inliers = 0
    try:
        _, mask = cv2.findHomography(
            points0,
            points1,
            method=cv2.RANSAC,
            ransacReprojThreshold=settings.homography_reprojection_threshold_px,
            maxIters=settings.max_iterations,
            confidence=settings.confidence,
        )
        homography_inliers = _mask_count(mask, tentative_count)
    except cv2.error:
        homography_inliers = 0

    if tentative_count >= 8:
        try:
            _, mask = cv2.findFundamentalMat(
                points0,
                points1,
                method=cv2.FM_RANSAC,
                ransacReprojThreshold=settings.fundamental_reprojection_threshold_px,
                confidence=settings.confidence,
                maxIters=settings.max_iterations,
            )
            fundamental_inliers = _mask_count(mask, tentative_count)
        except cv2.error:
            fundamental_inliers = 0

    if homography_inliers >= fundamental_inliers and homography_inliers > 0:
        selected_model = "homography"
        inlier_count = homography_inliers
    elif fundamental_inliers > 0:
        selected_model = "fundamental"
        inlier_count = fundamental_inliers
    else:
        return _failure_evidence(
            backend=backend,
            query_keypoints=query_keypoint_count,
            reference_keypoints=reference_keypoint_count,
            raw_matches=raw_match_count,
            tentative_matches=tentative_count,
            latency_ms=total_latency_ms(),
            reason="ransac_found_no_consistent_model",
        )

    inlier_ratio = inlier_count / tentative_count
    normalized_score = normalize_geometric_score(
        inlier_count,
        tentative_count,
        config=settings,
    )
    if not math.isfinite(normalized_score):  # defensive; inputs above are finite integers
        normalized_score = 0.0
    return GeometricEvidence(
        backend=backend,
        query_keypoint_count=query_keypoint_count,
        reference_keypoint_count=reference_keypoint_count,
        raw_match_count=raw_match_count,
        tentative_match_count=tentative_count,
        homography_inlier_count=homography_inliers,
        fundamental_inlier_count=fundamental_inliers,
        inlier_count=inlier_count,
        inlier_ratio=inlier_ratio,
        normalized_score=normalized_score,
        selected_model=selected_model,
        latency_ms=total_latency_ms(),
    )


def empty_evidence(
    *,
    backend: str,
    query_keypoints: int,
    reference_keypoints: int,
    latency_ms: float,
    reason: str,
) -> GeometricEvidence:
    """Return an explicit zero-evidence record for a recoverable pair failure."""

    return _failure_evidence(
        backend=backend,
        query_keypoints=query_keypoints,
        reference_keypoints=reference_keypoints,
        raw_matches=0,
        tentative_matches=0,
        latency_ms=latency_ms,
        reason=reason,
    )
