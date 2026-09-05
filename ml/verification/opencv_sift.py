"""Dependency-light SIFT matching baseline using OpenCV."""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PIL import Image, ImageOps, UnidentifiedImageError

from .base import BaseGeometricVerifier
from .geometry import RansacConfig, empty_evidence, estimate_geometry
from .models import (
    GeometricEvidence,
    ImageInput,
    OptionalVerificationDependencyError,
    VerificationInputError,
)


@dataclass(frozen=True, slots=True)
class OpenCvSiftConfig:
    max_keypoints: int = 2_048
    max_image_edge: int = 1_600
    lowe_ratio: float = 0.8
    mutual_check: bool = True
    ransac: RansacConfig = RansacConfig()

    def __post_init__(self) -> None:
        if self.max_keypoints < 4:
            raise ValueError("max_keypoints must be >= 4")
        if self.max_image_edge < 64:
            raise ValueError("max_image_edge must be >= 64")
        if not 0 < self.lowe_ratio < 1:
            raise ValueError("lowe_ratio must be in (0, 1)")


def _uint8_array(array: np.ndarray) -> np.ndarray:
    values = np.asarray(array)
    if values.size == 0:
        raise VerificationInputError("image array cannot be empty")
    if not np.isfinite(values).all():
        raise VerificationInputError("image array contains NaN or infinite values")
    if values.dtype == np.uint8:
        return values
    values = values.astype(np.float32)
    if float(values.max()) <= 1.0 and float(values.min()) >= 0.0:
        values = values * 255.0
    return np.clip(values, 0, 255).astype(np.uint8)


def load_grayscale(image: ImageInput, *, max_image_edge: int) -> np.ndarray:
    """Load, orient, bound, and convert an accepted image input to grayscale."""

    try:
        if isinstance(image, (str, Path)):
            with Image.open(image) as opened:
                pil_image = ImageOps.exif_transpose(opened).convert("L")
        elif isinstance(image, Image.Image):
            pil_image = ImageOps.exif_transpose(image).convert("L")
        elif isinstance(image, np.ndarray):
            array = _uint8_array(image)
            if array.ndim == 2:
                pil_image = Image.fromarray(array, mode="L")
            elif array.ndim == 3 and array.shape[2] in {1, 3, 4}:
                if array.shape[2] == 1:
                    pil_image = Image.fromarray(array[:, :, 0], mode="L")
                else:
                    pil_image = Image.fromarray(array).convert("L")
            else:
                raise VerificationInputError(
                    f"image array must have shape HxW, HxWx1, HxWx3, or HxWx4; got {array.shape}"
                )
        else:
            raise VerificationInputError(f"unsupported image input type: {type(image).__name__}")
    except (FileNotFoundError, OSError, UnidentifiedImageError) as exc:
        raise VerificationInputError(f"could not decode image: {exc}") from exc

    if min(pil_image.size) < 2:
        raise VerificationInputError(f"image is too small for local features: {pil_image.size}")
    if max(pil_image.size) > max_image_edge:
        scale = max_image_edge / max(pil_image.size)
        target = (
            max(2, round(pil_image.width * scale)),
            max(2, round(pil_image.height * scale)),
        )
        pil_image = pil_image.resize(target, Image.Resampling.LANCZOS)
    return np.ascontiguousarray(np.asarray(pil_image, dtype=np.uint8))


class OpenCvSiftVerifier(BaseGeometricVerifier):
    """SIFT + symmetric Lowe-ratio matching + two robust geometry models."""

    backend_name = "opencv_sift"

    def __init__(
        self,
        config: OpenCvSiftConfig | None = None,
        *,
        max_pairs: int = 20,
    ) -> None:
        super().__init__(max_pairs=max_pairs)
        self.config = config or OpenCvSiftConfig()
        try:
            import cv2
        except ImportError as exc:  # pragma: no cover - normal project dependency
            raise OptionalVerificationDependencyError(
                "opencv_sift verification requires opencv-python-headless"
            ) from exc
        if not hasattr(cv2, "SIFT_create"):
            raise OptionalVerificationDependencyError(
                "installed OpenCV build does not provide SIFT_create"
            )
        self._cv2 = cv2
        self._extractor = cv2.SIFT_create(nfeatures=self.config.max_keypoints)
        self._matcher = cv2.BFMatcher(cv2.NORM_L2, crossCheck=False)

    def _extract(self, image: ImageInput) -> tuple[list[object], np.ndarray | None]:
        grayscale = load_grayscale(image, max_image_edge=self.config.max_image_edge)
        keypoints, descriptors = self._extractor.detectAndCompute(grayscale, None)
        return list(keypoints), descriptors

    def _ratio_matches(
        self,
        descriptors0: np.ndarray,
        descriptors1: np.ndarray,
    ) -> tuple[int, list[object]]:
        if len(descriptors0) < 1 or len(descriptors1) < 2:
            return 0, []
        forward_pairs = self._matcher.knnMatch(descriptors0, descriptors1, k=2)
        forward = {
            first.queryIdx: first
            for pair in forward_pairs
            if len(pair) == 2
            for first, second in [pair]
            if first.distance < self.config.lowe_ratio * second.distance
        }
        if not self.config.mutual_check:
            return len(forward_pairs), list(forward.values())
        if len(descriptors0) < 2:
            return len(forward_pairs), []
        reverse_pairs = self._matcher.knnMatch(descriptors1, descriptors0, k=2)
        reverse = {
            first.queryIdx: first.trainIdx
            for pair in reverse_pairs
            if len(pair) == 2
            for first, second in [pair]
            if first.distance < self.config.lowe_ratio * second.distance
        }
        mutual = [
            match
            for query_index, match in forward.items()
            if reverse.get(match.trainIdx) == query_index
        ]
        return len(forward_pairs), mutual

    def verify(
        self,
        query_image: ImageInput,
        reference_images: Sequence[ImageInput],
    ) -> tuple[GeometricEvidence, ...]:
        self._validate_pair_count(reference_images)
        if not reference_images:
            return ()
        query_keypoints, query_descriptors = self._extract(query_image)
        if query_descriptors is None or len(query_keypoints) < 2:
            return tuple(
                empty_evidence(
                    backend=self.backend_name,
                    query_keypoints=len(query_keypoints),
                    reference_keypoints=0,
                    latency_ms=0.0,
                    reason="query_has_no_sift_features",
                )
                for _ in reference_images
            )

        evidence: list[GeometricEvidence] = []
        for reference_image in reference_images:
            started = time.perf_counter()
            try:
                reference_keypoints, reference_descriptors = self._extract(reference_image)
            except VerificationInputError as exc:
                evidence.append(
                    empty_evidence(
                        backend=self.backend_name,
                        query_keypoints=len(query_keypoints),
                        reference_keypoints=0,
                        latency_ms=(time.perf_counter() - started) * 1_000,
                        reason=f"invalid_reference_image:{exc}",
                    )
                )
                continue
            if reference_descriptors is None or len(reference_keypoints) < 2:
                evidence.append(
                    empty_evidence(
                        backend=self.backend_name,
                        query_keypoints=len(query_keypoints),
                        reference_keypoints=len(reference_keypoints),
                        latency_ms=(time.perf_counter() - started) * 1_000,
                        reason="reference_has_no_sift_features",
                    )
                )
                continue

            raw_count, matches = self._ratio_matches(query_descriptors, reference_descriptors)
            query_points = np.asarray(
                [query_keypoints[match.queryIdx].pt for match in matches], dtype=np.float32
            ).reshape(-1, 2)
            reference_points = np.asarray(
                [reference_keypoints[match.trainIdx].pt for match in matches], dtype=np.float32
            ).reshape(-1, 2)
            evidence.append(
                estimate_geometry(
                    query_points,
                    reference_points,
                    backend=self.backend_name,
                    query_keypoint_count=len(query_keypoints),
                    reference_keypoint_count=len(reference_keypoints),
                    raw_match_count=raw_count,
                    latency_ms=(time.perf_counter() - started) * 1_000,
                    config=self.config.ransac,
                )
            )
        return tuple(evidence)
