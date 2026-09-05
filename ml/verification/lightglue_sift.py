"""Optional official LightGlue matcher with its SIFT extractor."""

from __future__ import annotations

import importlib
import time
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from .base import BaseGeometricVerifier
from .geometry import RansacConfig, empty_evidence, estimate_geometry
from .models import (
    GeometricEvidence,
    ImageInput,
    OptionalVerificationDependencyError,
    VerificationError,
    VerificationInputError,
)
from .opencv_sift import load_grayscale


class LightGlueSiftVerifier(BaseGeometricVerifier):
    """Official ``cvg/LightGlue`` matcher configured for SIFT descriptors.

    Loading is lazy and explicit. No untrained model or OpenCV matcher is
    substituted when the requested package or pretrained weights are missing.
    """

    backend_name = "lightglue_sift"

    def __init__(
        self,
        *,
        max_keypoints: int = 2_048,
        max_image_edge: int = 1_600,
        device: str = "auto",
        max_pairs: int = 20,
        ransac: RansacConfig | None = None,
    ) -> None:
        super().__init__(max_pairs=max_pairs)
        if max_keypoints < 4:
            raise ValueError("max_keypoints must be >= 4")
        if max_image_edge < 64:
            raise ValueError("max_image_edge must be >= 64")
        if device not in {"auto", "cpu", "cuda"}:
            raise ValueError("LightGlue device must be auto, cpu, or cuda")
        self.max_keypoints = max_keypoints
        self.max_image_edge = max_image_edge
        self.requested_device = device
        self.ransac = ransac or RansacConfig()
        self._loaded = False
        self._device = "unloaded"
        self._torch: Any = None
        self._extractor: Any = None
        self._matcher: Any = None
        self._remove_batch_dimension: Any = None

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def device(self) -> str:
        return self._device

    def load(self) -> LightGlueSiftVerifier:
        if self._loaded:
            return self
        try:
            torch = importlib.import_module("torch")
            lightglue = importlib.import_module("lightglue")
            utilities = importlib.import_module("lightglue.utils")
        except (ImportError, ModuleNotFoundError) as exc:
            raise OptionalVerificationDependencyError(
                "lightglue_sift requires the official cvg/LightGlue package; "
                "install a pinned revision from https://github.com/cvg/LightGlue"
            ) from exc

        try:
            extractor_class = lightglue.SIFT
            matcher_class = lightglue.LightGlue
            if self.requested_device == "cuda":
                if not torch.cuda.is_available():
                    raise VerificationError("LightGlue CUDA was requested but is unavailable")
                device = "cuda"
            elif self.requested_device == "auto":
                device = "cuda" if torch.cuda.is_available() else "cpu"
            else:
                device = "cpu"
            self._extractor = extractor_class(max_num_keypoints=self.max_keypoints).eval().to(device)
            self._matcher = matcher_class(features="sift").eval().to(device)
            self._remove_batch_dimension = utilities.rbd
        except VerificationError:
            raise
        except Exception as exc:
            raise VerificationError(f"could not initialize official LightGlue SIFT: {exc}") from exc

        self._torch = torch
        self._device = device
        self._loaded = True
        return self

    def _tensor(self, image: ImageInput) -> Any:
        grayscale = load_grayscale(image, max_image_edge=self.max_image_edge)
        rgb = np.repeat(grayscale[:, :, None], 3, axis=2)
        tensor = self._torch.from_numpy(rgb).permute(2, 0, 1).float().div_(255.0)
        return tensor.to(self._device)

    def _extract(self, image: ImageInput) -> Mapping[str, Any]:
        try:
            with self._torch.inference_mode():
                return self._extractor.extract(self._tensor(image))
        except VerificationInputError:
            raise
        except Exception as exc:
            raise VerificationError(f"LightGlue SIFT feature extraction failed: {exc}") from exc

    def _unbatch(self, values: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._remove_batch_dimension(values)

    def verify(
        self,
        query_image: ImageInput,
        reference_images: Sequence[ImageInput],
    ) -> tuple[GeometricEvidence, ...]:
        self._validate_pair_count(reference_images)
        if not reference_images:
            return ()
        self.load()
        query_features = self._extract(query_image)
        query_unbatched = self._unbatch(query_features)
        query_count = int(query_unbatched["keypoints"].shape[0])

        results: list[GeometricEvidence] = []
        for reference_image in reference_images:
            started = time.perf_counter()
            try:
                reference_features = self._extract(reference_image)
            except VerificationInputError as exc:
                results.append(
                    empty_evidence(
                        backend=self.backend_name,
                        query_keypoints=query_count,
                        reference_keypoints=0,
                        latency_ms=(time.perf_counter() - started) * 1_000,
                        reason=f"invalid_reference_image:{exc}",
                    )
                )
                continue
            reference_unbatched = self._unbatch(reference_features)
            reference_count = int(reference_unbatched["keypoints"].shape[0])
            try:
                with self._torch.inference_mode():
                    match_output = self._matcher(
                        {"image0": query_features, "image1": reference_features}
                    )
                matches = self._unbatch(match_output)["matches"]
                query_points = query_unbatched["keypoints"][matches[:, 0]].detach().cpu().numpy()
                reference_points = (
                    reference_unbatched["keypoints"][matches[:, 1]].detach().cpu().numpy()
                )
            except Exception as exc:
                raise VerificationError(f"LightGlue matching failed: {exc}") from exc
            match_count = int(len(query_points))
            results.append(
                estimate_geometry(
                    query_points,
                    reference_points,
                    backend=self.backend_name,
                    query_keypoint_count=query_count,
                    reference_keypoint_count=reference_count,
                    raw_match_count=match_count,
                    latency_ms=(time.perf_counter() - started) * 1_000,
                    config=self.ransac,
                )
            )
        return tuple(results)


def lightglue_sift_available() -> bool:
    """Return package availability without importing or initializing weights."""

    return importlib.util.find_spec("lightglue") is not None
