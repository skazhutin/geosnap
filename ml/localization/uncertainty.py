"""Optional evaluation-backed uncertainty calibration."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CalibrationBin:
    min_confidence: float
    max_confidence: float
    p90_error_m: float
    sample_count: int

    def __post_init__(self) -> None:
        if not 0 <= self.min_confidence < self.max_confidence <= 1:
            raise ValueError("confidence bin must satisfy 0 <= min < max <= 1")
        if self.p90_error_m <= 0:
            raise ValueError("p90_error_m must be positive")
        if self.sample_count < 1:
            raise ValueError("sample_count must be positive")


class UncertaintyCalibration:
    """Confidence-conditioned p90 errors measured on a held-out dataset."""

    def __init__(
        self,
        bins: Sequence[CalibrationBin],
        *,
        dataset_id: str,
        minimum_bin_samples: int = 30,
    ) -> None:
        if not dataset_id.strip():
            raise ValueError("dataset_id is required for auditable uncertainty calibration")
        if minimum_bin_samples < 1:
            raise ValueError("minimum_bin_samples must be positive")
        ordered = sorted(bins, key=lambda item: item.min_confidence)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current.min_confidence < previous.max_confidence:
                raise ValueError("confidence calibration bins cannot overlap")
        self.bins = tuple(ordered)
        self.dataset_id = dataset_id
        self.minimum_bin_samples = minimum_bin_samples

    def estimate(self, confidence: float, *, observed_cluster_spread_m: float) -> float | None:
        for index, item in enumerate(self.bins):
            upper_inclusive = index == len(self.bins) - 1
            if item.min_confidence <= confidence < item.max_confidence or (
                upper_inclusive and confidence == item.max_confidence
            ):
                if item.sample_count < self.minimum_bin_samples:
                    return None
                return max(float(item.p90_error_m), float(observed_cluster_spread_m))
        return None
