"""Spatial candidate aggregation and interpretable coordinate estimation."""

from .geo import haversine_m
from .models import (
    Candidate,
    LocalizationResult,
    LocalizationStatus,
    LocationHypothesis,
)
from .pipeline import LocalizerConfig, SpatialLocalizer
from .service import LocalizationService, create_localization_service
from .uncertainty import CalibrationBin, UncertaintyCalibration

__all__ = [
    "CalibrationBin",
    "Candidate",
    "LocationHypothesis",
    "LocalizationResult",
    "LocalizationService",
    "LocalizationStatus",
    "LocalizerConfig",
    "SpatialLocalizer",
    "UncertaintyCalibration",
    "create_localization_service",
    "haversine_m",
]
