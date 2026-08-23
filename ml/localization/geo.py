"""Small dependency-free spherical geodesy helpers."""

from __future__ import annotations

import math
from collections.abc import Sequence

EARTH_RADIUS_M = 6_371_008.8


def validate_coordinate(lat: float, lon: float) -> None:
    if not math.isfinite(lat) or not math.isfinite(lon):
        raise ValueError("coordinates must be finite")
    if not -90.0 <= lat <= 90.0:
        raise ValueError(f"latitude outside [-90, 90]: {lat}")
    if not -180.0 <= lon <= 180.0:
        raise ValueError(f"longitude outside [-180, 180]: {lon}")


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    validate_coordinate(float(lat1), float(lon1))
    validate_coordinate(float(lat2), float(lon2))
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    value = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.asin(math.sqrt(min(1.0, value)))


def weighted_spherical_centroid(
    coordinates: Sequence[tuple[float, float]],
    weights: Sequence[float],
) -> tuple[float, float]:
    if not coordinates or len(coordinates) != len(weights):
        raise ValueError("coordinates and weights must have the same non-zero length")
    if any((not math.isfinite(weight) or weight < 0) for weight in weights):
        raise ValueError("weights must be finite and non-negative")
    total = sum(weights)
    if total <= 0:
        raise ValueError("at least one weight must be positive")
    x = y = z = 0.0
    for (lat, lon), weight in zip(coordinates, weights, strict=True):
        validate_coordinate(lat, lon)
        phi, lam = math.radians(lat), math.radians(lon)
        x += weight * math.cos(phi) * math.cos(lam)
        y += weight * math.cos(phi) * math.sin(lam)
        z += weight * math.sin(phi)
    horizontal = math.hypot(x, y)
    if horizontal == 0.0 and z == 0.0:
        raise ValueError("weighted spherical centroid is undefined")
    return math.degrees(math.atan2(z, horizontal)), math.degrees(math.atan2(y, x))


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    if not 0.0 <= q <= 1.0:
        raise ValueError("q must be in [0, 1]")
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction
