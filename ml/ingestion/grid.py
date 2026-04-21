"""Grid utilities for Moscow ingestion tiling.

This module intentionally keeps the bbox tile area <= 0.01 deg²,
which is required by Mapillary APIs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable


@dataclass(frozen=True)
class BBox:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    def as_dict(self) -> dict[str, float]:
        return asdict(self)


def build_grid(
    *,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    lat_step: float = 0.01,
    lon_step: float = 0.01,
) -> list[BBox]:
    """Split a bounding box into a regular grid of bbox tiles."""
    if min_lat >= max_lat:
        raise ValueError("min_lat must be less than max_lat")
    if min_lon >= max_lon:
        raise ValueError("min_lon must be less than max_lon")
    if lat_step <= 0 or lon_step <= 0:
        raise ValueError("lat_step and lon_step must be positive")
    if lat_step * lon_step > 0.01:
        raise ValueError("Tile area must be <= 0.01 deg² for Mapillary")

    tiles: list[BBox] = []
    eps = 1e-12
    lat = min_lat
    while lat < max_lat - eps:
        next_lat = min(lat + lat_step, max_lat)
        lon = min_lon
        while lon < max_lon - eps:
            next_lon = min(lon + lon_step, max_lon)
            tiles.append(BBox(min_lat=lat, max_lat=next_lat, min_lon=lon, max_lon=next_lon))
            lon = next_lon
        lat = next_lat
    return tiles


def iter_moscow_tiles(lat_step: float = 0.01, lon_step: float = 0.01) -> Iterable[BBox]:
    """Yield bbox tiles for Moscow AOI.

    Bounds:
      lat: 55.55..55.95
      lon: 37.30..37.90
    """
    return build_grid(
        min_lat=55.55,
        max_lat=55.95,
        min_lon=37.30,
        max_lon=37.90,
        lat_step=lat_step,
        lon_step=lon_step,
    )
