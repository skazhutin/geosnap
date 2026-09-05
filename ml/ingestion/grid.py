"""Deterministic geographic grids used by source ingestion jobs."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class BBox:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    def as_dict(self) -> dict[str, float]:
        return asdict(self)

    @property
    def center(self) -> tuple[float, float]:
        return ((self.min_lat + self.max_lat) / 2.0, (self.min_lon + self.max_lon) / 2.0)

    @property
    def key(self) -> str:
        return ":".join(f"{value:.7f}" for value in (self.min_lat, self.max_lat, self.min_lon, self.max_lon))

    def enclosing_radius_m(self, padding_m: float = 10.0) -> int:
        """Radius from center to the farthest corner, suitable for KartaView."""
        center_lat, center_lon = self.center
        lat_m = abs(self.max_lat - center_lat) * 111_320.0
        lon_m = abs(self.max_lon - center_lon) * 111_320.0 * math.cos(math.radians(center_lat))
        return max(1, int(math.ceil(math.hypot(lat_m, lon_m) + padding_m)))


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
    if not (-90 <= min_lat < max_lat <= 90):
        raise ValueError("latitude bounds must be within [-90, 90]")
    if not (-180 <= min_lon < max_lon <= 180):
        raise ValueError("longitude bounds must be within [-180, 180]")
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


def tile_centers(tiles: Iterable[BBox]) -> Iterable[tuple[str, float, float, int]]:
    """Yield stable tile key, center latitude/longitude and enclosing radius."""
    for tile in tiles:
        lat, lon = tile.center
        yield tile.key, lat, lon, tile.enclosing_radius_m()
