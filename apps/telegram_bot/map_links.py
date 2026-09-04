from __future__ import annotations

from urllib.parse import urlencode


def google_maps_url(lat: float, lon: float) -> str:
    return f"https://www.google.com/maps/search/?api=1&query={lat:.6f},{lon:.6f}"


def yandex_maps_url(lat: float, lon: float) -> str:
    point = f"{lon:.6f},{lat:.6f}"
    return "https://yandex.com/maps/?" + urlencode({"ll": point, "z": "16", "pt": f"{point},pm2rdm"})
