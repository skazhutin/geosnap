"""Metadata-only feasibility audit of an independent Moscow photography source.

No images, descriptors, labels derived from models, or private test coordinates
are inputs. Commons camera-location templates are required; object locations
are insufficient ground truth. Output is a candidate population, not a benchmark.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import time
from pathlib import Path

import numpy as np
import requests

from ml.ingestion.mapillary_citywide import load_aoi_boundary
from ml.research.prepare_queries import ROOT, score
from ml.research.seal import sha256

API = "https://commons.wikimedia.org/w/api.php"
OUT = ROOT / "acquisition/commons"


def request(session, params):
    key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
    path = OUT / "cache" / f"{key}.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            path.rename(path.with_suffix(".interrupted"))
    last_error = "no response"
    for attempt in range(6):
        try:
            r = session.get(API, params=params | {"format": "json", "formatversion": 2, "maxlag": 5}, timeout=45)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = type(exc).__name__
            delay = min(30, 5 * (attempt + 1))
            print(f"Commons connection interrupted; bounded retry in {delay}s", flush=True)
            time.sleep(delay)
            continue
        if r.status_code in (429, 502, 503, 504):
            last_error = f"HTTP {r.status_code}"
            retry_after = r.headers.get("Retry-After", "")
            delay = float(retry_after) if retry_after.isdigit() else max(15, 2**attempt)
            print(f"Commons {last_error}; respecting retry delay {delay}s", flush=True)
            while delay > 0:
                time.sleep(min(delay, 30))
                delay -= 30
            continue
        r.raise_for_status()
        data = r.json()
        if data.get("error", {}).get("code") in {"maxlag", "cirrussearch-too-busy-error", "ratelimited"}:
            last_error = data["error"]["code"]
            time.sleep(15)
            continue
        if "error" in data:
            raise RuntimeError(data["error"])
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False) + "\n")
        temporary.replace(path)
        time.sleep(2)
        return data
    raise RuntimeError(f"Commons metadata unavailable after bounded retry: {last_error}")


def plain(value):
    return html.unescape(re.sub("<[^>]+>", " ", value)).strip()


def run(metadata_cap=10000):
    if metadata_cap not in {10000, 30000}:
        raise ValueError("only the original cap or the whole discovered population extension is registered")
    suffix = "" if metadata_cap == 10000 else "_extended"
    if (OUT / f"population{suffix}.json").exists():
        raise ValueError("completed metadata population already exists; preserve its snapshot")
    (OUT / "cache").mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "GeoSnapResearch/0.1 (read-only geolocation metadata feasibility audit)"
    boundary = load_aoi_boundary(Path("data/raw/moscow/moscow_admin_boundary.geojson"))
    west, south, east, north = boundary.geometry.bounds
    centers = [
        (float(lat), float(lon))
        for lat in np.arange(south, north + 0.04, 0.04)
        for lon in np.arange(west, east + 0.07, 0.07)
        if boundary.covers(float(lon), float(lat))
    ]
    population = {}
    for i, (lat, lon) in enumerate(centers):
        data = request(
            session,
            {
                "action": "query",
                "list": "geosearch",
                "gscoord": f"{lat:.6f}|{lon:.6f}",
                "gsradius": 5000,
                "gsnamespace": 6,
                "gslimit": 500,
            },
        )
        for row in data.get("query", {}).get("geosearch", []):
            if boundary.covers(row["lon"], row["lat"]):
                population.setdefault(row["pageid"], row | {"discovery_cell": i})
        if (i + 1) % 10 == 0:
            print("Commons geographic cells", i + 1, "/", len(centers), "unique files", len(population), flush=True)
    # Deterministic balanced cap bounds API and metadata costs; not matchability selection.
    ordered = sorted(population.values(), key=lambda r: score("commons-metadata", r["pageid"]))
    ranks = {}
    for r in ordered:
        ranks[r["discovery_cell"]] = ranks.get(r["discovery_cell"], 0) + 1
        r["cell_rank"] = ranks[r["discovery_cell"]]
    ordered = sorted(ordered, key=lambda r: (r["cell_rank"], score("commons-metadata", r["pageid"])))[:metadata_cap]
    rows = []
    for start in range(0, len(ordered), 20):
        chunk = ordered[start : start + 20]
        lookup = {r["pageid"]: r for r in chunk}
        data = request(
            session,
            {
                "action": "query",
                "pageids": "|".join(str(r["pageid"]) for r in chunk),
                "prop": "imageinfo|revisions",
                "iiprop": "url|size|extmetadata|metadata|mime",
                "rvprop": "content",
                "rvslots": "main",
            },
        )
        for page in data.get("query", {}).get("pages", []):
            info = page.get("imageinfo", [{}])[0]
            ex = info.get("extmetadata", {})
            text = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")
            camera = bool(re.search(r"\{\{\s*(?:Location(?: dec)?|Camera location)\s*\|", text, re.I))

            def value(name, metadata=ex):
                return metadata.get(name, {}).get("value", "")

            original = value("DateTimeOriginal")
            year = re.search(r"\b(19\d{2}|20\d{2})\b", plain(original))
            row = lookup[page["pageid"]] | {
                "title": page["title"],
                "camera_location_template": camera,
                "width": info.get("width"),
                "height": info.get("height"),
                "url": info.get("url"),
                "description_url": info.get("descriptionurl"),
                "date_original": plain(original),
                "capture_year": int(year[0]) if year else None,
                "artist": plain(value("Artist")),
                "artist_html": value("Artist"),
                "license": value("LicenseShortName"),
                "license_url": value("LicenseUrl"),
                "categories": plain(value("Categories")),
                "description": plain(value("ImageDescription")),
                "metadata_latitude": value("GPSLatitude"),
                "metadata_longitude": value("GPSLongitude"),
                "mime": info.get("mime"),
                "camera_exif": {
                    item["name"]: item.get("value")
                    for item in info.get("metadata", [])
                    if item.get("name", "").startswith("GPS")
                    or item.get("name") in {"Make", "Model", "DateTimeOriginal", "Orientation"}
                },
            }
            rows.append(row)
        if start % 200 == 0:
            print("Commons hydrated metadata", start + len(chunk), "/", len(ordered), flush=True)
    target = OUT / f"population{suffix}.json"
    target.write_text(json.dumps(rows, ensure_ascii=False) + "\n")
    recent = [r for r in rows if r["camera_location_template"] and (r["capture_year"] or 0) >= 2024]
    report = {
        "status": "metadata_only_population_not_assigned_or_sealed",
        "geographic_cells": len(centers),
        "discovered_unique_files": len(population),
        "hydrated_metadata": len(rows),
        "camera_location_rows": sum(r["camera_location_template"] for r in rows),
        "camera_location_2024plus_rows": len(recent),
        "camera_location_2024plus_artist_strings": len(set(r["artist"] for r in recent)),
        "population_sha256": sha256(target),
        "images_downloaded": 0,
        "model_inference_performed": False,
        "limitations": [
            "camera-template positions may be manually placed",
            "artist strings need canonicalization before author holdout",
            "street-photo population and capture-date provenance still need audit",
            "no benchmark accuracy claims",
        ],
    }
    (OUT / f"feasibility{suffix}.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata-cap", type=int, choices=[10000, 30000], default=10000)
    run(parser.parse_args().metadata_cap)
