"""Metadata-only camera-position and photographer audit; never opens images.

Commons geosearch may return object positions. This audit requires a camera
template and original imageinfo EXIF GPS to agree, without consulting a gallery
or a localization model. Passing these checks does not certify GPS accuracy or
street-photo content. No split is assigned by this feasibility module.
"""

from __future__ import annotations

import html
import json
import math
import re
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from ml.ingestion.mapillary_citywide import load_aoi_boundary
from ml.localization.geo import haversine_m
from ml.research.commons_discovery import OUT
from ml.research.seal import sha256


def camera_position(text):
    # Exact template names prevent Object location and embedded Wikidata object
    # coordinates from being mistaken for a camera. Unsupported syntax fails closed.
    matches = re.findall(r"\{\{\s*(?:Location(?: dec)?|Camera location)\s*\|([^{}]*)\}\}", text, re.I)
    positions = []
    for match in matches:
        positional, named = [], {}
        for field in match.split("|"):
            if "=" in field:
                key, value = field.split("=", 1)
                named[key.strip().lower()] = value.strip()
            else:
                positional.append(field.strip())
        if named.get("secondary") == "1" or "wikidata" in named:
            continue
        args = [named.get(str(i + 1), v) for i, v in enumerate(positional)]
        if "1" in named and "2" in named:
            args = [named[str(i)] for i in range(1, 9) if str(i) in named]
        try:
            if len(args) >= 8 and args[3].upper() in {"N", "S"} and args[7].upper() in {"E", "W"}:
                values = [float(args[i]) for i in [0, 1, 2, 4, 5, 6]]
                if any(v < 0 for v in values) or any(values[i] >= 60 for i in [1, 2, 4, 5]):
                    continue
                lat = (values[0] + values[1] / 60 + values[2] / 3600) * (1 if args[3].upper() == "N" else -1)
                lon = (values[3] + values[4] / 60 + values[5] / 3600) * (1 if args[7].upper() == "E" else -1)
            else:
                lat, lon = float(args[0]), float(args[1])
            if math.isfinite(lat) and math.isfinite(lon) and abs(lat) <= 90 and abs(lon) <= 180:
                positions.append((lat, lon))
        except (ValueError, IndexError):
            continue
    unique = set(positions)
    return next(iter(unique)) if len(unique) == 1 else None


def normalized(value):
    return " ".join(unicodedata.normalize("NFKC", value).replace("_", " ").casefold().split())


def author_keys(artist_html):
    keys = set()
    for link in re.findall(r"href=[\"\']([^\"\']+)", artist_html, re.I):
        url = urlparse(html.unescape(link if not link.startswith("//") else "https:" + link))
        path = unquote(url.path)
        if url.hostname in {"www.wikidata.org", "wikidata.org"} and re.fullmatch(r"/wiki/Q\d+", path):
            keys.add("wikidata:" + path.rsplit("/", 1)[1])
        elif url.hostname == "commons.wikimedia.org":
            title = parse_qs(url.query).get("title", [path.removeprefix("/wiki/")])[0]
            if title.startswith("User:"):
                keys.add("commons:" + normalized(title.removeprefix("User:")))
        elif url.hostname in {"www.flickr.com", "flickr.com"}:
            match = re.match(r"/(?:people|photos)/([^/]+)", path)
            if match:
                keys.add("flickr:" + normalized(match[1]))
    # Public user page links Svetlov Artem to trolleway.com and his photo catalog:
    # https://commons.wikimedia.org/wiki/User:Svetlov_Artem
    # The Wikidata creator link is used in the same author's file descriptions.
    aliases = {"commons:svetlov artem", "wikidata:Q110446558"}
    if keys & aliases:
        keys -= aliases
        keys.add("known-mapillary:trolleway")
    return sorted(keys)


def audit_page(page, boundary):
    info = page.get("imageinfo", [{}])[0]
    exif = {r["name"]: r.get("value") for r in info.get("metadata", [])}
    ex = info.get("extmetadata", {})
    text = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("content", "")
    camera = camera_position(text)
    if camera is None:
        return None, "missing_or_ambiguous_camera_template"
    try:
        lat, lon = float(exif["GPSLatitude"]), float(exif["GPSLongitude"])
        date = datetime.strptime(str(exif["DateTimeOriginal"]), "%Y:%m:%d %H:%M:%S")
    except (KeyError, TypeError, ValueError):
        return None, "missing_original_exif_gps_or_capture_datetime"
    if not math.isfinite(lat) or not math.isfinite(lon) or not boundary.covers(lon, lat):
        return None, "exif_outside_moscow"
    if date.year < 2024 or date > datetime(2026, 9, 6, 23, 59, 59):
        return None, "not_recent_capture_2024_to_audit_date"
    disagreement = haversine_m(lat, lon, *camera)
    if disagreement > 25:
        return None, "camera_template_exif_disagree_gt25m"
    if info.get("mime") != "image/jpeg" or min(info.get("width", 0), info.get("height", 0)) < 480:
        return None, "not_sufficient_resolution_jpeg"
    artist = ex.get("Artist", {}).get("value", "")
    authors = author_keys(artist)
    if not authors:
        return None, "unresolved_author_identity"
    if "known-mapillary:trolleway" in authors:
        return None, "known_cross_provider_author_overlap"
    license_name = ex.get("LicenseShortName", {}).get("value", "")
    if license_name not in {
        "CC0",
        "CC BY 4.0",
        "CC BY-SA 4.0",
        "CC BY 3.0",
        "CC BY-SA 3.0",
        "CC BY 2.0",
        "CC BY-SA 2.0",
    }:
        return None, "license_needs_individual_review"
    return {
        "pageid": page["pageid"],
        "title": page["title"],
        "lat": lat,
        "lon": lon,
        "camera_template_exif_disagreement_m": disagreement,
        "captured_at": date.isoformat(),
        "authors": authors,
        "artist_html": artist,
        "license": license_name,
        "url": info.get("url"),
        "description_url": info.get("descriptionurl"),
        "categories": ex.get("Categories", {}).get("value", ""),
        "description": ex.get("ImageDescription", {}).get("value", ""),
        "gps_accuracy_m": exif.get("GPSHPositioningError"),
        "gps_precision_not_accuracy": True,
    }, "metadata_eligible_pending_street_content_and_alias_audit"


def run(extended=False):
    suffix = "_extended" if extended else ""
    population = OUT / f"population{suffix}.json"
    if not population.exists():
        raise ValueError("wait for the fixed metadata discovery population to finish")
    wanted = {r["pageid"] for r in json.loads(population.read_text())}
    pages = {}
    for path in sorted((OUT / "cache").glob("*.json")):
        for page in json.loads(path.read_text()).get("query", {}).get("pages", []):
            if page.get("pageid") in wanted and "metadata" in page.get("imageinfo", [{}])[0]:
                if page["pageid"] in pages and page != pages[page["pageid"]]:
                    raise ValueError("multiple different metadata revisions require explicit reconciliation")
                pages[page["pageid"]] = page
    boundary = load_aoi_boundary(Path("data/raw/moscow/moscow_admin_boundary.geojson"))
    counts, eligible = Counter(), []
    for page in pages.values():
        row, reason = audit_page(page, boundary)
        counts[reason] += 1
        if row is not None:
            eligible.append(row)
    target = OUT / f"metadata_eligible{suffix}.json"
    if target.exists():
        raise ValueError("completed provenance snapshot already exists")
    target.write_text(json.dumps(sorted(eligible, key=lambda r: r["pageid"]), ensure_ascii=False) + "\n")
    report = {
        "status": "feasibility_only_no_split_or_inference",
        "population_sha256": sha256(population),
        "metadata_pages": len(pages),
        "missing_metadata_pages": len(wanted - pages.keys()),
        "exclusion_counts": dict(counts),
        "eligible_sha256": sha256(target),
        "author_counts": dict(Counter(a for r in eligible for a in r["authors"])),
        "limitations": [
            "remaining cross-provider aliases unresolved",
            "anonymous provider accounts cannot establish author independence",
            "no visual street-content audit",
            "EXIF GPS may be edited or inaccurate; agreement is not independent verification",
        ],
    }
    (OUT / f"provenance_feasibility{suffix}.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--extended", action="store_true")
    run(parser.parse_args().extended)
