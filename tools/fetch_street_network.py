"""Cache a reference-independent OpenStreetMap road snapshot for Moscow coverage."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
import shapely

from tools.compare_research_coverage import AOI_SHA, ROOT, digest, save

ENDPOINT = "https://overpass-api.de/api/interpreter"
HIGHWAYS = "motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link|unclassified|residential|living_street|pedestrian|service|footway|cycleway|steps"


def output_root():
    cfg = json.loads((ROOT / "data/evaluation/moscow_research_v5/storage.json").read_text())
    root = Path(os.environ.get("GEOSNAP_DATA_ROOT", cfg["external_root"]))
    if not root.is_dir() or root.stat().st_dev == ROOT.stat().st_dev:
        raise RuntimeError("The external canonical volume must be connected")
    out = root / "coverage_comparison_20260910/streets"
    out.mkdir(exist_ok=True)
    return out


def main():
    out = output_root()
    raw_dir = out / "osm"
    raw_dir.mkdir(exist_ok=True)
    boundary_path = ROOT / "data/raw/moscow/moscow_admin_boundary.geojson"
    if digest(boundary_path) != AOI_SHA:
        raise RuntimeError("Exact Moscow R102269 changed")
    west, south, east, north = shapely.from_geojson(boundary_path.read_text()).bounds
    # Complete bbox coverage, then local clipping to the original polygon.
    west, south, east, north = west - .001, south - .001, east + .001, north + .001
    mid_lon, mid_lat = (west + east) / 2, (south + north) / 2
    boxes = [(s, w, n, e) for s, n in [(south, mid_lat), (mid_lat, north)]
             for w, e in [(west, mid_lon), (mid_lon, east)]]
    snapshot_path = raw_dir / "snapshot.json"
    snapshot = json.loads(snapshot_path.read_text()) if snapshot_path.exists() else {}
    for i, bbox in enumerate(boxes):
        target, receipt_path = raw_dir / f"tile_{i}.json", raw_dir / f"tile_{i}.receipt.json"
        if target.exists() and receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            if digest(target) != receipt["sha256"]:
                raise RuntimeError("Cached OSM response changed")
            print(json.dumps({"tile": i, "status": "cached", "ways": receipt["ways"]}), flush=True)
            continue
        date = f'[date:"{snapshot["timestamp"]}"]' if snapshot else ""
        query = f'[out:json][timeout:90][maxsize:268435456]{date};way["highway"~"^({HIGHWAYS})$"]({",".join(map(str, bbox))});out body geom;'
        (raw_dir / f"tile_{i}.overpassql").write_text(query + "\n")
        failure = None
        for attempt in range(3):
            print(json.dumps({"tile": i, "status": "downloading", "attempt": attempt + 1}), flush=True)
            try:
                response = requests.post(ENDPOINT, data={"data": query}, headers={"User-Agent": "GeoSnap-local-research-coverage/1.0"}, timeout=(15, 115))
                response.raise_for_status()
                result = response.json()
                if result.get("remark") or "elements" not in result:
                    raise RuntimeError(str(result.get("remark", "Incomplete Overpass response")))
                if any(e.get("type") != "way" or not e.get("geometry") for e in result["elements"]):
                    raise RuntimeError("Missing road geometry")
                temporary = target.with_suffix(".writing")
                temporary.write_bytes(response.content)
                os.replace(temporary, target)
                if not snapshot:
                    snapshot = {"timestamp": result["osm3s"]["timestamp_osm_base"], "endpoint": ENDPOINT,
                                "source": "OpenStreetMap contributors", "license": "ODbL 1.0",
                                "copyright_url": "https://www.openstreetmap.org/copyright", "aoi_sha256": AOI_SHA}
                    save(snapshot_path, snapshot)
                receipt = {"sha256": digest(target), "ways": len(result["elements"]), "bbox": bbox,
                           "query_sha256": digest(raw_dir / f"tile_{i}.overpassql"), "snapshot_timestamp": snapshot["timestamp"],
                           "downloaded_at_unix": time.time(), "endpoint": ENDPOINT}
                save(receipt_path, receipt)
                print(json.dumps({"tile": i, "status": "done", "ways": receipt["ways"], "bytes": target.stat().st_size}), flush=True)
                failure = None
                break
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                failure = f"{type(exc).__name__}: {str(exc)[:250]}"
                print(json.dumps({"tile": i, "status": "retry", "reason": failure}), flush=True)
                if attempt < 2:
                    time.sleep(10 * (attempt + 1))
        if failure:
            save(out / "download_failure.json", {"tile": i, "error": failure, "resume": "Rerun the same module; valid cached tiles are reused"})
            raise RuntimeError(failure)
    save(out / "download.done.json", {"snapshot": snapshot, "tiles": 4,
         "files": {str(p): digest(p) for p in sorted(raw_dir.glob("tile_*.json"))},
         "query_data_used": False, "source_sha256": digest(Path(__file__))})


if __name__ == "__main__":
    main()
