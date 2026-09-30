"""Length-weighted Moscow street coverage, independent of every query split."""
from __future__ import annotations

import gc
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from scipy.spatial import cKDTree

from tools.compare_research_coverage import AOI_SHA, RADIUS, ROOT, digest, save, xyz
from tools.fetch_street_network import output_root

MAIN = {"motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
        "secondary", "secondary_link", "tertiary", "tertiary_link", "unclassified",
        "residential", "living_street", "pedestrian"}
WALK = {"footway", "cycleway", "steps"}
GROUPS = {"streets": [0], "service_roads": [1], "walkways": [2], "extended": [0, 1, 2]}
GROUP_NAMES = {"streets": "Улицы", "service_roads": "Дворовые и служебные проезды",
               "walkways": "Пешеходные и велосипедные дорожки", "extended": "Улицы, проезды и дорожки"}


def linear_parts(geometry):
    if geometry.geom_type == "LineString":
        yield geometry
    elif geometry.geom_type in {"MultiLineString", "GeometryCollection"}:
        for part in geometry.geoms:
            yield from linear_parts(part)


def linework(out, boundary):
    cache, receipt = out / "segments.npz", out / "segments.receipt.json"
    download = json.loads((out / "download.done.json").read_text())
    if cache.exists() and receipt.exists():
        contract = json.loads(receipt.read_text())
        if digest(cache) != contract["sha256"] or digest(out / "download.done.json") != contract["download_sha256"]:
            raise RuntimeError("Street cache fingerprint mismatch")
        arrays = np.load(cache)
        return arrays["coordinates"], arrays["category"], arrays["length_m"], contract
    shapely.prepare(boundary)
    seen, geometry_parts, categories = set(), [], []
    exclusions, counts = Counter(), Counter()
    for i in range(4):
        path = out / "osm" / f"tile_{i}.json"
        if digest(path) != download["files"][str(path)]:
            raise RuntimeError("OSM download changed")
        raw = json.loads(path.read_bytes())
        for way in raw["elements"]:
            if way["id"] in seen:
                exclusions["duplicate_way_between_tiles"] += 1
                continue
            seen.add(way["id"])
            tags = way["tags"]
            if tags.get("area") == "yes" or tags.get("indoor") in {"yes", "true", "1"}:
                exclusions["area_or_indoor"] += 1
                continue
            if tags.get("tunnel", "no") not in {"no", "false", "0"}:
                exclusions["tunnel"] += 1
                continue
            highway = tags["highway"]
            category = 0 if highway in MAIN else 1 if highway == "service" else 2 if highway in WALK else None
            if category is None:
                raise RuntimeError("Unexpected OSM highway class")
            points = np.array([[p["lon"], p["lat"]] for p in way["geometry"]], dtype=float)
            if len(points) < 2 or not np.isfinite(points).all():
                exclusions["degenerate_geometry"] += 1
                continue
            geometry = shapely.LineString(points)
            if not boundary.intersects(geometry):
                exclusions["outside_exact_moscow"] += 1
                continue
            if not boundary.covers(geometry):
                geometry = geometry.intersection(boundary)
                exclusions["ways_clipped_at_exact_boundary"] += 1
            counts[highway] += 1
            for part in linear_parts(geometry):
                xy = np.asarray(part.coords)
                pairs = np.column_stack([xy[:-1, 1], xy[:-1, 0], xy[1:, 1], xy[1:, 0]])
                geometry_parts.append(pairs)
                categories.append(np.full(len(pairs), category, dtype=np.uint8))
        print(json.dumps({"phase": "clip", "tile": i, "retained_ways": sum(counts.values())}), flush=True)
        del raw
        gc.collect()
    coordinates, category = np.concatenate(geometry_parts), np.concatenate(categories)
    del geometry_parts, categories, seen
    # The same physical segment may occur in more than one OSM way. Count it once.
    keys = np.rint(coordinates * 1e7).astype(np.int32)
    reverse = (keys[:, 0] > keys[:, 2]) | ((keys[:, 0] == keys[:, 2]) & (keys[:, 1] > keys[:, 3]))
    keys[reverse] = keys[reverse][:, [2, 3, 0, 1]]
    order = np.lexsort((category, keys[:, 3], keys[:, 2], keys[:, 1], keys[:, 0]))
    sorted_keys = keys[order]
    keep = order[np.r_[True, np.any(sorted_keys[1:] != sorted_keys[:-1], axis=1)]]
    exclusions["duplicate_geographic_segments"] = len(keys) - len(keep)
    coordinates, category = coordinates[keep], category[keep]
    del keys, order, sorted_keys
    chord = np.linalg.norm(xyz(coordinates[:, :2]) - xyz(coordinates[:, 2:]), axis=1)
    lengths = 2 * RADIUS * np.arcsin(np.clip(chord / 2, 0, 1))
    valid = lengths > .01
    exclusions["zero_length_segments"] = int((~valid).sum())
    coordinates, category, lengths = coordinates[valid], category[valid], lengths[valid]
    np.savez_compressed(cache, coordinates=coordinates, category=category, length_m=lengths)
    contract = {"sha256": digest(cache), "download_sha256": digest(out / "download.done.json"),
                "aoi_sha256": AOI_SHA, "retained_ways_by_highway": dict(counts), "exclusions": dict(exclusions),
                "segments": len(lengths), "length_km_by_group": {k: float(lengths[np.isin(category, v)].sum() / 1000) for k, v in GROUPS.items()},
                "parallel_carriageways": "Distinct mapped carriageways remain distinct. Reverse duplicates of identical geometry count once.",
                "access_tags": "No population, traffic or access-based weighting. Access restrictions do not remove physically mapped outdoor streets."}
    save(receipt, contract)
    return coordinates, category, lengths, contract


def probe_batch(coordinates, lengths, spacing):
    counts = np.maximum(1, np.ceil(lengths / spacing).astype(np.int32))
    offsets = np.cumsum(counts, dtype=np.int64) - counts
    indices = np.repeat(np.arange(len(counts)), counts)
    fraction = (np.arange(counts.sum()) - np.repeat(offsets, counts) + .5) / counts[indices]
    # Follow the clipped OSM line geometry. A great-circle interpolation can
    # leave a border-following line even when its endpoints are on that line.
    latlon = coordinates[indices, :2] + (coordinates[indices, 2:] - coordinates[indices, :2]) * fraction[:, None]
    unit = xyz(latlon)
    weights = lengths[indices] / counts[indices]
    return indices, unit, latlon, weights


def weighted_stats(distances, weights):
    order = np.argsort(distances, kind="stable")
    total = float(weights.sum())
    cumulative = np.cumsum(weights[order])
    return {
        "mean_m": float(np.dot(distances, weights) / total),
        "median_m": float(distances[order[min(np.searchsorted(cumulative, .5 * total), len(order) - 1)]]),
        "p90_m": float(distances[order[min(np.searchsorted(cumulative, .9 * total), len(order) - 1)]]),
        "p95_m": float(distances[order[min(np.searchsorted(cumulative, .95 * total), len(order) - 1)]]),
        "coverage_pct": {str(k): float(weights[distances <= k].sum() / total * 100) for k in [25, 50, 100, 250, 500, 1000]},
    }


def evaluate(out, coordinates, category, lengths, boundary, trees, spacing, manifests):
    cache, receipt = out / f"probes_{spacing}m.npz", out / f"metrics_{spacing}m.json"
    inputs = {"segments_sha256": digest(out / "segments.npz"), "manifests": manifests, "spacing": spacing,
              "interpolation": "clipped_OSM_linear_lonlat_v1"}
    if cache.exists() and receipt.exists():
        result = json.loads(receipt.read_text())
        if result["inputs"] != inputs or digest(cache) != result["cache_sha256"]:
            raise RuntimeError("Cached street metric inputs changed")
        return result
    values, weights_all, category_all, latlon_all = [], [], [], []
    roundoff_weight, roundoff_max_distance = 0., 0.
    for start in range(0, len(lengths), 25000):
        sl = slice(start, start + 25000)
        index, unit, latlon, weights = probe_batch(coordinates[sl], lengths[sl], spacing)
        inside = shapely.intersects_xy(boundary, latlon[:, 1], latlon[:, 0])
        if not inside.all():
            # Only machine-roundoff deviations of points on clipped edges are
            # tolerated (1e-10 degrees is below 0.02 mm), never real outside roads.
            residual = shapely.distance(boundary, shapely.points(latlon[~inside, 1], latlon[~inside, 0]))
            roundoff_max_distance = max(roundoff_max_distance, float(residual.max()))
            if np.any(residual > 1e-10):
                raise RuntimeError("Street sample escaped its clipped OSM geometry")
            roundoff_weight += float(weights[~inside].sum())
        distances = []
        for tree in trees:
            chord, _ = tree.query(unit, workers=2)
            distances.append(2 * RADIUS * np.arcsin(np.clip(chord / 2, 0, 1)))
        values.append(np.column_stack(distances))
        weights_all.append(weights)
        category_all.append(category[sl][index])
        latlon_all.append(latlon)
    distance, weight, kinds, latlon = np.concatenate(values), np.concatenate(weights_all), np.concatenate(category_all), np.concatenate(latlon_all)
    if abs(weight.sum() - lengths.sum()) > 1e-6 * lengths.sum():
        raise RuntimeError("Street sampling changed total length")
    if np.any(np.diff(distance, axis=1) > 1e-6):
        raise RuntimeError("Nearest distance increased in nested galleries")
    groups = {}
    for group, allowed in GROUPS.items():
        mask = np.isin(kinds, allowed)
        groups[group] = {"name": GROUP_NAMES[group], "length_km": float(weight[mask].sum() / 1000),
                         "probe_count": int(mask.sum()), "statistics": [weighted_stats(distance[mask, i], weight[mask]) for i in range(3)]}
    np.savez_compressed(cache, distances_m=distance.astype(np.float32), weight_m=weight, category=kinds, latlon=latlon.astype(np.float64))
    result = {"inputs": inputs, "cache_sha256": digest(cache), "spacing_m": spacing, "probe_count": len(weight),
              "boundary_roundoff_affected_sample_length_m": roundoff_weight,
              "boundary_roundoff_max_distance_degrees": roundoff_max_distance, "groups": groups}
    save(receipt, result)
    print(json.dumps({"phase": "metrics", "spacing": spacing, "probes": len(weight), "groups": groups}), flush=True)
    return result


def main():
    out = output_root()
    area = json.loads((out.parent / "summary.json").read_text())
    boundary_path = ROOT / "data/raw/moscow/moscow_admin_boundary.geojson"
    if digest(boundary_path) != AOI_SHA:
        raise RuntimeError("Exact Moscow AOI changed")
    boundary = shapely.from_geojson(boundary_path.read_text())
    coordinates, category, lengths, geometry_receipt = linework(out, boundary)
    trees = []
    for path, expected in area["manifest_sha256"].items():
        if digest(path) != expected:
            raise RuntimeError("Previously fixed gallery changed")
        frame = pd.read_parquet(path, columns=["lat", "lon"])
        trees.append(cKDTree(xyz(frame.to_numpy(float))))
    results = [evaluate(out, coordinates, category, lengths, boundary, trees, spacing, area["manifest_sha256"]) for spacing in [20, 10]]
    convergence = {g: {"mean_max_difference_m": max(abs(a["mean_m"] - b["mean_m"]) for a, b in zip(results[0]["groups"][g]["statistics"], results[1]["groups"][g]["statistics"], strict=True)),
                       "100m_coverage_max_difference_pp": max(abs(a["coverage_pct"]["100"] - b["coverage_pct"]["100"]) for a, b in zip(results[0]["groups"][g]["statistics"], results[1]["groups"][g]["statistics"], strict=True))}
                   for g in GROUPS}
    summary = {"groups": results[1]["groups"], "counts": area["counts"], "spacing_m": 10,
               "convergence_20m_vs_10m": convergence, "geometry": geometry_receipt,
               "snapshot": json.loads((out / "osm/snapshot.json").read_text()),
               "method": "Uniform length measure on OSM way centerlines clipped to exact Moscow R102269; each original line segment divided into intervals <=10 m according to its great-circle length, linear midpoint sampled along the clipped OSM geometry, weighted by interval length. Nearest-photo great-circle distance, not routing distance.",
               "highways": {"streets": sorted(MAIN), "service_roads": ["service"], "walkways": sorted(WALK)},
               "excludes": ["area=yes", "indoor=yes", "tunnels", "tracks", "paths", "construction", "outside exact AOI"],
               "query_splits_opened": False, "gallery_selection_changed": False, "images_or_embeddings_processed": False,
               "200k_scope": area["scenario_audit"]["by_status"], "source_sha256": digest(Path(__file__))}
    save(out / "summary.json", summary)
    report = ["# Покрытие уличной сети Москвы", "",
              "Расстояние по прямой от равномерных по длине точек улиц до ближайшего снимка. Это не длина пешеходного или автомобильного маршрута.", "",
              f"Источник: [OpenStreetMap contributors](https://www.openstreetmap.org/copyright), ODbL 1.0. Снимок базы: {summary['snapshot']['timestamp']}.", "",
              "| Сеть | Длина, км | References | Среднее, м | Медиана, м | p90, м | ≤100 м, % |", "|---|---:|---:|---:|---:|---:|---:|"]
    for key in ["streets", "extended", "service_roads", "walkways"]:
        group = summary["groups"][key]
        for count, stat in zip(area["counts"], group["statistics"], strict=True):
            report.append(f"| {group['name']} | {group['length_km']:.1f} | {count}{'*' if count == 200000 else ''} | {stat['mean_m']:.2f} | {stat['median_m']:.2f} | {stat['p90_m']:.2f} | {stat['coverage_pct']['100']:.3f} |")
    report += ["", "200k* — предварительный сценарий до очистки, не готовая ML-gallery. Точный состав сохранён в summary.json.",
               "В основной сети — автомобильные и пешеходные улицы. Дворовые/служебные проезды, тротуары, велодорожки и ступени показаны отдельно и в расширенной сети. Тропы и грунтовые tracks не включены. Туннели, indoor и площадные объекты исключены.",
               "Все варианты используют одну сеть и одни веса длины. Отдельные проезжие части остаются отдельными; одинаковые геометрические отрезки не считаются дважды. Нет весов по населению, трафику, фотографиям или числу OSM-узлов.",
               "Граница — существующий exact Moscow R102269. Никакие query splits, calibration или final test не открывались. Gallery selection и модель не менялись.",
               "Сравнение шага 10 и 20 м, контрольные суммы и полный список классов сохранены в summary.json."]
    (out / "report.md").write_text("\n".join(report) + "\n")
    print(out / "report.md", flush=True)


if __name__ == "__main__":
    main()
