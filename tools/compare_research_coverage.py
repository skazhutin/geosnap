"""Reference-only geographic coverage. Never reads query splits or embeddings."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[1]
RADIUS = 6371008.8
COS_ORIGIN = np.cos(np.deg2rad(55.7))
AOI_SHA = "33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def xyz(latlon):
    lat, lon = np.deg2rad(latlon).T
    return np.column_stack((np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)))


def nearest(tree, coordinates):
    chord, _ = tree.query(xyz(coordinates), workers=2)
    return 2 * RADIUS * np.arcsin(np.clip(chord / 2, 0, 1))


def grid(boundary, spacing):
    """Equal-area cylindrical grid, clipped by the original polygon, not its display copy."""
    west, south, east, north = boundary.bounds
    x0, x1 = RADIUS * COS_ORIGIN * np.deg2rad([west, east])
    y0, y1 = RADIUS * np.sin(np.deg2rad([south, north])) / COS_ORIGIN
    x = np.arange(np.floor(x0 / spacing) * spacing + spacing / 2, x1, spacing)
    y = np.arange(np.floor(y0 / spacing) * spacing + spacing / 2, y1, spacing)
    xx, yy = np.meshgrid(x, y)
    lon = np.rad2deg(xx.ravel() / (RADIUS * COS_ORIGIN))
    lat = np.rad2deg(np.arcsin(yy.ravel() * COS_ORIGIN / RADIUS))
    inside = shapely.intersects_xy(boundary, lon, lat)
    return np.column_stack((lat[inside], lon[inside]))


def summarize(distances):
    return {
        "mean_m": float(np.mean(distances)), "median_m": float(np.median(distances)),
        "p90_m": float(np.quantile(distances, .9)), "p95_m": float(np.quantile(distances, .95)),
        "coverage_pct": {str(k): float(100 * np.mean(distances <= k)) for k in [25, 50, 100, 250, 500, 1000]},
    }


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def preliminary_scenario(available, previous, guard, forbidden, geometry, out):
    """User-authorized pre-cleaning geography, isolated from every ML manifest.

    Reference-to-reference near duplicates are allowed in this scenario only.
    Cached query identity/sequence gates stay in force; no query rows are opened.
    """
    from ml.evaluation.moscow_split import _PerceptualHashIndex
    from ml.research.gallery_scale import decorate, gallery_role, smart_order

    raw_path = previous / "candidate_metadata.parquet"
    future_path = ROOT / "data/evaluation/moscow_research_v5/prospective/gallery_acquisition_pool.parquet"
    plan_path = ROOT / "data/evaluation/moscow_research_v5/gallery_expansion/plan.json"
    plan = json.loads(plan_path.read_text())
    if digest(future_path) != plan["allowed_future_gallery_pool_sha256"]:
        raise RuntimeError("Pre-assigned reference-only discovery pool changed")
    raw, future = pd.read_parquet(raw_path), pd.read_parquet(future_path)
    cfg = json.loads((ROOT / "configs/moscow_gallery_scale.json").read_text())
    future = decorate(future, cfg)
    raw["coverage_status"] = "archive_before_reference_deduplication"
    future["coverage_status"] = "discovery_not_image_verified"
    raw = pd.concat([raw, future], ignore_index=True).drop_duplicates("id")
    raw = raw[~raw.id.isin(set(available.id))]
    raw = raw[shapely.intersects_xy(geometry, raw.lon, raw.lat)]
    raw = raw[[gallery_role(s, q) for s, q in zip(raw.source, raw.sequence_id, strict=True)]]
    raw = raw[(raw.source.eq("msls") & raw.license.eq("CC BY-NC-SA 4.0")) |
              (raw.source.isin(["mapillary", "kartaview"]) & raw.license.eq("CC BY-SA 4.0"))]
    db_path = previous / "image_audit.sqlite"
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as db:
        fingerprints = {i: json.loads(v) for i, v in db.execute("SELECT id,fingerprint FROM images")}
    query_index = _PerceptualHashIndex(4)
    for i, value in enumerate(guard["phashes"]):
        query_index.add(i, int(value, 16))
    identities = {tuple(x) for x in guard["identities"]}
    exact, pixels = set(guard["exact"]), set(guard["pixels"])
    leaked = set(forbidden)
    retained, excluded = [], Counter()
    for row in raw.to_dict("records"):
        provider = "mapillary" if row["source"] == "msls" else row["source"]
        if row["sequence_key"] in leaked or (provider, str(row["source_image_id"])) in identities:
            excluded["cached_query_identity_or_forbidden_sequence"] += 1
            continue
        fp = fingerprints.get(row["id"])
        if fp:
            if not fp["decode_ok"]:
                excluded["cached_decode_failure"] += 1
                continue
            if fp["file_sha256"] in exact or fp["pixel_sha256"] in pixels or query_index.matches(int(fp["perceptual_hash"], 16)):
                leaked.add(row["sequence_key"])
                excluded["cached_query_fingerprint_match"] += 1
                continue
            row.update(fp)
        retained.append(row)
    raw = pd.DataFrame(retained)
    excluded["whole_sequence_after_fingerprint"] = int(raw.sequence_key.isin(leaked).sum())
    raw = raw[~raw.sequence_key.isin(leaked)].copy()
    # Never use exact duplicate physical files to inflate the round number.
    for column in ["file_sha256", "pixel_sha256"]:
        before = len(raw)
        raw = raw[~raw[column].isin(set(available[column].dropna()))]
        raw = raw[raw[column].isna() | ~raw[column].duplicated()]
        excluded[column + "_duplicate"] = before - len(raw)
    known = set(zip(available.source.replace("msls", "mapillary"), available.source_image_id.astype(str), strict=True))
    keep = []
    for row in raw.itertuples():
        key = ("mapillary" if row.source == "msls" else row.source, str(row.source_image_id))
        if key not in known:
            keep.append(row.Index)
            known.add(key)
    raw = decorate(raw.loc[keep].reset_index(drop=True), cfg)
    order = smart_order(raw, available)
    needed = max(0, 200000 - len(available))
    addition = raw.loc[order[:needed]]
    scenario = pd.concat([available, addition], ignore_index=True)
    audit = {
        "target": 200000, "actual": len(scenario), "eligible_preclean_additions": len(raw),
        "by_status": scenario.coverage_status.value_counts().to_dict(), "exclusions": dict(excluded),
        "reference_phash_deduplication": "Deliberately omitted for this user-requested pre-cleaning geographic scenario only. All ML gates unchanged.",
        "cached_query_fingerprint_gate": True, "entire_implicated_sequences_excluded_from_new_additions": True,
        "selection": "Existing reference-only H3 density + sequence + heading + viewpoint + season smart_order",
        "files": {str(p): digest(p) for p in [raw_path, future_path, plan_path]},
        "not_an_ml_gallery": True, "discovery_candidates_may_be_unavailable_or_fail_image_audit": True,
    }
    save(out / "preliminary_scenario_audit.json", audit)
    return scenario, audit


def main():
    config = json.loads((ROOT / "data/evaluation/moscow_research_v5/storage.json").read_text())
    import os
    external = Path(os.environ.get("GEOSNAP_DATA_ROOT", config["external_root"]))
    if not external.is_dir() or external.stat().st_dev == ROOT.stat().st_dev:
        raise RuntimeError("Connect the canonical external volume before comparing reference pools")
    previous, night = external / "gallery_scale_v6", external / "night_v7_20260909"
    out = external / "coverage_comparison_20260910"
    out.mkdir(exist_ok=True)
    aoi = ROOT / "data/raw/moscow/moscow_admin_boundary.geojson"
    if digest(aoi) != AOI_SHA:
        raise RuntimeError("Exact R102269 changed")
    geometry = shapely.from_geojson(aoi.read_text())
    paths = [previous / "manifests/G2_smart.parquet", night / "reference_tranche3/gallery.parquet",
             previous / "candidate_pool.parquet"]
    frames = [pd.read_parquet(p) for p in paths]
    baseline = frames[0]
    if len(baseline) != 100000:
        raise RuntimeError("The historical 100k baseline changed")
    combined = pd.concat(frames, ignore_index=True).drop_duplicates("id").reset_index(drop=True)
    if not np.isfinite(combined[["lat", "lon"]].to_numpy(float)).all():
        raise RuntimeError("Non-finite reference coordinates")
    if not shapely.intersects_xy(geometry, combined.lon, combined.lat).all():
        raise RuntimeError("Reference outside exact Moscow")
    provider_ids = combined.source.replace("msls", "mapillary") + "::" + combined.source_image_id.astype(str)
    for values in [provider_ids, combined.file_sha256.dropna(), combined.pixel_sha256.dropna()]:
        if values.duplicated().any():
            raise RuntimeError("Duplicate physical identity in the union; do not count it twice")
    if not combined.id.iloc[:100000].equals(baseline.id.reset_index(drop=True)):
        raise RuntimeError("Comparison must retain the literal historical 100k prefix")
    guard_path = night / "live_expansion2/query_identity_guard.json"
    quarantine_path = night / "live_expansion2_quarantine/quarantine.receipt.json"
    guard, quarantine = json.loads(guard_path.read_text()), json.loads(quarantine_path.read_text())
    forbidden = set(guard["forbidden"]) | set(quarantine["quarantined_sequences"])
    extra = combined.iloc[100000:]
    if extra.sequence_key.isin(forbidden).any():
        raise RuntimeError("New reference intersects the cached forbidden-sequence ledger")
    if extra.file_sha256.isin(set(guard["exact"])).any() or extra.pixel_sha256.isin(set(guard["pixels"])).any():
        raise RuntimeError("New reference intersects the cached query identity ledger")
    historical_forbidden = int(baseline.sequence_key.isin(forbidden).sum())
    combined["coverage_status"] = "previously_audited_reference_pool"
    scenario, scenario_audit = preliminary_scenario(combined, previous, guard, forbidden, geometry, out)
    galleries = [baseline, combined, scenario]
    # Coordinates alone are sufficient. This map does not re-certify any gallery for ML.
    cols = ["id", "source", "source_image_id", "sequence_id", "sequence_key", "lat", "lon", "heading",
            "captured_at", "license", "usage_scope", "image_path", "archive_path", "archive_member",
            "file_sha256", "pixel_sha256", "cell"]
    manifests = [out / "G100_coordinates.parquet", out / f"available_{len(combined)}_coordinates.parquet",
                 out / f"preclean_scenario_{len(scenario)}_coordinates.parquet"]
    for frame, path in zip(galleries, manifests, strict=True):
        frame[cols + (["coverage_status"] if "coverage_status" in frame else [])].to_parquet(path, index=False)
    trees = [cKDTree(xyz(frame[["lat", "lon"]].to_numpy(float))) for frame in galleries]
    probes = grid(geometry, 50)
    distances = np.column_stack([nearest(tree, probes) for tree in trees])
    if np.any(np.diff(distances, axis=1) > 1e-6):
        raise RuntimeError("Nested-gallery nearest distance increased")
    stats = [summarize(distances[:, i]) for i in range(3)]
    density_diagnosis = {}
    for name, lower, upper in [("100k_to_available", 0, 1), ("available_to_preclean200k", 1, 2)]:
        new_coordinates = galleries[upper].iloc[len(galleries[lower]):][["lat", "lon"]].to_numpy(float)
        separation = nearest(trees[lower], new_coordinates)
        density_diagnosis[name] = {
            "additional_points": len(new_coordinates),
            "within_25m_of_existing_pct": float(100 * np.mean(separation <= 25)),
            "within_100m_of_existing_pct": float(100 * np.mean(separation <= 100)),
            "median_distance_to_existing_m": float(np.median(separation)),
            "p90_distance_to_existing_m": float(np.quantile(separation, .9)),
        }
    save(out / "addition_density_diagnosis.json", density_diagnosis)
    # A coarser independent grid checks numerical integration sensitivity.
    coarse = grid(geometry, 100)
    convergence = [summarize(nearest(tree, coarse)) for tree in trees]
    display = grid(geometry, 200)
    display_distances = np.column_stack([nearest(tree, display) for tree in trees])
    np.savez_compressed(out / "area_probes_50m.npz", latlon=probes.astype(np.float32), distances_m=distances.astype(np.float32))
    summary = {
        "status": "100k_vs_available_union_and_explicit_preclean_scenario",
        "counts": [len(frame) for frame in galleries], "target": 200000,
        "shortfall_to_200k": 200000 - len(combined),
        "sources": [frame.source.value_counts().to_dict() for frame in galleries],
        "occupied_h3_r9": [int(frame.cell.nunique()) for frame in galleries],
        "scenario_audit": scenario_audit,
        "metric": "Great-circle distance from a uniformly area-weighted point inside exact Moscow R102269 to its nearest reference",
        "grid_spacing_m": 50, "grid_points": len(probes), "estimated_area_km2": len(probes) * .0025,
        "sphere_radius_m": RADIUS, "all_moscow_area_in_denominator": True,
        "population_or_street_weighting": False, "route_distance": False,
        "statistics": stats, "100m_grid_check": convergence,
        "mean_reduction_m": stats[0]["mean_m"] - stats[1]["mean_m"],
        "mean_reduction_pct": 100 * (1 - stats[1]["mean_m"] / stats[0]["mean_m"]),
        "newly_within_100m_pct_of_moscow": float(100 * np.mean((distances[:, 0] > 100) & (distances[:, 1] <= 100))),
        "improved_by_over_100m_pct_of_moscow": float(100 * np.mean(distances[:, 0] - distances[:, 1] > 100)),
        "scenario_mean_reduction_pct": 100 * (1 - stats[2]["mean_m"] / stats[0]["mean_m"]),
        "scenario_newly_within_100m_pct_of_moscow": float(100 * np.mean((distances[:, 0] > 100) & (distances[:, 2] <= 100))),
        "display_grid_spacing_m": 200,
        "input_sha256": {str(p): digest(p) for p in [*paths, aoi, guard_path, quarantine_path]},
        "manifest_sha256": {str(p): digest(p) for p in manifests},
        "historical_baseline_refs_in_cached_forbidden_sequences": historical_forbidden,
        "scope": "Geographic inspection only. Historical G2 coordinates stay intact for comparison. The union is not a newly audited deployable/benchmark gallery; no new cross-pool pHash pass was performed. MSLS is research-only.",
        "query_splits_opened": False, "query_coordinates_used": False, "images_decoded": 0,
        "embeddings_computed": 0, "downloads": 0, "production_changes": False,
    }
    save(out / "summary.json", summary)
    sources = ["mapillary", "kartaview", "msls"]
    payload = {
        "summary": {k: v for k, v in summary.items() if k not in {"input_sha256", "manifest_sha256"}},
        "points": [[round(r.lat, 6), round(r.lon, 6), sources.index(r.source),
                    {"previously_audited_reference_pool": 0, "archive_before_reference_deduplication": 1,
                     "discovery_not_image_verified": 2}[r.coverage_status]] for r in scenario.itertuples()],
        "grid": np.column_stack([np.round(display, 6), np.round(display_distances)]).tolist(),
        "boundary": json.loads(shapely.to_geojson(geometry.simplify(.00003, preserve_topology=True))),
    }
    dest = ROOT / "data/evaluation/moscow_night_v7/map"
    template = (ROOT / "tools/research_coverage_compare.html").read_text()
    (dest / "comparison.html").write_text(template.replace("__DATA__", json.dumps(payload, ensure_ascii=False, separators=(",", ":"))))
    # Small portable summary remains available when the image disk is disconnected.
    save(dest / "comparison_summary.json", summary)
    lines = ["# Сравнение географического покрытия Москвы", "", "200 000 прошедших аудит references сейчас нет. По отдельному запросу пользователя показан сценарий до очистки.", "",
             "| Метрика | 100 000 | Доступный pool | Сценарий до очистки |", "|---|---:|---:|---:|"]
    for label, key in [("Среднее расстояние, м", "mean_m"), ("Медиана, м", "median_m"), ("p90, м", "p90_m")]:
        lines.append(f"| {label} | {stats[0][key]:.2f} | {stats[1][key]:.2f} | {stats[2][key]:.2f} |")
    lines += [f"| Снимков / кандидатов | {len(baseline)} | {len(combined)} | {len(scenario)} |",
              f"| Территория с reference ≤100 м, % | {stats[0]['coverage_pct']['100']:.3f} | {stats[1]['coverage_pct']['100']:.3f} | {stats[2]['coverage_pct']['100']:.3f} |", "",
              f"Равномерная по площади сетка 50 м: {len(probes):,} точек; вся Москва, включая леса, воду и Новую Москву.",
              "Расстояния по прямой на сфере; не по дорогам, не среднее по населению, не точность ML.",
              "Проверка на сетке 100 м и контрольные суммы сохранены в summary.json.",
              f"До 200k аудированных references не хватает {200000 - len(combined)}. Предварительный сценарий включает {scenario_audit['by_status'].get('archive_before_reference_deduplication', 0)} архивных кадров до прореживания похожих references и {scenario_audit['by_status'].get('discovery_not_image_verified', 0)} кандидатов без проверки изображений.",
              "Историческая G2 сохранена для сравнения координат. Объединённый pool не является новым допущенным ML-кандидатом; новый cross-pool pHash-аудит не проводился.",
              "MSLS — research-only. Query splits не открывались, изображения и дескрипторы не обрабатывались."]
    lines += ["", "Дополнительная диагностика:"]
    for label, name in [("100k → доступный pool", "100k_to_available"), ("Доступный pool → предварительные 200k", "available_to_preclean200k")]:
        values = density_diagnosis[name]
        lines.append(f"- {label}: {values['within_100m_of_existing_pct']:.2f}% добавленных точек уже находятся в пределах 100 м от прежних references; медиана расстояния {values['median_distance_to_existing_m']:.2f} м.")
    area_new = [float(np.count_nonzero((distances[:, i] > 100) & (distances[:, i + 1] <= 100)) * .0025) for i in range(2)]
    lines += ["", f"100k → доступный pool добавляет {area_new[0]:.2f} км² территории в радиусе 100 м; следующие предварительные кадры/кандидаты — {area_new[1]:.2f} км².",
              "Этот pool главным образом уплотняет существующие места. Новые ракурсы могут помочь ML даже при небольшом расширении географического покрытия."]
    (out / "report.md").write_text("\n".join(lines) + "\n")
    print(json.dumps({k: summary[k] for k in ["counts", "statistics", "mean_reduction_pct", "newly_within_100m_pct_of_moscow"]}, ensure_ascii=False), flush=True)
    print(dest / "comparison.html", flush=True)


if __name__ == "__main__":
    main()
