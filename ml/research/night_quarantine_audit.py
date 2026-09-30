"""Conservative common-gallery quarantine; reuse frozen fingerprints, never waive gates."""
from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_acquisition as acquisition
from ml.research import night_gallery_addition as addition
from ml.research import night_live_expansion as expansion
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import LOCAL

INCIDENT_SHA = "1375d152f8cb5e2040235748aeed719eb7bc332d6f1f7daa967fa86505afedb6"
QUARANTINE = {"kartaview::3616501", "mapillary::w776ztpb3kgskr90yaefy2"}
FILES = ("clean_G2.parquet", "clean_first.parquet", "addition.parquet", "gallery.parquet",
         "mapping.json", "quarantine.receipt.json", "physical_hashes.done.json")
IDENTITY_COLUMNS = ["id", "source", "source_image_id", "sequence_id", "file_sha256"]


def stage():
    cfg, gc, store, previous, night, original = expansion.stage()
    return cfg, gc, store, previous, night, original, night / "live_expansion2_quarantine"


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(LOCAL / "quarantine_audit_status.json", value)
    print(json.dumps(value), flush=True)


def sequence_keys(frame):
    return pd.Series([f"{expansion.provider(r.source)}::{str(r.sequence_id).removeprefix('msls:')}"
                      for r in frame.itertuples()], index=frame.index)


def normalized_forbidden(values):
    result = set(values)
    for key in values:
        source, sequence = key.split("::", 1)
        result.add(f"{expansion.provider(source)}::{sequence.removeprefix('msls:')}")
    return result


def quarantine_view(frame, sequences):
    """Stable subsequence plus explicit reversible row mapping; original remains untouched."""
    if frame.id.duplicated().any() or not sequence_keys(frame).equals(frame.sequence_key):
        raise RuntimeError("baseline identities or normalized provider sequence keys disagree")
    keep = np.flatnonzero(~frame.sequence_key.isin(sequences).to_numpy())
    inverse = np.full(len(frame), -1, dtype=np.int64)
    inverse[keep] = np.arange(len(keep))
    clean = frame.iloc[keep].reset_index(drop=True).copy()
    return clean, {"original_count": len(frame), "clean_count": len(clean),
                   "clean_to_original": keep.tolist(), "original_to_clean": inverse.tolist(),
                   "excluded_ids": frame.loc[frame.sequence_key.isin(sequences), "id"].tolist()}


def write_view(frame, path):
    """Do not coerce old values or columns while persisting an exact row view."""
    temporary = path.with_name(path.name + ".writing")
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def development_checks(queries, gallery):
    counts = {"stable_image_ids": len(set(queries.id) & set(gallery.id)),
              "normalized_provider_image_ids": len(expansion.identities(queries) & expansion.identities(gallery)),
              "normalized_provider_sequences": len(set(sequence_keys(queries)) & set(sequence_keys(gallery))),
              "file_sha256": len(set(queries.file_sha256) & set(gallery.file_sha256))}
    if any(counts.values()):
        raise RuntimeError("common gallery intersects fixed development identities")
    return counts


def inputs():
    cfg, gc, store, previous, night, original, out = stage()
    registered = expansion.verify_prepared(original, cfg, gc, previous, night, 6000)
    incident_path = original / "identity_incident.json"
    if digest(incident_path) != INCIDENT_SHA:
        raise RuntimeError("reviewed identity incident changed")
    incident = json.loads(incident_path.read_text())
    if set(incident["quarantined_sequence_keys"]) != QUARANTINE:
        raise RuntimeError("reviewed whole-sequence quarantine changed")
    for name, expected in incident["input_sha256"].items():
        if digest(Path(name)) != expected:
            raise RuntimeError(f"incident input changed: {Path(name).name}")
    if not (original / "query_identity_guard.receipt.json").exists():
        raise RuntimeError("cached guard required; never decode query images in this repair")
    download = json.loads((original / "download.done.json").read_text())
    if download["selected_sha256"] != registered["selected_sha256"]:
        raise RuntimeError("download population differs from frozen selection")
    first = night / "gallery_addition"
    addition.verify_audited(first, previous, night)
    files = [incident_path, original / "image_audit.sqlite", original / "selected.parquet",
             original / "query_identity_guard.json", original / "query_identity_guard.receipt.json",
             original / "live_expansion.started.json", original / "download.done.json",
             previous / "manifests/G2_smart.parquet", previous / "descriptor_pool.json",
             previous / "descriptor_contract.json", first / "gallery.parquet", first / "descriptor_pool.json",
             first / "audit.done.json", first / "done.json", first / "published.json",
             first / "baseline_identity_scope.json", night / "input_contract.json",
             night / "cpu_pipeline_added/gallery_addition/mean_scores.done.json",
             WORKSPACE / gc["aoi"], WORKSPACE / gc["query_manifest"]]
    contract = {"files": {str(p): digest(p) for p in files},
                "source_sha256": {p.name: digest(p) for p in (Path(__file__), Path(expansion.__file__),
                     Path(addition.__file__), Path(gallery_scale.__file__), Path(acquisition.__file__))},
                "incident_sha256": INCIDENT_SHA, "query_sha256": gc["query_sha256"],
                "quarantine": sorted(QUARANTINE), "fingerprints_reused": 5050,
                "original_counts": {"G2": 100000, "first": 102944},
                "expected_removed": {"G2": 24, "first": 29},
                "reference_selection_unchanged": True, "identity_filter_unchanged": True,
                "no_query_coordinates_for_selection": True, "no_new_images_or_encoding": True,
                "calibration_final_access": False, "production_changes": False}
    if contract["files"][str(WORKSPACE / gc["query_manifest"])] != gc["query_sha256"]:
        raise RuntimeError("fixed development identity population changed")
    if digest(WORKSPACE / gc["aoi"]) != acquisition.AOI_SHA:
        raise RuntimeError("exact Moscow R102269 AOI changed")
    return cfg, gc, store, previous, night, original, out, registered, incident, contract


def physical_hashes(out, selected, fingerprints):
    """Validate existing bytes once, checkpointing32 rows; no image decode or new fingerprints."""
    if set(fingerprints) - set(selected.id):
        raise RuntimeError("fingerprint database includes unselected identities")
    rows = [r for r in selected.itertuples() if r.id in fingerprints]
    directory = out / "physical_hashes"
    directory.mkdir(exist_ok=True)
    commits = {}
    with ThreadPoolExecutor(max_workers=2) as workers:
        for start in range(0, len(rows), 32):
            batch = rows[start:start + 32]
            before = [Path(r.image_path).stat() for r in batch]
            expected = [{"id": r.id, "path": r.image_path, "sha256": fingerprints[r.id]["file_sha256"],
                         "size": s.st_size, "mtime_ns": s.st_mtime_ns}
                        for r, s in zip(batch, before, strict=True)]
            receipt_path = directory / f"batch-{start:04d}.json"
            if receipt_path.exists():
                receipt = json.loads(receipt_path.read_text())
                if receipt != {"images": expected, "verified": True}:
                    raise RuntimeError("validated image bytes or fingerprint checkpoint changed")
            else:
                actual = list(workers.map(digest, [Path(r.image_path) for r in batch]))
                after = [Path(r.image_path).stat() for r in batch]
                if any(h != e["sha256"] or s.st_size != e["size"] or s.st_mtime_ns != e["mtime_ns"]
                       for h, e, s in zip(actual, expected, after, strict=True)):
                    raise RuntimeError("fingerprinted canonical image changed")
                save(receipt_path, {"images": expected, "verified": True})
            commits[receipt_path.name] = digest(receipt_path)
            state(out, "quarantine_byte_validation", completed=start + len(batch), total=len(rows))
    save(out / "physical_hashes.done.json", {"images": len(rows), "commits": commits,
         "no_redecode": True, "encoding_revalidates_new_image_sha256": True})


def validate_selected(selected, gc, store):
    required = ["source", "source_image_id", "sequence_id", "license", "attribution", "source_url"]
    if (selected[required].isna().any().any()
            or any(selected[c].astype(str).str.strip().eq("").any() for c in required)
            or not selected.source.isin(["mapillary", "kartaview"]).all()
            or not selected.production_compatible.all()
            or not selected.license.eq(acquisition.SETUP.get("license", "CC BY-SA 4.0")).all()
            or not all(gallery_scale.gallery_role(r.source, r.sequence_id) for r in selected.itertuples())
            or selected.id.duplicated().any() or selected.image_path.duplicated().any()):
        raise RuntimeError("selected source, provenance, identity, license or sequence-role gate failed")
    addition.verify_canonical_paths(selected.image_path, store)
    boundary = acquisition.load_aoi_boundary(WORKSPACE / gc["aoi"])
    if not shapely.covers(boundary.geometry, shapely.points(selected.lon, selected.lat)).all():
        raise RuntimeError("reference outside exact Moscow administrative polygon")


def audit():
    _, gc, store, previous, night, original, out, registered, incident, contract = inputs()
    out.mkdir(exist_ok=True)
    marker = out / "audit.started.json"
    if marker.exists():
        if json.loads(marker.read_text())["contract"] != contract:
            raise RuntimeError("quarantine source/input contract changed on resume")
    else:
        save(marker, {"started": time.time(), "contract": contract,
             "scope": "scientific repair of already-started data experiment; both common baselines quarantined",
             "no_gate_waiver": True, "original_results_preserved": True})
    if (out / "audit.done.json").exists():
        verify_audited()
        return
    original_g2 = pd.read_parquet(previous / "manifests/G2_smart.parquet")
    original_first = pd.read_parquet(night / "gallery_addition/gallery.parquet")
    g2, map_g2 = quarantine_view(original_g2, QUARANTINE)
    first, map_first = quarantine_view(original_first, QUARANTINE)
    if (len(g2) != 99976 or len(first) != 102915
            or map_first["excluded_ids"] != incident["excluded_baseline_reference_ids"]):
        raise RuntimeError("quarantine no longer matches reviewed29 reference identities")
    common = list(original_g2.columns)
    pd.testing.assert_frame_equal(first.iloc[:len(g2)][common].reset_index(drop=True), g2, check_dtype=False)
    selected = gallery_scale.decorate(pd.read_parquet(original / "selected.parquet"), gc)
    validate_selected(selected, gc, store)
    with sqlite3.connect(f"file:{original / 'image_audit.sqlite'}?mode=ro", uri=True) as db:
        fingerprints = {identity: json.loads(value) for identity, value in db.execute("SELECT id,fingerprint FROM images")}
    if len(fingerprints) != contract["fingerprints_reused"]:
        raise RuntimeError("reviewed fingerprint population changed")
    physical_hashes(out, selected, fingerprints)
    guard = expansion.cached_query_guard(gc, original, registered)
    queries = pd.read_parquet(WORKSPACE / gc["query_manifest"], columns=IDENTITY_COLUMNS)
    if len(queries) != 1184:
        raise RuntimeError("fixed development query denominator changed")
    forbidden = normalized_forbidden(guard[3]) | set(sequence_keys(queries)) | QUARANTINE
    guard = (*guard[:3], forbidden, guard[4])
    dev_checks = {"G2": development_checks(queries, g2), "first": development_checks(queries, first)}
    selected["is_pano"] = [bool(json.loads(r.metadata_json or "{}").get("is_pano", False))
        or str(json.loads(r.metadata_json or "{}").get("projection", "")).upper() == "SPHERE"
        for r in selected.itertuples()]
    # Re-run the original gate, including its fail-fast baseline implication.
    # The reviewed sequences are absent from BOTH common comparison baselines.
    state(out, "quarantine_unchanged_identity_filter", total=len(selected))
    retained, exclusions = addition.filter_fingerprints(selected, fingerprints, first, guard)
    added = pd.DataFrame(retained).reindex(columns=first.columns)
    gallery = pd.concat([first, added], ignore_index=True)
    if gallery.id.duplicated().any() or set(added.sequence_key) & forbidden:
        raise RuntimeError("retained references repeat identities or prohibited sequences")
    dev_checks["expanded"] = development_checks(queries, gallery)
    for name, frame in (("clean_G2.parquet", g2), ("clean_first.parquet", first),
                        ("addition.parquet", added), ("gallery.parquet", gallery)):
        write_view(frame, out / name)
    save(out / "mapping.json", {"G2": map_g2, "first": map_first})
    production_path = WORKSPACE / "data/evaluation/moscow_real_v4/gallery.parquet"
    production = pd.read_parquet(production_path, columns=["id", "source", "sequence_id"])
    production_overlap = {"excluded_ids": sorted(set(production.id) & set(map_first["excluded_ids"])),
                          "quarantined_sequence_references": int(sequence_keys(production).isin(QUARANTINE).sum())}
    save(out / "quarantine.receipt.json", {"incident_sha256": INCIDENT_SHA,
         "quarantined_sequences": sorted(QUARANTINE), "excluded_reference_ids": map_first["excluded_ids"],
         "removed": {"G2": 24, "first": 29}, "current_development_identity_checks": dev_checks,
         "production_gallery_sha256": digest(production_path), "production_overlap": production_overlap,
         "new_references_exclude_every_protected_sequence": True, "unchanged_filter": True,
         "original_baselines_results_and_production_preserved": True,
         "historical_protected_reference_count": int(first.sequence_key.isin(forbidden).sum()),
         "limitation": "Conservative whole-sequence quarantine, not proof that all29 images are duplicates. "
                       "Opaque KV evidence unresolved; historical v2 protected-sequence overlap remains uncertified. "
                       "Use only the common-quarantine board for comparisons; current calibration/final stay closed.",
         "no_query_coordinate_selection": True, "no_new_download_decode_or_descriptor_computation": True})
    hashes = {name: digest(out / name) for name in FILES}
    save(out / "audit.done.json", {"completed": time.time(), "contract": contract, "files": hashes,
         "gallery_sha256": hashes["gallery.parquet"], "addition_sha256": hashes["addition.parquet"],
         "baseline_count": len(first), "gallery_count": len(gallery), "added": len(added),
         "by_source": dict(Counter(added.source)), "exclusions": exclusions,
         "common_quarantine": True, "clean_first_prefix_unchanged": True,
         "descriptor_contract_sha256": digest(previous / "descriptor_contract.json")})
    verify_audited()
    state(out, "quarantine_audit_completed", added=len(added), gallery_count=len(gallery))


def verify_audited():
    _, gc, _, previous, night, _, out, _, incident, contract = inputs()
    registered = json.loads((out / "audit.started.json").read_text())
    done = json.loads((out / "audit.done.json").read_text())
    if registered["contract"] != contract or done["contract"] != contract or set(done["files"]) != set(FILES):
        raise RuntimeError("quarantine audit source/input contract changed")
    for name, expected in done["files"].items():
        if digest(out / name) != expected:
            raise RuntimeError(f"quarantine output changed: {name}")
    for name, expected in json.loads((out / "physical_hashes.done.json").read_text())["commits"].items():
        if Path(name).name != name or digest(out / "physical_hashes" / name) != expected:
            raise RuntimeError("canonical byte-validation receipt changed")
    g2, first, added, gallery = [pd.read_parquet(out / name) for name in FILES[:4]]
    original_g2 = pd.read_parquet(previous / "manifests/G2_smart.parquet")
    original_first = pd.read_parquet(night / "gallery_addition/gallery.parquet")
    expected_g2, map_g2 = quarantine_view(original_g2, QUARANTINE)
    expected_first, map_first = quarantine_view(original_first, QUARANTINE)
    if (json.loads((out / "mapping.json").read_text()) != {"G2": map_g2, "first": map_first}
            or map_first["excluded_ids"] != incident["excluded_baseline_reference_ids"]):
        raise RuntimeError("quarantine row mapping changed")
    for actual, expected in ((g2, expected_g2), (first, expected_first),
                             (gallery.iloc[:len(first)], first), (gallery.iloc[len(first):], added)):
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
    if (len(g2) != 99976 or len(first) != 102915 or len(gallery) != done["gallery_count"]
            or len(added) != done["added"] or gallery.id.duplicated().any()
            or set(gallery.sequence_key) & QUARANTINE):
        raise RuntimeError("quarantine population/count/identity contract violated")
    return gc, previous, night, out, g2, first, added, gallery


def main():
    *_, out = stage()
    out.mkdir(exist_ok=True)
    os.nice(10)
    with (LOCAL / "quarantine_audit.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            audit()
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            state(out, "quarantine_audit_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    main()
