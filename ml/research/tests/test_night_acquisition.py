"""Reference-only selection, immutable resume and bounded network-gate regressions."""
import json
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from ml.research import night_acquisition as acquisition


def selection_row(identity, cell, sequence, heading=0, month="2020-01"):
    return dict(id=identity, cell=cell, sequence_key=sequence, heading_bin=heading,
                month=month, selection_hash=acquisition.stable(identity))


def test_selection_fills_empty_cells_then_uses_new_heading_sequence_and_month():
    base = pd.DataFrame([selection_row("base", "dense", "known")])
    pool = pd.DataFrame([selection_row("dense", "dense", "fresh", heading=1),
                         selection_row("repeated", "empty", "known"),
                         selection_row("new", "empty", "fresh")])
    assert acquisition.smart_select(pool, base, 1).id.tolist() == ["new"]
    expected = acquisition.smart_select(pool, base, 3).id.tolist()
    assert acquisition.smart_select(pool.iloc[[2, 0, 1]], base, 3).id.tolist() == expected
    same_cell = pd.DataFrame([selection_row("oldheading", "dense", "newa"),
                              selection_row("newheading", "dense", "newb", heading=1)])
    assert acquisition.smart_select(same_cell, base, 1).id.tolist() == ["newheading"]
    months = pd.DataFrame([selection_row("oldmonth", "dense", "newa"),
                           selection_row("newmonth", "dense", "newb", month="2021-07")])
    assert acquisition.smart_select(months, base, 1).id.tolist() == ["newmonth"]
    with pytest.raises(ValueError, match="3000"):
        acquisition.smart_select(pool, base, 3001)


def test_reference_pool_rejects_query_roles_licenses_identity_aoi_and_unsafe_urls(tmp_path, monkeypatch):
    monkeypatch.setattr(acquisition, "gallery_role", lambda source, seq: seq == "gallery")
    row = dict(id="safe", source="mapillary", source_image_id="111", sequence_id="gallery",
               lat=55.75, lon=37.6, heading=0, captured_at="2020-01-01T00:00:00Z",
               license="CC BY-SA 4.0", download_url="https://example.fbcdn.net/photo.jpg")
    variants = [row, row | {"id": "query", "source_image_id": "2", "sequence_id": "query"},
                row | {"id": "license", "source_image_id": "3", "license": "research-only"},
                row | {"id": "known", "source_image_id": "4"},
                row | {"id": "outside", "source_image_id": "5", "lon": 40.},
                row | {"id": "url", "source_image_id": "6", "download_url": "https://untrusted.example/photo.jpg"},
                row | {"id": "physical", "source_image_id": "7"}]
    known = {("mapillary", "4")}
    boundary = SimpleNamespace(covers=lambda lon, lat: 37 < lon < 38 and 55 < lat < 56)
    selected, rejected = acquisition.eligible_pool(pd.DataFrame(variants), known, boundary, tmp_path,
                                                   {("mapillary", "physical_7.jpg")})
    assert selected.id.tolist() == ["safe"]
    assert known == {("mapillary", "4")}
    assert set(rejected) == {"query_or_unknown_sequence_role", "license", "known_identity",
                             "outside_exact_aoi", "invalid_download_url", "existing_physical_filename"}
    assert selected.image_path.iloc[0] == str(tmp_path / "geosnap/mapillary/safe_111.jpg")


def test_canonical_guards_preserve_existing_invalid_files_and_reject_escape(tmp_path):
    store = tmp_path / "store"
    target = acquisition.canonical_path(store, "mapillary", "ref", "123")
    target.parent.mkdir(parents=True)
    target.write_bytes(b"existing user bytes")
    frame = pd.DataFrame([dict(id="ref", source="mapillary", source_image_id="123", image_path=str(target))])
    with pytest.raises(RuntimeError, match="preserved"):
        acquisition.preflight_destinations(frame, store)
    assert target.read_bytes() == b"existing user bytes"
    with pytest.raises(ValueError, match="identity"):
        acquisition.canonical_path(store, "mapillary", "../escape", "123")
    outside = tmp_path / "outside"
    outside.mkdir()
    (store / "geosnap/kartaview").symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeError, match="escapes"):
        acquisition.canonical_path(store, "kartaview", "safe", "123")


def test_cutoff_allows_only_started_acquisition_and_smoke_threshold_is_inclusive():
    cfg = {"cutoff": "2026-09-10T06:00:00+03:00"}
    now = datetime.fromisoformat("2026-09-10T06:00:01+03:00")
    assert not acquisition.may_start(cfg, False, now)
    assert acquisition.may_start(cfg, True, now)
    assert acquisition.smoke_passes({"images_requested": 50, "images_downloaded": 35,
                                     "already_existing_images_skipped": 5})
    assert not acquisition.smoke_passes({"images_requested": 50, "images_downloaded": 39,
                                        "already_existing_images_skipped": 0})


def test_failed_smoke_never_starts_remainder_or_retries_automatically(tmp_path, monkeypatch):
    out = tmp_path / "outputs"
    out.mkdir()
    monkeypatch.setattr(acquisition, "LOCAL", tmp_path / "local")
    monkeypatch.setattr(acquisition, "stage", lambda: ({}, {}, tmp_path / "store", tmp_path, out))
    receipt = {"selected_count": 100, "selected_sha256": "selected",
               "files": {"smoke.parquet": "smoke", "batch-0000.parquet": "batch"}}
    monkeypatch.setattr(acquisition, "prepare", lambda limit: receipt)
    calls = []

    def fake_download(manifest, store, destination):
        calls.append(manifest.name)
        return {"images_requested": 50, "images_downloaded": 39, "already_existing_images_skipped": 0,
                "failed_downloads": 11}

    monkeypatch.setattr(acquisition, "download_batch", fake_download)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="network smoke failed"):
            acquisition.download(100)
    assert calls == ["smoke.parquet"]
    assert not (out / "download.done.json").exists()


def test_prepared_manifest_corruption_is_rejected_before_network(tmp_path):
    cfg = {"cutoff": "2026-09-10T06:00:00+03:00"}
    path = tmp_path / "selected.parquet"
    path.write_bytes(b"original manifest")
    fingerprint = acquisition.digest(path)
    acquisition.save(tmp_path / "acquisition.started.json", {
        "setup": acquisition.SETUP, "config": cfg, "limit": 100,
        "source_sha256": acquisition.digest(acquisition.Path(acquisition.__file__)),
        "files": {"selected.parquet": fingerprint}, "selected_sha256": fingerprint})
    acquisition.verify_prepared(tmp_path, cfg, 100)
    path.write_bytes(b"corrupted manifest")
    with pytest.raises(RuntimeError, match="manifest changed"):
        acquisition.verify_prepared(tmp_path, cfg, 100)


@pytest.mark.parametrize("expired", [False, True])
def test_download_stops_at_time_budget_or_failed_batch_and_preserves_partial_result(tmp_path, monkeypatch, expired):
    out = tmp_path / "outputs"
    out.mkdir()
    monkeypatch.setattr(acquisition, "LOCAL", tmp_path / "local")
    monkeypatch.setattr(acquisition, "stage", lambda: ({}, {}, tmp_path / "store", tmp_path, out))
    receipt = {"selected_count": 550, "selected_sha256": "selected",
               "files": {"smoke.parquet": "smoke", "batch-0000.parquet": "a", "batch-0001.parquet": "b"}}
    monkeypatch.setattr(acquisition, "prepare", lambda limit: receipt)
    monkeypatch.setattr(acquisition.time, "time", lambda: 2000.)
    acquisition.save(out / "download.started.json", {"started": 0. if expired else 2000., "selected_sha256": "selected"})
    calls = []

    def fake_download(manifest, store, destination):
        calls.append(manifest.name)
        n, valid = (50, 50) if manifest.name == "smoke.parquet" else (250, 100)
        return {"images_requested": n, "images_downloaded": valid, "already_existing_images_skipped": 0,
                "failed_downloads": n - valid}

    monkeypatch.setattr(acquisition, "download_batch", fake_download)
    acquisition.download(550)
    result = json.loads((out / "download.done.json").read_text())
    assert calls == (["smoke.parquet"] if expired else ["smoke.parquet", "batch-0000.parquet"])
    assert result["skipped_remaining"] == (500 if expired else 250)
    assert result["valid_files"] + result["failed_downloads"] + result["skipped_remaining"] == 550
    acquisition.download(550)
    assert len(calls) == (1 if expired else 2)  # Completed bounded result never starts more network work.
