import json

import pandas as pd
import pytest

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.research import night_live_expansion as expansion


def row(identity, cell):
    return dict(id=identity, cell=cell, sequence_key=identity, heading_bin=0,
                month="2020-01", selection_hash=expansion.acquisition.stable(identity))


def test_two_smart_rounds_continue_density_and_never_modify_original_cap(monkeypatch):
    monkeypatch.setitem(expansion.SETUP, "selection_round_limit", 2)
    baseline = pd.DataFrame([row("base", "dense")])
    pool = pd.DataFrame([row("a", "empty"), row("b", "empty"), row("c", "dense"), row("d", "dense")])
    original = expansion.acquisition.smart_select
    calls = []

    def select(frame, gallery, limit):
        calls.append((len(frame), len(gallery), limit))
        return original(frame, gallery, limit)

    monkeypatch.setattr(expansion.acquisition, "smart_select", select)
    selected = expansion.select_tranche(pool, baseline, 4)
    assert calls == [(4, 1, 2), (2, 3, 2)]
    assert selected.id.is_unique and set(selected.id) == set(pool.id)
    assert len(baseline) == 1 and expansion.acquisition.SETUP["maximum_references"] == 3000


def test_known_physical_id_normalizes_cross_source_msls_identity():
    frame = pd.DataFrame([dict(source="msls", source_image_id="msls:123"), dict(source="mapillary", source_image_id="123")])
    assert expansion.identities(frame) == {("mapillary", "123")}


def test_reused_download_is_bounded_and_restores_original_helper_globals(tmp_path, monkeypatch):
    monkeypatch.setattr(expansion, "stage", lambda: ({}, {}, tmp_path, tmp_path, tmp_path, tmp_path))
    receipt = {"selected_count": 6000, "selected_sha256": "selected",
               "files": {"smoke.parquet": "s", "batch-0000.parquet": "b", "batch-0001.parquet": "c"}}
    monkeypatch.setattr(expansion, "prepare", lambda limit: receipt)
    monkeypatch.setattr(expansion, "state", lambda *args, **kwargs: None)
    calls = []

    def download(path, store, out):
        calls.append(path.name)
        n, valid = (50, 50) if path.name == "smoke.parquet" else (250, 100)
        return {"images_requested": n, "images_downloaded": valid, "already_existing_images_skipped": 0,
                "failed_downloads": n - valid}

    monkeypatch.setattr(expansion.acquisition, "download_batch", download)
    originals = expansion.acquisition.stage, expansion.acquisition.prepare, expansion.acquisition.status
    expansion.download(6000)
    assert (expansion.acquisition.stage, expansion.acquisition.prepare, expansion.acquisition.status) == originals
    assert calls == ["smoke.parquet", "batch-0000.parquet"]
    done = json.loads((tmp_path / "download.done.json").read_text())
    assert done["reason"] == "committed_batch_valid_fraction_below50percent" and done["skipped_remaining"] == 5700
    expansion.download(6000)
    assert len(calls) == 2


def test_identity_guard_cache_reproduces_phash_gate_without_reopening_queries(tmp_path, monkeypatch):
    index = _PerceptualHashIndex(4)
    index.add(0, int("aaaaaaaaaaaaaaaa", 16))
    index.add(1, int("aaaaaaaaaaaaaaaa", 16))
    guard = ({"sha"}, {"pixels"}, {("mapillary", "1")}, {"mapillary::protected"}, index)
    monkeypatch.setattr(expansion.gallery_scale, "query_guard", lambda cfg: guard)
    contract = {"contract": {"files": {str(expansion.LEDGER): "ledger"}, "source_sha256": {"gallery_scale.py": "helper"}}}
    cfg = {"query_sha256": "query"}
    expansion.cached_query_guard(cfg, tmp_path, contract)
    monkeypatch.setattr(expansion.gallery_scale, "query_guard", lambda *_: pytest.fail("queries re-opened"))
    cached = expansion.cached_query_guard(cfg, tmp_path, contract)
    assert cached[:4] == guard[:4]
    assert bool(cached[4].matches(int("aaaaaaaaaaaaaaab", 16))) == bool(index.matches(int("aaaaaaaaaaaaaaab", 16)))
    (tmp_path / "query_identity_guard.json").write_text("{}")
    with pytest.raises(RuntimeError, match="guard changed"):
        expansion.cached_query_guard(cfg, tmp_path, contract)


def test_invalid_image_still_has_sha_for_checkpoint_resume(tmp_path):
    path = tmp_path / "invalid.jpg"
    path.write_bytes(b"not an image")
    fingerprint = expansion.fingerprint_reference(path)
    assert not fingerprint["decode_ok"] and fingerprint["file_sha256"] == expansion.digest(path)


def test_audited_gallery_cannot_reorder_or_modify_original_expanded_prefix(tmp_path, monkeypatch):
    monkeypatch.setitem(expansion.SETUP, "baseline_count", 2)
    first, out = tmp_path / "gallery_addition", tmp_path / "live_expansion2"
    first.mkdir()
    out.mkdir()
    base = pd.DataFrame(dict(id=["a", "b"], lat=[55., 56.], lon=[37., 38.], file_sha256=["a", "b"]))
    added = pd.DataFrame(dict(id=["c"], lat=[55.1], lon=[37.1], file_sha256=["c"]))
    base.to_parquet(first / "gallery.parquet", index=False)
    added.to_parquet(out / "addition.parquet", index=False)
    pd.concat([base, added], ignore_index=True).to_parquet(out / "gallery.parquet", index=False)
    registered = {"contract": {"files": {str(first / "gallery.parquet"): expansion.digest(first / "gallery.parquet")}}}
    receipt = {"addition_sha256": expansion.digest(out / "addition.parquet"), "gallery_sha256": expansion.digest(out / "gallery.parquet")}
    expansion.save(out / "audit.done.json", receipt)
    expansion.verify_audited(out, tmp_path, tmp_path, registered)
    pd.concat([base.iloc[::-1], added], ignore_index=True).to_parquet(out / "gallery.parquet", index=False)
    expansion.save(out / "audit.done.json", receipt | {"gallery_sha256": expansion.digest(out / "gallery.parquet")})
    with pytest.raises(AssertionError):
        expansion.verify_audited(out, tmp_path, tmp_path, registered)
