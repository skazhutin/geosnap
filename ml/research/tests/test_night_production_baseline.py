import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from ml.research import night_production_baseline as audit


def frozen_config():
    return {"dataset": {"gallery_manifest": "gallery.parquet", "gallery_sha256": "g"},
        "retriever": {"name": "sage-vitb", "checkpoint_sha256": "c", "preprocessing": "p", "descriptor_dimension": 8448, "normalized": True},
        "index": {"directory": "index", "index_faiss_sha256": "i", "id_mapping_sha256": "m", "reference_metadata_sha256": "r", "gallery_size": 20487, "artifact_generation": "generation"},
        "retrieval": {"index_type": "faiss.IndexFlatIP", "similarity": "cosine_via_inner_product", "top_k": 30, "query_aggregation": "single", "reranker": None},
        "localization": {"aggregation": "density_aware_mode_vote", "cluster_radius_m": 100., "max_cluster_diameter_m": 150., "estimator": "weighted_medoid", "score_temperature": .08, "rank_decay_exponent": .5}}


def metadata(identity, latitude=55.7):
    return dict(id=identity, lat=latitude, lon=37.6, source="mapillary", source_image_id=identity,
        sequence_id=f"sequence_{identity}", file_sha256=f"sha_{identity}", heading=90., captured_at="2020-01-01T00:00:00Z",
        local_gallery_density_25m=1, local_gallery_density_50m=2, local_gallery_density_100m=3)


def test_config_projection_excludes_unrequested_sections_and_matches_runtime_policy(tmp_path):
    config = frozen_config()
    config["unrequested_private_section"] = {"never_return_this": "private"}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    selected = audit.safe_config(path)
    assert set(selected) == {"dataset", "retriever", "index", "retrieval", "localization"}
    assert "private" not in json.dumps(selected)
    from ml.localization.product_runtime import ProductRuntimePolicy

    runtime = ProductRuntimePolicy(aggregation=audit.AggregationStrategy.DENSITY_AWARE_MODE_VOTE, confidence_threshold=.9)
    assert audit.policy(selected["localization"]) == runtime.aggregation_config
    config["retrieval"]["top_k"] = 100
    path.write_text(json.dumps(config))
    with pytest.raises(RuntimeError, match="unsupported"):
        audit.safe_config(path)


def test_index_mapping_uses_exact_index_order_and_rejects_metadata_coordinate_changes(tmp_path):
    rows = [metadata("a"), metadata("b", 55.71)]
    gallery = pd.DataFrame(rows)[list(audit.REFERENCE_FIELDS)]
    mapping = [dict(row=0, reference_id="b", artifact_generation="generation"), dict(row=1, reference_id="a", artifact_generation="generation")]
    (tmp_path / "id_mapping.json").write_text(json.dumps(mapping))

    def write_metadata():
        (tmp_path / "reference_metadata.jsonl").write_text("\n".join(json.dumps({"reference_id": row["id"], "metadata": row, "artifact_generation": "generation"}) for row in rows))

    write_metadata()
    reordered, compact = audit.references(tmp_path, gallery, frozen_config())
    assert reordered.id.tolist() == ["b", "a"]
    assert [row["id"] for row in compact] == ["b", "a"]
    rows[1]["lat"] += .001
    write_metadata()
    with pytest.raises(RuntimeError, match="coordinates/identity"):
        audit.references(tmp_path, gallery, frozen_config())


def test_geographic_glue_uses_only_top30_frozen_candidates_without_query_truth(monkeypatch):
    rows = [metadata(f"r{i}", 55.7 + .00001 * i) for i in range(40)]
    order = np.arange(39, -1, -1)
    scores = np.linspace(.8, .1, 40)
    calls = []

    def aggregate(candidates, **kwargs):
        calls.append((candidates, kwargs))
        return SimpleNamespace(lat=candidates[0].lat, lon=candidates[0].lon)

    monkeypatch.setattr(audit, "aggregate_geographic_modes", aggregate)
    policy = audit.policy(frozen_config()["localization"])
    result = audit.geographic_prediction(order, scores, rows, policy)
    candidates, kwargs = calls[0]
    assert len(candidates) == 30 and [c.rank for c in candidates] == list(range(1, 31))
    assert [c.reference_id for c in candidates] == [f"r{i}" for i in range(39, 9, -1)]
    assert [c.retrieval_score for c in candidates] == scores[:30].tolist()
    assert kwargs == {"config": policy}
    assert result == [rows[39]["lat"], rows[39]["lon"]]
    with pytest.raises(RuntimeError, match="distinct"):
        audit.geographic_prediction(np.zeros(40, int), scores, rows, policy)


def test_completed_replay_resume_checks_chunk_hashes_and_recovers_missing_verification(tmp_path, monkeypatch):
    out = tmp_path / "production_baseline"
    (out / "chunks").mkdir(parents=True)
    contract = {"query_gallery_identity_checks": {"stable_image_ids": 0}}
    for name in ("report.json", "rows.json"):
        audit.save(out / name, {"committed": True})
    chunk = out / "chunks/queries-0000.npz"
    chunk.write_bytes(b"committed evidence")
    audit.save(chunk.with_suffix(".json"), {"sha256": audit.digest(chunk)})
    audit.save(out / "done.json", {"contract": contract, "report_sha256": audit.digest(out / "report.json"),
        "rows_sha256": audit.digest(out / "rows.json"), "chunks": {chunk.name: {
            "data_sha256": audit.digest(chunk), "receipt_sha256": audit.digest(chunk.with_suffix(".json"))}}})
    np.save(tmp_path / "queries.npy", np.zeros((1, 1), np.float32))
    monkeypatch.setattr(audit, "LOCAL", tmp_path)
    monkeypatch.setattr(audit, "paths", lambda: (None, None, None, None, tmp_path))
    monkeypatch.setattr(audit, "load", lambda _: (None, np.load(tmp_path / "queries.npy", mmap_mode="r"), *([None] * 6), contract))
    monkeypatch.setattr(audit, "compute", lambda *_: pytest.fail("completed exact search repeated"))
    monkeypatch.setattr(audit, "status", lambda *a, **kw: None)
    monkeypatch.setattr(audit.os, "getpriority", lambda *args: 10)
    audit.run()
    assert json.loads((out / "verification.json").read_text())["done_sha256"] == audit.digest(out / "done.json")
    chunk.write_bytes(b"changed evidence")
    with pytest.raises(RuntimeError, match="evidence changed"):
        audit.run()
