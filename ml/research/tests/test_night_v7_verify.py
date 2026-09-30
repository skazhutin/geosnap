from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from ml.research import night_v7_verify as verify
from ml.research.metrics import raw_metrics
from ml.research.vector_evaluation import distances


@pytest.mark.parametrize("kind", ["fractional", "negative", "out_of_range", "boolean", "duplicate"])
def test_prefix_rejects_invalid_numpy_index_semantics(kind):
    prefix = np.arange(100)[None, :]
    if kind == "fractional":
        prefix = prefix.astype(float)
        prefix[0, 3] = 3.5
    elif kind == "negative":
        prefix[0, 3] = -1
    elif kind == "out_of_range":
        prefix[0, 3] = 120
    elif kind == "boolean":
        prefix = prefix.astype(bool)
    else:
        prefix[0, 3] = 2
    with pytest.raises(RuntimeError):
        verify.validate_prefix(prefix, 1, 120)


def test_complete_ranks_reproduce_stable_ties_and_verify_tail():
    scores = np.array([[1, .5, .5, .5, 0, 0], [0, 1, 2, 3, 4, 5],
                       [5, 4, 3, 2, 1, 0], [1, 1, 1, 1, 1, 1]], dtype=np.float32)
    positive_indices = [np.array([3, 5]), np.array([], int), np.array([5]), np.array([0, 4])]
    truth = {"positive_indices": positive_indices, "latitude": np.zeros(6),
             "positive_count": np.array([2, 0, 1, 2])}
    rows = {"positive_ranks": [4, None, 6, 1], "positive_count_100m": [2, 0, 1, 2]}
    actual = verify.verify_full_ranks(rows, truth, scores)
    np.testing.assert_array_equal(actual, [4, np.inf, 6, 1])
    for changed in ([3, None, 6, 1], [4, None, None, 1], [4, None, 6.0, 1]):
        with pytest.raises(RuntimeError):
            verify.verify_full_ranks(rows | {"positive_ranks": changed}, truth, scores)


def test_complete_rank_beyond_top100_cannot_silently_change():
    truth = {"positive_indices": [np.array([105])], "latitude": np.zeros(120), "positive_count": np.array([1])}
    scores = -np.arange(120, dtype=np.float32)[None, :]
    verify.verify_full_ranks({"positive_ranks": [106]}, truth, scores)
    with pytest.raises(RuntimeError, match="full-gallery"):
        verify.verify_full_ranks({"positive_ranks": [107]}, truth, scores)


def example_result():
    reference = pd.DataFrame({"id": [f"r{i}" for i in range(120)],
                              "lat": 60 + np.arange(120) * .01, "lon": 37.})
    reference.loc[[0, 101, 5], "lat"] = [55., 56., 57.]
    queries = pd.DataFrame({"id": ["q0", "q1", "q2", "q3"], "lat": [55., 56., 57., 58.], "lon": 37.})
    truth = verify.gallery_truth(queries, reference)
    prefix = np.broadcast_to(np.arange(100), (4, 100)).copy()
    errors = [float(distances(q.lat, q.lon, truth["latitude"][:1], truth["longitude"][:1])[0])
              for q in queries.itertuples()]
    ranks = np.array([1, np.inf, 6, np.inf])
    rows = {"query_ids": queries.id.tolist(), "errors_m": errors,
            "top100_gallery_rows": prefix.tolist(), "positive_ranks_through100": [1, None, 6, None]}
    result = {"raw": raw_metrics(errors), "recall_at": {str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
              "diagnosis": {"no_coverage": 1, "retrieval_miss_top100": 1,
                            "wrong_top1_positive_in_top100": 1, "correct_top1": 1}}
    return result, rows, queries, reference, truth


def test_reconciliation_accepts_both_rank_schemas_and_catches_same_recall_rank_error():
    args = example_result()
    assert verify.reconcile_result(*args)["raw_verified"]
    result, rows, queries, reference, truth = args
    full = deepcopy(rows)
    del full["positive_ranks_through100"]
    full["positive_ranks"] = [1, 102, 6, None]
    assert verify.reconcile_result(result, full, queries, reference, truth)["positive_rank_fields_verified"] == ["positive_ranks"]
    # Rank6 and rank7 have identical reported R@1/5/10/20/50/100, but only one
    # agrees with the saved candidate order.
    rows["positive_ranks_through100"][2] = 7
    with pytest.raises(RuntimeError, match="actual top100"):
        verify.reconcile_result(result, rows, queries, reference, truth)


def test_diagnosis_is_verified_by_component_not_only_total():
    result, rows, queries, reference, truth = example_result()
    result["diagnosis"]["no_coverage"] = 0
    result["diagnosis"]["retrieval_miss_top100"] = 2
    assert sum(result["diagnosis"].values()) == len(queries)
    with pytest.raises(RuntimeError, match="decomposition"):
        verify.reconcile_result(result, rows, queries, reference, truth)


def test_current_development_identity_checks_normalize_msls_provider_sequences():
    queries = pd.DataFrame([dict(id="q", source="mapillary", source_image_id="query", sequence_id="same", file_sha256="qsha")])
    gallery = pd.DataFrame([dict(id="r", source="msls", source_image_id="reference", sequence_id="msls:other", file_sha256="rsha")])
    assert not any(verify.development_identity_checks(queries, gallery).values())
    gallery.loc[0, "sequence_id"] = "msls:same"
    with pytest.raises(RuntimeError, match="current development"):
        verify.development_identity_checks(queries, gallery)


def test_current_identity_checks_catch_cross_source_prefixed_image_id():
    queries = pd.DataFrame([dict(id="q", source="mapillary", source_image_id="123", sequence_id="qseq", file_sha256="qsha")])
    gallery = pd.DataFrame([dict(id="r", source="msls", source_image_id="msls:123", sequence_id="rseq", file_sha256="rsha")])
    with pytest.raises(RuntimeError, match="current development"):
        verify.development_identity_checks(queries, gallery)


@pytest.mark.parametrize("raw_published", [False, True])
def test_research_freeze_waits_for_entire_registered_expansion_queue(tmp_path, monkeypatch, raw_published):
    out = tmp_path / "night"
    stage = out / "live_expansion2"
    stage.mkdir(parents=True)
    (stage / "live_expansion.started.json").write_text("{}")
    local = tmp_path / "local"
    local.mkdir()
    if raw_published:
        (stage / "published.json").write_text("{}")
        (local / "live_expansion2_queue_status.json").write_text('{"phase":"running"}')
    monkeypatch.setattr(verify, "LOCAL", local)
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))
    with pytest.raises(RuntimeError, match="second.*(tranche|queue)"):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()


def test_research_freeze_requires_actual_production_k30_audit(tmp_path, monkeypatch):
    out = tmp_path / "night"
    stage = out / "production_baseline"
    stage.mkdir(parents=True)
    for name in ("audit.started.json", "done.json", "verification.json"):
        (stage / name).write_text("{}")
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))
    with pytest.raises(RuntimeError, match="actual K30"):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()


def test_research_freeze_cannot_select_pre_quarantine_candidate_during_repair(tmp_path, monkeypatch):
    out = tmp_path / "night"
    (out / "live_expansion2_quarantine").mkdir(parents=True)
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))
    with pytest.raises(RuntimeError, match="common-quarantine"):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()


def test_research_freeze_waits_for_third_tranche_before_selecting(tmp_path, monkeypatch):
    out = tmp_path / "night"
    stage = out / "reference_tranche3"
    stage.mkdir(parents=True)
    (stage / "queue.started.json").write_text("{}")
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))
    with pytest.raises(RuntimeError, match="third registered"):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()


def test_research_freeze_waits_for_registered_third_licensed_transfer(tmp_path, monkeypatch):
    out = tmp_path / "night"
    stage = out / "licensed_third"
    stage.mkdir(parents=True)
    (stage / "intent.json").write_text("{}")
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))
    with pytest.raises(RuntimeError, match="third source-compatible"):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()


@pytest.mark.parametrize("failed", [False, True])
def test_research_freeze_waits_for_third_queue_validation_after_child_done(tmp_path, monkeypatch, failed):
    out = tmp_path / "night"
    stage = out / "reference_tranche3"
    queue = stage / "evaluation_queue"
    queue.mkdir(parents=True)
    (stage / "queue.started.json").write_text("{}")
    (stage / "evaluation.done.json").write_text("{}")
    (queue / "started.json").write_text("{}")
    if failed:
        (queue / "failure.json").write_text('{"error":"artifact hash mismatch"}')
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))
    with pytest.raises(RuntimeError, match="third.*queue"):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()


@pytest.mark.parametrize("audit_complete", [False, True])
def test_third_queue_cutoff_closure_requires_its_started_audit_to_finish(tmp_path, monkeypatch, audit_complete):
    out = tmp_path / "night"
    stage = out / "reference_tranche3"
    queue = stage / "evaluation_queue"
    queue.mkdir(parents=True)
    (stage / "queue.started.json").write_text("{}")
    (queue / "started.json").write_text("{}")
    (queue / "closed.json").write_text('{"reason":"cutoff before evaluator dispatch"}')
    if audit_complete:
        (stage / "audit.done.json").write_text("{}")
    local = tmp_path / "local"
    local.mkdir()
    monkeypatch.setattr(verify, "LOCAL", local)
    monkeypatch.setattr(verify, "inputs", lambda: (None, None, None, out, None, None, None, None))

    def after_guards():
        raise RuntimeError("passed completion guards")

    monkeypatch.setattr(verify, "report", after_guards)
    message = "passed completion guards" if audit_complete else "third registered"
    with pytest.raises(RuntimeError, match=message):
        verify.run()
    assert not (out / "candidate_frozen.json").exists()
