from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from ml.index import FaissExactIndex as CompatibilityIndex
from ml.indexing import FaissExactIndex, FaissIndexError, FaissIndexWorker
from ml.indexing.faiss_index import FaissExactIndexBuilder

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("faiss") is None,
    reason="faiss-cpu is not installed",
)


def _build() -> FaissExactIndex:
    return FaissExactIndex.build(
        np.asarray([[10.0, 0.0, 0.0], [0.0, 4.0, 0.0], [0.0, 0.0, 2.0]], dtype=np.float32),
        ["red", "green", "blue"],
        reference_metadata=[
            {"lat": 55.75, "lon": 37.61, "source": "fixture"},
            {"lat": 55.76, "lon": 37.62, "source": "fixture"},
            {"lat": 55.77, "lon": 37.63, "source": "fixture"},
        ],
        retriever_metadata={"model_name": "test-model", "checkpoint": "fixture"},
        index_id="moscow-test",
        city_id="moscow",
    )


def test_exact_cosine_search_and_compatibility_import() -> None:
    index = _build()
    assert isinstance(index, CompatibilityIndex)
    matches = index.search_one(np.asarray([0.9, 0.1, 0.0], dtype=np.float32), k=99)
    assert [match.reference_id for match in matches] == ["red", "green", "blue"]
    assert matches[0].rank == 1
    assert matches[0].city_id == "moscow"
    assert matches[0].metadata["index_id"] == "moscow-test"
    assert index.build_metadata["faiss_index_type"] == "IndexFlatIP"
    assert index.build_metadata["normalization"] == "L2"


def test_full_gallery_rank_diagnostics_do_not_depend_on_returned_top_k() -> None:
    diagnostics = _build().diagnose_one(
        np.asarray([0.9, 0.1, 0.0], dtype=np.float32),
        true_lat=55.76,
        true_lon=37.62,
    )

    within_100 = diagnostics["by_positive_distance_m"]["100"]
    assert diagnostics["rank_scope"] == "complete_exact_gallery"
    assert within_100["positive_rank"] == 2
    assert within_100["positive_reference"]["reference_id"] == "green"
    assert within_100["positive_minus_best_incorrect_margin"] < 0


def test_save_reload_produces_equivalent_neighbors_and_scores(tmp_path: Path) -> None:
    index = _build()
    queries = np.asarray([[1.0, 0.2, 0.0], [0.0, 0.1, 1.0]], dtype=np.float32)
    before = index.search(queries, k=3)
    index.save(tmp_path)
    loaded = FaissExactIndex.load(tmp_path)
    after = loaded.search(queries, k=3)
    assert [[item.reference_id for item in batch] for batch in after] == [
        [item.reference_id for item in batch] for batch in before
    ]
    np.testing.assert_allclose(
        [[item.score for item in batch] for batch in after],
        [[item.score for item in batch] for batch in before],
        atol=1e-7,
    )
    assert loaded.build_metadata == index.build_metadata
    generation = loaded.build_metadata["artifact_generation"]
    assert len(generation) == 32
    assert set(loaded.build_metadata["artifact_sha256"]) == {
        "index.faiss",
        "id_mapping.json",
        "reference_metadata.jsonl",
    }
    mapping = json.loads((tmp_path / "id_mapping.json").read_text())
    assert {row["artifact_generation"] for row in mapping} == {generation}
    reference_rows = [json.loads(line) for line in (tmp_path / "reference_metadata.jsonl").read_text().splitlines()]
    assert {row["artifact_generation"] for row in reference_rows} == {generation}


def test_index_validation_rejects_bad_inputs_and_corrupt_mapping(tmp_path: Path) -> None:
    with pytest.raises(FaissIndexError, match="zero references"):
        FaissExactIndex.build(np.empty((0, 3), dtype=np.float32), [])
    with pytest.raises(FaissIndexError, match="unique"):
        FaissExactIndex.build(np.eye(2, dtype=np.float32), ["same", "same"])
    with pytest.raises(FaissIndexError, match="zero-norm"):
        FaissExactIndex.build(np.zeros((1, 3), dtype=np.float32), ["zero"])
    index = _build()
    index.save(tmp_path)
    mapping_path = tmp_path / "id_mapping.json"
    mapping = json.loads(mapping_path.read_text())
    mapping[1]["row"] = 99
    mapping_path.write_text(json.dumps(mapping))
    with pytest.raises(FaissIndexError, match="SHA-256 mismatch.*id_mapping"):
        FaissExactIndex.load(tmp_path)


@pytest.mark.parametrize(
    ("metadata", "error"),
    [
        ({"index_id": "wrong-index"}, "index_id"),
        ({"city_id": "saint-petersburg"}, "city_id"),
    ],
)
def test_build_rejects_reference_scope_mismatch(metadata: dict[str, str], error: str) -> None:
    with pytest.raises(FaissIndexError, match=error):
        FaissExactIndex.build(
            np.ones((1, 3), dtype=np.float32),
            ["reference"],
            reference_metadata=[metadata],
            index_id="moscow-index",
            city_id="moscow",
        )


def test_load_rejects_same_shape_faiss_file_from_another_generation(tmp_path: Path) -> None:
    first_directory = tmp_path / "first"
    second_directory = tmp_path / "second"
    _build().save(first_directory)
    FaissExactIndex.build(
        np.asarray([[0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [1.0, 0.0, 0.0]], dtype=np.float32),
        ["other-red", "other-green", "other-blue"],
        index_id="other-index",
        city_id="moscow",
    ).save(second_directory)
    first_index = first_directory / "index.faiss"
    second_index = second_directory / "index.faiss"
    assert first_index.stat().st_size == second_index.stat().st_size
    shutil.copyfile(second_index, first_index)

    with pytest.raises(FaissIndexError, match="SHA-256 mismatch.*index.faiss"):
        FaissExactIndex.load(first_directory)


def test_load_validates_sidecar_generation_even_when_hash_is_updated(tmp_path: Path) -> None:
    first_directory = tmp_path / "first"
    second_directory = tmp_path / "second"
    _build().save(first_directory)
    _build().save(second_directory)
    first_mapping_path = first_directory / "id_mapping.json"
    shutil.copyfile(second_directory / "id_mapping.json", first_mapping_path)
    metadata_path = first_directory / "index_metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["artifact_sha256"]["id_mapping.json"] = hashlib.sha256(first_mapping_path.read_bytes()).hexdigest()
    metadata_path.write_text(json.dumps(metadata))

    with pytest.raises(FaissIndexError, match="artifact generation"):
        FaissExactIndex.load(first_directory)


def test_query_validation() -> None:
    index = _build()
    with pytest.raises(ValueError, match="k"):
        index.search_one(np.ones(3, dtype=np.float32), k=0)
    with pytest.raises(FaissIndexError, match="query shape"):
        index.search_one(np.ones(2, dtype=np.float32), k=1)
    with pytest.raises(FaissIndexError, match="zero-norm"):
        index.search_one(np.zeros(3, dtype=np.float32), k=1)


def test_incremental_builder_matches_one_shot_exact_search() -> None:
    descriptors = np.asarray(
        [[10.0, 0.0, 0.0], [0.0, 4.0, 0.0], [0.0, 0.0, 2.0], [2.0, 2.0, 0.0]],
        dtype=np.float32,
    )
    ids = ["red", "green", "blue", "yellow"]
    metadata = [
        {"lat": 55.75, "lon": 37.61},
        {"lat": 55.76, "lon": 37.62},
        {"lat": 55.77, "lon": 37.63},
        {"lat": 55.78, "lon": 37.64},
    ]
    one_shot = FaissExactIndex.build(
        descriptors,
        ids,
        reference_metadata=metadata,
        retriever_metadata={"model_name": "test-model"},
        index_id="moscow-test",
        city_id="moscow",
    )
    builder = FaissExactIndexBuilder(
        descriptor_dim=3,
        expected_size=4,
        retriever_metadata={"model_name": "test-model"},
        index_id="moscow-test",
        city_id="moscow",
    )
    builder.add_batch(descriptors[:2], ids[:2], reference_metadata=metadata[:2])
    builder.add_batch(descriptors[2:], ids[2:], reference_metadata=metadata[2:])
    streamed = builder.finish()

    query = np.asarray([0.9, 0.1, 0.0], dtype=np.float32)
    assert [match.reference_id for match in streamed.search_one(query, k=4)] == [
        match.reference_id for match in one_shot.search_one(query, k=4)
    ]
    np.testing.assert_allclose(
        [match.score for match in streamed.search_one(query, k=4)],
        [match.score for match in one_shot.search_one(query, k=4)],
        atol=1e-7,
    )
    assert streamed.build_metadata["gallery_size"] == 4
    assert all(match.metadata["index_id"] == "moscow-test" for match in streamed.search_one(query, k=4))


def test_incremental_builder_rejects_duplicate_dimension_and_incomplete_build() -> None:
    builder = FaissExactIndexBuilder(descriptor_dim=3, expected_size=2)
    builder.add_batch(np.asarray([[1.0, 0.0, 0.0]], dtype=np.float32), ["one"])
    with pytest.raises(FaissIndexError, match="unique across streamed"):
        builder.add_batch(np.asarray([[0.0, 1.0, 0.0]], dtype=np.float32), ["one"])
    with pytest.raises(FaissIndexError, match="dimension"):
        builder.add_batch(np.asarray([[0.0, 1.0]], dtype=np.float32), ["two"])
    with pytest.raises(FaissIndexError, match="does not match expected_size"):
        builder.finish()


def test_process_isolated_worker_keeps_index_loaded_and_returns_typed_results(
    tmp_path: Path,
) -> None:
    _build().save(tmp_path)
    worker = FaissIndexWorker(tmp_path, timeout_seconds=10).start()
    try:
        first = worker.search_one(np.asarray([1.0, 0.0, 0.0], dtype=np.float32), k=2)
        second = worker.search_one(np.asarray([0.0, 0.0, 1.0], dtype=np.float32), k=2)
        assert first[0].reference_id == "red"
        assert second[0].reference_id == "blue"
        assert worker.is_ready
    finally:
        worker.close()
    assert worker.is_ready is False


class _FaultInjectingConnection:
    def __init__(self, connection: Any, failure: str) -> None:
        self.connection = connection
        self.failure = failure

    def send(self, payload: Any) -> None:
        self.connection.send(payload)

    def poll(self, timeout: float) -> bool:
        if self.failure == "timeout":
            return False
        return bool(self.connection.poll(timeout))

    def recv(self) -> Any:
        response = self.connection.recv()
        if self.failure == "protocol":
            return dict(response) | {"request_id": "stale-request"}
        return response

    def close(self) -> None:
        self.connection.close()


@pytest.mark.parametrize(
    ("failure", "error"),
    [("timeout", "timed out"), ("protocol", "protocol mismatch")],
)
def test_worker_restarts_after_transport_protocol_failure_and_next_search_recovers(
    tmp_path: Path,
    failure: str,
    error: str,
) -> None:
    _build().save(tmp_path)
    worker = FaissIndexWorker(tmp_path, timeout_seconds=10).start()
    assert worker._parent_connection is not None
    worker._parent_connection = _FaultInjectingConnection(worker._parent_connection, failure)
    try:
        with pytest.raises(FaissIndexError, match=error):
            worker.search_one(np.asarray([1.0, 0.0, 0.0], dtype=np.float32), k=1)
        assert worker.is_ready
        recovered = worker.search_one(np.asarray([0.0, 0.0, 1.0], dtype=np.float32), k=1)
        assert recovered[0].reference_id == "blue"
    finally:
        worker.close()
