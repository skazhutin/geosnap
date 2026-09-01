from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from ml.retrieval.embedding_job import (
    EmbeddingJob,
    EmbeddingJobError,
    ReferenceImage,
    load_embedding_artifacts,
)
from ml.retrieval.testing import DeterministicFixtureRetriever


def _image(path: Path, color: tuple[int, int, int]) -> None:
    Image.new("RGB", (48, 32), color).save(path)


def test_embedding_job_records_mapping_metadata_and_failures(tmp_path: Path) -> None:
    first = tmp_path / "first.jpg"
    third = tmp_path / "third.jpg"
    _image(first, (240, 10, 10))
    _image(third, (10, 10, 240))
    records = [
        ReferenceImage("stable-a", first, {"lat": 55.75, "lon": 37.61}),
        ReferenceImage("stable-b", tmp_path / "missing.jpg", {"lat": 55.76, "lon": 37.62}),
        ReferenceImage("stable-c", third, {"lat": 55.77, "lon": 37.63}),
    ]
    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    artifacts = EmbeddingJob(retriever, tmp_path / "artifacts", batch_size=2).run(records)
    matrix, ids, metadata, build = load_embedding_artifacts(artifacts.root)
    assert ids == ["stable-a", "stable-c"]
    assert matrix.shape == (2, retriever.descriptor_dim)
    np.testing.assert_allclose(np.linalg.norm(matrix, axis=1), 1.0, atol=1e-6)
    assert [row["reference_id"] for row in metadata] == ids
    assert build["descriptor_count"] == 2
    assert build["failure_count"] == 1
    assert build["status"] == "complete"
    assert build["schema_version"] == 2
    assert set(build["artifact_sha256"]) == {
        "descriptors.npy",
        "id_mapping.json",
        "reference_metadata.jsonl",
        "failures.jsonl",
    }
    assert all(row["artifact_generation"] == build["artifact_generation"] for row in metadata)
    assert not (artifacts.root / ".embedding-checkpoints").exists()
    failures = [json.loads(line) for line in artifacts.failures_path.read_text().splitlines()]
    assert failures[0]["reference_id"] == "stable-b"
    assert "missing.jpg" in failures[0]["image_path"]


def test_embedding_job_resumes_after_checkpointed_interruption(tmp_path: Path) -> None:
    paths: list[Path] = []
    for index, color in enumerate(((240, 0, 0), (0, 240, 0), (0, 0, 240))):
        path = tmp_path / f"{index}.png"
        _image(path, color)
        paths.append(path)
    records = [ReferenceImage(f"id-{index}", path) for index, path in enumerate(paths)]

    class StopAfterFirstCheckpoint(Exception):
        pass

    callbacks = 0

    def stop(progress):  # type: ignore[no-untyped-def]
        nonlocal callbacks
        callbacks += 1
        assert progress.processed_inputs == 1
        raise StopAfterFirstCheckpoint

    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    output = tmp_path / "resume"
    with pytest.raises(StopAfterFirstCheckpoint):
        EmbeddingJob(retriever, output, batch_size=1, progress_callback=stop).run(records)
    state = json.loads((output / ".embedding-checkpoints/state.json").read_text())
    assert state["next_input_index"] == 1
    assert len(state["chunks"]) == 1

    artifacts = EmbeddingJob(retriever, output, batch_size=1).run(records)
    matrix, ids, _, _ = load_embedding_artifacts(artifacts.root)
    assert ids == ["id-0", "id-1", "id-2"]
    assert matrix.shape[0] == 3
    assert callbacks == 1


def test_embedding_resume_rejects_changed_ordered_manifest(tmp_path: Path) -> None:
    path = tmp_path / "one.jpg"
    _image(path, (10, 20, 30))
    output = tmp_path / "artifacts"
    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    EmbeddingJob(retriever, output).run([ReferenceImage("one", path)])
    with pytest.raises(EmbeddingJobError, match="different ordered manifest"):
        EmbeddingJob(retriever, output).run([ReferenceImage("changed", path)])


def test_embedding_job_rejects_duplicates_empty_and_all_failures(tmp_path: Path) -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    with pytest.raises(EmbeddingJobError, match="empty"):
        EmbeddingJob(retriever, tmp_path / "empty").run([])
    missing = tmp_path / "missing.jpg"
    with pytest.raises(EmbeddingJobError, match="duplicate"):
        EmbeddingJob(retriever, tmp_path / "duplicate").run(
            [ReferenceImage("same", missing), ReferenceImage("same", missing)]
        )
    with pytest.raises(EmbeddingJobError, match="zero descriptors"):
        EmbeddingJob(retriever, tmp_path / "failed").run([ReferenceImage("missing", missing)])
    failed_rows = (tmp_path / "failed/failures.jsonl").read_text().splitlines()
    assert json.loads(failed_rows[0])["reference_id"] == "missing"
    failed_metadata = json.loads((tmp_path / "failed/build_metadata.json").read_text())
    assert failed_metadata["status"] == "failed_no_descriptors"


def test_failed_no_resume_rebuild_invalidates_previous_committed_generation(tmp_path: Path) -> None:
    image = tmp_path / "one.jpg"
    _image(image, (20, 40, 60))
    output = tmp_path / "artifacts"
    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    EmbeddingJob(retriever, output).run([ReferenceImage("old-id", image)])
    assert load_embedding_artifacts(output)[1] == ["old-id"]

    with pytest.raises(EmbeddingJobError, match="zero descriptors"):
        EmbeddingJob(retriever, output).run(
            [ReferenceImage("new-id", tmp_path / "missing.jpg")],
            resume=False,
        )

    with pytest.raises(EmbeddingJobError, match="not complete"):
        load_embedding_artifacts(output)


def test_resume_signature_hashes_image_bytes_not_only_stat_metadata(tmp_path: Path) -> None:
    image = tmp_path / "same-size.ppm"
    Image.new("RGB", (16, 16), (255, 0, 0)).save(image, format="PPM")
    original_stat = image.stat()
    output = tmp_path / "artifacts"
    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    records = [ReferenceImage("one", image)]
    EmbeddingJob(retriever, output).run(records)

    Image.new("RGB", (16, 16), (0, 0, 255)).save(image, format="PPM")
    assert image.stat().st_size == original_stat.st_size
    os.utime(image, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    with pytest.raises(EmbeddingJobError, match="different ordered manifest"):
        EmbeddingJob(retriever, output).run(records)


def test_finalization_streams_chunks_without_full_matrix_concatenate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = []
    for index in range(4):
        path = tmp_path / f"{index}.jpg"
        _image(path, (20 * index, 30, 80))
        paths.append(path)

    def forbidden_concatenate(*_args, **_kwargs):
        raise AssertionError("full descriptor concatenation is forbidden")

    monkeypatch.setattr(np, "concatenate", forbidden_concatenate)
    artifacts = EmbeddingJob(
        DeterministicFixtureRetriever(allow_test_only=True),
        tmp_path / "streamed",
        batch_size=1,
    ).run([ReferenceImage(f"id-{index}", path) for index, path in enumerate(paths)])
    assert load_embedding_artifacts(artifacts.root)[0].shape[0] == 4


def test_incremental_job_reuses_matching_committed_descriptors(tmp_path: Path) -> None:
    paths = [tmp_path / f"{index}.jpg" for index in range(3)]
    for index, path in enumerate(paths):
        _image(path, (20 * index, 40, 80))
    base_records = [
        ReferenceImage(f"id-{index}", path, {"source": "fixture", "source_image_id": str(index)})
        for index, path in enumerate(paths[:2])
    ]
    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    base = tmp_path / "base"
    EmbeddingJob(retriever, base, batch_size=2).run(base_records)
    base_matrix = load_embedding_artifacts(base)[0].copy()

    target_records = [
        ReferenceImage(f"id-{index}", path, {"source": "fixture", "source_image_id": str(index)})
        for index, path in enumerate(paths)
    ]
    target = tmp_path / "target"
    EmbeddingJob(
        DeterministicFixtureRetriever(allow_test_only=True),
        target,
        batch_size=3,
        reuse_from=base,
    ).run(target_records)

    matrix, ids, _, metadata = load_embedding_artifacts(target)
    assert ids == ["id-0", "id-1", "id-2"]
    np.testing.assert_array_equal(matrix[:2], base_matrix)
    assert metadata["reused_descriptor_count"] == 2
    assert metadata["computed_descriptor_count"] == 1
    assert metadata["reuse_provenance"]["eligible_descriptor_count"] == 2


def test_loader_rejects_tampered_descriptor_bytes(tmp_path: Path) -> None:
    image = tmp_path / "one.jpg"
    _image(image, (20, 40, 60))
    output = tmp_path / "artifacts"
    EmbeddingJob(
        DeterministicFixtureRetriever(allow_test_only=True),
        output,
    ).run([ReferenceImage("one", image)])
    descriptor_path = output / "descriptors.npy"
    payload = bytearray(descriptor_path.read_bytes())
    payload[-1] ^= 1
    descriptor_path.write_bytes(payload)

    with pytest.raises(EmbeddingJobError, match="SHA-256 mismatch"):
        load_embedding_artifacts(output)
