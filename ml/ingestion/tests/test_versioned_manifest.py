from __future__ import annotations

from pathlib import Path

from ml.ingestion.schema import canonical_record, manifest_dataframe, read_manifest, write_manifest
from ml.ingestion.versioned_manifest import run


def _record(identity: str, path: Path, quality: float) -> dict:
    return canonical_record(
        source="mapillary",
        source_image_id=identity,
        lat=55.75,
        lon=37.61,
        image_path=str(path),
        quality_score=quality,
        sequence_id=f"sequence-{identity}",
        license_name="CC BY-SA 4.0",
        attribution="Mapillary image by test",
        source_url=f"https://www.mapillary.com/app/?focus=photo&pKey={identity}",
        download_url=f"https://images.test/{identity}.jpg",
    )


def test_versioned_union_prefers_quality_and_never_mutates_inputs(tmp_path: Path) -> None:
    prior = tmp_path / "prior.parquet"
    tranche = tmp_path / "tranche.parquet"
    output = tmp_path / "v2.parquet"
    report = tmp_path / "report.json"
    write_manifest(manifest_dataframe([_record("same", tmp_path / "old.jpg", 0.4)]), prior)
    write_manifest(
        manifest_dataframe(
            [
                _record("same", tmp_path / "better.jpg", 0.9),
                _record("new", tmp_path / "new.jpg", 0.7),
            ]
        ),
        tranche,
    )
    prior_bytes = prior.read_bytes()
    tranche_bytes = tranche.read_bytes()

    summary = run([prior, tranche], output, report)

    result = read_manifest(output)
    assert result["source_image_id"].tolist() == ["new", "same"]
    assert Path(result.loc[result["source_image_id"] == "same", "image_path"].item()).name == "better.jpg"
    assert summary["duplicate_source_identities_removed"] == 1
    assert prior.read_bytes() == prior_bytes
    assert tranche.read_bytes() == tranche_bytes


def test_equal_quality_prefers_later_tranche_input_deterministically(tmp_path: Path) -> None:
    prior = tmp_path / "prior.parquet"
    tranche = tmp_path / "tranche.parquet"
    output = tmp_path / "v2.parquet"
    first = _record("same", tmp_path / "old.jpg", 0.8)
    second = _record("same", tmp_path / "new.jpg", 0.8)
    write_manifest(manifest_dataframe([first]), prior)
    write_manifest(manifest_dataframe([second]), tranche)

    run([prior, tranche], output, tmp_path / "report.json")

    assert Path(read_manifest(output).iloc[0]["image_path"]).name == "new.jpg"
