from __future__ import annotations

import io
import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest
from PIL import Image

from ml.ingestion.extract_msls_images import OFFICIAL_VOLUMES, MslsExtractionError, run
from ml.ingestion.schema import canonical_record, manifest_dataframe, write_manifest


def _jpeg() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (96, 72), color=(40, 90, 140)).save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


def _manifest(root: Path, *, image_path: Path | None = None, member: str | None = None) -> Path:
    source_id = "frame_1"
    member = member or f"train_val/moscow/query/images/{source_id}.jpg"
    record = canonical_record(
        source="msls",
        source_image_id=source_id,
        city_id="moscow",
        lat=55.75,
        lon=37.61,
        image_path=str(image_path or root / "images" / "msls" / "query" / f"{source_id}.jpg"),
        sequence_id="msls:sequence_1",
        captured_at="2019-01-01T00:00:00Z",
        heading=10,
        license_name="CC BY-NC-SA 4.0",
        attribution="MSLS contributors",
        source_url="https://www.mapillary.com/dataset/places",
        metadata={},
    )
    record.update(
        {
            "msls_original_split": "query",
            "msls_archive_member": member,
            "msls_volume": "msls_images_vol_1.zip",
        }
    )
    path = root / "manifest.parquet"
    write_manifest(
        manifest_dataframe(
            [record],
            extra_columns=("msls_original_split", "msls_archive_member", "msls_volume"),
        ),
        path,
        allow_empty=False,
    )
    return path


def _archive(root: Path) -> Path:
    path = root / "volume.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("train_val/moscow/query/images/frame_1.jpg", _jpeg())
    return path


def _allow_fixture_archive(monkeypatch: pytest.MonkeyPatch, archive: Path) -> None:
    monkeypatch.setitem(
        OFFICIAL_VOLUMES,
        "msls_images_vol_1.zip",
        {"size_bytes": archive.stat().st_size, "md5": "fixture-not-checked"},
    )


def test_extracts_selected_image_atomically_and_resumes(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        manifest = _manifest(root)
        archive = _archive(root)
        _allow_fixture_archive(monkeypatch, archive)
        kwargs = {
            "manifest_path": manifest,
            "archives": {"msls_images_vol_1.zip": archive},
            "image_root": root / "images",
            "stats_path": root / "stats.json",
            "errors_path": root / "errors.json",
            "verify_md5": False,
            "min_valid_size_bytes": 100,
        }
        first = run(**kwargs)
        assert first["complete"] is True
        assert first["images_extracted"] == 1
        assert first["failed_images"] == 0
        second = run(**kwargs)
        assert second["images_extracted"] == 0
        assert second["already_valid_images_skipped"] == 1


def test_rejects_output_path_outside_image_root(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        manifest = _manifest(root, image_path=root / "outside.jpg")
        archive = _archive(root)
        _allow_fixture_archive(monkeypatch, archive)
        with pytest.raises(MslsExtractionError, match="escapes image_root"):
            run(
                manifest,
                {"msls_images_vol_1.zip": archive},
                root / "images",
                root / "stats.json",
                root / "errors.json",
                verify_md5=False,
            )


def test_rejects_archive_member_that_does_not_match_source_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        manifest = _manifest(root, member="train_val/moscow/query/images/other.jpg")
        archive = _archive(root)
        _allow_fixture_archive(monkeypatch, archive)
        with pytest.raises(MslsExtractionError, match="invalid MSLS archive member"):
            run(
                manifest,
                {"msls_images_vol_1.zip": archive},
                root / "images",
                root / "stats.json",
                root / "errors.json",
                verify_md5=False,
            )
