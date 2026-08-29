from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pyarrow.parquet as pq

from ml.ingestion.msls_moscow import (
    MSLS_ARCHIVE_PREFIX,
    MSLS_DATASET_URL,
    MSLS_LICENSE,
    MslsMetadataError,
    load_moscow_metadata,
    run,
)
from ml.ingestion.schema import read_manifest, stable_reference_id, validate_manifest_schema


def _row(
    key: str,
    sequence: str,
    frame: int,
    *,
    lat: float,
    lon: float,
    captured_at: str,
    heading: float = 0.0,
    pano: bool = False,
    night: bool = False,
    view_direction: str = "Forward",
) -> dict[str, object]:
    return {
        "key": key,
        "sequence_key": sequence,
        "frame_number": frame,
        "lon": lon,
        "lat": lat,
        "ca": heading,
        "captured_at": captured_at,
        "pano": pano,
        "easting": 400_000.0 + frame,
        "northing": 6_100_000.0 + frame,
        "unique_cluster": frame,
        "control_panel": False,
        "night": night,
        "view_direction": view_direction,
    }


def _csv_text(fieldnames: list[str], rows: list[dict[str, object]]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows([{field: row[field] for field in fieldnames} for row in rows])
    return stream.getvalue()


def _write_metadata_zip(
    path: Path,
    splits: dict[str, list[dict[str, object]]],
    *,
    omit_seq_key: tuple[str, str] | None = None,
) -> None:
    fields = {
        "raw": ["key", "lon", "lat", "ca", "captured_at", "pano"],
        "seq_info": ["key", "sequence_key", "frame_number"],
        "postprocessed": [
            "key",
            "easting",
            "northing",
            "unique_cluster",
            "control_panel",
            "night",
            "view_direction",
        ],
    }
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for split in ("database", "query"):
            rows = splits[split]
            for table, table_fields in fields.items():
                table_rows = rows
                if table == "seq_info" and omit_seq_key and omit_seq_key[0] == split:
                    table_rows = [row for row in rows if row["key"] != omit_seq_key[1]]
                archive.writestr(
                    f"{MSLS_ARCHIVE_PREFIX}/{split}/{table}.csv",
                    _csv_text(table_fields, table_rows),
                )


def _fixture_rows() -> dict[str, list[dict[str, object]]]:
    return {
        "database": [
            _row(
                "db-a",
                "shared-seq",
                0,
                lat=55.75,
                lon=37.600000,
                captured_at="2019-01-10",
            ),
            _row(
                "db-near",
                "shared-seq",
                1,
                lat=55.75,
                lon=37.600005,
                captured_at="2019-01-10",
            ),
            _row(
                "db-far",
                "shared-seq",
                2,
                lat=55.75,
                lon=37.600400,
                captured_at="2019-01-10",
            ),
        ],
        "query": [
            _row(
                "q-turn",
                "shared-seq",
                3,
                lat=55.75,
                lon=37.600450,
                captured_at="2019-07-11",
                heading=90,
                night=True,
                view_direction="Sideways",
            ),
            _row(
                "q-east",
                "query-seq",
                0,
                lat=55.82,
                lon=37.76,
                captured_at="2018-10-01",
                heading=180,
                pano=True,
                view_direction="Backward",
            ),
        ],
    }


class MslsMoscowTests(unittest.TestCase):
    def test_exact_join_selection_and_canonical_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata_zip = root / "msls_metadata.zip"
            _write_metadata_zip(metadata_zip, _fixture_rows())
            parquet = root / "selected.parquet"
            output_json = root / "selected.json"
            stats_path = root / "stats.json"
            coverage = root / "coverage.json"

            stats = run(
                metadata_zip,
                parquet,
                output_json,
                stats_path,
                coverage_path=coverage,
                image_root=root / "images",
                min_sequence_spacing_m=20.0,
                min_heading_spacing_m=2.0,
                heading_change_deg=45.0,
            )

            self.assertEqual(stats["metadata_audit"]["valid_moscow_rows"], 5)
            self.assertEqual(stats["metadata_audit"]["unique_provider_sequences"], 2)
            self.assertEqual(stats["metadata_audit"]["cross_original_split_sequences"], 1)
            self.assertEqual(stats["selection"]["near_identical_frames_removed"], 1)
            self.assertEqual(stats["selection"]["selected_records"], 4)
            self.assertFalse(stats["selection"]["global_cap_applied"])
            self.assertEqual(
                stats["coverage_and_diversity"]["selected_gallery"]["volume_mapping"]["required_image_volumes"],
                ["msls_images_vol_1.zip", "msls_images_vol_5.zip"],
            )

            manifest = read_manifest(parquet, allow_empty=False)
            arrow_schema = pq.read_schema(parquet)
            self.assertEqual(validate_manifest_schema(manifest, allow_empty=False), [])
            self.assertEqual(set(manifest["source"]), {"msls"})
            self.assertEqual(set(manifest["license"]), {MSLS_LICENSE})
            self.assertEqual(set(manifest["source_url"]), {MSLS_DATASET_URL})
            self.assertTrue(manifest["sequence_id"].str.startswith("msls:").all())
            self.assertEqual(
                manifest.loc[manifest["source_image_id"] == "db-a", "id"].item(),
                stable_reference_id("msls", "db-a"),
            )
            self.assertNotIn("db-near", set(manifest["source_image_id"]))
            self.assertEqual(
                manifest.loc[manifest["source_image_id"] == "q-turn", "msls_volume"].item(),
                "msls_images_vol_1.zip",
            )
            self.assertEqual(
                manifest.loc[manifest["source_image_id"] == "db-a", "msls_volume"].item(),
                "msls_images_vol_5.zip",
            )
            metadata = json.loads(manifest.iloc[0]["metadata_json"])
            self.assertIn(metadata["original_split"], {"database", "query"})
            self.assertIn("archive_member", metadata)
            self.assertIsNotNone(manifest.iloc[0]["captured_at"])
            self.assertEqual(str(arrow_schema.field("msls_compressed_size_bytes").type), "int64")
            self.assertEqual(str(arrow_schema.field("msls_uncompressed_size_bytes").type), "int64")
            self.assertTrue(stats_path.is_file())
            self.assertTrue(stats_path.with_suffix(".md").is_file())
            self.assertIn("all_msls_moscow", json.loads(coverage.read_text())["layers"])

    def test_join_key_mismatch_fails_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata_zip = root / "bad.zip"
            _write_metadata_zip(
                metadata_zip,
                _fixture_rows(),
                omit_seq_key=("database", "db-near"),
            )
            with self.assertRaisesRegex(MslsMetadataError, "key mismatch"):
                run(
                    metadata_zip,
                    root / "selected.parquet",
                    root / "selected.json",
                    root / "stats.json",
                )
            self.assertFalse((root / "selected.parquet").exists())
            self.assertFalse((root / "stats.json").exists())

    def test_aoi_is_strict_by_default_and_exclusions_are_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = _fixture_rows()
            rows["query"][0]["lat"] = 56.2
            metadata_zip = root / "outside.zip"
            _write_metadata_zip(metadata_zip, rows)
            with self.assertRaisesRegex(MslsMetadataError, "outside configured Moscow bounds"):
                load_moscow_metadata(metadata_zip)
            frame, audit = load_moscow_metadata(metadata_zip, strict_aoi=False)
            self.assertEqual(audit["outside_moscow_rows"], 1)
            self.assertEqual(len(frame), 4)
            self.assertTrue(frame["lat"].between(55.55, 55.95).all())

    def test_global_cap_is_deterministic_and_spatially_balanced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata_zip = root / "msls.zip"
            _write_metadata_zip(metadata_zip, _fixture_rows())
            selections: list[list[str]] = []
            hashes: list[str] = []
            for suffix in ("a", "b"):
                stats = run(
                    metadata_zip,
                    root / f"{suffix}.parquet",
                    root / f"{suffix}.json",
                    root / f"{suffix}.stats.json",
                    min_sequence_spacing_m=0,
                    min_heading_spacing_m=0,
                    max_records=2,
                )
                manifest = read_manifest(root / f"{suffix}.parquet", allow_empty=False)
                selections.append(list(manifest["source_image_id"]))
                hashes.append(stats["selection"]["selected_content_sha256"])
                self.assertEqual(manifest["msls_h3_cell"].nunique(), 2)
                self.assertTrue(stats["selection"]["global_cap_applied"])
            self.assertEqual(selections[0], selections[1])
            self.assertEqual(hashes[0], hashes[1])

    def test_safe_volume_inventory_adds_exact_entry_sizes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata_zip = root / "msls.zip"
            _write_metadata_zip(metadata_zip, _fixture_rows())
            inventory = root / "volumes.json"
            inventory.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "archive_member": f"{MSLS_ARCHIVE_PREFIX}/database/images/db-a.jpg",
                                "volume": "msls_images_vol_5.zip",
                                "compressed_size_bytes": 123,
                                "uncompressed_size_bytes": 456,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            run(
                metadata_zip,
                root / "selected.parquet",
                root / "selected.json",
                root / "stats.json",
                volume_index_path=inventory,
                min_sequence_spacing_m=0,
                min_heading_spacing_m=0,
            )
            manifest = read_manifest(root / "selected.parquet", allow_empty=False)
            row = manifest[manifest["source_image_id"] == "db-a"].iloc[0]
            self.assertEqual(row["msls_compressed_size_bytes"], 123)
            self.assertEqual(row["msls_uncompressed_size_bytes"], 456)

    def test_signed_or_conflicting_volume_inventory_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata_zip = root / "msls.zip"
            _write_metadata_zip(metadata_zip, _fixture_rows())
            for volume, expected in (
                ("https://signed.example/vol.zip?token=secret", "credential-free"),
                ("msls_images_vol_1.zip", "conflicts with verified"),
            ):
                inventory = root / f"inventory-{len(volume)}.json"
                inventory.write_text(
                    json.dumps(
                        {
                            "entries": [
                                {
                                    "archive_member": f"{MSLS_ARCHIVE_PREFIX}/database/images/db-a.jpg",
                                    "volume": volume,
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(MslsMetadataError, expected):
                    run(
                        metadata_zip,
                        root / f"{len(volume)}.parquet",
                        root / f"{len(volume)}.json",
                        root / f"{len(volume)}.stats.json",
                        volume_index_path=inventory,
                    )


if __name__ == "__main__":
    unittest.main()
