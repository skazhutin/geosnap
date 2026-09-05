import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ml.ingestion.schema import (
    CANONICAL_COLUMNS,
    ManifestSchemaError,
    canonical_record,
    manifest_dataframe,
    normalize_captured_at,
    normalize_metadata_json,
    read_manifest,
    validate_manifest_schema,
    write_manifest,
)


class SchemaTests(unittest.TestCase):
    def test_metadata_json_dict_and_string_have_identical_encoding(self) -> None:
        expected = '{"a":1,"b":2}'
        self.assertEqual(normalize_metadata_json({"b": 2, "a": 1}), expected)
        self.assertEqual(normalize_metadata_json('{"b": 2, "a": 1}'), expected)

    def test_metadata_normalizes_nested_missing_values_and_sets_deterministically(self) -> None:
        record = canonical_record(
            source="kartaview",
            source_image_id="nested",
            lat=55.75,
            lon=37.61,
            image_path="image.jpg",
            metadata={"missing": pd.NA, "values": {"b", "a"}},
        )
        self.assertEqual(record["metadata_json"], '{"missing":null,"values":["a","b"]}')

    def test_timestamp_normalization(self) -> None:
        self.assertEqual(normalize_captured_at(1_704_067_200_000), "2024-01-01T00:00:00Z")
        self.assertEqual(normalize_captured_at("2024-01-01 03:00:00+03:00"), "2024-01-01T00:00:00Z")
        self.assertIsNone(normalize_captured_at("not-a-date"))

    def test_empty_manifest_roundtrip_retains_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.parquet"
            write_manifest(manifest_dataframe([]), path)
            raw = pd.read_parquet(path)
            self.assertTrue(set(CANONICAL_COLUMNS).issubset(raw.columns))
            self.assertEqual(len(raw), 0)
            self.assertEqual(validate_manifest_schema(raw), [])

    def test_noncanonical_metadata_is_rejected_before_write(self) -> None:
        record = canonical_record(
            source="mapillary",
            source_image_id="1",
            lat=55.75,
            lon=37.61,
            image_path="image.jpg",
            metadata={"a": 1},
        )
        df = manifest_dataframe([record])
        df.at[0, "metadata_json"] = '{"z": 1, "a": 2}'
        # write_manifest normalizes all input representations deterministically.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.parquet"
            write_manifest(df, path)
            loaded = read_manifest(path)
            self.assertEqual(loaded.at[0, "metadata_json"], '{"a":2,"z":1}')

    def test_duplicate_ids_are_rejected(self) -> None:
        record = canonical_record(
            source="mapillary",
            source_image_id="1",
            lat=55.75,
            lon=37.61,
            image_path="image.jpg",
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ManifestSchemaError, "duplicate_id"):
                write_manifest(manifest_dataframe([record, record]), Path(tmp) / "bad.parquet")

    def test_structural_validation_rejects_empty_required_identity_fields(self) -> None:
        record = canonical_record(
            source="mapillary",
            source_image_id="1",
            lat=55.75,
            lon=37.61,
            image_path="image.jpg",
        )
        for column in ("city_id", "source", "source_image_id", "image_path"):
            frame = manifest_dataframe([record])
            frame.at[0, column] = "  "
            self.assertIn(f"missing_{column}", validate_manifest_schema(frame))

    def test_strict_reference_validation_rejects_bad_provenance_scope_and_quality(self) -> None:
        record = canonical_record(
            source="mapillary",
            source_image_id="1",
            lat=55.75,
            lon=37.61,
            image_path="image.jpg",
            heading=15,
            quality_score=0.8,
            license_name="CC BY-SA 4.0",
            attribution="Mapillary contributor",
            source_url="https://www.mapillary.com/app/?pKey=1",
        )
        self.assertEqual(
            validate_manifest_schema(
                manifest_dataframe([record]),
                strict_reference=True,
            ),
            [],
        )

        cases = {
            "license": ("", "missing_license"),
            "attribution": ("", "missing_attribution"),
            "source_url": ("not-a-url", "invalid_source_url:0"),
            "quality_score": (None, "missing_or_invalid_quality_score"),
            "lat": (56.1, "coordinate_outside_moscow_bounds"),
            "heading": (360.0, "invalid_heading"),
        }
        for column, (value, expected) in cases.items():
            frame = manifest_dataframe([record])
            frame.at[0, column] = value
            self.assertIn(
                expected,
                validate_manifest_schema(frame, strict_reference=True),
                column,
            )


if __name__ == "__main__":
    unittest.main()
