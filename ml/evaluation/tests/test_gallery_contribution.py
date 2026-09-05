from __future__ import annotations

from pathlib import Path

import pandas as pd

from ml.evaluation.gallery_contribution import build_contribution_report


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_gallery_contribution_compares_same_calibration_queries(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.parquet"
    gallery = tmp_path / "gallery.parquet"
    calibration = tmp_path / "calibration_queries.parquet"
    legacy_row = {
        "id": "legacy",
        "source": "mapillary",
        "source_image_id": "legacy",
        "lat": 55.75,
        "lon": 37.61,
    }
    new_row = {
        "id": "new",
        "source": "kartaview",
        "source_image_id": "new",
        "lat": 55.76,
        "lon": 37.62,
    }
    _write(legacy, [legacy_row])
    _write(gallery, [legacy_row, new_row])
    _write(
        calibration,
        [
            {
                "id": "query",
                "source": "kartaview",
                "source_image_id": "query",
                "lat": 55.76001,
                "lon": 37.62001,
                "evaluation_split": "calibration",
            }
        ],
    )

    report = build_contribution_report(
        legacy_manifest=legacy,
        gallery_manifest=gallery,
        calibration_manifest=calibration,
    )

    assert report["scope"] == "calibration_only"
    assert report["gallery_rows"]["new_tranche_origin"] == 1
    assert report["coordinate_oracle"]["legacy_origin_gallery"]["coverage_within_m"]["100"] == 0.0
    assert report["coordinate_oracle"]["full_v2_gallery"]["coverage_within_m"]["100"] == 1.0
    assert report["coordinate_oracle"]["queries_with_strictly_closer_new_gallery_coverage"] == 1
