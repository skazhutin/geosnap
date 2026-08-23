from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from ml.ingestion.common import read_json, write_json
from ml.ingestion.select_kartaview_frames import run


def _row(area: str, sequence: str, index: int, lat: float, *, heading: float = 0.0) -> dict:
    source_id = f"{sequence}-{index}"
    return {
        "id": source_id,
        "source_image_id": source_id,
        "sequence_id": sequence,
        "sequence_index": index,
        "lat": lat,
        "lon": 37.61,
        "heading": heading,
        "metadata_json": json.dumps(
            {"_geosnap_acquisition_query": f"config:moscow_v1:{area}:000", "sequenceIndex": index}
        ),
    }


def test_selection_spatially_samples_and_balances_areas_and_sequences() -> None:
    rows = []
    for area in ("center", "west"):
        for sequence in (f"{area}-a", f"{area}-b"):
            for index in range(8):
                rows.append(_row(area, sequence, index, 55.70 + index * 0.0001))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, rows)
        summary = run(
            source,
            output,
            report,
            max_records=8,
            min_spacing_m=20,
            heading_diversity_deg=60,
            min_heading_spacing_m=10,
            max_per_sequence=3,
        )
        selected = read_json(output, default=[])
        assert len(selected) == 8
        assert summary["selected_areas"] == 2
        assert summary["selected_sequences"] == 4
        assert summary["selected_per_area"] == {"center": 4, "west": 4}
        assert len({row["source_image_id"] for row in selected}) == len(selected)


def test_heading_diversity_preserves_a_distinct_nearby_view() -> None:
    rows = [
        _row("center", "s1", 0, 55.7, heading=0),
        _row("center", "s1", 1, 55.70015, heading=100),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, rows)
        run(source, output, report, min_spacing_m=40, min_heading_spacing_m=10, heading_diversity_deg=60)
        assert len(read_json(output, default=[])) == 2


def test_selection_fails_closed_without_valid_moscow_frames() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "source.json"
        write_json(source, [_row("outside", "s", 1, 50.0)])
        with pytest.raises(ValueError, match="no valid Moscow"):
            run(source, root / "output.json", root / "report.json")
