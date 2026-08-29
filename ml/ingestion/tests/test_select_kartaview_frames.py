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


def test_selection_refreshes_legacy_storage_url_from_retained_metadata() -> None:
    row = _row("center", "s1", 0, 55.7)
    row["download_url"] = "https://storage7.openstreetcam.org/unavailable.jpg"
    metadata = json.loads(row["metadata_json"])
    metadata["imageProcUrl"] = "https://cdn.kartaview.org/proxy-token"
    row["metadata_json"] = json.dumps(metadata)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, [row])
        run(source, output, report)
        [selected] = read_json(output, default=[])
        assert selected["download_url"] == "https://cdn.kartaview.org/proxy-token"
        assert selected["image_url"] == "https://cdn.kartaview.org/proxy-token"


def test_default_selection_has_no_global_or_per_sequence_cap() -> None:
    rows = [
        _row("center", "long-sequence", index, 55.60 + index * 0.00006)
        for index in range(1_301)
    ]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, rows)

        summary = run(source, output, report)

        assert len(read_json(output, default=[])) == 1_301
        assert summary["selected_records"] == 1_301
        assert summary["max_records"] is None
        assert summary["max_per_sequence"] is None
        assert summary["global_cap_configured"] is False
        assert summary["per_sequence_cap_configured"] is False
        assert summary["optional_global_limit_removed"] == 0
        assert summary["optional_sequence_limit_removed"] == 0


def test_opt_in_thinning_only_removes_close_similar_heading_sequence_neighbours() -> None:
    rows = [
        _row("center", "s1", 0, 55.70000, heading=0),
        _row("center", "s1", 1, 55.70002, heading=5),
        _row("center", "s1", 2, 55.70002, heading=90),
        _row("center", "s1", 3, 55.70008, heading=90),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, rows)

        summary = run(source, output, report, min_spacing_m=5)
        selected = read_json(output, default=[])

        assert [row["source_image_id"] for row in selected] == ["s1-0", "s1-2", "s1-3"]
        assert summary["sequence_neighbor_proxies_removed"] == 1


def test_explicit_limits_remain_available_and_are_audited() -> None:
    rows = [
        _row(area, f"{area}-sequence", index, 55.70 + index * 0.00010)
        for area in ("center", "west")
        for index in range(5)
    ]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, rows)

        summary = run(source, output, report, max_records=3, max_per_sequence=2)

        assert len(read_json(output, default=[])) == 3
        assert summary["global_cap_configured"] is True
        assert summary["per_sequence_cap_configured"] is True
        assert summary["optional_sequence_limit_removed"] == 6
        assert summary["optional_global_limit_removed"] == 1


def test_exact_source_id_duplicates_are_removed_before_balancing() -> None:
    original = _row("center", "s1", 0, 55.70)
    duplicate = dict(original)
    duplicate["lat"] = 55.80
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source, output, report = root / "source.json", root / "selected.json", root / "report.json"
        write_json(source, [original, duplicate])

        summary = run(source, output, report)

        assert len(read_json(output, default=[])) == 1
        assert summary["duplicate_source_ids_removed"] == 1
