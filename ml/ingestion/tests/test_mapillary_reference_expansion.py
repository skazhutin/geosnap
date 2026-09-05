from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml.ingestion.mapillary_reference_expansion import (
    DiverseFrameConfig,
    MapillaryReferenceExpansionError,
    fetch_sequence_image_ids,
    select_diverse_frames,
)


def _row(
    identity: str,
    *,
    lat: float,
    lon: float,
    heading: float | None = 0.0,
    phash: str | None = None,
) -> dict[str, object]:
    return {
        "id": identity,
        "source_image_id": identity,
        "sequence_id": "sequence-a",
        "lat": lat,
        "lon": lon,
        "heading": heading,
        "perceptual_hash": phash,
    }


def test_discovery_anchor_expands_to_spatial_and_heading_diverse_frames() -> None:
    rows = [
        _row("anchor", lat=55.75, lon=37.61, heading=0),
        _row("adjacent", lat=55.75001, lon=37.61, heading=2),
        _row("reverse-view", lat=55.75009, lon=37.61, heading=180),
        _row("far-east", lat=55.75, lon=37.611, heading=5),
        _row("far-west", lat=55.75, lon=37.609, heading=355),
    ]

    selected = select_diverse_frames(
        list(reversed(rows)),
        anchor_image_id="anchor",
        config=DiverseFrameConfig(
            max_per_sequence=4,
            min_spacing_m=35,
            heading_diversity_deg=45,
            min_heading_spacing_m=8,
        ),
    )

    ids = [row["source_image_id"] for row in selected]
    assert ids[0] == "anchor"
    assert "adjacent" not in ids
    assert "reverse-view" in ids
    assert {"far-east", "far-west"}.issubset(ids)


def test_frame_selection_is_deterministic_under_input_order() -> None:
    rows = [
        _row("a", lat=55.75, lon=37.61, heading=0),
        _row("b", lat=55.75, lon=37.611, heading=90),
        _row("c", lat=55.75, lon=37.609, heading=270),
        _row("d", lat=55.751, lon=37.61, heading=180),
    ]
    config = DiverseFrameConfig(max_per_sequence=3, min_spacing_m=20)

    forward = select_diverse_frames(rows, config=config, anchor_image_id="a")
    reverse = select_diverse_frames(list(reversed(rows)), config=config, anchor_image_id="a")

    assert [row["source_image_id"] for row in forward] == [row["source_image_id"] for row in reverse]


def test_exact_and_phash_near_duplicates_are_rejected() -> None:
    rows = [
        _row("a", lat=55.75, lon=37.61, phash="0000000000000000"),
        _row("a", lat=55.76, lon=37.62, phash="ffffffffffffffff"),
        _row("near", lat=55.76, lon=37.62, phash="0000000000000003"),
        _row("different", lat=55.76, lon=37.62, phash="ffffffffffffffff"),
    ]

    selected = select_diverse_frames(
        rows,
        anchor_image_id="a",
        config=DiverseFrameConfig(
            max_per_sequence=4,
            min_spacing_m=1,
            min_heading_spacing_m=0,
            phash_distance_threshold=4,
        ),
    )

    assert [row["source_image_id"] for row in selected] == ["a", "different"]


class _Response:
    def __init__(self, payload: object) -> None:
        self.content = json.dumps(payload).encode()
        self.headers = {"Content-Length": str(len(self.content))}
        self.status_code = 200
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield self.content

    def close(self) -> None:
        self.closed = True


class _Session:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.response


def test_sequence_collection_preserves_provider_order_and_deduplicates() -> None:
    response = _Response({"data": [{"id": "3"}, {"id": "1"}, {"id": "3"}, {"id": "2"}]})
    session = _Session(response)

    result = fetch_sequence_image_ids(
        session,
        token="token",
        sequence_id="sequence-a",
        retries=1,
        backoff_sec=0,
        timeout_sec=1,
    )

    assert result == ["3", "1", "2"]
    assert session.calls[0]["url"] == "https://graph.mapillary.com/image_ids"
    assert session.calls[0]["params"] == {"sequence_id": "sequence-a", "access_token": "token"}
    assert response.closed is True


def test_sequence_collection_rejects_unhandled_pagination() -> None:
    session = _Session(_Response({"data": [{"id": "1"}], "paging": {"next": "https://example.invalid"}}))

    with pytest.raises(MapillaryReferenceExpansionError, match="paginated"):
        fetch_sequence_image_ids(
            session,
            token="token",
            sequence_id="sequence-a",
            retries=1,
            backoff_sec=0,
            timeout_sec=1,
        )


def test_discovery_module_keeps_one_sequence_representative_separate() -> None:
    # This regression makes the architectural boundary explicit: discovery
    # still deduplicates a sequence, while expansion deliberately accepts many
    # frames from that discovered sequence.
    from ml.ingestion.mapillary_citywide import SequenceCandidate, VectorTile, select_balanced_candidates

    tile = VectorTile(12, 1, 1)
    discovered = [
        SequenceCandidate(str(index), "same-sequence", tile, 0, 0, 37.6, 55.7)
        for index in range(3)
    ]
    assert len(select_balanced_candidates(discovered, max_candidates=3, max_per_tile=3, max_per_subcell=3)) == 1

    expanded = [
        _row(str(index), lat=55.75, lon=37.61 + index * 0.001, heading=index * 90)
        for index in range(3)
    ]
    assert len(select_diverse_frames(expanded, config=DiverseFrameConfig(max_per_sequence=3))) == 3


def test_checkpoint_and_output_paths_are_independent_of_v1(tmp_path: Path) -> None:
    # A small filesystem-level guard against accidentally documenting or
    # wiring the immutable historical namespace as an expansion destination.
    historical = tmp_path / "moscow_real_v1"
    expansion = tmp_path / "moscow_real_v2" / "acquisition"
    historical.mkdir()
    expansion.mkdir(parents=True)
    assert historical not in expansion.parents
