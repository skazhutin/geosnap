from __future__ import annotations

from shapely.geometry import box

from ml.ingestion.moscow_acquisition_plan import build_plan

BOUNDS = [37.0, 55.0, 38.0, 56.0]


def _row(
    identity: str,
    *,
    x: int,
    y: int,
    source: str,
    sequence: str,
    heading: float,
) -> dict[str, object]:
    return {
        "id": identity,
        "source_image_id": identity,
        "source": source,
        "sequence_id": sequence,
        "lon": BOUNDS[0] + (x + 0.5) / 20,
        "lat": BOUNDS[1] + (y + 0.5) / 20,
        "heading": heading,
    }


def test_plan_distinguishes_source_availability_from_gallery_coverage() -> None:
    mapillary = [_row("m-empty", x=0, y=0, source="mapillary", sequence="m1", heading=0)]
    kartaview = [_row("k-empty", x=0, y=0, source="kartaview", sequence="k1", heading=90)]
    gallery = [
        _row(
            f"m-single-{index}",
            x=1,
            y=0,
            source="mapillary",
            sequence=f"m{2 + index % 2}",
            heading=float((index % 2) * 90),
        )
        for index in range(10)
    ]
    mapillary.append(_row("m-single-available", x=1, y=0, source="mapillary", sequence="m3", heading=90))

    plan = build_plan(
        gallery_rows=gallery,
        mapillary_rows=mapillary,
        kartaview_rows=kartaview,
        aoi_geometry=box(*BOUNDS),
        aoi_sha256="a" * 64,
        bounds=BOUNDS,
        tranche_quotas={"empty_available": 2, "single_provider": 2},
    )
    cells = {(cell["x"], cell["y"]): cell for cell in plan["cells"]}

    assert cells[(0, 0)]["need_category"] == "empty_available"
    assert cells[(0, 0)]["source_availability"] == "both_available"
    assert cells[(1, 0)]["need_category"] == "single_provider"
    assert cells[(2, 0)]["need_category"] == "unsupported_no_source_observed"


def test_many_frames_from_one_sequence_are_not_called_diverse() -> None:
    mapillary = [
        _row(str(index), x=2, y=2, source="mapillary", sequence="same", heading=0)
        for index in range(25)
    ]
    plan = build_plan(
        gallery_rows=[],
        mapillary_rows=mapillary,
        kartaview_rows=[],
        aoi_geometry=box(*BOUNDS),
        aoi_sha256="a" * 64,
        bounds=BOUNDS,
        tranche_quotas={"empty_available": 1},
    )
    cell = next(value for value in plan["cells"] if value["x"] == 2 and value["y"] == 2)

    assert cell["available"]["mapillary_representatives"] == 25
    assert cell["available"]["mapillary_sequences"] == 1
    assert cell["available"]["many_images_but_low_visual_proxy_diversity"] is True


def test_exact_aoi_excludes_bbox_only_cells() -> None:
    narrow_aoi = box(37.0, 55.0, 37.1, 56.0)
    source = [_row("outside", x=19, y=0, source="mapillary", sequence="m1", heading=0)]

    plan = build_plan(
        gallery_rows=[],
        mapillary_rows=source,
        kartaview_rows=[],
        aoi_geometry=narrow_aoi,
        aoi_sha256="a" * 64,
        bounds=BOUNDS,
        tranche_quotas={"empty_available": 10},
    )
    cell = next(value for value in plan["cells"] if value["x"] == 19 and value["y"] == 0)

    assert cell["need_category"] == "outside_exact_aoi"
    assert all(value["need_category"] != "outside_exact_aoi" for value in plan["tranche"]["cells"])


def test_historical_calibration_rank_affects_priority_without_test_input() -> None:
    source = [
        _row("a", x=3, y=3, source="mapillary", sequence="m1", heading=0),
        _row("b", x=4, y=3, source="mapillary", sequence="m2", heading=0),
    ]
    historical = {
        (3, 3): {"query_count": 4, "recall_at_10_within_100m": 0.0, "median_nearest_positive_rank_within_100m": 5000},
        (4, 3): {"query_count": 4, "recall_at_10_within_100m": 0.5, "median_nearest_positive_rank_within_100m": 10},
    }

    plan = build_plan(
        gallery_rows=[],
        mapillary_rows=source,
        kartaview_rows=[],
        aoi_geometry=box(*BOUNDS),
        aoi_sha256="a" * 64,
        bounds=BOUNDS,
        historical_retrieval=historical,
        tranche_quotas={"empty_available": 1},
    )

    assert (plan["tranche"]["cells"][0]["x"], plan["tranche"]["cells"][0]["y"]) == (3, 3)
    assert plan["rules"]["historical_retrieval_scope"].endswith("never v1 frozen test outcomes")


def test_plan_fingerprint_and_selection_are_reproducible() -> None:
    rows = [_row("m", x=0, y=0, source="mapillary", sequence="m1", heading=0)]
    kwargs = {
        "gallery_rows": [],
        "mapillary_rows": rows,
        "kartaview_rows": [],
        "aoi_geometry": box(*BOUNDS),
        "aoi_sha256": "a" * 64,
        "bounds": BOUNDS,
        "tranche_quotas": {"empty_available": 1},
    }

    first = build_plan(**kwargs)
    second = build_plan(**kwargs)

    assert first == second
    assert len(first["plan_fingerprint_sha256"]) == 64
