from __future__ import annotations

import pandas as pd
import pytest

from ml.evaluation.moscow_v3_split import MoscowV3SplitError, assign_policy_splits


def _policy_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for group_number in range(6):
        for row_number in range(group_number % 3 + 1):
            rows.append(
                {
                    "evaluation_geo_group_id": f"group-{group_number}",
                    "evaluation_area_h3": f"area-{group_number}",
                    "source": "mapillary" if group_number % 2 == 0 else "kartaview",
                    "sequence_id": f"sequence-{group_number}",
                    "width": 2048 if row_number % 2 else 1280,
                    "height": 1024,
                }
            )
    return pd.DataFrame(rows)


def test_policy_component_assignment_is_deterministic_and_group_disjoint() -> None:
    frame = _policy_frame()
    first = assign_policy_splits(frame, seed=20260902)
    second = assign_policy_splits(frame.sample(frac=1, random_state=7), seed=20260902)

    assert first == second
    assert set(first.values()) == {"development", "calibration"}
    assert set(first) == set(frame["evaluation_geo_group_id"])


def test_policy_component_assignment_requires_multiple_groups() -> None:
    frame = _policy_frame().loc[lambda value: value["evaluation_geo_group_id"] == "group-0"]
    with pytest.raises(MoscowV3SplitError, match="at least two geographic components"):
        assign_policy_splits(frame, seed=1)
