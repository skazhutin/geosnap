from __future__ import annotations

import json
from pathlib import Path

from ml.ingestion.common import read_json
from ml.ingestion.state import IngestionState


def _load(root: Path) -> IngestionState:
    return IngestionState.load(
        source="fixture",
        output_json=root / "records.json",
        checkpoint_path=root / "checkpoint.json",
        stats_path=root / "stats.json",
        config_fingerprint="fingerprint",
    )


def test_incremental_save_journals_then_final_save_compacts(tmp_path: Path) -> None:
    state = _load(tmp_path)
    state.records.append({"id": "one", "value": 1})
    state.completed_tiles.add("tile-1")
    state.save()

    assert not state.output_json.exists()
    assert json.loads(state.journal_path.read_text(encoding="utf-8")) == {
        "id": "one",
        "value": 1,
    }
    resumed = _load(tmp_path)
    assert resumed.records == [{"id": "one", "value": 1}]

    resumed.records.append({"id": "two", "value": 2})
    resumed.save(compact=True)
    assert read_json(resumed.output_json, default=[]) == [
        {"id": "one", "value": 1},
        {"id": "two", "value": 2},
    ]
    assert resumed.journal_path.read_text(encoding="utf-8") == ""


def test_truncated_last_journal_line_is_repaired_and_does_not_poison_resume(tmp_path: Path) -> None:
    journal = tmp_path / "records.journal.jsonl"
    journal.write_text('{"id":"one"}\n{"id":"partial', encoding="utf-8")

    state = _load(tmp_path)

    assert state.records == [{"id": "one"}]
    assert journal.read_text(encoding="utf-8") == '{"id":"one"}\n'
    state.records.append({"id": "two"})
    state.save()
    assert [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()] == [
        {"id": "one"},
        {"id": "two"},
    ]


def test_resume_preserves_cumulative_stats(tmp_path: Path) -> None:
    state = _load(tmp_path)
    state.stats["metadata_normalized"] = 3
    state.stats["elapsed_seconds"] = 1.5
    state.save()

    resumed = _load(tmp_path)

    assert resumed.stats["metadata_normalized"] == 3
    assert resumed.stats["elapsed_seconds"] == 1.5
    assert resumed.stats["runs_started"] == 2
