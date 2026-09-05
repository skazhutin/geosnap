"""Restart-safe state shared by live metadata ingestion jobs."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json

CHECKPOINT_SCHEMA_VERSION = 2


def ingestion_config_fingerprint(config: Mapping[str, Any]) -> str:
    """Return a stable identity for the acquisition geometry and source query.

    Operational retry and page-budget settings should deliberately be excluded by
    callers so an incomplete tile can be resumed with a larger budget.
    """

    canonical = json.dumps(
        config,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


@dataclass
class IngestionState:
    source: str
    output_json: Path
    checkpoint_path: Path
    stats_path: Path
    config_fingerprint: str
    journal_path: Path
    journaled_record_count: int
    records: list[dict[str, Any]] = field(default_factory=list)
    completed_tiles: set[str] = field(default_factory=set)
    failed_tiles: dict[str, str] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(
        cls,
        *,
        source: str,
        output_json: Path,
        checkpoint_path: Path | None = None,
        stats_path: Path | None = None,
        config_fingerprint: str,
    ) -> IngestionState:
        checkpoint = checkpoint_path or output_json.with_suffix(".checkpoint.json")
        statistics = stats_path or output_json.with_suffix(".stats.json")
        journal = output_json.with_suffix(".journal.jsonl")
        records = read_json(output_json, default=[])
        if not isinstance(records, list):
            raise ValueError(f"{output_json} must contain a JSON array")
        payload = read_json(checkpoint, default={})
        if not isinstance(payload, dict):
            raise ValueError(f"{checkpoint} must contain a JSON object")
        completed = payload.get("completed_tiles", [])
        failed = payload.get("failed_tiles", {})
        if not isinstance(completed, list) or not isinstance(failed, dict):
            raise ValueError(f"invalid checkpoint structure: {checkpoint}")
        checkpoint_source = payload.get("source")
        if checkpoint_source is not None and str(checkpoint_source) != source:
            raise ValueError(
                f"checkpoint source mismatch in {checkpoint}: "
                f"expected {source!r}, found {checkpoint_source!r}"
            )
        stored_fingerprint = payload.get("config_fingerprint")
        has_checkpoint_progress = bool(completed or failed)
        if has_checkpoint_progress and stored_fingerprint != config_fingerprint:
            if stored_fingerprint is None:
                detail = "legacy checkpoint has no acquisition fingerprint"
            else:
                detail = "acquisition configuration changed"
            raise ValueError(
                f"{detail} for {checkpoint}; use the matching configuration or a new checkpoint path"
            )

        seen_ids = {str(row.get("id")) for row in records if isinstance(row, dict) and row.get("id") is not None}
        if journal.exists():
            journal_text = journal.read_text(encoding="utf-8")
            lines = journal_text.splitlines()
            valid_journal_lines: list[str] = []
            repair_trailing_line = bool(journal_text) and not journal_text.endswith("\n")
            for index, line in enumerate(lines):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    # A killed append can leave only its last line incomplete.
                    if index == len(lines) - 1 and not journal_text.endswith("\n"):
                        break
                    raise ValueError(f"invalid ingestion journal: {journal}") from exc
                if not isinstance(row, dict) or row.get("id") is None:
                    raise ValueError(f"invalid ingestion journal record: {journal}")
                valid_journal_lines.append(line)
                row_id = str(row["id"])
                if row_id not in seen_ids:
                    records.append(row)
                    seen_ids.add(row_id)
            if repair_trailing_line:
                temporary = journal.with_name(f".{journal.name}.{os.getpid()}.repair.tmp")
                try:
                    with temporary.open("w", encoding="utf-8") as fp:
                        if valid_journal_lines:
                            fp.write("\n".join(valid_journal_lines) + "\n")
                        fp.flush()
                        os.fsync(fp.fileno())
                    os.replace(temporary, journal)
                finally:
                    temporary.unlink(missing_ok=True)

        previous_stats = read_json(statistics, default={})
        if not isinstance(previous_stats, dict):
            raise ValueError(f"{statistics} must contain a JSON object")

        cumulative_counter_names = (
            "source_records_discovered",
            "metadata_normalized",
            "invalid_records",
            "duplicate_records",
            "tiles_attempted",
            "tiles_succeeded",
            "tiles_failed",
            "tiles_skipped_checkpoint",
            "pages_fetched",
            "truncated_tiles",
            "tiles_incomplete",
            "network_attempts",
            "retry_attempts",
            "timeout_events",
            "retryable_http_events",
            "rate_limit_events",
        )

        base_stats: dict[str, Any] = {
            "source": source,
            "existing_records": len(records),
            "tiles_total": 0,
            "final_records": len(records),
            "runs_started": int(previous_stats.get("runs_started", 0)) + 1,
            "elapsed_seconds": float(previous_stats.get("elapsed_seconds", 0.0)),
        }
        for name in cumulative_counter_names:
            base_stats[name] = int(previous_stats.get(name, 0))
        return cls(
            source=source,
            output_json=output_json,
            checkpoint_path=checkpoint,
            stats_path=statistics,
            config_fingerprint=config_fingerprint,
            journal_path=journal,
            journaled_record_count=len(records),
            records=records,
            completed_tiles=set(str(item) for item in completed),
            failed_tiles={str(key): str(value) for key, value in failed.items()},
            stats=base_stats,
        )

    @property
    def seen_ids(self) -> set[str]:
        return {str(row.get("id")) for row in self.records if row.get("id") is not None}

    def _append_new_records(self) -> None:
        new_records = self.records[self.journaled_record_count :]
        if not new_records:
            return
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.journal_path.open("a", encoding="utf-8") as fp:
            for row in new_records:
                fp.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    + "\n"
                )
            fp.flush()
            os.fsync(fp.fileno())
        self.journaled_record_count = len(self.records)

    def _clear_journal_atomically(self) -> None:
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.journal_path.with_name(f".{self.journal_path.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as fp:
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(temporary, self.journal_path)
        finally:
            temporary.unlink(missing_ok=True)

    def save(self, *, compact: bool = False) -> None:
        self.stats["final_records"] = len(self.records)
        self.stats["tiles_completed_current"] = len(self.completed_tiles)
        self.stats["tiles_failed_current"] = len(self.failed_tiles)
        accounted_tiles = self.completed_tiles | set(self.failed_tiles)
        self.stats["tiles_outstanding_current"] = max(
            0,
            int(self.stats.get("tiles_total", 0)) - len(accounted_tiles),
        )
        # The append-only journal keeps checkpoint writes proportional to newly
        # discovered rows. A final compaction writes the consumer-facing array
        # once; crash-time duplicates are removed by stable IDs on reload.
        self._append_new_records()
        if compact:
            write_json(self.output_json, self.records)
            self._clear_journal_atomically()
        write_json(
            self.checkpoint_path,
            {
                "schema_version": CHECKPOINT_SCHEMA_VERSION,
                "source": self.source,
                "config_fingerprint": self.config_fingerprint,
                "completed_tiles": sorted(self.completed_tiles),
                "failed_tiles": dict(sorted(self.failed_tiles.items())),
            },
        )
        write_json(self.stats_path, self.stats)
