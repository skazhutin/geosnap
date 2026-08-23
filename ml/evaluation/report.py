"""Atomic machine-readable and human-readable evaluation reports."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .metrics import EvaluationBundle


def _format_percent(value: float | None) -> str:
    return "n/a" if value is None else f"{100.0 * value:.2f}%"


def _format_number(value: float | None, suffix: str = "") -> str:
    return "n/a" if value is None else f"{value:.2f}{suffix}"


def _markdown(bundle: EvaluationBundle) -> str:
    lines = [
        f"# Evaluation: {bundle.dataset_id} / {bundle.split_name}",
        "",
        "> Values in this report are computed from the supplied observations. "
        "No benchmark or Moscow result is inserted as a default.",
        "",
    ]
    if bundle.model:
        lines.extend(["## Model", ""])
        for key, value in bundle.model.items():
            lines.append(f"- {key}: `{value}`")
        lines.append("")
    if bundle.retrieval is not None:
        metric = bundle.retrieval
        lines.extend(
            [
                "## Retrieval",
                "",
                f"Positive: a retrieved reference is within **{metric.positive_distance_threshold_m:g} m** "
                "of query ground truth. Denominator: all held-out queries.",
                "",
                "| Metric | Value |",
                "|---|---:|",
            ]
        )
        for k, value in metric.recall_at.items():
            lines.append(f"| Recall@{k} | {_format_percent(value)} |")
        lines.extend(
            [
                f"| Queries | {metric.query_count} |",
                f"| Missing predictions | {metric.missing_prediction_count} |",
                "",
            ]
        )
    if bundle.localization is not None:
        metric = bundle.localization
        lines.extend(
            [
                "## Localization",
                "",
                "Primary threshold accuracy uses all queries; abstentions count as failures. "
                "Median/p90 errors use answered queries and are labelled accordingly.",
                "",
                "| Metric | Value |",
                "|---|---:|",
                f"| Answer rate | {_format_percent(metric.answer_rate)} |",
                f"| Low-confidence rate | {_format_percent(metric.low_confidence_rate)} |",
                f"| Out-of-coverage rate | {_format_percent(metric.out_of_coverage_rate)} |",
            ]
        )
        for threshold, value in metric.accuracy_within_m.items():
            lines.append(f"| Accuracy ≤ {threshold} m (all) | {_format_percent(value)} |")
        for threshold, value in metric.conditional_accuracy_within_m.items():
            lines.append(
                f"| Accuracy ≤ {threshold} m (answered) | {_format_percent(value)} |"
            )
        lines.extend(
            [
                f"| Median error (answered) | {_format_number(metric.median_error_m, ' m')} |",
                f"| P90 error (answered) | {_format_number(metric.p90_error_m, ' m')} |",
                "",
            ]
        )
    if bundle.latency is not None:
        lines.extend(
            [
                "## Latency",
                "",
                "| Stage | N | Mean | Median | P90 | P95 | Max |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for name, summary in bundle.latency.stages.items():
            lines.append(
                f"| {name} | {summary.count} | {_format_number(summary.mean_ms, ' ms')} | "
                f"{_format_number(summary.median_ms, ' ms')} | "
                f"{_format_number(summary.p90_ms, ' ms')} | "
                f"{_format_number(summary.p95_ms, ' ms')} | "
                f"{_format_number(summary.max_ms, ' ms')} |"
            )
        if bundle.latency.gallery_embedding_images_per_second is not None:
            lines.extend(
                [
                    "",
                    "Gallery embedding throughput: "
                    f"**{bundle.latency.gallery_embedding_images_per_second:.2f} images/s**",
                ]
            )
        lines.append("")
    if bundle.notes:
        lines.extend(["## Notes", ""])
        lines.extend(f"- {note}" for note in bundle.notes)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_evaluation_reports(
    bundle: EvaluationBundle,
    output_dir: str | Path,
    *,
    stem: str = "evaluation",
) -> tuple[Path, Path]:
    if not stem or Path(stem).name != stem:
        raise ValueError("stem must be a non-empty filename stem")
    output_dir = Path(output_dir)
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    payload = json.dumps(bundle.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    _atomic_text(json_path, payload)
    _atomic_text(markdown_path, _markdown(bundle))
    return json_path, markdown_path
