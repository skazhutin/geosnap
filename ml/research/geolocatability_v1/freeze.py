"""Aggregate raw teacher outputs and freeze the outcome-blind canonical table."""
from __future__ import annotations

import io
import json
import os
import statistics
from collections import Counter

import numpy as np
import pandas as pd

from .annotate import BOOLEAN, NUMERIC, PROMPT_VERSION, SCHEMA_VERSION, validate
from .common import OUT, ROOT, TECHNICAL, json_once, now, sha256, verify_production, write_once


def aggregate(passes: list[dict]) -> dict:
    result = {}
    for key in NUMERIC:
        values = [float(p[key]) for p in passes]
        result.update({f"{key}_pass{i+1}": v for i, v in enumerate(values)})
        result[f"{key}_median"] = statistics.median(values)
        result[f"{key}_mean"] = statistics.mean(values)
        result[f"{key}_std"] = statistics.pstdev(values)
        result[f"{key}_range"] = max(values) - min(values)
    for key in BOOLEAN:
        values = [p[key] for p in passes]
        result.update({f"{key}_pass{i+1}": v for i, v in enumerate(values)})
        result[key] = sum(values) >= 2
        result[f"{key}_unanimous"] = len(set(values)) == 1
    reasons = [p["retake_reason"] for p in passes]
    result.update({f"retake_reason_pass{i+1}": v for i, v in enumerate(reasons)})
    counts = Counter(reasons)
    result["retake_reason_consensus"] = counts.most_common(1)[0][0] if max(counts.values()) >= 2 else None
    for i, p in enumerate(passes):
        result[f"short_reason_pass{i+1}"] = p["short_reason"]
    median_value = result["geolocatability_median"]
    median_index = next(i for i, p in enumerate(passes) if p["geolocatability"] == median_value)
    result["short_reason"] = passes[median_index]["short_reason"]
    result["annotation_uncertain"] = (result["geolocatability_range"] >= 0.30
        or not result["usable_single_photo_unanimous"]
        or not result["retake_recommended_unanimous"])
    result["high_consensus"] = (result["geolocatability_range"] <= 0.20
        and result["usable_single_photo_unanimous"]
        and result["retake_recommended_unanimous"])
    return result


def failure_type(detail: str) -> str:
    if "TimeoutError" in detail:
        return "timeout"
    if "JSONDecodeError" in detail:
        return "malformed_json"
    if "ValueError" in detail:
        return "schema_invalid"
    if "unreadable" in detail.lower() or "sha-256 mismatch" in detail.lower():
        return "unreadable_image"
    return "model_or_inference_error"


def run() -> None:
    guard = verify_production()
    manifest = OUT / "annotation_input_manifest.jsonl"
    input_receipt = json.loads((OUT / "input_receipt.json").read_text())
    if sha256(manifest) != input_receipt["annotation_manifest_sha256"]:
        raise RuntimeError("Annotation input changed")
    settings = json.loads((OUT / "inference_settings_qwen35_full1024.json").read_text())
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    with np.load(TECHNICAL, allow_pickle=False) as z:
        ids = z["query_ids"].tolist()
        names = z["feature_names"].tolist()
        features = z["features"]
    if ids != [r["query_id"] for r in rows] or sha256(TECHNICAL) != input_receipt["technical_feature_cache_sha256"]:
        raise RuntimeError("Technical feature cache changed or misaligned")

    output = []
    statuses = Counter()
    normalized_reasons = 0
    recovered_passes = 0
    for i, item in enumerate(rows):
        ident = item["query_id"]
        folder = OUT / "annotations/raw_qwen35_full1024" / ident
        base = {"query_id": ident, "image_sha256": item["image_sha256"],
                "width": item["width"], "height": item["height"],
                "aspect_ratio": item["width"] / item["height"],
                "schema_version": SCHEMA_VERSION, "prompt_version": PROMPT_VERSION,
                "model_repo": settings["model_repo"], "model_revision": settings["model_revision"],
                "model_weight_sha256": settings["model_weight_sha256"]}
        base.update({name: float(features[i, j]) for j, name in enumerate(names)
                     if name not in {"width", "height"}})
        raw_files = [folder / f"pass{number}.json" for number in (1, 2, 3)]
        if all(path.is_file() for path in raw_files):
            passes = []
            for path in raw_files:
                record = json.loads(path.read_text())
                recovered_passes += int(record.get("attempt") == "targeted_reason_recovery")
                parsed = validate(record["raw_text"])
                if parsed != record["parsed"]:
                    raise RuntimeError(f"Pass parse changed: {path}")
                original = json.loads(record["raw_text"])
                normalized_reasons += int(original["retake_reason"] != parsed["retake_reason"])
                passes.append(parsed)
            base["annotation_status"] = "valid"
            base.update(aggregate(passes))
        elif (folder / "failure.json").is_file() or (folder / "recovery_failure.json").is_file():
            base["annotation_status"] = "failure"
            failure_path = ((folder / "recovery_failure.json") if
                            (folder / "recovery_failure.json").is_file() else (folder / "failure.json"))
            failure = json.loads(failure_path.read_text())
            base["failure_record_path"] = str(failure_path)
            base["failure_type"] = ("unreadable_image" if failure.get("type") == "unreadable_image"
                                    else failure_type(failure.get("last_error", "")))
            base["failure_detail"] = failure.get("last_error", failure.get("detail", ""))
        else:
            raise RuntimeError(f"Missing annotation or explicit failure for {ident}")
        statuses[base["annotation_status"]] += 1
        output.append(base)
    if len(output) != 1184 or sum(statuses.values()) != 1184:
        raise RuntimeError("Incomplete annotation population")
    canonical_jsonl = OUT / "geolocatability_annotations.jsonl"
    canonical_parquet = OUT / "geolocatability_annotations.parquet"
    payload = "".join(json.dumps(r, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n" for r in output).encode()
    buffer = io.BytesIO()
    pd.DataFrame(output).to_parquet(buffer, index=False)
    for path, data in ((canonical_jsonl, payload), (canonical_parquet, buffer.getvalue())):
        if path.exists():
            if sha256(path) != __import__("hashlib").sha256(data).hexdigest():
                raise RuntimeError(f"Existing canonical artifact differs: {path}")
        else:
            write_once(path, data)
        os.chmod(path, 0o444)
    json_once(OUT / "annotation_freeze.json", {
        "frozen_at": now(), "annotation_status_counts": dict(statuses),
        "query_count": len(output), "input_manifest_sha256": sha256(manifest),
        "canonical_jsonl_sha256": sha256(canonical_jsonl),
        "canonical_parquet_sha256": sha256(canonical_parquet),
        "model_weight_sha256": settings["model_weight_sha256"],
        "model_revision": settings["model_revision"], "schema_version": SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION, "retake_reason_to_OTHER_normalizations": normalized_reasons,
        "targeted_short_reason_recovered_passes": recovered_passes,
        "targeted_short_reason_recovery_code_sha256": sha256(
            ROOT / "ml/research/geolocatability_v1/recover_missing_reasons.py"),
        "targeted_short_reason_recovery_settings_sha256": (
            sha256(OUT / "missing_reason_recovery_settings.json")
            if (OUT / "missing_reason_recovery_settings.json").exists() else None),
        "vlm_input_manifest_sha256": settings["vlm_input_manifest_sha256"],
        "vlm_input_preprocessing": settings["vlm_input_preprocessing"],
        "outcome_data_accessed": False, "production_guard": guard,
    })
    print(json.dumps({"rows": len(output), "statuses": dict(statuses),
                      "canonical_sha256": sha256(canonical_jsonl)}))


if __name__ == "__main__":
    run()
