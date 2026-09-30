"""Replay the six supplied views through the versioned serving adapter.

Known coordinates are read only after all serving predictions are computed.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from app.schemas.api import LocalizeResponse
from app.services.multi_photo import combine_views, distance_m

from ml.localization.clean_sage_l import create_clean_sage_l_service

ROOT = Path(__file__).resolve().parents[1]
CASE = ROOT / "data/evaluation/user_six_view_case_20260930"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> None:
    manifest = json.loads((CASE / "input_manifest.json").read_text())["images"]
    expected = json.loads((CASE / "cleaned_sage_l_replay_v1/inference.json").read_text())["individual"]
    service = create_clean_sage_l_service().load()
    try:
        predictions = []
        responses = []
        for item in manifest:
            path = Path(item["path"])
            if digest(path) != item["sha256"]:
                raise RuntimeError(f"input image {item['image']} changed")
            raw = service.localize(path)
            predictions.append({
                "image": item["image"], "reference_id": raw["matches"][0]["reference_id"],
                "prediction": raw["prediction"],
                "retrieval_ms": raw["diagnostics"]["retrieval_ms"],
                "embedding_ms": raw["diagnostics"]["embedding_ms"],
            })
            for match in raw["matches"]:
                match.pop("thumbnail_available", None)
            responses.append(LocalizeResponse.model_validate(raw | {"request_id": f"view-{item['image']}"}))
            print(f"view {item['image']}: {predictions[-1]['reference_id']}", flush=True)
        combined = combine_views(responses, submitted=6, request_id="six")
    finally:
        service.close()
    artifact = {
        "query_ground_truth_accessed": False,
        "input_manifest_sha256": digest(CASE / "input_manifest.json"),
        "individual": predictions,
        "combined": combined.model_dump(mode="json"),
    }
    output = CASE / "cleaned_sage_l_replay_v1/serving_adapter_six_v1.json"
    if output.exists():
        raise FileExistsError(output)
    output.write_text(json.dumps(artifact, indent=2, ensure_ascii=False) + "\n")
    print(f"frozen: {output}", flush=True)
    parity = [
        prediction["reference_id"] == item["candidates"][0]["reference_id"]
        for prediction, item in zip(predictions, expected, strict=True)
    ]
    truth = json.loads((CASE / "analysis.json").read_text())["ground_truth"]
    point = combined.prediction
    distance = None if point is None else distance_m((point.lat, point.lon), (truth["lat"], truth["lon"]))
    print(json.dumps({"top1_parity": parity, "combined_distance_m": distance}, indent=2), flush=True)


if __name__ == "__main__":
    main()
