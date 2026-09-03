"""Verify the authoritative Moscow production artifacts and run one gallery smoke."""

from __future__ import annotations

import argparse
import json
import os
import resource
import time
from pathlib import Path
from typing import Any

from PIL import Image

from ml.ingestion.schema import read_manifest
from ml.localization.service import create_localization_service
from ml.runtime_config import FrozenRuntimeConfig, RuntimeConfigError, sha256_file

PIPELINE_ENVIRONMENT = (
    "RETRIEVER",
    "RETRIEVAL_TOP_K",
    "QUERY_AGGREGATION",
    "CITY_ID",
    "INDEX_ID",
    "GEOSNAP_INDEX_DIR",
    "CONFIDENCE_THRESHOLD",
    "OOC_SIMILARITY_THRESHOLD",
    "MINIMUM_CLUSTER_MASS",
    "MINIMUM_CLUSTER_MASS_MARGIN",
    "MINIMUM_CLUSTER_CANDIDATES",
    "LOCALIZATION_CLUSTER_RADIUS_M",
    "LOCALIZATION_MAX_CLUSTER_DIAMETER_M",
    "COORDINATE_ESTIMATOR",
    "VERIFICATION_ENABLED",
    "VERIFY_TOP_K",
    "VERIFICATION_BACKEND",
    "VERIFICATION_GEOMETRIC_WEIGHT",
)


def _verify_hash(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or sha256_file(path) != expected:
        raise RuntimeConfigError(f"{label} is missing or has the wrong SHA-256: {path}")


def verify_artifacts(config: FrozenRuntimeConfig) -> dict[str, Any]:
    root = config.path.parent.parent
    payload = config.payload
    dataset = payload["dataset"]
    gallery = root / str(dataset["gallery_manifest"])
    _verify_hash(gallery, str(dataset["gallery_sha256"]), "gallery manifest")

    index = payload["index"]
    index_dir = config.index_dir
    for filename, key in (
        ("index.faiss", "index_faiss_sha256"),
        ("index_metadata.json", "index_metadata_sha256"),
        ("id_mapping.json", "id_mapping_sha256"),
        ("reference_metadata.jsonl", "reference_metadata_sha256"),
    ):
        _verify_hash(index_dir / filename, str(index[key]), f"index {filename}")

    embedding = payload["embedding"]
    embedding_dir = root / str(embedding["directory"])
    for filename, key in (
        ("build_metadata.json", "build_metadata_sha256"),
        ("descriptors.npy", "descriptors_sha256"),
        ("id_mapping.json", "id_mapping_sha256"),
        ("reference_metadata.jsonl", "reference_metadata_sha256"),
    ):
        _verify_hash(
            embedding_dir / filename,
            str(embedding[key]),
            f"embedding {filename}",
        )

    final = payload["final_evaluation"]
    for key, hash_key in (
        ("comparison", "comparison_sha256"),
        ("selected_report", "selected_report_sha256"),
        ("receipt", "receipt_sha256"),
    ):
        _verify_hash(root / str(final[key]), str(final[hash_key]), key)
    return {
        "gallery_count": int(index["gallery_size"]),
        "descriptor_dimension": int(payload["retriever"]["descriptor_dimension"]),
        "index_size_bytes": (index_dir / "index.faiss").stat().st_size,
    }


def _smoke_image(root: Path, case: dict[str, Any]) -> Image.Image:
    kind = str(case["kind"])
    if kind == "synthetic_rgb":
        color = tuple(int(value) for value in case["color"])
        if len(color) != 3 or any(value < 0 or value > 255 for value in color):
            raise RuntimeConfigError("synthetic smoke color must be an RGB triplet")
        return Image.new(
            "RGB",
            (int(case["width"]), int(case["height"])),
            color,
        )
    if kind != "manifest_image":
        raise RuntimeConfigError(f"unsupported production smoke kind: {kind}")
    manifest = read_manifest(root / str(case["manifest"]), allow_empty=False)
    selected = manifest.loc[manifest["id"].astype(str) == str(case["query_id"])]
    if len(selected) != 1:
        raise RuntimeConfigError(
            f"production smoke query {case['query_id']} must resolve exactly once"
        )
    image_path = Path(str(selected.iloc[0]["image_path"]))
    with Image.open(image_path) as source:
        return source.convert("RGB")


def _run_smoke_set(
    service: Any,
    *,
    root: Path,
    smoke_path: Path,
) -> list[dict[str, Any]]:
    try:
        payload = json.loads(smoke_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeConfigError(f"cannot read production smoke set: {smoke_path}") from exc
    if payload.get("schema_version") != 1 or not payload.get("cases"):
        raise RuntimeConfigError("production smoke set must be a non-empty schema-version-1 object")
    results: list[dict[str, Any]] = []
    for case in payload["cases"]:
        image = _smoke_image(root, dict(case))
        result = service.localize(image)
        expected = {str(value) for value in case["expected_statuses"]}
        if result["status"] not in expected:
            raise RuntimeError(
                f"production smoke {case['name']} returned {result['status']}, expected {sorted(expected)}"
            )
        prediction = result["prediction"]
        if result["status"] == "ok" and (
            prediction is None
            or not all(
                isinstance(prediction.get(key), int | float)
                for key in ("lat", "lon", "confidence")
            )
        ):
            raise RuntimeError(f"production smoke {case['name']} returned an invalid prediction")
        if not result["matches"]:
            raise RuntimeError(f"production smoke {case['name']} returned no reference matches")
        results.append(
            {
                "name": str(case["name"]),
                "status": result["status"],
                "confidence_score": (
                    None if prediction is None else prediction["confidence"]
                ),
                "match_count": len(result["matches"]),
                "timings_ms": {
                    key: result["diagnostics"].get(key)
                    for key in ("embedding_ms", "retrieval_ms", "query_ms")
                },
            }
        )
    return results


def run(config_path: Path) -> dict[str, Any]:
    config = FrozenRuntimeConfig.load(config_path, verify_index=True)
    artifacts = verify_artifacts(config)
    root = config.path.parent.parent
    smoke = dict(config.payload.get("runtime_smoke", {}))
    smoke_path = root / str(smoke.get("manifest", "configs/moscow_production_smoke_set.json"))
    expected_smoke_sha256 = str(smoke.get("manifest_sha256", ""))
    if not expected_smoke_sha256:
        raise RuntimeConfigError("production configuration does not bind a runtime smoke set")
    _verify_hash(smoke_path, expected_smoke_sha256, "runtime smoke set")
    for name in PIPELINE_ENVIRONMENT:
        os.environ.pop(name, None)
    os.environ["GEOSNAP_RUNTIME_CONFIG"] = str(config.path)
    started = time.perf_counter()
    service = create_localization_service()
    try:
        service.load()
        load_ms = (time.perf_counter() - started) * 1000.0
        readiness = dict(service.readiness())
        if not all(readiness.values()):
            raise RuntimeError(f"production service is not ready: {readiness}")
        smoke_results = _run_smoke_set(service, root=root, smoke_path=smoke_path)
    finally:
        service.close()
    return {
        "configuration": str(config.path),
        "configuration_sha256": config.sha256,
        "artifacts": artifacts,
        "readiness": readiness,
        "model_and_index_load_ms": load_ms,
        "smoke": smoke_results,
        "peak_rss_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/moscow_production_frozen.json"),
    )
    args = parser.parse_args()
    print(json.dumps(run(args.config), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
