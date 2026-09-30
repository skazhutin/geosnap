"""Audit downloaded images and seal prospective query manifests without inference."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import h3
import imagehash
import pandas as pd
from PIL import Image, ImageOps

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.ingestion.schema import write_manifest
from ml.research.prepare_queries import ROOT, distant_mask
from ml.research.seal import sha256, write_once
from ml.retrieval.image_io import load_rgb_image


def image_identity(path):
    image = load_rgb_image(path)
    rotations = [
        image,
        image.transpose(Image.Transpose.ROTATE_90),
        image.transpose(Image.Transpose.ROTATE_180),
        image.transpose(Image.Transpose.ROTATE_270),
    ]
    hashes = [int(str(imagehash.phash(im)), 16) for im in rotations]
    hashes += [int(str(imagehash.phash(ImageOps.mirror(im))), 16) for im in rotations]
    return {
        "file_sha256": sha256(Path(path)),
        "perceptual_hash": f"{hashes[0]:016x}",
        "width": image.width,
        "height": image.height,
    }, hashes


def run():
    bundle = ROOT / "prospective"
    if (bundle / "seal.json").exists():
        raise ValueError("benchmark already sealed; rebuilding/replacing it is forbidden")
    assignment = json.loads((bundle / "assignment.json").read_text())
    history_paths = sorted(Path("data/evaluation").glob("moscow_real*/*.parquet"))
    history = pd.concat([pd.read_parquet(p) for p in history_paths], ignore_index=True).drop_duplicates("id")
    index = _PerceptualHashIndex(4)
    exact = set()
    counter = 0
    for row in history.itertuples():
        if pd.notna(row.file_sha256):
            exact.add(str(row.file_sha256))
        if pd.notna(row.perceptual_hash):
            index.add(counter, int(str(row.perceptual_hash), 16))
            counter += 1
    # The union of both independently planned reference arms participates in
    # duplicate auditing before the query seal. Neither arm sees query labels.
    reference_path = ROOT / "gallery_expansion/download_union.parquet"
    references = pd.read_parquet(reference_path)
    valid_reference = []
    reference_counts = Counter()
    for row in references.to_dict("records"):
        try:
            identity, hashes = image_identity(row["image_path"])
        except (OSError, ValueError, RuntimeError):
            reference_counts["unavailable_or_undecodable"] += 1
            continue
        if identity["file_sha256"] in exact:
            reference_counts["exact_duplicate"] += 1
            continue
        # Do not let repeated adjacent reference views swamp gallery ablations.
        if index.matches(hashes[0]):
            reference_counts["near_duplicate"] += 1
            continue
        row.update(identity)
        row["h3_coarse"] = h3.latlng_to_cell(float(row["lat"]), float(row["lon"]), 6)
        row["h3_fine"] = h3.latlng_to_cell(float(row["lat"]), float(row["lon"]), 9)
        valid_reference.append(row)
    valid = pd.DataFrame(valid_reference)
    # Cross-arm duplicates are handled independently per arm; the union audit
    # must not favor whichever arm appears first. All retained union images
    # still exclude duplicate queries before sealing.
    for row in valid.itertuples():
        exact.add(str(row.file_sha256))
        index.add(counter, int(str(row.perceptual_hash), 16))
        counter += 1
    write_manifest(valid, ROOT / "gallery_expansion/audited_union.parquet")
    reference_counts["retained"] = len(valid)
    selected = {}
    reports = {}
    splits = {}
    for stage in ("development", "calibration", "final"):
        entry = assignment["private_manifests"][stage]
        if sha256(Path(entry["path"])) != entry["sha256"]:
            raise ValueError("assigned query metadata changed")
        assigned = pd.read_parquet(entry["path"])
        kept = []
        counts = Counter()
        for row in assigned.to_dict("records"):
            try:
                identity, hashes = image_identity(row["image_path"])
            except (OSError, ValueError, RuntimeError):
                counts["unavailable_or_undecodable"] += 1
                continue
            if identity["file_sha256"] in exact:
                counts["exact_duplicate"] += 1
                continue
            if any(index.matches(v) for v in hashes):
                counts["near_duplicate_including_rotations_and_mirror"] += 1
                continue
            row.update(identity)
            row["h3_coarse"] = h3.latlng_to_cell(float(row["lat"]), float(row["lon"]), 6)
            row["h3_fine"] = h3.latlng_to_cell(float(row["lat"]), float(row["lon"]), 9)
            row["evaluation_area_h3"] = row["evaluation_geo_group_id"]
            kept.append(row)
            exact.add(identity["file_sha256"])
            for value in hashes:
                index.add(counter, value)
                counter += 1
        current = pd.DataFrame(kept)
        if len(current) < 100:
            raise ValueError(
                f"{stage} has fewer than 100 audited independent queries; do not declare a strong benchmark"
            )
        selected[stage] = current
        target = bundle / ("development.parquet" if stage == "development" else f"private/{stage}.parquet")
        write_manifest(current, target)
        target.chmod(0o600)
        splits[stage] = {"path": str(target.relative_to(bundle)), "sha256": sha256(target), "count": len(current)}
        reports[stage] = {
            "assigned": len(assigned),
            "retained": len(current),
            "exclusions": dict(counts),
            "providers": dict(Counter(current.source)),
            "geographic_groups": current.evaluation_geo_group_id.nunique(),
            "photographers": dict(Counter(current.attribution)),
        }
    for a, left in selected.items():
        for b, right in selected.items():
            if a != b and not distant_mask(left, right, 250).all():
                raise ValueError("cross-split geographic embargo violated")
    report = {
        "assignment_sha256": sha256(bundle / "assignment.json"),
        "reference_counts": dict(reference_counts),
        "query_audit": reports,
        "pHash_hamming_threshold": 4,
        "rotation_and_mirror_aware_query_dedup": True,
        "query_quality_filter": "decodable only; no blur, brightness, matchability or positive-distance selection",
        "ground_truth": "provider geotags; camera GPS/SfM uncertainty remains; not survey-grade ground truth",
        "model_inference_performed": False,
    }
    write_once(bundle / "audit.json", report)
    write_once(
        bundle / "seal.json",
        {
            "status": "sealed_before_inference",
            "protocol_sha256": assignment["protocol_sha256"],
            "assignment_sha256": sha256(bundle / "assignment.json"),
            "audit_sha256": sha256(bundle / "audit.json"),
            "splits": splits,
            "maximum_openings_per_stage": 1,
            "bound_reference_acquisition": sha256(ROOT / "gallery_expansion/plan.json"),
        },
    )
    # Publish only aggregate counts, never private query coordinates or outcomes.
    print(
        json.dumps(
            {
                "status": "sealed_before_inference",
                "counts": {s: r["retained"] for s, r in reports.items()},
                "seal_sha256": sha256(bundle / "seal.json"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    run()
