"""Assemble reviewable offline runtime assets, without freezing or opening a test."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from ml.research.prepare_queries import ROOT
from ml.research.seal import SealError, sha256, write_once
from ml.retrieval.embedding_job import load_embedding_artifacts

CHECKPOINTS = {
    "sage-vitb": "8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e",
    "sage-vitl": "31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc",
}


def copy_verified(source, target):
    source, target = Path(source), Path(target)
    digest = sha256(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if sha256(target) != digest:
            raise SealError(f"existing runtime asset differs: {target}")
        return
    temporary = target.with_name(target.name + ".copying")
    if temporary.exists():
        if sha256(temporary) != digest:
            raise SealError("interrupted runtime copy differs; source left untouched")
        temporary.replace(target)
        return
    try:
        os.link(source.resolve(), temporary)
    except OSError:
        shutil.copyfile(source, temporary)
    if sha256(temporary) != digest:
        raise SealError("runtime asset copy failed verification")
    temporary.replace(target)


def source_contract(bundle):
    packages = [
        "torch",
        "torchvision",
        "numpy",
        "pillow",
        "faiss-cpu",
        "pandas",
        "pyarrow",
        "scikit-learn",
        "scipy",
        "threadpoolctl",
        "h3",
        "timm",
        "huggingface-hub",
    ]
    sources = sorted(Path("ml").rglob("*.py"))
    for path in sources:
        # Source copies are independent: later workspace edits cannot silently
        # alter the archived implementation through hard links.
        target = bundle / "assets/workspace_source" / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and sha256(target) != sha256(path):
            raise SealError("staged runtime sources changed; prepare a new preflight bundle")
        if not target.exists():
            shutil.copyfile(path, target)
    write_once(
        bundle / "runtime_contract.json",
        {
            "workspace_sources": {str(p): sha256(p) for p in sources},
            "dependencies": {name: importlib.metadata.version(name) for name in packages},
            "python": __import__("sys").version,
            "execution": "source hashes checked against archived copies before every worker; offline official SAGE",
        },
    )


def assemble(bundle, confidence_path, weight=0.5):
    if not 0 < weight < 1:
        raise ValueError("this preflight builder supports a positive B/L cosine mixture")
    bundle.mkdir(parents=True, exist_ok=True)
    if any(
        (bundle / name).exists()
        for name in ("architecture_frozen.json", "candidate_frozen.json", "runtime_contract.json")
    ):
        raise SealError("use a fresh preflight bundle; never overwrite frozen or staged policies")
    baseline_snapshot = json.loads((ROOT / "baseline_snapshot.json").read_text())
    for path, digest in baseline_snapshot["files"].items():
        # Snapshot entries are immutable production paths and digests.
        if sha256(Path(path)) != digest:
            raise SealError("production baseline changed")
    baseline_gallery = Path("data/evaluation/moscow_real_v4/gallery.parquet")
    union_gallery = ROOT / "gallery_expansion/gallery_union.parquet"
    copy_verified(baseline_gallery, bundle / "assets/baseline_gallery.parquet")
    copy_verified(union_gallery, bundle / "assets/candidate_gallery.parquet")
    source = Path(".cache/torch/hub/chenshunpeng_SAGE_c7d6241c4885526d99d6c78c158024fc2a37097c")
    for path in sorted(source.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            copy_verified(path, bundle / "assets/sage_source" / path.relative_to(source))
    index = Path("data/indexes/moscow_real_v4/sage-vitb")
    for filename in ("index.faiss", "id_mapping.json"):
        copy_verified(index / filename, bundle / "assets/baseline_index" / filename)
    copy_verified("configs/moscow_real_v3_confidence_model.json", bundle / "assets/baseline_confidence.json")
    copy_verified(confidence_path, bundle / "assets/candidate_confidence.json")
    ids = pd.read_parquet(union_gallery).id.tolist()
    # Runtime stores a plain ordered list; serialize it without changing its order.
    (bundle / "assets/candidate_descriptor_ids.json").write_text(json.dumps(ids) + "\n")
    models = {}
    for name, digest in CHECKPOINTS.items():
        checkpoint = Path(".cache/huggingface/hub/models--shunpeng--SAGE/blobs") / digest
        if sha256(checkpoint) != digest:
            raise SealError("official checkpoint changed")
        copy_verified(checkpoint, bundle / f"assets/{name}.pth")
        base_path = (
            Path("data/embeddings/moscow_real_v4/sage-vitb")
            if name == "sage-vitb"
            else Path("data/embeddings/moscow_research_v5") / name
        )
        base, base_ids, _, base_meta = load_embedding_artifacts(base_path)
        extra, extra_ids, _, extra_meta = load_embedding_artifacts(
            Path("data/embeddings/moscow_research_v5/gallery_union") / name
        )
        for key in ("checkpoint", "preprocessing", "descriptor_dim"):
            if base_meta["retriever"][key] != extra_meta["retriever"][key]:
                raise SealError("base and extra reference encoders differ")
        target = bundle / f"assets/{name}_union.npy"
        vectors = np.lib.format.open_memmap(
            target.with_suffix(".partial"), mode="w+", dtype=np.float32, shape=(len(ids), base.shape[1])
        )
        lookup = {rid: (False, i) for i, rid in enumerate(base_ids)}
        if set(base_ids) & set(extra_ids):
            raise SealError("duplicate gallery identity across descriptor sources")
        lookup.update({rid: (True, i) for i, rid in enumerate(extra_ids)})
        for i, identity in enumerate(ids):
            added, position = lookup[identity]
            vectors[i] = (extra if added else base)[position]
        vectors.flush()
        del vectors, base, extra
        target.with_suffix(".partial").replace(target)
        models[name] = {
            "source": "assets/sage_source",
            "checkpoint": f"assets/{name}.pth",
            "descriptors": f"assets/{name}_union.npy",
            "descriptor_ids": "assets/candidate_descriptor_ids.json",
            "weight": weight if name == "sage-vitl" else 1 - weight,
            "reference_provenance": {"base": base_meta["artifact_sha256"], "added": extra_meta["artifact_sha256"]},
        }
        print("assembled offline model assets", name, flush=True)
    common = {"runtime_contract": "runtime_contract.json", "device": "mps"}
    write_once(
        bundle / "baseline.json",
        common
        | {
            "name": "exact_production",
            "gallery": "assets/baseline_gallery.parquet",
            "models": {"sage-vitb": {k: v for k, v in models["sage-vitb"].items() if k in {"source", "checkpoint"}}},
            "search": "exact_faiss_production",
            "index": "assets/baseline_index/index.faiss",
            "index_ids": "assets/baseline_index/id_mapping.json",
            "coordinates": "geographic",
            "confidence": "assets/baseline_confidence.json",
            "confidence_kind": "production_logistic",
            "threshold": 0.9349250249145314,
        },
    )
    write_once(
        bundle / "candidate_architecture.json",
        common
        | {
            "name": "development_leader_preflight_not_selected",
            "gallery": "assets/candidate_gallery.parquet",
            "models": models,
            "search": "exact_cosine_fusion",
            "coordinates": "top1",
            "confidence": "assets/candidate_confidence.json",
            "confidence_kind": "portable_development",
            "threshold": None,
        },
    )
    source_contract(bundle)
    write_once(
        bundle / "assembly.json",
        {
            "status": "preflight_only_no_architecture_freeze",
            "production_snapshot_sha256": sha256(ROOT / "baseline_snapshot.json"),
            "artifacts": {str(p.relative_to(bundle)): sha256(p) for p in sorted(bundle.rglob("*")) if p.is_file()},
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--confidence", type=Path, required=True)
    parser.add_argument("--large-weight", type=float, default=0.5)
    args = parser.parse_args()
    assemble(args.bundle, args.confidence, args.large_weight)
