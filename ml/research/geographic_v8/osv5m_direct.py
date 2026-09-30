"""Pinned, official OSV-5M baseline on the fixed GeoSnap development queries."""
from __future__ import annotations

import argparse
import difflib
import importlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import torch
from PIL import Image

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, ROOT, image_inputs

CODE_REVISION = "4e6075387ecde4255410785ffb83830c9aa099f6"
MODEL_ID = "osv5m/baseline"
CLIP_ID = "laion/CLIP-ViT-L-14-DataComp.XL-s13B-b90K"
OUT = LOCAL / "osv5m"


def receipt(repo: str) -> dict:
    result = json.loads((LOCAL / "audit" / f"{repo.replace('/', '--')}_download.json").read_text())
    for filename, expected in result["files"].items():
        if digest(Path(result["path"]) / filename) != expected:
            raise RuntimeError(f"Pinned artifact changed: {repo}/{filename}")
    return result


def load():
    source = ROOT / "sources/osv5m"
    if subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() != CODE_REVISION:
        raise RuntimeError("OSV-5M source revision changed")
    checkpoint, clip = receipt(MODEL_ID), receipt(CLIP_ID)
    port = LOCAL / "runtime_sources/osv5m"
    port.mkdir(parents=True, exist_ok=True)
    for package in ("models", "utils"):
        shutil.copytree(source / package, port / package, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("._*", "__pycache__"))
    original = (source / "models/networks/backbones.py").read_text()
    needle = "self.clip = CLIPVisionModel.from_pretrained(path)"
    if original.count(needle) != 2:
        raise RuntimeError("OSV-5M source no longer has the audited CLIP constructors")
    # The published OSV checkpoint contains every CLIP tensor. Construct the exact
    # pinned architecture locally, then strictly load the complete official state.
    replacement = ("self.clip = CLIPVisionModel("
                   f"CLIPConfig.from_pretrained({clip['path']!r}, local_files_only=True).vision_config)")
    patched = original.replace("    CLIPVisionConfig,", "    CLIPVisionConfig,\n    CLIPConfig,").replace(needle, replacement)
    (port / "models/networks/backbones.py").write_text(patched)
    patch_path = LOCAL / "audit/osv5m_local_constructor.patch"
    patch_path.write_text("".join(difflib.unified_diff(original.splitlines(True), patched.splitlines(True),
        fromfile=f"upstream/{CODE_REVISION}/backbones.py", tofile="research/pinned_config/backbones.py")))
    sys.path.insert(0, str(port))
    for name in list(sys.modules):
        if name == "models" or name.startswith("models.") or name == "utils" or name.startswith("utils."):
            del sys.modules[name]
    from omegaconf import OmegaConf
    config = json.loads((Path(checkpoint["path"]) / "config.json").read_text())
    config["model"]["head"]["instance"]["quadtree_path"] = str(source / "utils/quadtree_10_1000.csv")
    Geolocalizer = importlib.import_module("models.huggingface").Geolocalizer
    model = Geolocalizer(config)
    state = torch.load(Path(checkpoint["path"]) / "pytorch_model.bin", map_location="cpu", weights_only=True, mmap=True)
    # Upstream defines these quadtree tensors as ordinary attributes while its
    # published checkpoint serializes them. Register the identical values so
    # the complete state, including geographic cell geometry, loads strictly.
    for name in ("cell_center", "cell_size_up", "cell_size_down"):
        current = getattr(model.head, name)
        if not torch.allclose(current, state[f"model.head.{name}"], atol=1e-7, rtol=0):
            raise RuntimeError(f"Official quadtree differs from checkpoint: {name}")
        delattr(model.head, name)
        model.head.register_buffer(name, current)
    model.load_state_dict(state, strict=True)
    model.eval().requires_grad_(False).float().to("mps")
    contract = {"code_repository": "https://github.com/gastruc/osv5m", "code_revision": CODE_REVISION,
        "code_sha256": digest(source / "models/huggingface.py"),
        "constructor_patch_sha256": digest(patch_path), "model": MODEL_ID,
        "model_revision": checkpoint["revision"], "model_files": checkpoint["files"],
        "CLIP_config_revision": clip["revision"], "CLIP_config_files": clip["files"],
        "quadtree_sha256": digest(source / "utils/quadtree_10_1000.csv"),
        "preprocessing": "official checkpoint config: resize224, center crop224, CLIP normalization",
        "inference": "official Geolocalizer.forward returns latitude/longitude in radians; convert to degrees",
        "strict_checkpoint_load": True, "quadtree_tensors_registered_as_buffers": True,
        "MPS_float64_quadtree_cast_to_float32": True,
        "architecture_changed": False, "device": "mps",
        "license": "official code/model card MIT; underlying DataComp training rights require separate production review",
        "training_data": "OSV-5M; LAION DataComp CLIP initialized and subsequently fully loaded from OSV checkpoint",
        "inference_query_GT": False}
    return model, contract


def run(smoke_only=False):
    if not (LOCAL / "baseline/report.json").exists():
        raise RuntimeError("Complete SAGE baseline first")
    torch.set_num_threads(2)
    model, contract = load()
    q = image_inputs()
    OUT.mkdir(exist_ok=True)
    save(OUT / "contract.json", contract)
    count = 2 if smoke_only else len(q)
    gps = np.empty((count, 2), np.float32)
    started = time.perf_counter()
    signature = __import__("hashlib").sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    for start in range(0, count, 8):
        stop = min(start + 8, count)
        path = OUT / "chunks" / f"{start:04d}.npz"
        path.parent.mkdir(exist_ok=True)
        if path.exists() and not smoke_only:
            with np.load(path, allow_pickle=False) as prior:
                if str(prior["signature"]) != signature or prior["query_ids"].tolist() != q.id.iloc[start:stop].tolist():
                    raise RuntimeError("OSV-5M resume contract changed")
                values = prior["gps"]
        else:
            images = []
            for row in q.iloc[start:stop].itertuples():
                if digest(row.image_path) != row.file_sha256:
                    raise RuntimeError("Query image SHA changed")
                with Image.open(row.image_path) as image:
                    images.append(model.transform(image.convert("RGB")))
            with torch.inference_mode():
                radians = model(torch.stack(images).to("mps"))
                values = np.degrees(radians.cpu().numpy())
            if not smoke_only:
                temp = path.with_suffix(".writing")
                with temp.open("wb") as f:
                    np.savez(f, signature=signature, query_ids=q.id.iloc[start:stop].to_numpy(str), gps=values)
                temp.replace(path)
        if values.shape != (stop-start, 2) or not np.isfinite(values).all():
            raise RuntimeError("Invalid direct GPS output")
        gps[start:stop] = values
        if start % 80 == 0:
            print(f"OSV-5M inference {stop}/{count}", flush=True)
    if smoke_only:
        print(json.dumps({"query_ids": q.id.iloc[:2].tolist(), "predictions_degrees": gps.tolist(),
                          "strict_checkpoint_load": True}), flush=True)
        return
    np.save(OUT / "predictions.npy", gps)
    save(OUT / "complete.json", {"contract": contract, "runtime_s": time.perf_counter()-started,
        "hashes": {"predictions.npy": digest(OUT / "predictions.npy")}})
    print("OSV-5M complete", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    run(parser.parse_args().smoke)
