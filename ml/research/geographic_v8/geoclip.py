"""Official GeoCLIP image/GPS embeddings on the frozen Moscow reference coordinates."""
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
from torch.nn.functional import normalize

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, ROOT, frames, image_inputs

CODE_REVISION = "a32dff094064c2e2b91122fc626f622c0a38719c"
CLIP_ID = "openai/clip-vit-large-patch14"
CLIP_REVISION = "32bd64288804d66eefd0ccbe215aa642df71cc41"
OUT = LOCAL / "geoclip"


def load():
    source = ROOT / "sources/geoclip"
    if subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() != CODE_REVISION:
        raise RuntimeError("Official GeoCLIP source revision changed")
    clip_path = LOCAL / f"hf_cache/models--openai--clip-vit-large-patch14/snapshots/{CLIP_REVISION}"
    clip_files = ("config.json", "preprocessor_config.json", "pytorch_model.bin")
    if not all((clip_path / f).exists() for f in clip_files):
        raise RuntimeError("Pinned OpenAI CLIP base is incomplete")
    port = LOCAL / "runtime_sources/geoclip"
    shutil.copytree(source / "geoclip", port / "geoclip", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("._*", "__pycache__"))
    original = (source / "geoclip/model/image_encoder.py").read_text()
    if original.count(f'"{CLIP_ID}"') != 2:
        raise RuntimeError("GeoCLIP upstream base-model routing changed")
    patched = original.replace(f'"{CLIP_ID}"', repr(str(clip_path)))
    (port / "geoclip/model/image_encoder.py").write_text(patched)
    patch_path = LOCAL / "audit/geoclip_base_routing.patch"
    patch_path.write_text("".join(difflib.unified_diff(original.splitlines(True), patched.splitlines(True),
        fromfile=f"upstream/{CODE_REVISION}/image_encoder.py", tofile="research/pinned_clip/image_encoder.py")))
    sys.path.insert(0, str(port))
    for name in list(sys.modules):
        if name == "geoclip" or name.startswith("geoclip."):
            del sys.modules[name]
    GeoCLIP = importlib.import_module("geoclip.model.GeoCLIP").GeoCLIP
    model = GeoCLIP(from_pretrained=True).eval().requires_grad_(False)
    model.image_encoder.to("mps")
    bundled = source / "geoclip/model/weights"
    contract = {"code_repository": "https://github.com/VicenteVivan/geo-clip", "code_revision": CODE_REVISION,
        "code_sha256": digest(source / "geoclip/model/GeoCLIP.py"),
        "routing_patch_sha256": digest(patch_path), "model": "GeoCLIP official bundled pretrained",
        "model_revision": CODE_REVISION, "bundle_hashes": {p.name: digest(p) for p in bundled.glob("*.pth")},
        "base_model": CLIP_ID, "base_revision": CLIP_REVISION,
        "base_hashes": {f: digest(clip_path / f) for f in clip_files},
        "preprocessing": "official OpenAI CLIP AutoProcessor at pinned revision, RGB",
        "scoring": "official normalized 512d image/GPS dot product; logit_scale does not affect rank",
        "training_data": "MP-16, 4.7M image/GPS pairs; OpenAI CLIP base",
        "license": "GeoCLIP official code MIT; model/data chain production suitability unassessed",
        "device": {"image": "mps", "location": "cpu"}, "inference_query_GT": False}
    return model, contract


def run(smoke_only=False):
    if not (LOCAL / "baseline/report.json").exists():
        raise RuntimeError("Complete SAGE baseline first")
    torch.set_num_threads(2)
    model, contract = load()
    q, g = frames()
    inputs = image_inputs()
    if q.id.tolist() != inputs.id.tolist():
        raise RuntimeError("Query inference/evaluation order mismatch")
    OUT.mkdir(exist_ok=True)
    save(OUT / "contract.json", contract)
    n = 2 if smoke_only else len(q)
    unique_gps, inverse = np.unique(g[["lat", "lon"]].to_numpy(), axis=0, return_inverse=True)
    if smoke_only:
        unique_gps, inverse = unique_gps[:2], inverse
    with torch.inference_mode():
        location = np.empty((len(unique_gps), 512), np.float32)
        loc_started = time.perf_counter()
        for start in range(0, len(unique_gps), 512):
            stop = min(start + 512, len(unique_gps))
            values = model.location_encoder(torch.tensor(unique_gps[start:stop], dtype=torch.float32))
            location[start:stop] = normalize(values, dim=1).numpy()
            if start % 16384 == 0:
                print(f"GeoCLIP GPS {stop}/{len(unique_gps)}", flush=True)
        location_runtime = time.perf_counter() - loc_started
        query = np.empty((n, 512), np.float32)
        query_started = time.perf_counter()
        for start in range(0, n, 8):
            stop = min(start + 8, n)
            images = []
            for row in inputs.iloc[start:stop].itertuples():
                if digest(row.image_path) != row.file_sha256:
                    raise RuntimeError("Query image SHA changed")
                with Image.open(row.image_path) as image:
                    images.append(image.convert("RGB"))
            pixels = model.image_encoder.image_processor(images=images, return_tensors="pt")["pixel_values"]
            query[start:stop] = normalize(model.image_encoder(pixels.to("mps")), dim=1).cpu().numpy()
            if start % 80 == 0:
                print(f"GeoCLIP images {stop}/{n}", flush=True)
        query_runtime = time.perf_counter() - query_started
    if smoke_only:
        print(json.dumps({"query_shape": query.shape, "location_shape": location.shape,
                          "query_norms": np.linalg.norm(query, axis=1).tolist()}), flush=True)
        return
    np.save(OUT / "location.npy", location[inverse])
    np.save(OUT / "query.npy", query)
    save(OUT / "complete.json", {"contract": contract | {"query_runtime_s": query_runtime,
        "location_runtime_s": location_runtime, "unique_location_count": len(unique_gps)},
        "hashes": {f: digest(OUT / f) for f in ("location.npy", "query.npy")}})
    print("GeoCLIP complete", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    run(parser.parse_args().smoke)
