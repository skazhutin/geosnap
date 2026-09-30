"""Audited G3 device-only port and official image/GPS embedding expressions.

Upstream hard-codes two CUDA allocations. The saved patch routes only those
allocations to the input device. No layer, tensor shape, weight or math changes.
The redundant base-CLIP weight download is omitted: the complete G3 checkpoint
must load strictly, including every frozen CLIP tensor. CPU/MPS parity is gated.
"""
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
from huggingface_hub import HfApi, snapshot_download
from PIL import Image

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import GALLERY, LOCAL, ROOT, image_inputs, initialize
from ml.research.geographic_v8.stage import STAGE

REVISION = "b4e3acf7c0ac51221f21b7877fefb4826715c9e2"
MODEL_REVISION = "12d886fc2a1e59b3b52821acee193084420409cc"


def load_model():
    initialize()
    download = json.loads((LOCAL / "audit/g3_download.json").read_text())
    if digest(download["path"]) != download["sha256"] or download["revision"] != MODEL_REVISION:
        raise RuntimeError("G3 checkpoint changed")
    root = ROOT / "sources/g3"
    if subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip() != REVISION:
        raise RuntimeError("G3 official code revision changed")
    port = LOCAL / "runtime_sources/g3"
    port.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root / "utils", port / "utils", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("._*", "__pycache__"))
    shutil.copy(root / "LICENSE", port / "LICENSE")
    pin_path = LOCAL / "audit/clip_revision.json"
    if not pin_path.exists():
        save(pin_path, {"repo": "openai/clip-vit-large-patch14", "revision": HfApi().model_info("openai/clip-vit-large-patch14").sha})
    pin = json.loads(pin_path.read_text())
    clip = snapshot_download(pin["repo"], revision=pin["revision"],
        allow_patterns=["config.json", "preprocessor_config.json", "tokenizer.json", "tokenizer_config.json",
                        "vocab.json", "merges.txt", "special_tokens_map.json"], cache_dir=str(LOCAL / "hf_cache"))
    source = (root / "utils/G3.py").read_text()
    if source.count(".to('cuda')") != 2:
        raise RuntimeError("Upstream device patch no longer matches audited source")
    patched = source.replace(".to('cuda')", ".to(input.device)")
    patched = patched.replace("from transformers import CLIPTokenizer", "from transformers import CLIPConfig, CLIPTokenizer")
    patched = patched.replace('CLIPModel.from_pretrained("openai/clip-vit-large-patch14")', f"CLIPModel(CLIPConfig.from_pretrained({clip!r}))")
    patched = patched.replace('"openai/clip-vit-large-patch14"', repr(clip))
    (port / "utils/G3.py").write_text(patched)
    (LOCAL / "audit/g3_device_port.patch").write_text("".join(difflib.unified_diff(source.splitlines(True), patched.splitlines(True),
        fromfile=f"upstream/{REVISION}/G3.py", tofile="research/device_port/G3.py")))
    sys.path.insert(0, str(port))
    cls = importlib.import_module("utils.G3").G3
    model = cls("cpu")
    state = torch.load(download["path"], map_location="cpu", weights_only=True, mmap=True)
    model.load_state_dict(state, strict=True, assign=True)
    model.eval().requires_grad_(False)
    return model, {"repo": "https://github.com/Applied-Machine-Learning-Lab/G3", "code_revision": REVISION,
        "model_revision": MODEL_REVISION, "checkpoint_sha256": download["sha256"],
        "upstream_source_sha256": digest(root / "utils/G3.py"), "port_sha256": digest(port / "utils/G3.py"),
        "patch_sha256": digest(LOCAL / "audit/g3_device_port.patch"), "clip_config": pin,
        "strict_all_tensor_loading": True, "architecture_changed": False,
        "preprocessing": "official CLIPImageProcessor 224; RGB; official G3 quick-use embedding expressions"}


def image_embedding(model, pixels):
    x = model.vision_projection_else_2(model.vision_projection(model.vision_model(pixels)[1]))
    return x / x.norm(p=2, dim=-1, keepdim=True)


def location_embedding(model, gps):
    x = model.location_encoder(gps)
    x = model.location_projection_else(x.reshape(len(gps), -1))
    return x / x.norm(p=2, dim=-1, keepdim=True)


def run():
    import pandas as pd

    torch.manual_seed(20260928)
    torch.set_num_threads(2)
    if not (LOCAL / "baseline/report.json").exists():
        raise RuntimeError("Baseline must finish before new model inference")
    out = LOCAL / "g3"
    out.mkdir(exist_ok=True)
    model, contract = load_model()
    q = image_inputs()
    g = pd.read_parquet(STAGE / "gallery.parquet", columns=["id", "lat", "lon"])
    coords, inverse = np.unique(g[["lat", "lon"]].to_numpy(), axis=0, return_inverse=True)
    # Fixed positions are selected without evaluation labels.
    pixels = model.vision_processor(images=[Image.open(p).convert("RGB") for p in q.image_path.iloc[:2]], return_tensors="pt")["pixel_values"]
    gps = torch.tensor(coords[:32], dtype=torch.float32)
    with torch.inference_mode():
        cpu_image = image_embedding(model, pixels).numpy()
        cpu_gps = location_embedding(model, gps).numpy()
        model.to("mps")
        mps_image = image_embedding(model, pixels.to("mps")).cpu().numpy()
        mps_gps = location_embedding(model, gps.to("mps")).cpu().numpy()
        reverse = image_embedding(model, pixels.flip(0).to("mps")).cpu().numpy()[::-1].copy()
    np.testing.assert_allclose(cpu_image, mps_image, atol=1e-4, rtol=1e-3)
    np.testing.assert_allclose(cpu_gps, mps_gps, atol=1e-4, rtol=1e-3)
    np.testing.assert_allclose(reverse, mps_image, atol=1e-5, rtol=1e-4)
    contract["parity"] = {"cpu_mps_image_max_abs": float(np.max(abs(cpu_image-mps_image))),
        "cpu_mps_gps_max_abs": float(np.max(abs(cpu_gps-mps_gps))), "batch_order_max_abs": float(np.max(abs(reverse-mps_image))),
        "verified": True, "cuda_numerics": "not tested on this host"}
    contract |= {"device": "mps", "query_batch": 8, "location_batch": 2048, "seed": 20260928,
                 "query_ids": q.id.tolist(), "image_hashes": q.file_sha256.tolist(), "gallery_sha256": digest(GALLERY),
                 "coordinate_deduplication": {"unique": len(coords), "references": len(g)}}
    signature = __import__("hashlib").sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    save(LOCAL / "audit/g3_runtime.json", contract)
    started = time.perf_counter()
    for kind, count, batch in [("query", len(q), 8), ("location", len(coords), 2048)]:
        pieces = []
        directory = out / kind
        directory.mkdir(exist_ok=True)
        t = time.perf_counter()
        for start in range(0, count, batch):
            path = directory / f"{start:06d}.npz"
            if path.exists():
                with np.load(path, allow_pickle=False) as chunk:
                    if str(chunk["signature"]) != signature:
                        raise RuntimeError("G3 resume contract changed")
                    values = chunk["vectors"]
            else:
                with torch.inference_mode():
                    if kind == "query":
                        images = []
                        for row in q.iloc[start:start+batch].itertuples():
                            if digest(row.image_path) != row.file_sha256:
                                raise RuntimeError("Query image changed")
                            with Image.open(row.image_path) as im:
                                images.append(im.convert("RGB"))
                        pix = model.vision_processor(images=images, return_tensors="pt")["pixel_values"].to("mps")
                        values = image_embedding(model, pix).cpu().numpy()
                    else:
                        values = location_embedding(model, torch.tensor(coords[start:start+batch], dtype=torch.float32, device="mps")).cpu().numpy()
                tmp = path.with_suffix(".writing")
                with tmp.open("wb") as f:
                    np.savez(f, signature=signature, vectors=values)
                tmp.replace(path)
            if not np.isfinite(values).all():
                raise RuntimeError("G3 produced nonfinite representations")
            pieces.append(values)
            if start % (batch * 10) == 0:
                print(f"G3 {kind}: {min(start+batch,count)}/{count}", flush=True)
        vectors = np.concatenate(pieces)
        np.save(out / f"{kind}.npy", vectors if kind == "query" else vectors[inverse])
        contract[f"{kind}_runtime_s"] = time.perf_counter()-t
    contract["total_runtime_s"] = time.perf_counter()-started
    save(out / "complete.json", {"contract": contract, "hashes": {n: digest(out / n) for n in ["query.npy", "location.npy"]}})
    print("G3 embeddings complete", flush=True)


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    run()
