"""Pinned official PLONK_OSV_5M direct inference with an audited MPS device port."""
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

CODE_REVISION = "76d46410910c9dfec9e19ed371450ebc7051cdf3"
MODEL_ID = "nicolas-dufour/PLONK_OSV_5M"
STREETCLIP_ID = "geolocal/StreetCLIP"
SEED = 20260928
OUT = LOCAL / "plonk"


def load():
    source = ROOT / "sources/plonk"
    if subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() != CODE_REVISION:
        raise RuntimeError("Official PLONK source revision changed")
    receipts = {}
    for repo in (MODEL_ID, STREETCLIP_ID):
        receipt = json.loads((LOCAL / "audit" / f"{repo.replace('/', '--')}_download.json").read_text())
        for name, sha in receipt["files"].items():
            if digest(Path(receipt["path"]) / name) != sha:
                raise RuntimeError(f"Pinned {repo} artifact changed: {name}")
        receipts[repo] = receipt
    port = LOCAL / "runtime_sources/plonk"
    port.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source / "plonk", port / "plonk", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("._*", "__pycache__"))
    original = (source / "plonk/pipe.py").read_text()
    if original.count(f'"{STREETCLIP_ID}"') != 2 or original.count("Plonk.from_pretrained(model_path)") != 1:
        raise RuntimeError("PLONK artifact-routing patch differs from audited source")
    patched = original.replace(f'"{STREETCLIP_ID}"', repr(receipts[STREETCLIP_ID]["path"]))
    patched = patched.replace("Plonk.from_pretrained(model_path)",
                              f"Plonk.from_pretrained({receipts[MODEL_ID]['path']!r})")
    (port / "plonk/pipe.py").write_text(patched)
    patch = LOCAL / "audit/plonk_artifact_port.patch"
    patch.write_text("".join(difflib.unified_diff(original.splitlines(True), patched.splitlines(True),
        fromfile=f"upstream/{CODE_REVISION}/pipe.py", tofile="research/pinned_artifacts/pipe.py")))
    sys.path.insert(0, str(port))
    for name in list(sys.modules):
        if name == "plonk" or name.startswith("plonk."):
            del sys.modules[name]
    Pipeline = importlib.import_module("plonk.pipe").PlonkPipeline
    # The official model ID still chooses official architecture/sampler defaults;
    # only its checkpoint and StreetCLIP dependencies resolve to pinned local files.
    pipe = Pipeline(MODEL_ID, device=torch.device("mps"))
    pipe.cond_preprocessing.emb_model.to("mps").eval().requires_grad_(False)
    pipe.cond_preprocessing.device = torch.device("mps")
    pipe.network.eval().requires_grad_(False)
    contract = {"code_repository": "https://github.com/nicolas-dufour/plonk", "code_revision": CODE_REVISION,
        "code_sha256": digest(source / "plonk/pipe.py"), "port_sha256": digest(port / "plonk/pipe.py"),
        "port_patch_sha256": digest(patch), "model": MODEL_ID,
        "model_revision": receipts[MODEL_ID]["revision"],
        "model_files": receipts[MODEL_ID]["files"], "StreetCLIP_revision": receipts[STREETCLIP_ID]["revision"],
        "StreetCLIP_files": receipts[STREETCLIP_ID]["files"],
        "license": "PLONK card MIT; StreetCLIP dependency CC-BY-NC-4.0; research-only pending chain audit",
        "training_data": "OSV-5M, StreetCLIP pretrained data not fully certified",
        "sampler": "official riemannian_flow_sampler, 250 default steps, sigmoid scheduler(-7,3,1), cfg=0",
        "input_preprocessing": "official StreetCLIP CLIPProcessor, RGB",
        "device": "mps", "seed": SEED, "architecture_changed": False,
        "inference_query_GT": False}
    return pipe, contract


def images_at(q, start, stop):
    values = []
    for row in q.iloc[start:stop].itertuples():
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError("Query image SHA changed")
        with Image.open(row.image_path) as im:
            values.append(im.convert("RGB"))
    return values


def sample_from_embedding(pipe, embedding, noise):
    with torch.inference_mode():
        batch = {"y": noise.to(pipe.device), "emb": embedding.to(pipe.device)}
        output = pipe.sampler(pipe.model, batch, conditioning_keys="emb", scheduler=pipe.scheduler,
                              cfg_rate=0, generator=None)
        return np.degrees(pipe.postprocessing(output).cpu().numpy())


def smoke(pipe, q):
    images = images_at(q, 0, 2)
    noise = np.random.default_rng(SEED).standard_normal((2, 3), dtype=np.float32)
    x = torch.from_numpy(noise)
    with torch.inference_mode():
        official = pipe(images, x_N=x)
        embedding = pipe.cond_preprocessing({"img": images})["emb"]
        cached_path = sample_from_embedding(pipe, embedding, x)
    np.testing.assert_allclose(official, cached_path, atol=1e-5, rtol=1e-5)
    if embedding.shape != (2, 1024) or not np.isfinite(official).all():
        raise RuntimeError("PLONK smoke failed")
    return {"query_ids": q.id.iloc[:2].tolist(), "official_vs_cached_max_abs_degrees": float(np.max(abs(official-cached_path))),
            "pipeline_semantics_verified": True, "sample_predictions_degrees": official.tolist()}


def run(smoke_only=False):
    if not (LOCAL / "baseline/report.json").exists():
        raise RuntimeError("Complete baseline before PLONK")
    torch.set_num_threads(2)
    pipe, contract = load()
    q = image_inputs()
    check = smoke(pipe, q)
    contract["smoke"] = check
    OUT.mkdir(exist_ok=True)
    save(OUT / "contract.json", contract)
    if smoke_only:
        print(json.dumps(check), flush=True)
        return
    batch_size = 8
    noise = np.random.default_rng(SEED).standard_normal((len(q), 3), dtype=np.float32)
    values = np.empty((len(q), 2), np.float32)
    evidence = np.empty((len(q), 1024), np.float32)
    started = time.perf_counter()
    signature = __import__("hashlib").sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    for start in range(0, len(q), batch_size):
        stop = min(start+batch_size, len(q))
        path = OUT / "chunks" / f"{start:04d}.npz"
        path.parent.mkdir(exist_ok=True)
        if path.exists():
            with np.load(path, allow_pickle=False) as saved:
                if str(saved["signature"]) != signature or saved["query_ids"].tolist() != q.id.iloc[start:stop].tolist():
                    raise RuntimeError("PLONK resume contract changed")
                gps, embedding = saved["gps"], saved["embedding"]
        else:
            images = images_at(q, start, stop)
            with torch.inference_mode():
                embedding = pipe.cond_preprocessing({"img": images})["emb"]
                gps = sample_from_embedding(pipe, embedding, torch.from_numpy(noise[start:stop]))
                embedding = embedding.cpu().numpy()
            temp = path.with_suffix(".writing")
            with temp.open("wb") as f:
                np.savez(f, signature=signature, query_ids=q.id.iloc[start:stop].to_numpy(str), gps=gps, embedding=embedding)
            temp.replace(path)
        if gps.shape != (stop-start, 2) or not np.isfinite(gps).all() or not np.isfinite(embedding).all():
            raise RuntimeError("PLONK invalid output")
        values[start:stop] = gps
        evidence[start:stop] = embedding
        if start % 80 == 0:
            print(f"PLONK inference {stop}/{len(q)}", flush=True)
    np.save(OUT / "predictions.npy", values)
    np.save(OUT / "query_embeddings.npy", evidence)
    save(OUT / "complete.json", {"contract": contract, "runtime_s": time.perf_counter()-started,
        "hashes": {name: digest(OUT / name) for name in ("predictions.npy", "query_embeddings.npy")}})
    print("PLONK complete", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    run(parser.parse_args().smoke)
