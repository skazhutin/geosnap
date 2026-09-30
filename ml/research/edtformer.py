"""Pinned official EDTformer research adapter; isolated from production registry."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any

from ml.research.seal import sha256
from ml.retrieval.base import ModelLoadError
from ml.retrieval.sage import _safe_load_legacy_numpy_checkpoint
from ml.retrieval.torch_hub import OfficialTorchHubRetriever


class EDTformerRetriever(OfficialTorchHubRetriever):
    model_name = "edtformer"
    descriptor_dim = 4096
    repository = "Tong-Jin01/EDTformer"
    revision = "d0b85da3475e776b7baf87fbab5d5bfa6b50d723"
    entrypoint = "EDTformer"
    checkpoint = "https://github.com/Tong-Jin01/EDTformer/releases/download/v1.0.0/EDTformer.pth"
    checkpoint_sha256 = "57406a40806e4effb9320b593f0eeda02e493cf3aaf3b37d9cb152112bf1154d"
    preprocessing_version = "official-imagenet-normalize-then-resize-322-v1"
    resize_before_normalize = False

    def _hub_load(self, torch: Any) -> Any:
        root = Path("data/models/research_v5/edtformer").resolve()
        checkpoint = root.parent / "EDTformer.pth"
        if sha256(checkpoint) != self.checkpoint_sha256:
            raise ModelLoadError("EDTformer checkpoint changed")
        revision = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        if revision != self.revision or subprocess.run(["git", "-C", str(root), "diff", "--quiet", "HEAD"]).returncode:
            raise ModelLoadError("EDTformer source does not match pinned revision")
        if "backbone" in sys.modules:
            raise ModelLoadError(
                "EDTformer requires an isolated model process to avoid official module name collisions"
            )
        sys.path.insert(0, str(root))
        try:
            spec = importlib.util.spec_from_file_location("_geosnap_edtformer_network", root / "network.py")
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            model = module.VPRNet()
            import numpy as np

            with torch.serialization.safe_globals([(np.core.multiarray.scalar, "numpy.core.multiarray.scalar")]):
                weights = _safe_load_legacy_numpy_checkpoint(torch, checkpoint, "cpu")["model_state_dict"]
            # Official hub uses DataParallel; stripping its wrapper preserves all learned tensors.
            model.load_state_dict({k.removeprefix("module."): v for k, v in weights.items()}, strict=True)
            return model
        finally:
            sys.path.remove(str(root))


if __name__ == "__main__":
    import json
    import time

    import numpy as np
    import pandas as pd
    import torch

    torch.set_num_threads(2)
    model = EDTformerRetriever(device="cpu", allow_device_fallback=False, batch_size=3)
    model.load()
    rows = pd.read_parquet("data/evaluation/moscow_real_v4/development_queries.parquet").image_path.iloc[:3].tolist()
    started = time.monotonic()
    single = model.embed_batch(rows[:1])
    batch = model.embed_batch(rows)
    reordered = model.embed_batch(rows[::-1])[::-1]
    result = {
        "model": model.metadata.to_dict() if hasattr(model.metadata, "to_dict") else str(model.metadata),
        "checkpoint_sha256": model.checkpoint_sha256,
        "shape": list(batch.shape),
        "finite": bool(np.isfinite(batch).all()),
        "norms": np.linalg.norm(batch, axis=1).tolist(),
        "max_batch_component_change": float(np.max(np.abs(single[0] - batch[0]))),
        "max_order_component_change": float(np.max(np.abs(batch - reordered))),
        "seconds_for_7_cpu_images": time.monotonic() - started,
    }
    if not np.allclose(single[0], batch[0], atol=1e-5) or not np.allclose(batch, reordered, atol=1e-5):
        raise RuntimeError("EDTformer independent descriptor contract failed")
    Path("data/evaluation/moscow_research_v5/model_audit/edtformer_smoke.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(json.dumps(result, indent=2))
