"""Pinned official DINOv2-BoQ, including its otherwise floating backbone dependency."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from ml.research.seal import sha256
from ml.retrieval.base import ModelLoadError
from ml.retrieval.torch_hub import OfficialTorchHubRetriever


class BoQRetriever(OfficialTorchHubRetriever):
    model_name = "boq"
    descriptor_dim = 12288
    repository = "amaralibey/Bag-of-Queries"
    revision = "1a4965ea7dfd9bd0dd846adf7a0e430f68101d12"
    backbone_revision = "7764ea0f912e53c92e82eb78a2a1631e92725fc8"
    entrypoint = "get_trained_boq"
    checkpoint = "https://github.com/amaralibey/Bag-of-Queries/releases/download/v1.0/dinov2_12288.pth"
    checkpoint_sha256 = "d72ee0ce899e2790be6cf6d1d57f75dfad0c76475bd2198e6dc0c3f0221b3583"
    preprocessing_version = "official-tensor-bicubic-resize322-then-imagenet-normalize-v1"

    @property
    def metadata(self):
        base = super().metadata
        return replace(
            base,
            extra=dict(base.extra)
            | {
                "checkpoint_sha256": self.checkpoint_sha256,
                "backbone_revision": self.backbone_revision,
                "backbone_initialization": "pretrained=False; complete official BoQ state_dict loaded strictly",
            },
        )

    def _build_transform(self):
        from torchvision import transforms as T

        return T.Compose(
            [
                T.ToTensor(),
                T.Resize((322, 322), interpolation=T.InterpolationMode.BICUBIC, antialias=True),
                T.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
            ]
        )

    def _hub_load(self, torch):
        root = Path("data/models/research_v5/boq").resolve()
        weights = root.parent / "boq_dinov2_12288.pth"
        if sha256(weights) != self.checkpoint_sha256:
            raise ModelLoadError("BoQ checkpoint changed")
        actual = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        if actual != self.revision or subprocess.run(["git", "-C", str(root), "diff", "--quiet", "HEAD"]).returncode:
            raise ModelLoadError("BoQ official source changed")
        if {"backbones", "boq"} & set(sys.modules):
            raise ModelLoadError("BoQ requires an isolated model process")
        if self.cache_dir is not None:
            torch.hub.set_dir(str(self.cache_dir))
        original_load, original_weights = torch.hub.load, torch.hub.load_state_dict_from_url

        def pinned_load(repo, model, *args, **kwargs):
            if repo == "facebookresearch/dinov2":
                return original_load(
                    f"facebookresearch/dinov2:{self.backbone_revision}",
                    model,
                    pretrained=False,
                    trust_repo=True,
                    skip_validation=True,
                    verbose=False,
                )
            return original_load(repo, model, *args, **kwargs)

        def pinned_weights(url, *args, **kwargs):
            if url != self.checkpoint:
                raise ModelLoadError("unexpected unpinned BoQ weights request")
            return torch.load(weights, map_location="cpu", weights_only=True)

        torch.hub.load, torch.hub.load_state_dict_from_url = pinned_load, pinned_weights
        try:
            return original_load(str(root), self.entrypoint, source="local", backbone_name="dinov2", output_dim=12288)
        finally:
            torch.hub.load, torch.hub.load_state_dict_from_url = original_load, original_weights

    def _forward_tensor(self, tensor, torch):
        with torch.inference_mode():
            result = self._model(tensor)[0]
        if result.ndim != 2 or result.shape[1] != self.descriptor_dim:
            raise ModelLoadError("unexpected official BoQ output")
        return result
