"""Official DINOv2 + SALAD adapter."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

from .base import RetrieverMetadata
from .torch_hub import OfficialTorchHubRetriever

_HUB_PATCH_LOCK = threading.Lock()


class DinoV2SaladRetriever(OfficialTorchHubRetriever):
    """DINOv2-SALAD loaded from the authors' official v1.0.0 checkpoint."""

    model_name = "dinov2-salad"
    descriptor_dim = 8448  # 64 * 128 cluster descriptor + 256 token descriptor
    repository = "serizba/salad"
    revision = "6aede13a3f6c25750bf7fde10209c06cb73060bb"
    entrypoint = "dinov2_salad"
    checkpoint = "github:serizba/salad/releases/download/v1.0.0/dino_salad.ckpt"
    checkpoint_sha256 = "6b3f1720954293e83da6966c5cfcfc6713200d7fefadcca76fc51aeb80b3cada"
    preprocessing_version = "official-bilinear-resize-322-imagenet-v1"
    resize_before_normalize = True
    hub_kwargs = {"pretrained": True}
    dinov2_repository = "facebookresearch/dinov2"
    dinov2_revision = "7764ea0f912e53c92e82eb78a2a1631e92725fc8"

    @property
    def metadata(self) -> RetrieverMetadata:
        base = super().metadata
        return replace(
            base,
            extra=dict(base.extra)
            | {
                "nested_repository": f"https://github.com/{self.dinov2_repository}",
                "nested_revision": self.dinov2_revision,
                "checkpoint_sha256": self.checkpoint_sha256,
            },
        )

    def _verify_checkpoint(self, torch: Any) -> None:
        hub_root = self.cache_dir or Path(torch.hub.get_dir())
        checkpoint_path = Path(hub_root) / "checkpoints" / "dino_salad.ckpt"
        if not checkpoint_path.is_file():
            raise RuntimeError("official SALAD checkpoint was not persisted in the Torch Hub cache")
        digest = hashlib.sha256()
        with checkpoint_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != self.checkpoint_sha256:
            raise RuntimeError("SALAD checkpoint SHA-256 mismatch; refusing incompatible weights")

    def _hub_load(self, torch: Any) -> Any:
        """Pin SALAD's otherwise mutable nested DINOv2 Torch Hub request."""

        with _HUB_PATCH_LOCK:
            original_load = torch.hub.load

            def pinned_load(repo_or_dir: str, model: str, *args: Any, **kwargs: Any) -> Any:
                if repo_or_dir == self.dinov2_repository:
                    repo_or_dir = f"{self.dinov2_repository}:{self.dinov2_revision}"
                    kwargs.setdefault("source", "github")
                    kwargs.setdefault("trust_repo", True)
                    kwargs.setdefault("skip_validation", True)
                return original_load(repo_or_dir, model, *args, **kwargs)

            torch.hub.load = pinned_load
            try:
                model = super()._hub_load(torch)
                self._verify_checkpoint(torch)
                return model
            finally:
                torch.hub.load = original_load
