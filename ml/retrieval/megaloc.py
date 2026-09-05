"""Official MegaLoc adapter."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

from .base import ModelLoadError, RetrieverMetadata
from .torch_hub import OfficialTorchHubRetriever

_HF_PATCH_LOCK = threading.Lock()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class MegaLocRetriever(OfficialTorchHubRetriever):
    """MegaLoc loaded from the authors' official Torch Hub entrypoint.

    The pinned repository revision delegates weight acquisition to the official
    ``gberton/MegaLoc`` Hugging Face model and validates the state dict.  A load
    error is surfaced; this adapter never creates an untrained model.
    """

    model_name = "megaloc"
    descriptor_dim = 8448
    repository = "gmberton/MegaLoc"
    revision = "5fe0dd697c4a70ba3e23607f6716ab3c606b16db"
    entrypoint = "get_trained_model"
    checkpoint_repository = "gberton/MegaLoc"
    checkpoint_revision = "37bb43d65dd6388d1578052de5eb0bcdceb497e7"
    checkpoint_sha256 = "d4f9f2bcb60018f91eb6a8e061ed054fd55654e10c2569cf13841ea986ffb4f8"
    checkpoint = (
        "huggingface:gberton/MegaLoc@"
        "37bb43d65dd6388d1578052de5eb0bcdceb497e7/model.safetensors"
    )
    preprocessing_version = "official-imagenet-normalize-then-resize-322-v1"
    resize_before_normalize = False

    @property
    def metadata(self) -> RetrieverMetadata:
        base = super().metadata
        return replace(
            base,
            extra=dict(base.extra)
            | {
                "checkpoint_repository": self.checkpoint_repository,
                "checkpoint_revision": self.checkpoint_revision,
                "checkpoint_sha256": self.checkpoint_sha256,
            },
        )

    def _hub_load(self, torch: Any) -> Any:
        """Pin and verify the official Hub checkpoint requested by MegaLoc."""

        try:
            import huggingface_hub
        except ImportError as exc:  # pragma: no cover - declared runtime dependency
            raise ModelLoadError("huggingface-hub is required to load MegaLoc") from exc

        with _HF_PATCH_LOCK:
            original_download = huggingface_hub.hf_hub_download

            def pinned_download(*args: Any, **kwargs: Any) -> Any:
                repo_id = kwargs.get("repo_id", args[0] if args else None)
                filename = kwargs.get("filename", args[1] if len(args) > 1 else None)
                if repo_id == self.checkpoint_repository:
                    kwargs["revision"] = self.checkpoint_revision
                path = original_download(*args, **kwargs)
                if repo_id == self.checkpoint_repository and filename == "model.safetensors":
                    actual = _sha256_file(Path(path))
                    if actual != self.checkpoint_sha256:
                        raise ModelLoadError(
                            "MegaLoc checkpoint SHA-256 mismatch; refusing incompatible weights"
                        )
                return path

            huggingface_hub.hf_hub_download = pinned_download
            try:
                return super()._hub_load(torch)
            finally:
                huggingface_hub.hf_hub_download = original_download
