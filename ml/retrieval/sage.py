"""Official SAGE ViT-B no-cross-image-encoder adapter."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

from .base import ModelLoadError, RetrieverMetadata
from .torch_hub import OfficialTorchHubRetriever

_CHECKPOINT_PATCH_LOCK = threading.Lock()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_load_legacy_numpy_checkpoint(torch: Any, path: Path, map_location: Any) -> Any:
    """Load the official checkpoint without enabling arbitrary pickle execution."""
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - transitive torch dependency
        raise ModelLoadError("NumPy is required to load the SAGE checkpoint") from exc

    numpy_globals = [
        (
            np.core.multiarray._reconstruct,
            "numpy.core.multiarray._reconstruct",
        ),
        np.ndarray,
        np.dtype,
        type(np.dtype(np.float32)),
        type(np.dtype(np.float64)),
    ]
    with torch.serialization.safe_globals(numpy_globals):
        return torch.load(path, map_location=map_location, weights_only=True)


class SageVitBRetriever(OfficialTorchHubRetriever):
    """Pinned independent-image SAGE variant with an explicitly MIT-tagged checkpoint."""

    model_name = "sage-vitb"
    descriptor_dim = 8448
    repository = "chenshunpeng/SAGE"
    revision = "c7d6241c4885526d99d6c78c158024fc2a37097c"
    entrypoint = "sage_vitb"
    checkpoint_repository = "shunpeng/SAGE"
    checkpoint_revision = "2a2ea9964cdbdfd2211e7c625064a9d5e4678245"
    checkpoint_filename = "SAGE_No-Encoder_Vit-B.pth"
    checkpoint_sha256 = "8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e"
    checkpoint = (
        "huggingface:shunpeng/SAGE@"
        "2a2ea9964cdbdfd2211e7c625064a9d5e4678245/SAGE_No-Encoder_Vit-B.pth"
    )
    preprocessing_version = "official-imagenet-normalize-then-resize-322-v1"
    resize_before_normalize = False
    hub_kwargs = {"pretrained": True, "progress": False}

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
                "variant": "ViT-B without cross-image encoder",
            },
        )

    def _hub_load(self, torch: Any) -> Any:
        try:
            import huggingface_hub
        except ImportError as exc:  # pragma: no cover - declared runtime dependency
            raise ModelLoadError("huggingface-hub is required to load SAGE") from exc

        with _CHECKPOINT_PATCH_LOCK:
            original_load_from_url = torch.hub.load_state_dict_from_url

            def pinned_load_from_url(
                url: str,
                *args: Any,
                map_location: Any = None,
                **kwargs: Any,
            ) -> Any:
                if not str(url).endswith(f"/{self.checkpoint_filename}"):
                    return original_load_from_url(
                        url,
                        *args,
                        map_location=map_location,
                        **kwargs,
                    )
                path = Path(
                    huggingface_hub.hf_hub_download(
                        repo_id=self.checkpoint_repository,
                        filename=self.checkpoint_filename,
                        revision=self.checkpoint_revision,
                    )
                )
                digest = _sha256_file(path)
                if digest != self.checkpoint_sha256:
                    raise ModelLoadError(
                        "SAGE checkpoint SHA-256 mismatch; refusing incompatible weights"
                    )
                return _safe_load_legacy_numpy_checkpoint(
                    torch,
                    path,
                    map_location or "cpu",
                )

            torch.hub.load_state_dict_from_url = pinned_load_from_url
            try:
                return super()._hub_load(torch)
            finally:
                torch.hub.load_state_dict_from_url = original_load_from_url
