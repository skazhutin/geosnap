"""Official SAGE ViT-B no-cross-image-encoder adapter."""

from __future__ import annotations

import hashlib
import os
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
        artifact_root = os.environ.get("GEOSNAP_ARTIFACT_DIR")
        source_value = os.environ.get("GEOSNAP_SAGE_SOURCE_DIR")
        checkpoint_value = os.environ.get("GEOSNAP_SAGE_CHECKPOINT")
        if artifact_root:
            root = Path(artifact_root).expanduser()
            source_value = source_value or str(root / "models/sage/source")
            checkpoint_value = checkpoint_value or str(root / "models/sage/SAGE_No-Encoder_Vit-B.pth")
        source_dir = Path(source_value).expanduser().resolve() if source_value else None
        checkpoint_path = Path(checkpoint_value).expanduser().resolve() if checkpoint_value else None
        if source_value and (source_dir is None or not (source_dir / "hubconf.py").is_file()):
            raise ModelLoadError("verified local SAGE source is missing")
        if checkpoint_value and (checkpoint_path is None or not checkpoint_path.is_file()):
            raise ModelLoadError("verified local SAGE checkpoint is missing")

        huggingface_hub: Any | None = None
        if checkpoint_path is None:
            try:
                import huggingface_hub as imported_huggingface_hub
            except ImportError as exc:  # pragma: no cover - development-only network path
                raise ModelLoadError("huggingface-hub is required to download SAGE") from exc
            huggingface_hub = imported_huggingface_hub

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
                path = checkpoint_path
                if path is None:
                    assert huggingface_hub is not None
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
                if source_dir is not None:
                    try:
                        return torch.hub.load(
                            str(source_dir),
                            self.entrypoint,
                            source="local",
                            trust_repo=True,
                            **dict(self.hub_kwargs),
                        )
                    except Exception as exc:
                        raise ModelLoadError(
                            "failed to load SAGE from verified local source; no network fallback was used"
                        ) from exc
                return super()._hub_load(torch)
            finally:
                torch.hub.load_state_dict_from_url = original_load_from_url
