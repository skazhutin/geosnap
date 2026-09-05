"""Base adapter for official PyTorch Hub VPR checkpoints."""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .base import (
    BaseRetriever,
    ImageInput,
    ModelDependencyError,
    ModelLoadError,
    RetrieverMetadata,
)
from .device import device_candidates, import_torch
from .image_io import load_rgb_image

logger = logging.getLogger(__name__)


class OfficialTorchHubRetriever(BaseRetriever):
    """Common lifecycle for a pinned, official Torch Hub model."""

    repository: str
    revision: str
    entrypoint: str
    checkpoint: str
    preprocessing_version: str
    image_size: tuple[int, int] = (322, 322)
    resize_before_normalize: bool = True
    hub_kwargs: dict[str, Any] = {}

    def __init__(
        self,
        *,
        device: str = "auto",
        allow_device_fallback: bool = True,
        batch_size: int = 16,
        cache_dir: str | Path | None = None,
    ) -> None:
        super().__init__()
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.preferred_device = device
        self.allow_device_fallback = allow_device_fallback
        self.batch_size = batch_size
        env_cache = os.environ.get("GEOSNAP_MODEL_CACHE")
        self.cache_dir = Path(cache_dir or env_cache).expanduser() if (cache_dir or env_cache) else None
        self._model: Any | None = None
        self._transform: Any | None = None

    @property
    def metadata(self) -> RetrieverMetadata:
        return RetrieverMetadata(
            model_name=self.model_name,
            descriptor_dim=self.descriptor_dim,
            device=self.device,
            preprocessing=self.preprocessing_version,
            checkpoint=self.checkpoint,
            repository=f"https://github.com/{self.repository}",
            revision=self.revision,
            normalized=True,
            extra={"image_size": list(self.image_size), "loader": "torch.hub"},
        )

    def _build_transform(self) -> Any:
        try:
            from torchvision import transforms as transforms
        except (ImportError, OSError) as exc:
            raise ModelDependencyError(
                "torchvision is required for official VPR preprocessing"
            ) from exc
        resize = transforms.Resize(
            list(self.image_size),
            interpolation=transforms.InterpolationMode.BILINEAR,
            antialias=True,
        )
        normalize = transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225],
        )
        operations = [resize, transforms.ToTensor(), normalize]
        if not self.resize_before_normalize:
            operations = [transforms.ToTensor(), normalize, resize]
        return transforms.Compose(operations)

    def _hub_load(self, torch: Any) -> Any:
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            torch.hub.set_dir(str(self.cache_dir))
        repo_spec = f"{self.repository}:{self.revision}"
        try:
            return torch.hub.load(
                repo_spec,
                self.entrypoint,
                source="github",
                trust_repo=True,
                force_reload=False,
                verbose=False,
                # The official revisions are pinned commit SHAs, not mutable
                # branch names. Torch Hub's fork validation only enumerates
                # branches/tags and otherwise rejects a valid SHA.
                skip_validation=True,
                **dict(self.hub_kwargs),
            )
        except Exception as exc:  # torch hub wraps network/import/state-dict errors variably
            raise ModelLoadError(
                f"failed to load pretrained {self.model_name} from official "
                f"{repo_spec} entrypoint {self.entrypoint!r}: {exc}. "
                "Check network access, model dependencies, and GEOSNAP_MODEL_CACHE. "
                "No random-weight fallback was used."
            ) from exc

    def _forward_tensor(self, tensor: Any, torch: Any) -> Any:
        assert self._model is not None
        with torch.inference_mode():
            output = self._model(tensor)
        if not hasattr(output, "ndim") or output.ndim != 2:
            shape = getattr(output, "shape", type(output).__name__)
            raise ModelLoadError(f"{self.model_name} returned invalid smoke output {shape}")
        return output

    def _activate_device(self, model: Any, torch: Any) -> str:
        assert self._transform is not None
        failures: list[str] = []
        smoke_image = Image.new("RGB", self.image_size[::-1], color=(127, 127, 127))
        smoke = self._transform(smoke_image).unsqueeze(0)
        for candidate in device_candidates(
            self.preferred_device, allow_fallback=self.allow_device_fallback
        ):
            try:
                model = model.to(candidate)
                self._model = model.eval()
                output = self._forward_tensor(smoke.to(candidate), torch)
                if output.shape != (1, self.descriptor_dim):
                    raise ModelLoadError(
                        f"expected smoke descriptor shape (1, {self.descriptor_dim}), "
                        f"got {tuple(output.shape)}"
                    )
                if not bool(torch.isfinite(output).all().item()):
                    raise ModelLoadError("smoke descriptor contains non-finite values")
                return candidate
            except Exception as exc:  # device/operator failures must trigger safe fallback
                failures.append(f"{candidate}: {type(exc).__name__}: {exc}")
                logger.warning("%s inference smoke failed on %s: %s", self.model_name, candidate, exc)
                try:
                    model = model.to("cpu")
                except Exception:  # best effort before the next device attempt
                    pass
        raise ModelLoadError(
            f"{self.model_name} loaded but inference smoke failed on every candidate device: "
            + " | ".join(failures)
        )

    def load(self) -> OfficialTorchHubRetriever:
        if self.is_loaded:
            return self
        torch = import_torch()
        self._transform = self._build_transform()
        model = self._hub_load(torch)
        try:
            model.eval()
        except Exception as exc:
            raise ModelLoadError(f"loaded object for {self.model_name} is not a torch model") from exc
        self._device = self._activate_device(model, torch)
        self._loaded = True
        logger.info(
            "loaded VPR retriever",
            extra={"model": self.model_name, "device": self.device, "revision": self.revision},
        )
        return self

    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        self.require_loaded()
        if not images:
            return np.empty((0, self.descriptor_dim), dtype=np.float32)
        torch = import_torch()
        assert self._transform is not None
        chunks: list[np.ndarray] = []
        for start in range(0, len(images), self.batch_size):
            batch_images = images[start : start + self.batch_size]
            tensors = [self._transform(load_rgb_image(image)) for image in batch_images]
            tensor = torch.stack(tensors, dim=0).to(self.device)
            output = self._forward_tensor(tensor, torch)
            chunks.append(output.detach().float().cpu().numpy())
        matrix = np.concatenate(chunks, axis=0)
        return self.validate_descriptors(matrix, expected_rows=len(images), normalize=True)

    def close(self) -> None:
        model = self._model
        if model is not None:
            try:
                model.to("cpu")
            except Exception:
                pass
        self._model = None
        self._transform = None
        super().close()
