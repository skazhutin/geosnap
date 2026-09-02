"""Official SelaVPR++ ViT-B global and two-stage retrieval adapters."""

from __future__ import annotations

import hashlib
import os
import threading
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

from .base import ModelLoadError, RetrieverMetadata, l2_normalize
from .torch_hub import OfficialTorchHubRetriever

_CHECKPOINT_LOCK = threading.Lock()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verified_weights(torch: Any, *, url: str, sha256: str, cache_dir: Path) -> Any:
    """Download one official asset, verify it, then use PyTorch's restricted loader."""

    checkpoint_dir = cache_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    path = checkpoint_dir / url.rsplit("/", 1)[-1]
    if not path.is_file() or _sha256_file(path) != sha256:
        temporary = checkpoint_dir / f".{path.name}.{uuid.uuid4().hex}.download"
        try:
            torch.hub.download_url_to_file(url, str(temporary), progress=False)
            if _sha256_file(temporary) != sha256:
                raise ModelLoadError("SelaVPR++ checkpoint SHA-256 mismatch")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    if _sha256_file(path) != sha256:
        raise ModelLoadError("SelaVPR++ checkpoint SHA-256 mismatch")
    try:
        import numpy as np

        numpy_globals = [
            (np.core.multiarray._reconstruct, "numpy.core.multiarray._reconstruct"),
            (np.core.multiarray.scalar, "numpy.core.multiarray.scalar"),
            np.ndarray,
            np.dtype,
            type(np.dtype(np.float32)),
            type(np.dtype(np.float64)),
        ]
        with torch.serialization.safe_globals(numpy_globals):
            return torch.load(path, map_location="cpu", weights_only=True)
    except Exception as exc:
        raise ModelLoadError(
            "official SelaVPR++ checkpoint could not be read by the restricted weights-only loader"
        ) from exc


class _SelaVPRPlusPlusBase(OfficialTorchHubRetriever):
    repository = "Lu-Feng/SelaVPRplusplus"
    revision = "56bd921cbd3d53e9c5f91d0aafff147f95fb362a"
    entrypoint = "SelaVPRplusplus"
    preprocessing_version = "official-imagenet-normalize-then-resize-322-v1"
    resize_before_normalize = False
    image_size = (322, 322)
    checkpoint_release = "SelaVPR++"

    @property
    def metadata(self) -> RetrieverMetadata:
        base = super().metadata
        return replace(
            base,
            extra=dict(base.extra)
            | {
                "checkpoint_sha256": self.checkpoint_sha256,
                "checkpoint_release": self.checkpoint_release,
                "backbone": "dinov2-base",
            },
        )

    def _hub_load(self, torch: Any) -> Any:
        cache_root = self.cache_dir or Path(torch.hub.get_dir())
        with _CHECKPOINT_LOCK:
            original = torch.hub.load_state_dict_from_url

            def verified(url: str, *_args: Any, **_kwargs: Any) -> Any:
                if url != self.checkpoint_url:
                    raise ModelLoadError(
                        f"SelaVPR++ requested unexpected checkpoint URL {url!r}"
                    )
                return _verified_weights(
                    torch,
                    url=url,
                    sha256=self.checkpoint_sha256,
                    cache_dir=cache_root,
                )

            torch.hub.load_state_dict_from_url = verified
            try:
                return super()._hub_load(torch)
            finally:
                torch.hub.load_state_dict_from_url = original


class SelaVPRPlusPlusBaseRetriever(_SelaVPRPlusPlusBase):
    """Independent 2,048-D floating-point SelaVPR++ global descriptor."""

    model_name = "selavprplusplus-base"
    descriptor_dim = 2048
    checkpoint_url = (
        "https://github.com/Lu-Feng/SelaVPRplusplus/releases/download/"
        "SelaVPR%2B%2B/SelaVPRplusplus_base.pth"
    )
    checkpoint_sha256 = "b048490dbd1c27dee67fce6faaec7bec267d19a044c85877af94b8588e596a62"
    checkpoint = (
        "github:Lu-Feng/SelaVPRplusplus/releases/download/"
        "SelaVPR%2B%2B/SelaVPRplusplus_base.pth"
    )
    hub_kwargs = {
        "backbone": "dinov2-base",
        "aggregation": "gem",
        "hashing": False,
        "rerank": False,
    }


class SelaVPRPlusPlusRerankRetriever(_SelaVPRPlusPlusBase):
    """Official binary-retrieval + float-reranking descriptor pair.

    The persisted vector concatenates a normalized 512-D binary descriptor and
    the normalized 2,048-D float descriptor, then normalizes the concatenation.
    Evaluation/serving must split and renormalize the two branches; treating the
    2,560-D storage representation as a single cosine descriptor is forbidden.
    """

    model_name = "selavprplusplus-base-rerank"
    descriptor_dim = 2560
    binary_descriptor_dim = 512
    float_descriptor_dim = 2048
    checkpoint_url = (
        "https://github.com/Lu-Feng/SelaVPRplusplus/releases/download/"
        "SelaVPR%2B%2B/SelaVPRplusplus_base_rerank.pth"
    )
    checkpoint_sha256 = "da31138202b9a746916588ecd97499a56bae304e61444a50d6f34761377cdcbf"
    checkpoint = (
        "github:Lu-Feng/SelaVPRplusplus/releases/download/"
        "SelaVPR%2B%2B/SelaVPRplusplus_base_rerank.pth"
    )
    hub_kwargs = {
        "backbone": "dinov2-base",
        "aggregation": "gem",
        "hashing": True,
        "rerank": True,
    }

    @property
    def metadata(self) -> RetrieverMetadata:
        base = super().metadata
        return replace(
            base,
            extra=dict(base.extra)
            | {
                "binary_descriptor_dim": self.binary_descriptor_dim,
                "float_descriptor_dim": self.float_descriptor_dim,
                "storage_contract": "concat_then_l2_normalize; split_and_renormalize",
                "retrieval": "binary branch cosine/Hamming-equivalent",
                "reranking": "float branch cosine",
            },
        )

    def _forward_tensor(self, tensor: Any, torch: Any) -> Any:
        assert self._model is not None
        with torch.inference_mode():
            output = self._model(tensor)
        if not isinstance(output, (tuple, list)) or len(output) != 3:
            raise ModelLoadError("SelaVPR++ rerank checkpoint returned an invalid branch tuple")
        _continuous_hash, binary, floating = output
        if binary.ndim != 2 or floating.ndim != 2:
            raise ModelLoadError("SelaVPR++ rerank branches must be matrices")
        if binary.shape[1] != self.binary_descriptor_dim or floating.shape[1] != self.float_descriptor_dim:
            raise ModelLoadError("SelaVPR++ rerank branch dimensions do not match the contract")
        binary = torch.nn.functional.normalize(binary.float(), p=2, dim=-1)
        floating = torch.nn.functional.normalize(floating.float(), p=2, dim=-1)
        return torch.nn.functional.normalize(torch.cat((binary, floating), dim=-1), p=2, dim=-1)

    @classmethod
    def split_descriptor(cls, matrix: Any) -> tuple[Any, Any]:
        import numpy as np

        array = np.asarray(matrix, dtype=np.float32)
        if array.shape[-1] != cls.descriptor_dim:
            raise ValueError("SelaVPR++ two-stage descriptor dimension mismatch")
        return (
            l2_normalize(array[..., : cls.binary_descriptor_dim]),
            l2_normalize(array[..., cls.binary_descriptor_dim :]),
        )
