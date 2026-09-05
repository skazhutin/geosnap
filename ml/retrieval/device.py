"""Torch device selection with capability-aware fallback ordering."""

from __future__ import annotations

import sys
from typing import Any

from .base import ModelDependencyError


def import_torch() -> Any:
    if sys.platform == "darwin" and "faiss" in sys.modules and "torch" not in sys.modules:
        raise ModelDependencyError(
            "loading PyTorch into a process that already imported FAISS is unsafe with "
            "common macOS wheels; use the process-isolated LocalizationService"
        )
    try:
        import torch
    except (ImportError, OSError) as exc:
        raise ModelDependencyError(
            "PyTorch is required for production VPR inference. Install a compatible "
            "torch and torchvision build; no untrained fallback is available."
        ) from exc
    return torch


def device_candidates(preferred: str = "auto", *, allow_fallback: bool = True) -> list[str]:
    """Return usable candidates in CUDA -> MPS -> CPU order.

    Availability is only the first gate.  The retriever subsequently performs a
    real forward-pass smoke test because some model operators can still fail on
    MPS/CUDA.
    """

    torch = import_torch()
    preferred = preferred.lower().strip()
    if preferred not in {"auto", "cuda", "mps", "cpu"}:
        raise ValueError("device must be one of: auto, cuda, mps, cpu")

    available: list[str] = []
    if bool(torch.cuda.is_available()):
        available.append("cuda")
    mps = getattr(torch.backends, "mps", None)
    if mps is not None and bool(mps.is_built()) and bool(mps.is_available()):
        available.append("mps")
    available.append("cpu")

    if preferred == "auto":
        return available
    if preferred in available:
        if allow_fallback and preferred != "cpu":
            return [preferred, "cpu"]
        return [preferred]
    if allow_fallback:
        return ["cpu"]
    raise ModelDependencyError(f"requested torch device {preferred!r} is not available")
