"""Experimental retrievers stay outside the frozen production registry."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

from ml.research.boq import BoQRetriever
from ml.research.edtformer import EDTformerRetriever
from ml.retrieval import create_retriever
from ml.retrieval.sage import SageVitBRetriever


class SageVitLRetriever(SageVitBRetriever):
    model_name = "sage-vitl"
    entrypoint = "sage_vitl"
    checkpoint_filename = "SAGE_No-Encoder_Vit-L.pth"
    checkpoint_sha256 = "31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc"
    checkpoint = "huggingface:shunpeng/SAGE@2a2ea9964cdbdfd2211e7c625064a9d5e4678245/SAGE_No-Encoder_Vit-L.pth"

    @property
    def metadata(self):
        base = super().metadata
        return replace(base, extra=dict(base.extra) | {"variant": "ViT-L without cross-image encoder"})


class SageFullFeaturesRetriever(SageVitBRetriever):
    model_name = "sage-full-features"
    entrypoint = "sage"
    checkpoint_filename = "SAGE.pth"
    checkpoint_sha256 = "9e6d7b7cf042754ace3a467159f5db44da4475815640ff85ee1c0505cb550b01"
    checkpoint = "huggingface:shunpeng/SAGE@2a2ea9964cdbdfd2211e7c625064a9d5e4678245/SAGE.pth#pre-encoder"

    @property
    def metadata(self):
        base = super().metadata
        return replace(
            base,
            extra=dict(base.extra)
            | {
                "variant": "official full SAGE checkpoint, independent pre-encoder features",
                "contextual_encoder": "reserved for per-query retrieval-context reranking; never mixes evaluation queries",
            },
        )

    def _hub_load(self, torch):
        import numpy as np

        previous = os.environ.get("GEOSNAP_SAGE_CHECKPOINT")
        os.environ["GEOSNAP_SAGE_CHECKPOINT"] = previous or str(Path("data/models/research_v5/SAGE_full.pth").resolve())
        try:
            with torch.serialization.safe_globals([(np.core.multiarray.scalar, "numpy.core.multiarray.scalar")]):
                model = super()._hub_load(torch)
            model.crossimage_encoder = False
            return model
        finally:
            if previous is None:
                os.environ.pop("GEOSNAP_SAGE_CHECKPOINT", None)
            else:
                os.environ["GEOSNAP_SAGE_CHECKPOINT"] = previous


def research_retriever(name, **kwargs):
    if name == "boq":
        return BoQRetriever(**kwargs)
    if name == "edtformer":
        return EDTformerRetriever(**kwargs)
    if name == "sage-vitl":
        return SageVitLRetriever(**kwargs)
    if name == "sage-full-features":
        return SageFullFeaturesRetriever(**kwargs)
    return create_retriever(name, **kwargs)
