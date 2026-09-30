"""Independent-query contextual reranking; no labels or cross-query attention."""

from __future__ import annotations

import numpy as np


def apply_context(base, queries, references, *, checkpoint, depth, mixing):
    import torch

    if not 0 < mixing <= 1 or depth not in {30, 100}:
        raise ValueError("unregistered context mixing or depth")
    if queries.shape != (len(base), 8448) or references.shape != (base.shape[1], 8448):
        raise ValueError("context requires aligned 8448-D SAGE L descriptors")
    torch.set_num_threads(2)
    encoder = torch.nn.TransformerEncoder(
        torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=0.1, batch_first=False
        ),
        num_layers=2,
    )
    encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    result = base.copy()
    selected = np.argsort(-base, axis=1, kind="stable")[:, :depth]
    with torch.inference_mode():
        for i, positions in enumerate(selected):
            features = np.concatenate([queries[i : i + 1], references[positions]])
            output = encoder(torch.from_numpy(features).view(depth + 1, 11, 768)).flatten(1)
            output = torch.nn.functional.normalize(output, dim=1)
            new = (output[0] @ output[1:].T).numpy()
            old = base[i, positions]
            aligned = (new - new.mean()) / max(float(new.std()), 1e-6) * max(float(old.std()), 1e-6) + old.mean()
            blended = (1 - mixing) * old + mixing * aligned
            floor = float(np.partition(base[i], -depth - 1)[-depth - 1])
            if blended.min() <= floor:
                blended = blended + (floor - blended.min() + 1e-6)
            result[i, positions] = blended
    return result
