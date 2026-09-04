from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from ml.retrieval import (
    DinoV2SaladRetriever,
    MegaLocRetriever,
    SageVitBRetriever,
    SelaVPRPlusPlusBaseRetriever,
    SelaVPRPlusPlusRerankRetriever,
)
from ml.retrieval.testing import DeterministicFixtureRetriever


def test_fixture_retriever_requires_explicit_test_guard() -> None:
    with pytest.raises(RuntimeError, match="TEST ONLY"):
        DeterministicFixtureRetriever()


def test_fixture_retriever_is_deterministic_normalized_and_not_exported() -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()
    image = Image.new("RGB", (31, 47), (220, 30, 10))
    first = retriever.embed_batch([image, image])
    second = retriever.embed_query(image)
    assert first.shape == (2, retriever.descriptor_dim)
    np.testing.assert_array_equal(first[0], first[1])
    np.testing.assert_array_equal(first[0], second)
    np.testing.assert_allclose(np.linalg.norm(first, axis=1), 1.0, atol=1e-6)
    assert retriever.metadata.extra["production_eligible"] is False
    import ml.retrieval as package

    assert not hasattr(package, "DeterministicFixtureRetriever")


def _run_torch_script(script: str) -> None:
    completed = subprocess.run(
        [sys.executable, "-c", script],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr


def test_official_adapter_uses_inference_mode_batches_and_enforces_normalization() -> None:
    if importlib.util.find_spec("torch") is None:
        pytest.skip("torch is not installed")
    _run_torch_script(
        """
import numpy as np
import torch
from PIL import Image
from ml.retrieval import MegaLocRetriever

class FakeModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.batch_sizes = []
        self.grad_enabled = []
    def forward(self, value):
        self.batch_sizes.append(int(value.shape[0]))
        self.grad_enabled.append(torch.is_grad_enabled())
        return torch.ones((value.shape[0], 8448), device=value.device) * 3.0

model = FakeModel()
retriever = MegaLocRetriever(device="cpu", batch_size=2)
retriever._hub_load = lambda _torch: model
retriever.load()
images = [Image.new("RGB", (40, 30), (index * 20, 40, 80)) for index in range(5)]
descriptors = retriever.embed_batch(images)
assert descriptors.shape == (5, 8448)
np.testing.assert_allclose(np.linalg.norm(descriptors, axis=1), 1.0, atol=1e-5)
assert model.batch_sizes == [1, 2, 2, 1]
assert model.grad_enabled == [False, False, False, False]
assert retriever.device == "cpu"
"""
    )


def test_official_load_failure_is_explicit_and_has_no_random_fallback() -> None:
    _run_torch_script(
        """
import torch
from ml.retrieval import MegaLocRetriever, ModelLoadError

def fail(*args, **kwargs):
    raise OSError("network unavailable")

torch.hub.load = fail
retriever = MegaLocRetriever(device="cpu")
try:
    retriever.load()
except ModelLoadError as exc:
    assert "No random-weight fallback" in str(exc)
else:
    raise AssertionError("expected ModelLoadError")
assert retriever.is_loaded is False
"""
    )


def test_production_adapters_point_to_pinned_official_sources() -> None:
    mega = MegaLocRetriever(device="cpu")
    salad = DinoV2SaladRetriever(device="cpu")
    sage = SageVitBRetriever(device="cpu")
    sela = SelaVPRPlusPlusBaseRetriever(device="cpu")
    sela_rerank = SelaVPRPlusPlusRerankRetriever(device="cpu")
    assert mega.repository == "gmberton/MegaLoc"
    assert mega.entrypoint == "get_trained_model"
    assert len(mega.revision) == 40
    assert mega.checkpoint.startswith("huggingface:gberton/MegaLoc@")
    assert len(mega.checkpoint_revision) == 40
    assert len(mega.checkpoint_sha256) == 64
    assert salad.repository == "serizba/salad"
    assert salad.entrypoint == "dinov2_salad"
    assert len(salad.revision) == 40
    assert len(salad.dinov2_revision) == 40
    assert len(salad.checkpoint_sha256) == 64
    assert salad.metadata.extra["nested_revision"] == salad.dinov2_revision
    assert "v1.0.0/dino_salad.ckpt" in salad.checkpoint
    assert sage.repository == "chenshunpeng/SAGE"
    assert sage.entrypoint == "sage_vitb"
    assert len(sage.revision) == 40
    assert sage.checkpoint.startswith("huggingface:shunpeng/SAGE@")
    assert len(sage.checkpoint_revision) == 40
    assert len(sage.checkpoint_sha256) == 64
    assert sage.metadata.extra["variant"] == "ViT-B without cross-image encoder"
    assert mega.descriptor_dim == salad.descriptor_dim == sage.descriptor_dim == 8448
    assert sela.repository == sela_rerank.repository == "Lu-Feng/SelaVPRplusplus"
    assert sela.revision == sela_rerank.revision == "56bd921cbd3d53e9c5f91d0aafff147f95fb362a"
    assert sela.descriptor_dim == 2048
    assert sela_rerank.descriptor_dim == 2560
    assert sela_rerank.metadata.extra["binary_descriptor_dim"] == 512
    assert len(sela.checkpoint_sha256) == len(sela_rerank.checkpoint_sha256) == 64


def test_sage_production_loader_uses_local_verified_source_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "models/sage/source"
    source.mkdir(parents=True)
    (source / "hubconf.py").write_text("# pinned test source\n", encoding="utf-8")
    checkpoint = tmp_path / "models/sage/SAGE_No-Encoder_Vit-B.pth"
    checkpoint.write_bytes(b"verified-test-checkpoint")
    monkeypatch.setenv("GEOSNAP_ARTIFACT_DIR", str(tmp_path))

    calls: list[tuple[str, str, dict[str, object]]] = []

    class FakeHub:
        load_state_dict_from_url = staticmethod(lambda *args, **kwargs: None)

        @staticmethod
        def load(repo: str, entrypoint: str, **kwargs: object) -> str:
            calls.append((repo, entrypoint, kwargs))
            return "local-model"

    class FakeTorch:
        hub = FakeHub()

    retriever = SageVitBRetriever(device="cpu")
    assert retriever._hub_load(FakeTorch()) == "local-model"
    assert calls == [
        (
            str(source.resolve()),
            "sage_vitb",
            {"source": "local", "trust_repo": True, "pretrained": True, "progress": False},
        )
    ]


def test_selavprplusplus_rerank_branch_contract() -> None:
    binary = np.zeros((2, 512), dtype=np.float32)
    floating = np.zeros((2, 2048), dtype=np.float32)
    binary[0, 0] = binary[1, 1] = 1.0
    floating[0, 0] = floating[1, 1] = 1.0
    storage = np.concatenate((binary, floating), axis=1) / np.sqrt(2.0)
    split_binary, split_floating = SelaVPRPlusPlusRerankRetriever.split_descriptor(storage)
    np.testing.assert_allclose(split_binary, binary)
    np.testing.assert_allclose(split_floating, floating)
    with pytest.raises(ValueError, match="dimension mismatch"):
        SelaVPRPlusPlusRerankRetriever.split_descriptor(np.ones((1, 100)))


def test_salad_rewrites_mutable_nested_dinov2_hub_dependency() -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    class FakeHub:
        def set_dir(self, _path: str) -> None:
            pass

        def load(self, repo: str, model: str, *args, **kwargs):
            calls.append((repo, model, dict(kwargs)))
            if repo.startswith("serizba/salad:"):
                return self.load("facebookresearch/dinov2", "dinov2_vitb14")
            return "pinned-dinov2"

    class FakeTorch:
        hub = FakeHub()

    retriever = DinoV2SaladRetriever(device="cpu")
    retriever._verify_checkpoint = lambda _torch: None
    assert retriever._hub_load(FakeTorch()) == "pinned-dinov2"
    nested_repo, nested_model, nested_kwargs = calls[1]
    assert nested_repo == f"facebookresearch/dinov2:{retriever.dinov2_revision}"
    assert nested_model == "dinov2_vitb14"
    assert nested_kwargs["skip_validation"] is True


@pytest.mark.skipif(
    os.environ.get("GEOSNAP_RUN_REAL_MODEL_TESTS") != "1",
    reason="downloads the official MegaLoc checkpoint; opt in explicitly",
)
def test_real_megaloc_checkpoint_smoke() -> None:
    _run_torch_script(
        """
import numpy as np
from PIL import Image
from ml.retrieval import MegaLocRetriever
retriever = MegaLocRetriever(device="auto", batch_size=1).load()
descriptor = retriever.embed_query(Image.new("RGB", (322, 322), (100, 120, 140)))
assert descriptor.shape == (8448,)
assert np.isclose(np.linalg.norm(descriptor), 1.0, atol=1e-4)
"""
    )
