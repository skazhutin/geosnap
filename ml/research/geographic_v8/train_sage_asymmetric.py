"""Reference-only SAGE-L query-backbone adaptation against frozen gallery vectors."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F
from PIL import Image

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.common import LOCAL, frames
from ml.research.retrievers import SageVitLRetriever

OUT = LOCAL / "sage_adaptation"
CHECKPOINT = Path(".cache/huggingface/hub/models--shunpeng--SAGE/blobs/31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc")
SOURCE = Path(".cache/torch/hub/chenshunpeng_SAGE_c7d6241c4885526d99d6c78c158024fc2a37097c")
SEED = 20260928
BATCH = 4
EPOCHS = 2
TEMP = .07
ANCHOR_WEIGHT = .2


def load_model(train=False):
    if digest(CHECKPOINT) != "31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc":
        raise RuntimeError("Pinned SAGE-L checkpoint changed")
    if not (SOURCE / "hubconf.py").exists():
        raise RuntimeError("Pinned SAGE-L source missing")
    os.environ["GEOSNAP_SAGE_SOURCE_DIR"] = str(SOURCE.resolve())
    os.environ["GEOSNAP_SAGE_CHECKPOINT"] = str(CHECKPOINT.resolve())
    retriever = SageVitLRetriever(device="mps", allow_device_fallback=False, batch_size=BATCH)
    retriever.load()
    model = retriever._model
    if train:
        for parameter in model.backbone.model.blocks[-1].parameters():
            parameter.requires_grad = True
    model.eval()
    return model, retriever._transform


def images(rows, g, transform):
    values = []
    for item in rows:
        row = g.iloc[item["anchor"]]
        if bool(row.is_pano):
            raise RuntimeError("Panorama view needs a separate transform; excluded from training")
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError("Reference image hash changed")
        with Image.open(row.image_path) as image:
            values.append(transform(image.convert("RGB")))
    return torch.stack(values).to("mps")


def targets(rows, lookup, vectors):
    positions = np.asarray([[r["positive"], *r["negatives"]] for r in rows], int)
    anchor = np.asarray([r["anchor"] for r in rows], int)
    return (torch.from_numpy(vectors[[lookup[i] for i in positions.flat]].reshape(len(rows), 9, 8448)).to("mps"),
            torch.from_numpy(vectors[[lookup[i] for i in anchor]]).to("mps"))


def objective(model, images_batch, positive_negative, base_anchor):
    query = F.normalize(model(images_batch), dim=1)
    logits = torch.einsum("bd,bkd->bk", query, positive_negative) / TEMP
    contrastive = F.cross_entropy(logits, torch.zeros(len(query), dtype=torch.long, device="mps"))
    conservation = (1 - (query * base_anchor).sum(1)).mean()
    return contrastive + ANCHOR_WEIGHT * conservation, contrastive.detach(), conservation.detach()


def evaluate_reference(model, records, g, transform, lookup, vectors):
    model.eval()
    losses, correctly_ordered = [], []
    with torch.inference_mode():
        for start in range(0, len(records), BATCH):
            batch = records[start:start+BATCH]
            x = images(batch, g, transform)
            pn, anchor = targets(batch, lookup, vectors)
            query = F.normalize(model(x), dim=1)
            logits = torch.einsum("bd,bkd->bk", query, pn) / TEMP
            losses.extend(F.cross_entropy(logits, torch.zeros(len(batch), dtype=torch.long, device="mps"),
                                         reduction="none").cpu().tolist())
            correctly_ordered.extend((logits.argmax(1) == 0).cpu().tolist())
    return {"reference_heldout_contrastive_loss": float(np.mean(losses)),
            "reference_heldout_positive_top1": float(np.mean(correctly_ordered)),
            "heldout_anchor_count": len(records)}


def save_trainable(model, path):
    state = {name: param.detach().cpu() for name, param in model.named_parameters() if param.requires_grad}
    temp = path.with_suffix(".writing")
    torch.save(state, temp)
    temp.replace(path)
    return {"sha256": digest(path), "trainable_tensor_count": len(state),
            "trainable_parameter_count": sum(t.numel() for t in state.values())}


def apply_trainable(model, path):
    state = torch.load(path, map_location="cpu", weights_only=True)
    expected = {name for name, param in model.named_parameters() if param.requires_grad}
    if set(state) != expected:
        raise RuntimeError("Trainable SAGE tensor keys changed")
    for name, param in model.named_parameters():
        if name in state:
            param.data.copy_(state[name].to(param.device))


def run(smoke_only=False):
    torch.manual_seed(SEED)
    torch.set_num_threads(2)
    q, g = frames()
    pairs_path = OUT / "pairs.json"
    if digest(pairs_path) != json.loads((OUT / "pairs_sha256.json").read_text())["sha256"]:
        raise RuntimeError("Reference pair mining evidence changed")
    pairs = json.loads(pairs_path.read_text())
    train = [r for r in pairs["train"] if not bool(g.iloc[r["anchor"]].is_pano)]
    validation = pairs["validation"][:128]
    if len(train) < 1000 or len(validation) < 100:
        raise RuntimeError("Too few valid reference-only pairs")
    if set(r["cell"] for r in train) & set(r["cell"] for r in validation):
        raise RuntimeError("Reference geographic train/validation overlap")
    train_image_ids = {g.iloc[r["anchor"]].id for r in train}
    if train_image_ids & set(q.id):
        raise RuntimeError("Development query identity in training")
    model, transform = load_model(train=True)
    selected = np.unique(np.asarray([i for r in train+validation for i in
                                     [r["anchor"], r["positive"], *r["negatives"]]], dtype=int))
    vectors = selected_vectors(g, selected, staged_entries(g))
    lookup = {int(index): pos for pos, index in enumerate(selected)}
    probe = train[:2]
    with torch.inference_mode():
        embedded = F.normalize(model(images(probe, g, transform)), dim=1).cpu().numpy()
    base = vectors[[lookup[r["anchor"]] for r in probe]]
    agreement = np.sum(embedded*base, axis=1)
    if np.min(agreement) < .999:
        raise RuntimeError(f"Official anchor preprocessing/cache parity failed: {agreement.tolist()}")
    contract = {"SAGE_source_revision": "c7d6241c4885526d99d6c78c158024fc2a37097c",
        "SAGE_checkpoint_sha256": digest(CHECKPOINT), "pairs_sha256": digest(pairs_path),
        "train_anchors": len(train), "heldout_reference_anchors": len(validation),
        "spatial_split": "reference H3r6 cell hash, disjoint train/validation",
        "training_query_ids_used": False, "anchor_preprocessing_cosine": agreement.tolist(),
        "architecture": "official SAGE-L No-Encoder, last DINOv2 block + existing aggregator/DPN trainable",
        "gallery_descriptor_target": "frozen existing 322 reference SAGE-L vector, unchanged primary gallery",
        "loss": "cross-entropy one independent-sequence <=25m positive versus eight >=200m mined visual negatives",
        "distillation_weight": ANCHOR_WEIGHT, "temperature": TEMP, "batch": BATCH,
        "epochs": EPOCHS, "backbone_last_block_lr": 1e-5, "aggregator_lr": 5e-5,
        "optimizer": "AdamW weight_decay=0.01 reset each epoch for deterministic epoch resume",
        "seed": SEED, "device": "mps", "inference_query_GT": False}
    contract_path = OUT / "training_contract.json"
    if contract_path.exists() and json.loads(contract_path.read_text()) != contract:
        raise RuntimeError("Training contract changed")
    if not contract_path.exists():
        save(contract_path, contract)
    if smoke_only:
        print(json.dumps({"anchor_preprocessing_cosine": agreement.tolist(),
                          "train": len(train), "validation": len(validation),
                          "trainable_params": sum(p.numel() for p in model.parameters() if p.requires_grad)}), flush=True)
        return
    history = []
    for epoch in range(1, EPOCHS+1):
        output = OUT / f"epoch{epoch}_trainable.pth"
        receipt = OUT / f"epoch{epoch}.json"
        if receipt.exists():
            previous = json.loads(receipt.read_text())
            if digest(output) != previous["checkpoint"]["sha256"]:
                raise RuntimeError("Saved SAGE adaptation epoch changed")
            apply_trainable(model, output)
            history.append(previous)
            print("resumed complete epoch", epoch, flush=True)
            continue
        backbone = list(model.backbone.model.blocks[-1].parameters())
        backbone_ids = {id(p) for p in backbone}
        other = [p for p in model.parameters() if p.requires_grad and id(p) not in backbone_ids]
        optimizer = torch.optim.AdamW([{"params": backbone, "lr": 1e-5},
                                       {"params": other, "lr": 5e-5}], weight_decay=.01)
        rng = np.random.default_rng(SEED+epoch)
        order = rng.permutation(len(train))
        began = time.perf_counter()
        losses = []
        for step, start in enumerate(range(0, len(order), BATCH), 1):
            batch = [train[i] for i in order[start:start+BATCH]]
            x = images(batch, g, transform)
            pn, anchor = targets(batch, lookup, vectors)
            loss, contrastive, conservation = objective(model, x, pn, anchor)
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            torch.nn.utils.clip_grad_norm_([p for p in model.parameters() if p.requires_grad], 1.)
            optimizer.step()
            losses.append(float(loss.detach().cpu()))
            if step % 50 == 0:
                print("SAGE adaptation epoch", epoch, "step", step, "of", int(np.ceil(len(train)/BATCH)),
                      "mean loss", float(np.mean(losses[-50:])), flush=True)
        validation_result = evaluate_reference(model, validation, g, transform, lookup, vectors)
        checkpoint = save_trainable(model, output)
        row = {"epoch": epoch, "training_loss_mean": float(np.mean(losses)),
               "runtime_s": time.perf_counter()-began, "checkpoint": checkpoint,
               "reference_validation": validation_result}
        save(receipt, row)
        history.append(row)
        print("SAGE adaptation epoch complete", json.dumps(row), flush=True)
    chosen = min(history, key=lambda value: value["reference_validation"]["reference_heldout_contrastive_loss"])
    save(OUT / "complete.json", {"contract": contract, "contract_sha256": digest(contract_path),
        "history": history, "selected_epoch": chosen["epoch"],
        "selection_basis": "heldout reference-only geographic validation, no development query labels",
        "selected_checkpoint_sha256": chosen["checkpoint"]["sha256"]})
    print("SAGE reference adaptation complete, selected epoch", chosen["epoch"], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    run(parser.parse_args().smoke)
