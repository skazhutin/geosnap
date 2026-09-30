"""Full-gallery evaluation of the reference-only adapted SAGE query tower."""
from __future__ import annotations

import hashlib
import json
import time

import numpy as np
import torch
from PIL import Image
from threadpoolctl import threadpool_limits
from torchvision import transforms

from ml.research import gallery_scale, night_scale_context as context, night_v7
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.common import GALLERY, LOCAL, compare, frames, image_inputs, metrics, registry, topk
from ml.research.geographic_v8.stage import STAGE
from ml.research.geographic_v8.train_sage_asymmetric import apply_trainable, load_model

OUT = LOCAL / "sage_adaptation_v3"


def transform504():
    return transforms.Compose([transforms.ToTensor(),
        transforms.Normalize(mean=[.485, .456, .406], std=[.229, .224, .225]),
        transforms.Resize([504, 504], interpolation=transforms.InterpolationMode.BILINEAR, antialias=True)])


def query_images(rows, transform):
    values = []
    for row in rows.itertuples():
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError("Query image changed")
        with Image.open(row.image_path) as image:
            values.append(transform(image.convert("RGB")))
    return torch.stack(values).to("mps")


def encode(model, transform, inputs, name, signature):
    directory = OUT / f"query{name}"
    directory.mkdir(exist_ok=True)
    vectors = np.empty((len(inputs), 8448), np.float32)
    began = time.perf_counter()
    with torch.inference_mode():
        for start in range(0, len(inputs), 2):
            stop = min(start+2, len(inputs))
            path = directory / f"{start:04d}.npz"
            if path.exists():
                with np.load(path, allow_pickle=False) as old:
                    if str(old["signature"]) != signature or old["query_ids"].tolist() != inputs.id.iloc[start:stop].tolist():
                        raise RuntimeError("Adapted SAGE query cache contract changed")
                    encoded = old["vectors"]
            else:
                x = query_images(inputs.iloc[start:stop], transform)
                encoded = torch.nn.functional.normalize(model(x), dim=1).cpu().numpy()
                temp = path.with_suffix(".writing")
                with temp.open("wb") as f:
                    np.savez(f, signature=signature, query_ids=inputs.id.iloc[start:stop].to_numpy(str), vectors=encoded)
                temp.replace(path)
            if encoded.shape != (stop-start, 8448) or not np.isfinite(encoded).all():
                raise RuntimeError("Invalid adapted SAGE query descriptor")
            vectors[start:stop] = encoded
            if start % 100 == 0:
                print("adapted query", name, stop, "/", len(inputs), flush=True)
    np.save(OUT / f"query{name}.npy", vectors)
    return vectors, time.perf_counter()-began


def rerank(prefix, q322, q504, g, entries, encoder):
    chosen = prefix[:, :30]
    unique, remap = np.unique(chosen, return_inverse=True)
    vectors = selected_vectors(g, unique, entries)
    remap = remap.reshape(chosen.shape)
    means = context.mean_queries(q322, q504)
    contextual = np.empty(chosen.shape, np.float32)
    original = np.empty(chosen.shape, np.float32)
    with threadpool_limits(limits=2):
        for i in range(len(prefix)):
            contextual[i], original[i] = context.one_query(encoder, means[i], q322[i], q504[i], vectors[remap[i]])
    return context.rank_prefix(prefix, contextual, original), contextual, original


def record(name, q, g, ranked, contract, runtime):
    report, errors, _ = metrics(q, g, ranked)
    paired = compare(q, errors)
    if len(g) == 44995:
        with np.load(LOCAL / "secondary_baseline/predictions.npz", allow_pickle=False) as old:
            secondary_errors = old["errors_m"]
        before, after = secondary_errors <= 100, errors <= 100
        paired["primary_failure_bucket_analysis"] = paired.pop("buckets")
        paired["transitions"] = {"old_wrong_new_correct": int((~before & after).sum()),
            "old_correct_new_wrong": int((before & ~after).sum()),
            "old_wrong_new_wrong": int((~before & ~after).sum()),
            "old_correct_new_correct": int((before & after).sum())}
        paired["paired_geographic_bootstrap"] = paired_group_bootstrap(secondary_errors, errors, q.h3_coarse.tolist())
        paired["comparison_baseline"] = "44,995-reference release-compatible SAGE, 380/1184"
    report |= paired | {"variant": name, "model": contract, "runtime": runtime,
        "scope": "reference-only trained SAGE-L; same fixed gallery; exploratory development"}
    folder = OUT / name
    folder.mkdir(exist_ok=True)
    np.savez(folder / "predictions.npz", query_ids=q.id.to_numpy(str),
             gallery_ids=g.id.to_numpy(str)[ranked], indices=ranked, errors_m=errors,
             prediction_gps=g[["lat", "lon"]].to_numpy()[ranked[:, 0]])
    save(folder / "report.json", report)
    registry(name, {"model_revision": contract["selected_checkpoint_sha256"],
        "gallery": {"path": str(GALLERY), "sha256": digest(GALLERY),
                    "selected_ids_sha256": hashlib.sha256("\n".join(g.id.astype(str)).encode()).hexdigest(),
                    "count": len(g), "msls": "research-only" if len(g) == 112163 else "excluded"},
        "preprocessing": "official SAGE-L reference322 query322+504",
        "candidate_generation": "exact cosine fixed gallery with adapted query tower",
        "reranking": "frozen SAGE context encoder top30, fixed 0.5 blend", "fusion": "query322/504 mean",
        "hyperparameters": contract["training"], "fitted": True, "fitted_parameters": True,
        "fitting_split": "gallery reference-reference H3r6 one-ring embargo for all roles, provider-sequence disjoint train/validation; 0 development queries",
        "evaluation_split": "1184 repeatedly used exploratory development",
        "raw25": report["raw"]["accuracy_25m"], "raw50": report["raw"]["accuracy_50m"],
        "raw100": report["raw"]["accuracy_100m"], "r_at_k": report["recall_at"],
        "median_m": report["raw"]["median_error_m"], "p90_m": report["raw"]["p90_error_m"],
        "gt500_rate": report["raw"]["catastrophic_gt500m_rate"], "runtime": runtime,
        "result_status": "complete", "decision": "paired comparison against frozen baseline",
        "artifact_paths": [str(folder / "predictions.npz"), str(folder / "report.json")],
        "artifact_hashes": {f: digest(folder / f) for f in ("predictions.npz", "report.json")},
        "paired_bootstrap": paired["paired_geographic_bootstrap"]})
    print(name, "RAW100", int((errors <= 100).sum()), "R100", report["recall_at"]["100"],
          "transitions", paired["transitions"], flush=True)
    return report


def run():
    torch.set_num_threads(2)
    complete = json.loads((OUT / "complete.json").read_text())
    split = json.loads((OUT / "strict_split_gate.json").read_text())
    if (not split["verified"] or split["all_role_sequence_overlap"] != 0
            or split["all_role_geographic_disjoint"] is not True):
        raise RuntimeError("Strict geographic/sequence reference split not verified")
    exclusion = json.loads((OUT / "development_exclusion_gate.json").read_text())
    if any(exclusion[k] != 0 for k in ("query_gallery_sequence_overlap", "query_gallery_id_overlap",
                                       "query_gallery_file_sha_overlap")):
        raise RuntimeError("Development/gallery training exclusion failed")
    epoch = complete["selected_epoch"]
    checkpoint = OUT / f"epoch{epoch}_trainable.pth"
    if digest(checkpoint) != complete["selected_checkpoint_sha256"]:
        raise RuntimeError("Adapted SAGE checkpoint changed")
    inputs = image_inputs()
    q, g = frames()
    if inputs.id.tolist() != q.id.tolist():
        raise RuntimeError("Inference/evaluation query order changed")
    model, transform322 = load_model(train=True)
    with torch.inference_mode():
        baseline = torch.nn.functional.normalize(model(query_images(inputs.iloc[:2], transform322)), dim=1).cpu().numpy()
        baseline504 = torch.nn.functional.normalize(model(query_images(inputs.iloc[:2], transform504())), dim=1).cpu().numpy()
    old322 = np.load(STAGE / "query322.npy", mmap_mode="r")[:2]
    agreement = np.sum(baseline*old322, axis=1)
    first504 = sorted((STAGE / "query504").glob("chunk-*.npz"))[0]
    with np.load(first504, allow_pickle=False) as old:
        agreement504 = np.sum(baseline504*old["vectors"][:2], axis=1)
    if agreement.min() < .999 or agreement504.min() < .999:
        raise RuntimeError("Unadapted query encoder differs from frozen SAGE baseline")
    apply_trainable(model, checkpoint)
    contract = {"selected_epoch": epoch, "selected_checkpoint_sha256": digest(checkpoint),
        "training": complete["contract"], "training_complete_sha256": digest(OUT / "complete.json"),
        "strict_split_gate_sha256": digest(OUT / "strict_split_gate.json"),
        "development_exclusion_gate_sha256": digest(OUT / "development_exclusion_gate.json"),
        "query_base_parity_cosine": {"322": agreement.tolist(), "504": agreement504.tolist()},
        "gallery_changed": False,
        "query_ground_truth_in_encoder": False}
    save(OUT / "evaluation_contract.json", contract)
    signature = hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    q322, t322 = encode(model, transform322, inputs, "322", signature)
    q504, t504 = encode(model, transform504(), inputs, "504", signature)
    entries = staged_entries(g)
    started = time.perf_counter()
    with threadpool_limits(limits=2):
        score322 = gallery_scale.exact_scores(g, q322, entries)
        score504 = gallery_scale.exact_scores(g, q504, entries)
    scores = .5 * (score322+score504)
    score_runtime = time.perf_counter()-started
    np.save(OUT / "mean_scores.npy", scores)
    checkpoint_context = "data/models/research_v5/sage_context_encoder.pth"
    if digest(checkpoint_context) != night_v7.ENCODER_SHA:
        raise RuntimeError("Frozen SAGE context checkpoint changed")
    encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
        d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1,
        batch_first=False), 2)
    encoder.load_state_dict(torch.load(checkpoint_context, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    runtime = {"query322_s": t322, "query504_s": t504, "full_exact_cosine_s": score_runtime}
    started = time.perf_counter()
    primary_prefix = topk(scores)
    primary, contextual, original = rerank(primary_prefix, q322, q504, g, entries, encoder)
    runtime["primary_context_s"] = time.perf_counter()-started
    np.savez(OUT / "primary_context.npz", chosen=primary_prefix[:, :30], contextual=contextual, original=original)
    record(f"sage_asymmetric_v3_refonly_epoch{epoch}_primary", q, g, primary, contract, runtime)
    allowed = np.flatnonzero(g.source.isin(["mapillary", "kartaview"]).to_numpy())
    if len(allowed) != 44995:
        raise RuntimeError("Release-compatible gallery identity changed")
    started = time.perf_counter()
    sub_prefix = topk(scores[:, allowed])
    sub_ranked, sub_contextual, sub_original = rerank(sub_prefix, q322, q504, g.iloc[allowed].reset_index(drop=True),
                                                     {g.id.iloc[i]: entries[g.id.iloc[i]] for i in allowed}, encoder)
    runtime["secondary_context_s"] = time.perf_counter()-started
    np.savez(OUT / "secondary_context.npz", chosen=sub_prefix[:, :30],
             contextual=sub_contextual, original=sub_original)
    record(f"sage_asymmetric_v3_refonly_epoch{epoch}_licensed_secondary", q,
           g.iloc[allowed].reset_index(drop=True), sub_ranked, contract, runtime)
    save(OUT / "evaluation_complete.json", {"contract": contract,
        "artifacts": {f: digest(OUT / f) for f in ("query322.npy", "query504.npy", "mean_scores.npy",
                                                  "primary_context.npz", "secondary_context.npz")}})
    print("adapted SAGE evaluation complete", flush=True)


if __name__ == "__main__":
    run()
