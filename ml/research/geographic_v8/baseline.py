"""Recompute fixed dual-scale exact retrieval and SAGE context from cached vectors."""
import argparse
import json
import time
from pathlib import Path

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_scale_context as context
from ml.research import night_v7 as sage
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.geographic_v8.common import FROZEN, GALLERY, LOCAL, NIGHT, ROOT, frames, guard, initialize, metrics, registry, topk
from ml.research.geographic_v8.stage import STAGE


def staged_entries(gallery):
    pool = json.loads((STAGE / "descriptor_pool.json").read_text())["entries"]
    mapping = json.loads((STAGE / "paths.json").read_text())
    entries = {}
    for identity in gallery.id:
        entry = pool[identity].copy()
        entry["path"] = mapping[entry["path"]]["path"]
        entries[identity] = entry
    return entries


def selected_vectors(gallery, chosen, entries):
    from collections import defaultdict

    paths = defaultdict(list)
    for pos, index in enumerate(chosen):
        row = gallery.iloc[int(index)]
        entry = entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("Context descriptor identity mismatch")
        paths[entry["path"]].append((pos, entry["row"]))
    vectors = np.empty((len(chosen), 8448), np.float32)
    for filename, assignments in paths.items():
        block = np.load(filename, mmap_mode="r", allow_pickle=False)
        destination, source = np.asarray(assignments).T
        vectors[destination] = block[source]
    if not np.isfinite(vectors).all():
        raise RuntimeError("Invalid staged context descriptors")
    return vectors


def run(recompute=False):
    initialize()
    print(json.dumps(guard("before")), flush=True)
    out = LOCAL / "baseline"
    out.mkdir(exist_ok=True)
    if not (STAGE / "complete.json").exists():
        raise RuntimeError("Verified local research mirror must finish first")
    q, g = frames()
    source = GALLERY.parent
    frozen = json.loads(FROZEN.read_text())
    hashes = {}
    for name in ("mean_scores.npy", "context_evidence.npz"):
        expected = json.loads((source / Path(name).with_suffix(".json")).read_text())["sha256"]
        hashes[name] = digest(STAGE / name)
        if hashes[name] != expected:
            raise RuntimeError(f"Source hash mismatch: {name}")
    expected_rows = json.loads((STAGE / "context_rows.json").read_text())
    if expected_rows["query_ids"] != q.id.tolist():
        raise RuntimeError("Query order changed")
    start = time.perf_counter()
    original_scores = np.load(STAGE / "mean_scores.npy", mmap_mode="r", allow_pickle=False)
    prefix = topk(original_scores)
    original = np.take_along_axis(original_scores, prefix[:, :30], axis=1)
    with np.load(STAGE / "context_evidence.npz", allow_pickle=False) as old:
        ranked = context.rank_prefix(prefix, old["contextual"], original)
    if not np.array_equal(ranked, expected_rows["top100_gallery_rows"]):
        raise RuntimeError("Cached score/context baseline cannot reproduce all candidate IDs")
    print("Cached score replay: all 1184 top100 lists agree", flush=True)
    timing = {"cached_score_replay_s": time.perf_counter() - start}
    if recompute:
        gc = json.loads((WORKSPACE / "configs/moscow_gallery_scale.json").read_text())
        q322 = np.load(STAGE / "query322.npy", allow_pickle=False)
        contract = json.loads((gallery_scale.V5 / "development/global_sage-vitl_new/query_contract.json").read_text())
        if digest(STAGE / "query322.npy") != contract["descriptors_sha256"] or contract["contract"]["image_sha256"] != q.file_sha256.tolist():
            raise RuntimeError("Staged 322 query descriptor fingerprint changed")
        pieces = []
        for p in sorted((STAGE / "query504").glob("chunk-*.npz")):
            with np.load(p, allow_pickle=False) as chunk:
                i = sum(len(v) for v in pieces)
                if chunk["ids"].tolist() != q.id.iloc[i:i+len(chunk["ids"])].tolist():
                    raise RuntimeError("504 query identity mismatch")
                pieces.append(chunk["vectors"])
            hashes[str(p)] = digest(p)
        q504 = np.concatenate(pieces)
        pool = staged_entries(g)
        t = time.perf_counter()
        print("Recomputing both full-gallery exact cosine matrices from cached embeddings", flush=True)
        scores = (gallery_scale.exact_scores(g, q322, pool) + gallery_scale.exact_scores(g, q504, pool)) * .5
        timing["exact_retrieval_s"] = time.perf_counter() - t
        np.testing.assert_allclose(scores, original_scores, atol=2e-6, rtol=1e-5)
        new_prefix = topk(scores)
        if not np.array_equal(prefix, new_prefix):
            raise RuntimeError("Fresh exact retrieval top100 differs; diagnose before new models")
        chosen = prefix[:, :30]
        unique, remap = np.unique(chosen, return_inverse=True)
        vectors = selected_vectors(g, unique, pool)
        remap = remap.reshape(chosen.shape)
        checkpoint = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"
        if digest(checkpoint) != sage.ENCODER_SHA:
            raise RuntimeError("Context checkpoint hash mismatch")
        torch.set_num_threads(2)
        encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1, batch_first=False), 2)
        encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
        encoder.eval()
        means = context.mean_queries(q322, q504)
        evidence = np.empty(chosen.shape, np.float32)
        t = time.perf_counter()
        with threadpool_limits(limits=2):
            for i in range(len(q)):
                evidence[i], direct = context.one_query(encoder, means[i], q322[i], q504[i], vectors[remap[i]])
                np.testing.assert_allclose(direct, original[i], atol=2e-6, rtol=1e-5)
                if i % 100 == 0:
                    print(f"Fresh context {i}/{len(q)}", flush=True)
        timing["context_model_s"] = time.perf_counter() - t
        ranked = context.rank_prefix(prefix, evidence, np.take_along_axis(scores, chosen, axis=1))
        if not np.array_equal(ranked, expected_rows["top100_gallery_rows"]):
            raise RuntimeError("Fresh context baseline differs; diagnose before new models")
        np.savez(out / "context.npz", chosen=chosen, contextual=evidence, original=original)
        del scores, vectors, encoder
    report, errors, d = metrics(q, g, ranked)
    np.testing.assert_allclose(errors, expected_rows["errors_m"], rtol=0, atol=1e-7)
    if int((errors <= 100).sum()) != 411:
        raise RuntimeError("Baseline is not 411/1184")
    mean_rows = json.loads((STAGE / "mean_rows.json").read_text())
    covered = np.asarray(mean_rows["positive_count_100m"]) > 0
    retrievable = (d <= 100).any(axis=1)
    buckets = np.where(~covered, "no_coverage", np.where(~retrievable, "retrieval_failure", np.where(errors <= 100, "correct", "ranking_failure")))
    counts = {name: int((buckets == name).sum()) for name in np.unique(buckets)}
    assert counts == dict(no_coverage=262, retrieval_failure=309, ranking_failure=202, correct=411)
    np.savez(out / "predictions.npz", query_ids=q.id.to_numpy(str), gallery_ids=g.id.to_numpy(str), indices=ranked,
             scores=np.take_along_axis(original_scores, ranked, axis=1), errors_m=errors, buckets=buckets,
             prediction_gps=g[["lat", "lon"]].to_numpy()[ranked[:, 0]])
    report |= {"diagnosis": counts, "top100_query_agreement": 1184, "hashes": hashes,
               "runtime": timing, "recomputed_from_embeddings": recompute,
               "image_encoder_runtime": "cached; previous two-image fresh smoke is historical, not a new full encoding run",
               "original_candidate_sha256": digest(FROZEN)}
    save(out / "report.json", report)
    registry("baseline_embedding_replay" if recompute else "baseline_score_replay", {
        **report, "model_revision": frozen["candidate"].get("contract_sha256"),
        "preprocessing": "SAGE-L reference322 query322+504; exact original checkpoints",
        "candidate_generation": "full-gallery exact cosine mean; stable manifest ties",
        "reranking": "context30 standardized blend", "hyperparameters": {"mixing": .5},
        "result_status": "reproduced", "decision": "baseline retained",
        "artifact_paths": [str(out / "predictions.npz"), str(out / "report.json")]})
    print(json.dumps(report | {"hashes": "see report"}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--recompute", action="store_true")
    run(parser.parse_args().recompute)
