"""Resume the same FoL trial using CPU matching of committed MPS features.

No model loading, image reads, encoding, new ML parameters or query-population
changes. The main worker lock is held only during handoff and publication.

Unlaunched fallback draft: the original MPS worker accelerated after graph
specialization and was retained. Runtime handoff tests/review are still required
before any future use; this module is not part of the active night scheduler.
"""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research import night_fol as fol
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import digest, save

LOCAL = evaluator.LOCAL
NAMES = ("fol_global_in_prefix", "fol_mnn", "fol_blend", "fol_learned_oof")


def state(out, phase, **extra):
    value = {"phase": phase, "pid": os.getpid(), "updated": time.time(), "runtime": "CPU matching; existing MPS features", **extra}
    save(out / "status.json", value)
    save(LOCAL / "fol_cpu_status.json", value)
    print(json.dumps(value), flush=True)


def atomic_copy(source, target):
    payload = Path(source).read_bytes()
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".writing")
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    return hashlib.sha256(payload).hexdigest()


def validate_pairs(path, prefix, query_count, depth, complete=False):
    with np.load(path, allow_pickle=False) as saved:
        if not np.array_equal(saved["prefix"], prefix):
            raise RuntimeError("FoL CPU continuation changed original candidate membership")
        values = saved["evidence"]
    if values.shape != (query_count, depth, 4) or values.dtype != np.float32 or np.isinf(values).any():
        raise RuntimeError("invalid FoL evidence checkpoint")
    finite = np.isfinite(values)
    committed = finite.all(axis=2)
    if not np.array_equal(finite.any(axis=2), committed) or (complete and not committed.all()):
        raise RuntimeError("partial or missing committed FoL pair evidence")
    return values, committed


def validate_metadata(plan, feature_index, audit, cfg, queries, gallery, night):
    expected = {"checkpoint_sha256": audit["checkpoint_sha256"], "code_revision": audit["code_revision"],
                "device": "mps", "size": cfg["fol_size"], "dtype": "float32",
                "local_rule": "official225 regions; remove zero padding only"}
    if feature_index["contract"] != expected:
        raise RuntimeError("FoL feature encoder contract changed")
    prefix = np.asarray(plan["prefix"], dtype=np.int32)
    if prefix.shape != (len(queries), 100) or plan["depth"] != cfg["fol_depth"]:
        raise RuntimeError("original FoL query count or candidate depth changed")
    if (prefix < 0).any() or (prefix >= len(gallery)).any() or any(len(set(row)) != 100 for row in prefix):
        raise RuntimeError("invalid original FoL retrieval prefix")
    query_ids = queries.id.tolist()
    reference_ids = set(gallery.id.iloc[np.unique(prefix[:, :plan["depth"]])])
    expected_ids = set(query_ids) | reference_ids
    records = plan["images"]
    if len(records) != len(expected_ids) or {r["id"] for r in records} != expected_ids:
        raise RuntimeError("FoL image plan differs from fixed queries and reference prefix")
    if [r["id"] for r in records if r["role"] == "query"] != query_ids:
        raise RuntimeError("FoL query order or identity changed")
    if {r["id"] for r in records if r["role"] == "reference"} != reference_ids:
        raise RuntimeError("FoL reference image membership changed")
    entries = feature_index["entries"]
    if set(entries) != expected_ids:
        raise RuntimeError("FoL extraction is incomplete or includes images outside the original plan")
    hashes = dict(zip(queries.id, queries.file_sha256, strict=True))
    hashes.update(zip(gallery.id, gallery.file_sha256, strict=True))
    for row in records:
        entry = entries[row["id"]]
        if row["sha256"] != hashes[row["id"]] or entry["image_sha256"] != row["sha256"]:
            raise RuntimeError("FoL cached image identity changed")
        path = Path(entry["path"])
        if path.parent != night / "fol_features" or path.suffix != ".npz" or not path.name.startswith("chunk-"):
            raise RuntimeError("feature index points outside the original committed cache")
        if not isinstance(entry["row"], int) or entry["row"] < 0:
            raise RuntimeError("invalid feature row pointer")
    return prefix, expected


class VerifiedFeatureStore:
    """One read/hash/receipt validation per chunk, four cached unpacked arrays."""
    def __init__(self, entries, contract, max_chunks=4):
        self.entries, self.contract, self.max_chunks = entries, contract, max_chunks
        self.chunks = OrderedDict()
        self.validated = {}

    def _read(self, filename):
        path = Path(filename)
        stat = path.stat()
        receipt_path = path.with_suffix(".json")
        receipt_bytes = receipt_path.read_bytes()
        receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        if filename in self.validated:
            previous = self.validated[filename]
            if (stat.st_size, stat.st_mtime_ns, receipt_sha) != (previous["bytes"], previous["mtime_ns"], previous["receipt_sha256"]):
                raise RuntimeError("validated FoL chunk changed during matching")
            archive = np.load(path, allow_pickle=False)
        else:
            receipt = json.loads(receipt_bytes)
            if receipt["contract"] != self.contract:
                raise RuntimeError("FoL chunk encoder contract differs")
            payload = path.read_bytes()
            if hashlib.sha256(payload).hexdigest() != receipt["sha256"]:
                raise RuntimeError("FoL committed feature chunk checksum failed")
            archive = np.load(io.BytesIO(payload), allow_pickle=False)
        with archive as saved:
            local, offsets, global_vectors, ids = saved["local"], saved["offsets"], saved["global_vectors"], saved["ids"]
        if filename not in self.validated:
            if local.ndim != 2 or local.shape[1] != 128 or local.dtype != np.float32:
                raise RuntimeError("invalid committed local FoL descriptors")
            if global_vectors.shape != (len(ids), 8448) or global_vectors.dtype != np.float32:
                raise RuntimeError("invalid committed global FoL descriptors")
            if (offsets.shape != (len(ids) + 1,) or not np.issubdtype(offsets.dtype, np.integer)
                    or offsets[0] != 0 or offsets[-1] != len(local) or (np.diff(offsets) < 0).any()):
                raise RuntimeError("invalid packed FoL descriptor offsets")
            if ids.tolist() != [row["id"] for row in receipt["images"]]:
                raise RuntimeError("FoL receipt identities differ from stored arrays")
            for i, row in enumerate(receipt["images"]):
                entry = self.entries.get(row["id"])
                if entry is None or entry != {"path": filename, "row": i, "image_sha256": row["sha256"]}:
                    raise RuntimeError("FoL receipt identity/row differs from complete feature index")
            for values in (global_vectors, local):
                for start in range(0, len(values), 8192):
                    block = values[start:start + 8192]
                    if not np.isfinite(block).all() or not np.allclose(np.linalg.norm(block, axis=1), 1, atol=1e-5):
                        raise RuntimeError("FoL matching requires finite unit descriptors")
            self.validated[filename] = {"sha256": receipt["sha256"], "receipt_sha256": receipt_sha,
                                       "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns, "images": len(ids)}
        return local, offsets, global_vectors

    def get(self, identity):
        entry = self.entries[identity]
        filename = entry["path"]
        if filename not in self.chunks:
            self.chunks[filename] = self._read(filename)
            if len(self.chunks) > self.max_chunks:
                self.chunks.popitem(last=False)
        self.chunks.move_to_end(filename)
        local, offsets, global_vectors = self.chunks[filename]
        row = entry["row"]
        return local[offsets[row]:offsets[row + 1]], global_vectors[row]


def prepare(night, out):
    with (LOCAL / "controller.lock").open("a") as main:
        try:
            fcntl.flock(main, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("main worker is still live; stop original FoL only after its checkpoint before CPU handoff") from exc
        if (night / "fol.done.json").exists():
            return None
        original_status = json.loads((night / "status.json").read_text()) if (night / "status.json").exists() else {}
        if original_status.get("phase") in {"fol_extract", "fol_match"}:
            try:
                os.kill(int(original_status["pid"]), 0)
            except ProcessLookupError:
                pass
            else:
                raise RuntimeError("original FoL PID is still alive; CPU handoff refused")
        cfg, _, _, actual_night, queries, _, gallery, _ = evaluator.inputs()
        if actual_night != night or not (night / "fol.started.json").exists():
            raise RuntimeError("CPU runtime must continue the already-started FoL experiment")
        plan = json.loads((night / "fol_plan.json").read_text())
        feature_index = json.loads((night / "fol_feature_index.json").read_text())
        audit = json.loads((night / "fol_source_audit.json").read_text())
        prefix, feature_contract = validate_metadata(plan, feature_index, audit, cfg, queries, gallery, night)
        index_sha, plan_sha = digest(night / "fol_feature_index.json"), digest(night / "fol_plan.json")
        parity_path = night / "fol_cpu_runtime_parity.json"
        parity = json.loads(parity_path.read_text())
        if (parity.get("verified") is not True or parity.get("CPU_count_exact_match") is not True
                or len(parity.get("pairs", [])) < 10 or parity["index_sha256"] != index_sha or parity["plan_sha256"] != plan_sha):
            raise RuntimeError("CPU/MPS runtime parity has not passed on the frozen features")
        for pair in parity["pairs"]:
            if pair["mps"][0] != pair["cpu"][0] or not np.allclose(pair["mps"][1:], pair["cpu"][1:], rtol=0, atol=2e-5):
                raise RuntimeError("CPU/MPS committed parity evidence disagrees")
        contract = {"wrapper_source_sha256": digest(Path(__file__)), "fol_source_sha256": digest(Path(fol.__file__)),
             "evaluator_source_sha256": digest(Path(evaluator.__file__)), "input_contract_sha256": digest(night / "input_contract.json"),
             "plan_sha256": plan_sha, "feature_index_sha256": index_sha, "source_audit_sha256": digest(night / "fol_source_audit.json"),
             "CPU_count_parity_sha256": digest(parity_path), "feature_contract": feature_contract,
             "matching_device": "cpu", "threads": 1, "nice": 10, "config": cfg,
             "model_or_image_encoding": False, "runtime_change_only": True}
        marker = out / "started.json"
        original_pairs = out / "original_mps_pairs.npz"
        pair_path = out / "fol_pair_evidence.npz"
        if marker.exists():
            registered = json.loads(marker.read_text())
            if registered["contract"] != contract or digest(original_pairs) != registered["initial_pair_sha256"]:
                raise RuntimeError("FoL CPU continuation contract or preserved MPS evidence changed")
        else:
            initial_sha = atomic_copy(night / "fol_pair_evidence.npz", original_pairs)
            original, committed = validate_pairs(original_pairs, prefix, len(queries), plan["depth"])
            for pair in parity["pairs"]:
                i, j = pair["query_index"], pair["candidate_position"]
                if not committed[i, j] or not np.allclose(original[i, j, :3], pair["mps"], rtol=0, atol=2e-5):
                    raise RuntimeError("parity pairs differ from preserved original MPS checkpoint")
            atomic_copy(original_pairs, pair_path)
            registered = {"started": time.time(), "contract": contract, "initial_pair_sha256": initial_sha,
                          "initial_completed_pairs": int(committed.sum())}
            save(marker, registered)
        validate_pairs(pair_path, prefix, len(queries), plan["depth"])
        base = out / "base_scores.npy"
        if not base.exists():
            base.symlink_to(night / "base_scores.npy")
        if not base.is_symlink() or base.resolve() != (night / "base_scores.npy").resolve():
            raise RuntimeError("private FoL score path must reference the original exact score cache")
        save(out / "ready.json", {"extraction_complete": True, "feature_index_sha256": index_sha, "plan_sha256": plan_sha,
             "CPU_count_parity_sha256": contract["CPU_count_parity_sha256"], "plan_images": len(plan["images"]),
             "feature_validation": "complete index identity contract checked; source chunk SHA/IDs/normalization checked on first read",
             "ready": time.time(), "pid": os.getpid()})
    return cfg, queries, gallery, plan, feature_index, registered


def compute(out, prepared):
    cfg, queries, gallery, plan, feature_index, registered = prepared
    entries = feature_index["entries"]
    stores = []
    old = (fol.match, fol.FeatureStore, fol.status, fol.threadpool_limits, evaluator.paths, evaluator.LOCAL)

    def cpu_match(a, b, device="cpu"):
        if device != "cpu":
            raise RuntimeError("CPU continuation cannot dispatch to another matching device")
        return old[0](a, b, device="cpu")

    def store_factory(requested):
        if requested is not entries:
            raise RuntimeError("unexpected FoL feature-index replacement")
        store = VerifiedFeatureStore(entries, feature_index["contract"])
        stores.append(store)
        return store

    fol.match, fol.FeatureStore = cpu_match, store_factory
    fol.status = lambda phase, completed=0, total=0, **extra: state(out, phase, completed=completed, total=total, **extra)
    fol.threadpool_limits = lambda *args, **kwargs: threadpool_limits(limits=1)
    evaluator.paths = lambda: (*old[4]()[:4], out)
    evaluator.LOCAL = out
    try:
        fol.rerank(plan, entries, out, cfg, queries, gallery)
    finally:
        fol.match, fol.FeatureStore, fol.status, fol.threadpool_limits, evaluator.paths, evaluator.LOCAL = old
    prefix = np.asarray(plan["prefix"], dtype=np.int32)
    final, _ = validate_pairs(out / "fol_pair_evidence.npz", prefix, len(queries), plan["depth"], complete=True)
    original, committed = validate_pairs(out / "original_mps_pairs.npz", prefix, len(queries), plan["depth"])
    if not np.array_equal(final[committed], original[committed]):
        raise RuntimeError("CPU continuation altered already-committed MPS evidence")
    save(out / "validated_chunks.json", {path: value for store in stores for path, value in store.validated.items()})
    filenames = ["fol_pair_evidence.npz", "fol_ranker.json", "fol_ranker_folds.json", "validated_chunks.json"]
    filenames += [f"results/{name}{suffix}" for name in NAMES for suffix in (".json", "_rows.json")]
    save(out / "done.json", {"completed": time.time(), "contract": registered["contract"],
         "original_MPS_pairs_preserved_exactly": int(committed.sum()),
         "files": {filename: digest(out / filename) for filename in filenames}})


def publish(night, out, registered):
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != registered["contract"]:
        raise RuntimeError("completed FoL CPU runtime contract changed")
    for filename, expected in done["files"].items():
        if digest(out / filename) != expected:
            raise RuntimeError("private FoL result changed before publication")
    state(out, "fol_cpu_waiting_to_publish")
    with (LOCAL / "controller.lock").open("a") as main:
        fcntl.flock(main, fcntl.LOCK_EX)
        target_pair = night / "fol_pair_evidence.npz"
        if digest(target_pair) not in {registered["initial_pair_sha256"], done["files"]["fol_pair_evidence.npz"]}:
            raise RuntimeError("another FoL worker changed the original checkpoint during CPU continuation")
        for filename in done["files"]:
            if filename == "validated_chunks.json":
                continue
            source, target = out / filename, night / filename
            if filename != "fol_pair_evidence.npz" and target.exists() and digest(target) != done["files"][filename]:
                raise RuntimeError("refusing to replace another completed FoL result")
            if not target.exists() or digest(target) != done["files"][filename]:
                atomic_copy(source, target)
        save(night / "fol_cpu_runtime.json", {"contract": registered["contract"], "done_sha256": digest(out / "done.json"),
             "original_pair_sha256": registered["initial_pair_sha256"], "runtime_change_only": True,
             "original_MPS_pairs_preserved_exactly": done["original_MPS_pairs_preserved_exactly"]})
        evaluator.report()
        save(out / "published.json", {"published": time.time(), "done_sha256": digest(out / "done.json")})
        # Last main commit marker: every result, ranker and pair cache is now ready.
        save(night / "fol.done.json", {"completed": time.time(), "phase": "fol", "runtime": "CPU matching over committed MPS features",
             "runtime_receipt_sha256": digest(night / "fol_cpu_runtime.json")})
    state(out, "fol_cpu_completed")


def main():
    *_, night = evaluator.paths()
    out = night / "fol_cpu"
    out.mkdir(exist_ok=True)
    current_nice = os.getpriority(os.PRIO_PROCESS, 0)
    if current_nice < 10:
        os.nice(10 - current_nice)
    torch.set_num_threads(1)
    with (LOCAL / "fol_cpu.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            prepared = prepare(night, out)
            if prepared is None:
                state(out, "fol_already_completed_no_restart")
                return
            failure = out / "failure.json"
            if failure.exists():
                archived = out / f"failure-{digest(failure)}.json"
                if not archived.exists():
                    atomic_copy(failure, archived)
                failure.unlink()
            if not (out / "done.json").exists():
                state(out, "fol_cpu_matching_started", already_committed=prepared[-1]["initial_completed_pairs"])
                compute(out, prepared)
            publish(night, out, prepared[-1])
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc),
                 "resume": "original feature chunks and private committed pair evidence retained"})
            state(out, "fol_cpu_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    main()
