# Research resumed on 2026-09-28

The pause below is historical. The user subsequently requested continuation. The current status is tracked by `data/evaluation/geographic_v8_20260928/` and `docs/geographic_v8_research_report_20260928.md`.

The 411/1184 baseline and 380/1184 release-compatible baseline were reproduced from pinned descriptors and fresh context reranking, with full per-query agreement. Official G3, PLONK, OSV-5M, and GeoCLIP zero-shot experiments, candidate unions, and geographically embargoed OOF reranking completed. Strict all-role-disjoint reference-only SAGE-L adaptation scored 406/1184, so the old 411/1184 candidate remains best. The strongest measured candidate union reaches 652/1184 oracle recall and recovers 39 of the original 309 retrieval misses; this is not final prediction accuracy. Two earlier adaptation pilots were aborted before development scoring and marked invalid: the first had provider-sequence overlap, and v2 had 4301 training negative instances in the validation geographic embargo. The complete readout is `docs/geographic_v8_research_report_20260928.md`; the new candidate was not frozen because it failed the precommitted raw-accuracy gate. The independent smartphone population does not yet exist, so no new final accuracy claim is possible. Final integrity verification passed: 85 production files unchanged, 45 fixed inputs, 503 descriptor shards and both research-source snapshots verified. A posthoc query-quality proxy audit and a fixed random sample of ten covered all-model retrieval misses are saved under `query_quality_audit/`; the user observed a mix of unusable and normal-looking frames, so no quality prevalence was inferred.

## Historical pause record

2026-09-28. User wants to continue this chat with another Codex model. Do not resume until the user asks. No research processes remain running. No subagents were used.

## Verified state

- Actual latest candidate: `tranche3_context`, 411/1184 RAW100, 112163 references; frozen metadata at `data/evaluation/moscow_night_v7/candidate_frozen.json`.
- Current gallery is on the external NTFS/Mounty volume: `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/night_v7_20260909/reference_tranche3/gallery.parquet`.
- Gallery SHA256: `8d92a0bdf3492c4a73ce4032ab0966b3380794ab2860d2d68c8cfd5d7b9d094a`; dev SHA256: `6bcc0b4a618682e05e15305dc76b9f02af635e73573e06ea2567d6094ad3e7d9`.
- Production BEFORE hashes verified: all historical 42 files plus extended guard of 85 files. Receipt: `data/evaluation/geographic_v8_20260928/production_before.json`. AFTER verification is still pending. No production files were edited.
- Cached full mean-score matrix + cached context evidence reproduce all 1184 top100 lists exactly. This is a cached-score replay, not yet a completed fresh descriptor/model reproduction.
- Fresh dual-scale cosine/context replay exited with code 138 without a Python traceback; log ends at full-gallery exact scoring. Cause is unverified. Do not claim a successful baseline reproduction beyond cached-score parity. Log preserved at `data/evaluation/geographic_v8_20260928/baseline_attempt.log`.
- Machine: Apple GPU available via MPS, 16 GiB unified memory; no CUDA. User explicitly reminded us to use the Mac GPU.

## Changes and artifacts

Only new research code at `ml/research/geographic_v8/`: `common.py`, `baseline.py`, `inventory.py`, `g3.py`. These are WORK IN PROGRESS: not linted, tested, or fully run.

Local research root: `data/evaluation/geographic_v8_20260928`. Large-data root: `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/geographic_v8_20260928`.

Machine-readable `inventory.json` contains 490 historical registry rows, closed families, model/preprocessing/fusion/adapter/confidence inventory, leakage restrictions and source licenses. Pool: 122611 cached identities in 503 existing shards (gallery selects 112163). It parses prior evidence, does not rerun it. Original user changes/untracked research code must be preserved. Initial git status was dirty, with `.gitignore`, frontend package.json and substantial v5-v7 research files predating this task.

Isolated runtime: `data/evaluation/geographic_v8_20260928/runtime/bin/python`. It reads the existing environment through a `.pth` file and installs additional dependencies only in its own directory. Transformers 4.42.0 requires NumPy<2, so isolated NumPy1.26.4 was installed; original env remains NumPy2.5.2/Torch2.13.0. Torch/Transformers/pyproj import smoke passed (Torch2.13.0, Transformers4.42.0). Record a complete environment lock before results. First stdlib venv attempt failed due dylib path; replaced only this newly created environment with `uv venv`.

## Immediate technical issues to diagnose on resume

1. G3 download returned a path on the external volume, but subsequent SHA256 read failed with `OSError: [Errno 5] Input/output error`. No `audit/g3_download.json` success receipt exists. Verify mount/read health; don't trust the downloaded bytes. Avoid rereading/recomputing all old descriptors repeatedly. Check baseline process exit result; the log contains no Python traceback.
2. Baseline exact scoring currently makes two full matrices and sorts them. On this machine, review peak-memory behavior and use bounded blocks/memmaps if necessary. Existing `gallery_scale.exact_scores` groups by shard and reads complete shard files. Do not silently relax exact query-level agreement.
3. `g3.py` is written but never run. It preserves upstream architecture, strictly loads every tensor, records a diff that changes two hard-coded CUDA allocations to input.device, and avoids redundant base-CLIP weight loading because full G3 checkpoint supplies all weights. Official preprocessing and mathematical expressions retained. CPU/MPS image/GPS parity gates are planned in code, not yet passed. Audit this port before inference.
4. New-model evaluation must wait until baseline reproduction is completed. `baseline/report.json` is deliberately required by the G3 runner. No new model accuracy has been measured. Secondary licensed-only baseline replay is still needed.

## Fresh model audit findings

Official repositories cloned to external `sources/`: G3, PLONK, OSV5M, GeoCLIP. Public model metadata, cards/configs, GeoRanker source and PIGEON/GAEA README snapshots saved under local `audit/`.

- G3 official code: Applied-Machine-Learning-Lab/G3 @ `b4e3acf7c0ac51221f21b7877fefb4826715c9e2`; checkpoint Jia-py/G3-checkpoint @ `12d886fc2a1e59b3b52821acee193084420409cc`, Apache2 code/card, MP16-Pro training. Two CUDA allocations in `CustomLocationEncoder.forward`; otherwise ordinary PyTorch operations. Same-gallery image-GPS scoring is official documented usage. Checkpoint hash not yet verified due I/O error.
- PLONK_OSV_5M @ `e23229f4dd91d52560e8827f5bb2c68257fa162f`, MIT card, OSV5M, official code nicolas-dufour/plonk. Actual class is `PlonkPipeline` (card's capitalization differs). `plonk/pipe.py` supports CPU, uses StreetCLIP CLS features, official sampler and likelihood. Source read; no weights/inference yet. Explicit device routing needed for MPS; preserve official sampler and defaults.
- OSV5M official code gastruc/osv5m @ `4e6075387ecde4255410785ffb83830c9aa099f6`; osv5m/baseline @ `71548b90ac4a1aa7c37839841f411a06da82b1a6`. `models.huggingface.Geolocalizer` has official transform and returns lat/lon in RADIANS. Hydra config saved. No inference yet.
- GeoRanker official code Applied-Machine-Learning-Lab/GeoRanker @ `2100e9e7c4b95000e16434c7d1fd4e6a4b8424d6`; Apache2 code, LoRA weights present upstream, Qwen2-VL-7B-Instruct base, CUDA/FlashAttention. Quick-start `gt_lat/gt_lon` are used after prediction for reporting, not reward inputs. A safe wrapper must omit GT entirely. No compatible full runtime on this 16GB Mac has been established; don't quantize/change architecture silently.
- GeoCLIP: VicenteVivan/geo-clip cloned; current source has `GeoCLIP(from_pretrained=True, queue_size=4096)`, native `.to()` paths and bundled small learned weights; full CLIP model needed. Not evaluated.
- GeoSpot: sdan/geospot-base @ `32f164c81d62bc880d7f8c4fc3092a0d36eac1a2`, Apache2 card, SigLIP2-so400m512 + geographic encoder. Card says ~10.6M streetview images but incomplete provenance; documented `encoder_name='siglip2'` not in canonical GeoCLIP class and `torch.load` on safetensors is suspect. Do not invent an implementation/checkpoint match. Need official matching code.
- Chipoint2: chiikabu-labs/chipoint-2 @ `a150120cd968d48310d092caf26854f109c90b51`, MIT card; RUN_GUIDE/code saved. Includes 4.89M street-view and 14.5M photo external galleries. `run_chipoint.py` only loads/verifies reranker heads; full 23-feature extraction/query tower procedure is not supplied in that file. Need verify complete inference recipe before claiming usable. Never download/use huge external gallery as primary model-only test.
- PIGEON/PIGEOTTO: official LukasHaas/PIGEON @ `561f7edf4f61d61d2862f2947a8f8b68892bfcfc` still explicitly says model weights/geocells/datasets are not released. Reject pending official usable weights.
- GAEA: official UCF-CRCV/GAEA @ `b9f9014de6fad0e2bcee3f36847ebfdb314a885e` discovered, README saved, full viability not yet audited.

Research usability is separate from production suitability; externally pretrained dataset overlap with Mapillary queries remains uncertified. MSLS stays research-only. Historical final population is known exposed. No old final/calibration manifests were opened; reading requested historical production config exposed its already-known embedded historical metrics, never used for new model selection.

## Remote compute check

Hugging Face Jobs skill was read. Connector `hf_whoami` and `hf_jobs(ps)` failed with transport errors; local HF token absent. The sole configured SSH host timed out. No job submitted, no cost incurred, no data uploaded. Asked asynchronously for compute; user replied "mac has gpu". Responded that MPS is available and a documented device-only G3 port with CPU/MPS parity would be assessed. Do not ask what experiment to run.

## Next work, after explicit resume

Diagnose I/O/process exit, complete baseline (primary and secondary) with runtime/hashes and all query agreement. Validate G3 port and run official zero-shot image-GPS full-gallery ranking, SAGE top30/50/100 reranking and union oracle. Then follow original user's priority order. Build comparison, failure transitions, candidate complementarity, grouped nested/OOF priors only after useful signals. Keep original 262/309/202/411 buckets. Do not claim this pause as research completion or improvement.

Commands written so far:

```sh
.venv/bin/python -m ml.research.geographic_v8.inventory
.venv/bin/python -m ml.research.geographic_v8.baseline --recompute
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v8.g3
```

These need completion/validation before being called reproducible final experiment commands. No new candidate freeze, independent accuracy claim, confidence fitting, full fine-tuning, or final deliverables have been completed.
