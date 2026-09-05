# Part 3 productionization report

Part 3 turns the frozen GeoSnap localization core into one deployable platform for the existing website and a separate Telegram client. It does not redesign the website, create a v5 model, tune localization, or change the production operating point.

## Initial audit

The repository began clean on `finalize-geosnap` at `cbcfb0dd3caf1dcdc86c7f720b58da8a73f79d50`. The frozen config matched SHA-256 `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`.

The development backend image occupied 17.5 GB in Docker Desktop (6.30 GB content). Its broad project dependency set pulled GPU/CUDA/NVIDIA and development/data tooling into runtime, while model/index loading depended on local data/cache paths and Torch Hub behavior. Production artifacts were not represented by one validated distribution contract. The frontend defaulted to a direct backend URL and public standard OSM tiles. Upload validation existed but the public topology had no production proxy, request-rate/capacity enforcement, operational metrics, structured correlation logging, or bot service. Reference previews could ultimately depend on the historical raw corpus.

The audit found no committed Telegram token, map token, password, storage credential, or production-secret value and no production-relevant tracked `/Users/Daniil/...` path. Local `.env`, data, caches and generated dependency directories remain excluded from Git/build contexts.

## Frozen boundary

The shipped service retains SAGE ViT-B source revision `c7d6241c4885526d99d6c78c158024fc2a37097c`, checkpoint revision `2a2ea9964cdbdfd2211e7c625064a9d5e4678245`, checkpoint SHA-256 `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e`, normalized float32 dimension 8,448, exact `faiss.IndexFlatIP`, 20,487 gallery references, top-K 30, single-query aggregation, density-aware geographic-mode voting with rank decay and sequence-deduplicated support, weighted-medoid coordinates, 14-feature confidence model and threshold `0.9349250249145314`. Reranking and approximate retrieval remain disabled.

Production environment variables can change operational values only. The frozen loader rejects conflicting ML overrides. Checkpoint loading remains `weights_only=True` with only the exact required known NumPy safe globals. SAGE source loads from a verified local snapshot and never from unpinned repository HEAD.

## Artifact distribution

`configs/production_artifacts.json` declares ten required, versioned objects. The exact official SAGE commit archive and exact official Hugging Face checkpoint revision remain upstream sources. GeoSnap-generated FAISS index, index metadata, ID mapping, reference metadata, gallery, two smoke inputs, and indexed-reference thumbnails are immutable assets in GitHub Release `geosnap-moscow-v4-artifacts-v1`.

The release was created against the pre-Part-3 application commit solely as an artifact carrier; no application release or PR merge occurred. All uploaded GitHub asset digests were checked against the manifest. Total external source payload is 1,384,949,790 bytes.

The provisioner uses HTTPS without URL credentials, bounded retry/resume, temporary downloads, byte/SHA validation, atomic rename, safe destinations, safe archive extraction, and deterministic extracted-tree hashes. It rejects traversal, links/devices, missing and corrupt inputs. `GEOSNAP_ARTIFACT_DIR` defaults to `/var/lib/geosnap/artifacts` on a named persistent volume. Repeated provisioning reused all ten validated artifacts. Requests and restarts need no model-repository network.

Production thumbnails cover exactly 20,487 indexed IDs. Images are bounded to 480x320, encoded deterministic WebP quality 72 without metadata, mapped by opaque ID to SHA-derived filenames, and retain provider/license/source attribution in reference metadata. The compressed bundle is 215,798,483 bytes with SHA-256 `749ed61ba06c122397763e4fdd5a712dc50cf1c35b71f86f673a129ca6cd5215`; its deterministic extracted tree SHA is `2a72b5daa9c369002a9d2f69d9a72ce1b4871bd26a7ec629d1183df8fbe93e68`. The raw 12+ GB corpus is not shipped.

## Images and topology

The backend uses a pinned multi-stage Python 3.12 slim base, a hash-locked production-only environment, official `torch==2.13.0+cpu`/`torchvision==0.28.0+cpu`, and FAISS CPU. The clean image content size is 293,694,900 bytes; local unpacked/deduplicated Docker disk use is 1.39 GB. CPU Torch is the largest necessary component (`torch` 695 MiB inside a 904 MiB virtualenv). CUDA is unavailable and no NVIDIA runtime/library package is present. Pytest, Ruff, pandas, PyArrow, Transformers, cache data and build tools are absent; the previously missing PrettyTable runtime import is present.

The final Telegram image is 46,523,144 bytes and contains `python-telegram-bot` 22.8 but no Torch, FAISS, SAGE code, checkpoint, index or artifact volume. The final product-UI proxy content image is 21,911,439 bytes. All services run non-root, read-only, capability-dropped containers with bounded tmpfs and stdout log rotation.

Caddy serves the existing built frontend, proxies same-origin `/api/*`, terminates automatic TLS for an operator-supplied domain, compresses, enforces body/time bounds, sets security/cache headers, and keeps metrics private. FastAPI is internal only. The long-polling bot reaches the same `/localize` authority over the internal network.

## Backend safeguards and observability

Production accepts decoded JPEG, PNG and single-frame WebP only, with MIME/signature/format agreement. Defaults are 10 MiB compressed, 10,000 pixels per dimension and 25 million decoded pixels. Decompression bombs, malformed/empty/corrupt content, animation and mismatches fail with safe 4xx responses. Pixels remain in memory; no upload volume exists and EXIF GPS is not an input.

A process-local token bucket defaults to 12 localization requests/minute/client with burst three and `Retry-After`. One active inference, two queued requests, five-second queue wait, and 30-second processing timeout bound resource use. The fourth immediate burst returned 429; four simultaneous expensive requests produced three handled requests and one fast 503 overload. One Uvicorn worker avoids model/index duplication.

Each HTTP request has a safe bounded ID. JSON stdout logs and Prometheus metrics include low-cardinality outcomes/durations/readiness/load timing without coordinates, image content or user identifiers. `/health` is liveness; `/ready` fails closed until every frozen dependency is valid. Missing/corrupt tests return alive/unready and never substitute a model/index.

## Telegram client

The bot implements `/start`, `/help`, highest-useful-resolution photo selection, pre/post-download byte limits, in-memory forwarding, request-ID propagation, per-user cooldown and bounded concurrency. It maps `ok` to accepted-evidence wording, coordinates, map links, attribution and native location; its score is explicitly not a probability. A `low_confidence` candidate is exposed as an explicitly tentative point with coordinates and map buttons but no native Telegram location. `out_of_coverage` and the defensive no-prediction case show no point. Backend rejection/rate/not-ready/timeout/malformed-response and Telegram failures are user-safe. It stores no photo, message, username or persistent Telegram identifier.

Tests mock Telegram and backend networking, so no credential is needed. Production uses long polling. Tokenless validation-only mode checks configuration without Telegram contact; a real token was not present, so no real Bot API smoke was attempted.

## Website scope and validation

The product interface now exposes two clearly distinguished display tiers without changing backend decisions: accepted `ok` results and warning-labelled tentative `low_confidence` candidates. The latter uses a distinct marker, coordinates, map links, and possible matches; true no-prediction and out-of-coverage responses remain point-free. This is a presentation-semantics correction, not a change to the frozen ML operating point.

After the tentative-result correction, the final local gate passed Ruff, 441 Python tests (one explicit network-heavy opt-in skip), the isolated 36-test Telegram suite, 26 frontend unit tests, the production build, and 18/18 Playwright cases across Chromium, WebKit, and mobile Chromium. Compose validation, artifact integrity, real-index inference, security/routing checks, tokenless bot validation, and the end-to-end production verifier also passed. npm and bot dependency audits previously reported no known vulnerabilities. Backend auditing previously reported no known issues among identified pinned packages but could not map the direct CPU Torch/TorchVision wheel URLs to advisory package identities; those wheel hashes remain pinned and this limitation is operationally documented.

## Performance and resources

Linux/amd64 under Docker Desktop emulation on Apple M1 Pro reached runtime-ready in 27.43 seconds (SAGE 13.10 seconds, index 5.23 seconds). Warm p50/p90/p95 was 2.258/2.428/2.529 seconds; mean embedding/retrieval/policy was 2.202/0.033/0.002 seconds. Observed backend RSS was 1.55 GiB idle, 1.89 GiB after the benchmark and 1.97 GiB cgroup peak. The non-native environment missed the optional two-second p95 goal, so native x86 measurements are required before an SLO. The deployment recommendation is 8 modern CPU cores, 8 GiB RAM and 8 GiB free disk; minimum is 4 cores, 4 GiB RAM and 4 GiB disk.

Operational procedures, alerts, rollback and troubleshooting are in `docs/operations.md`. Clean-machine provisioning and HTTPS configuration are in `docs/deployment.md`. No further backend, infrastructure, artifact, or Telegram productionization phase is recommended before the dedicated frontend redesign.
