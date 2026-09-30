# GeoSnap architecture

GeoSnap is one visual-localization platform with two clients. FastAPI is the only localization authority; the website and Telegram bot cannot select or implement a model policy independently.

```text
Browser
   |
   v
Caddy reverse proxy (TLS, limits, headers, compression)
   |                         external raster tile provider
   +---- / -> map-first frontend -------------------^
   |             +---- frozen aggregate coverage grid
   |
   +---- /api/* -> FastAPI (one CPU process)
                         |
                         +---- selected SAGE runtime (frozen ViT-B by default)
                         +---- exact FAISS IndexFlatIP
                         +---- selected geographic/confidence policy
                         +---- read-only frozen volume or local candidate binds
                         +---- internal Prometheus metrics

Telegram user -> Telegram Bot API -> bot container
                                          |
                                          +---- internal FastAPI /localize or /localize/multi
```

## Runtime components

| Component | Responsibility | State/dependencies |
| --- | --- | --- |
| Caddy/proxy | Serves the built frontend, removes `/api`, proxies to FastAPI, terminates TLS, applies body/time limits, compression, security/cache headers, and denies public metrics. | Caddy certificate/config volumes; no model data. |
| Frontend | Keeps a Moscow map visible, sends one multipart image to same-origin `/api/localize`, renders the three product statuses and safe reference thumbnails, and optionally overlays the frozen aggregate coverage grid. | Static hashed assets only. Tile URL/attribution/public token and optional public Telegram URL are build inputs. |
| FastAPI | Validates uploads, assigns request IDs, enforces rate/capacity limits, owns one selected localization service, exposes liveness/readiness and internal metrics, and returns typed safe responses. | Read-only artifacts; one model/index in memory. |
| Localization service | Embeds RGB pixels and retrieves exact FAISS candidates. The default frozen service uses top-30 geographic/confidence policy; the optional cleaned SAGE-L service uses top-100 retrieval and context reranking. | SAGE checkpoint, FAISS index and metadata; default service also uses a confidence artifact. |
| Telegram bot | Long-polls Telegram, manages in-memory Russian/English preference and up to ten photos from one place, downloads bounded photo bytes into memory, calls internal FastAPI, maps accepted or tentative responses to localized chat text, and adds cooldown/concurrency protection. | Bot token and network access to Telegram/FastAPI. No ML dependency. |
| Artifact provisioner | Downloads pinned assets, validates bytes/SHA-256, safely extracts archives, validates tree hashes, and installs atomically. | Temporary egress plus named persistent artifact volume. |

The production services are stateless apart from the artifact and Caddy volumes. User photos, chat messages, and localization results are not written to persistent storage.

## Frozen localization boundary

For the **default release**, `configs/moscow_production_frozen.json` with SHA-256 `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e` is authoritative. Default-release startup fails when an environment override conflicts with its ML fields.

The frozen core is SAGE ViT-B at source revision `c7d6241c4885526d99d6c78c158024fc2a37097c`, checkpoint revision `2a2ea9964cdbdfd2211e7c625064a9d5e4678245` and checkpoint SHA-256 `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e`. Descriptors are normalized float32 with dimension 8,448. Retrieval is exact `faiss.IndexFlatIP` with K=30 over 20,487 production references.

The production policy retains density-aware geographic-mode voting, rank decay, sequence-deduplicated support, weighted-medoid coordinates, the 14-feature confidence artifact, and threshold `0.9349250249145314`. Reranking and the approximate tier are disabled. Deployment changes do not alter these semantics.

The benchmark must be interpreted as an abstaining system: 127/1,499 answers (8.47% answer rate), with 96.85% <=100 m conditional accuracy among accepted answers. It is incorrect to call this “96.85% accuracy in Moscow.”

The separately cleaned 111,032-image gallery has its own exact FAISS index and SAGE-L local runtime. It was verified through the site and bot backend on 30 September, then stopped with Docker Desktop at the user's request. It contains research-only MSLS and has no calibrated confidence model or independently established product accuracy. The 43,932-image Mapillary/KartaView subset has no built and calibrated replacement production runtime. See the [local rollout](cleaned_sage_l_local_rollout_20260930.md) and [research status](research.md).

## Artifact distribution and offline runtime

Git contains frozen JSON configuration and the small confidence artifact, not the large checkpoint/index/gallery/thumbnail payload. `configs/production_artifacts.json` declares 10 exact artifacts and their version, immutable HTTPS source, size, SHA-256, destination, provenance, requirement status, and archive tree hash when relevant.

SAGE source comes from the exact official Git commit archive and its checkpoint from the exact official Hugging Face revision. GeoSnap-generated index, metadata, gallery, smoke inputs, and production thumbnail bundle are immutable assets of `geosnap-moscow-v4-artifacts-v1` on the repository's GitHub Releases page.

The named artifact volume is mounted at `GEOSNAP_ARTIFACT_DIR` (default `/var/lib/geosnap/artifacts`). The provisioner downloads to temporary files and atomically replaces only validated targets. The backend performs validation before loading and opens the SAGE snapshot through local Torch Hub source. Neither startup after provisioning nor requests depend on GitHub/Hugging Face access. Restarting reuses the volume.

Frozen-release previews cover exactly its indexed reference set. Reference IDs map to SHA-256 filenames in a validated manifest; the service never uses the historical raw `image_path` in artifact mode and never performs remote thumbnail fetching. The cleaned local candidate does not have a matching thumbnail bundle, so it reports matches without previews.

## HTTP and status flow

The browser posts to `/api/localize`; Caddy strips `/api` and calls FastAPI `/localize`. The bot posts directly to `http://backend:8000/localize` for one photo or `/localize/multi` for a completed set. Each image in a set is independently localized through the same selected model and index; the separate [multi-photo consensus](multi_photo.md) is tentative and has no independent smartphone accuracy claim.

`ok` has a prediction and matches and is displayed as the accepted tier. `low_confidence` may retain a best candidate prediction and evidence, but both clients display it as a warning-labelled tentative tier. Under the default release it has not passed the frozen threshold; under the cleaned local runtime every output is tentative because no confidence model is calibrated. The website uses a distinct marker, while Telegram deliberately omits its native location pin. `low_confidence` without a prediction and `out_of_coverage` display no point. Failures are distinct typed 4xx/5xx statuses. Thumbnail URLs are backend-relative `/thumbnails/{opaque-id}` when the selected runtime has a validated thumbnail bundle.

FastAPI accepts one bounded, decoded JPEG/PNG/WebP and does not use EXIF GPS. One Uvicorn worker avoids duplicating model/index memory; a semaphore and bounded queue control expensive work. Timeout cancellation does not release capacity while a worker thread is still computing.

## Observability

Every request receives a bounded request ID, returned as `X-Request-ID`. JSON stdout logs include timestamp, level, endpoint, status, duration, localization outcome and stage timings without image bytes or predicted coordinates. Docker log rotation is configured.

`/health` reports process liveness only. `/ready` succeeds only after the selected runtime's config, checkpoint, exact index, metadata, and any required confidence policy load; it reports the active index identity and gallery count. `/metrics` exposes low-cardinality Prometheus counters, histograms and gauges internally; Caddy returns 404 for public `/api/metrics`.

## Map boundary and attribution

Tiles are fetched by the browser from an operator-selected production provider. The production build requires URL and attribution and rejects the standard public `tile.openstreetmap.org` endpoint. Caddy's CSP admits only the configured tile origin. A tile token, if used, is necessarily public. Provider, OpenStreetMap-data, Mapillary, and KartaView attribution remains part of the display contract.

The current coverage overlay is a 28.5 KB static aggregate derived from the **frozen 20,487-reference gallery** and bound to its SHA-256. It exposes 93 occupied cells of the existing 20×20 Moscow grid, relative density classes, counts and provider diversity—not raw reference points. It loads only when selected (or with `?coverage=1`) and is explicitly described as coverage, not accuracy. It does **not** describe the opt-in 111,032-image gallery; this mismatch must be resolved before presenting that overlay as coverage for the cleaned candidate.

## Development and production separation

The existing development Compose file retains direct localhost ports and mounted developer data/cache paths. Production uses `docker-compose.prod.yml`: no reload/debug/mock fallback, no public backend port, strict artifact validation, one worker, same-origin routing, safe CORS defaults, and immutable external artifacts. Research/evaluation commands remain available but are outside the deployment path.
