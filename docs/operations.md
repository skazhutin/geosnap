# Production operations

This runbook applies to `docker-compose.prod.yml`. FastAPI is deliberately a single CPU process because every additional Uvicorn worker would duplicate SAGE and the exact FAISS index in memory. The default is one active localization plus a bounded queue of two.

## Service checks

- `GET /api/health` is liveness. It remains HTTP 200 while model startup has failed so the process can be inspected.
- `GET /api/ready` is strict readiness. It is HTTP 200 only after the frozen config, all required artifact hashes, SAGE source/checkpoint, index, reference metadata, gallery, thumbnails, and confidence policy are validated and loaded.
- `GET /metrics` is Prometheus text on the internal backend network. Caddy intentionally returns 404 for `/api/metrics`.

Use the public endpoint for normal checks:

```bash
curl -fsS https://your-domain.example/api/health
curl -fsS https://your-domain.example/api/ready
```

For internal metrics inspection:

```bash
docker compose --env-file .env -f docker-compose.prod.yml exec -T backend \
  python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/metrics').read().decode())"
```

Readiness includes the application version, short frozen-config hash, and a non-secret runtime identity. It never exposes a token or host filesystem path.

## Logs and request IDs

Applications write JSON to stdout. Backend request events include timestamp, level, request ID, normalized endpoint, HTTP status, duration, localization status, embedding/retrieval/policy durations, and safe error category when applicable. Coordinates, uploads, EXIF, reference IDs, usernames, Telegram IDs, and secrets are omitted by default.

An incoming `X-Request-ID` is retained only when it matches the bounded safe syntax; otherwise the backend creates a UUID. The ID is returned in the response header and is the preferred correlation key for support.

```bash
docker compose --env-file .env -f docker-compose.prod.yml logs --since=10m backend
docker compose --env-file .env -f docker-compose.prod.yml logs --since=10m telegram-bot
```

Compose uses Docker's `json-file` driver with three 10 MiB files per service. A hosting platform may replace this with its own bounded stdout collector.

## Prometheus metrics

The backend exposes low-cardinality series for:

- `geosnap_http_requests_total` and `geosnap_http_request_duration_seconds`;
- `geosnap_localization_requests_total`, including `ok`, `low_confidence`, `out_of_coverage`, overload, and rate-limit outcomes;
- `geosnap_invalid_uploads_total`, `geosnap_rate_limited_total`, and `geosnap_http_5xx_total`;
- embedding, retrieval, and policy duration histograms;
- backend readiness, most recent model-load duration, and most recent index-load duration.

Do not add request IDs, coordinates, Telegram users, or reference IDs as metric labels.

Recommended alerts are: readiness unavailable for five minutes; repeated `localization_service_unavailable` or checksum failures; sustained 5xx increase; p95 latency materially above the established host baseline; RSS above 80% of the container/host budget; frequent 429/overload responses; disk pressure; and repeated Telegram backend timeout/unavailable categories.

## Measured CPU runtime

Measurements were taken on 4 September 2026 using the final Linux/amd64 CPU container under Docker Desktop x86 emulation on an Apple M1 Pro (10 host CPU cores, 16 GiB host RAM, Docker allocation 10 CPUs/7.75 GiB, `BACKEND_CPU_THREADS=4`). This is a reproducibility environment, not a native x86 production benchmark; emulation makes the latency result conservative.

| Measurement | Result |
| --- | ---: |
| Backend process start to runtime-ready log | 27.43 s |
| Readiness observed through proxy/health polling | 29 s |
| SAGE load gauge | 13.10 s |
| FAISS index load gauge | 5.23 s |
| Warm request p50 / p90 / p95 | 2.258 / 2.428 / 2.529 s |
| Warm request mean / min / max (10 requests) | 2.297 / 2.196 / 2.529 s |
| Mean embedding / retrieval / policy | 2.202 / 0.033 / 0.002 s |
| Backend RSS, idle / observed post-benchmark | 1.55 / 1.89 GiB |
| Backend cgroup peak during benchmark | 1.97 GiB |

The emulated host did not meet the aspirational warm p95 below two seconds. No frozen model or policy setting was changed to compensate. Deploy on native x86-64 with AVX2/AVX-512 before establishing an external SLO.

Concurrency measurements used the default one active request, queue length two, and five-second queue timeout:

| Submitted together | Outcome | Wall time / request behavior |
| ---: | --- | --- |
| 1 | 1/1 HTTP 200 | 2.654 s |
| 2 | 2/2 HTTP 200 | 4.911 s wall; requests serialized at 2.570/4.911 s |
| 4 | 3 HTTP 200, 1 immediate HTTP 503 | 6.808 s wall; the fourth request was rejected in 0.073 s |

This establishes a safe default, not a throughput target. Increase concurrency only after measuring peak RSS and tail latency on the actual native host; do not increase Uvicorn worker count casually.

## Capacity and disk recommendation

- Minimum: 4 native x86-64 CPU cores, 4 GiB RAM, and 4 GiB free persistent/runtime disk.
- Recommended: 8 modern x86-64 CPU cores, 8 GiB RAM, and 8 GiB free disk.
- Artifact payload sources total 1,384,949,790 bytes; the installed persistent volume is 1,603,730,319 bytes across 20,526 files because it retains verified download archives plus extracted SAGE/thumbnails.
- Final compressed/content image sizes are 293,694,900 bytes backend, 46,523,144 bytes bot, and 21,911,439 bytes proxy. The backend's unpacked runtime is about 1.1 GiB; `/opt/venv` is 904 MiB, dominated by CPU Torch at 695 MiB.
- Container writable layers remain effectively empty (about 33 KiB backend and 4 KiB each proxy/bot during the smoke). Reserve remaining recommended disk for Docker layer unpacking, Caddy state, bounded logs, and rollback images.

The production volume contains no raw 12+ GiB historical imagery. Its preview set is the optimized, indexed-only WebP bundle.

## Restart, failure, and recovery

Normal restart preserves the named artifact volume:

```bash
docker compose --env-file .env -f docker-compose.prod.yml restart backend
make verify-production
```

The measured restart reused all ten artifacts (`already_valid` on a provisioner rerun), reached readiness in 29 seconds, and returned the same real smoke outcome. Ordinary startup performs local validation and never redownloads artifacts.

If readiness fails:

1. Inspect the backend's `localization_service_unavailable` event and artifact error category.
2. Run `make provision-production`; valid objects are retained and missing/corrupt objects are reacquired safely.
3. Restart the backend and wait for `/api/ready`.
4. Run `make verify-production` before restoring traffic.

Do not enable a mock model/index, weaken a checksum, modify the frozen threshold, or delete the production volume as a first response. If a release source is unavailable but the current volume validates, a restart remains network-independent.

## Upgrade and rollback

Keep image digests, the frozen config hash, and artifact manifest version with every deployment record. Build the next application images, retain the same volume, deploy, wait for readiness, and run the verifier. Roll back to the prior image digest if verification fails. Rollback does not regenerate or mutate localization artifacts. Any future ML change requires a new explicitly evaluated/versioned artifact/config release outside routine operations.
