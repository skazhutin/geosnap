# GeoSnap

GeoSnap is an experimental visual-geolocation product for supported Moscow street scenes. A user sends a street photo from the map-first website or bilingual Telegram bot; one shared FastAPI service retrieves indexed street-view references and returns either an accepted estimate, an explicitly tentative best guess, or no point when coverage is absent. The model and answer policy depend on the selected runtime configuration.

Useful for street-scene matching in covered Moscow areas, experimenting with visual place recognition, and building photo-to-map interfaces with explicit uncertainty. It is not a citywide navigation or location-verification service.

GeoSnap does not claim complete Moscow coverage. In the **historical frozen production evaluation** it answered 127/1,499 queries (8.47%, bootstrap 95% CI 7.07–9.94%). Among those accepted answers, 96.85% were within 100 m (Wilson 95% CI 92.18–98.77%). That conditional result must always be presented together with answer rate and abstention—not as “96.85% accuracy in Moscow.” This historically opened set is not a new independent validation.

The separate SAGE-L research pipeline reached **411/1,184 = 34.71% RAW within 100 m** on the original 112,163-image gallery, with every development query in the denominator. The cleaned gallery retains **111,032** images, including 67,100 research-only MSLS images; its measured development result was 409/1,184 = 34.54%. The 400/1,071 = 37.35% result describes a different, filtered query population, not a model improvement. The local site and bot were verified with a separate cleaned-gallery SAGE-L runtime on 30 September, then Docker Desktop was stopped at the user's request. The default frozen SAGE-B release remains available for rollback. See [research results](docs/research.md) and the [local rollout record](docs/cleaned_sage_l_local_rollout_20260930.md).

## Production architecture

```text
Browser -> Caddy -> current static frontend
                  -> /api/* -> FastAPI -> selected SAGE + exact FAISS + policy

Telegram -> Bot API -> lightweight bot -> internal FastAPI /localize or /localize/multi
                                           |
                                           +-- validated frozen volume or read-only local candidate index
```

FastAPI is the only localization authority. The bot has no Torch, SAGE, or FAISS dependency. The Docker topology uses one CPU model worker, bounded inference concurrency and queueing, per-client rate limiting, structured logs, internal Prometheus metrics, strict readiness, same-origin browser routing, and Caddy TLS/security headers. Docker is currently stopped on the original Mac; these services are not live until it is started again.

## Product flow

- **Website:** an `ok` response shows an accepted estimate. A `low_confidence` response that contains a candidate shows a visibly distinct tentative marker, coordinates, map links, and possible visual matches with an explicit warning. Coverage gaps and defensive no-prediction responses show no point.
- **Telegram:** accepted results retain a native location pin. Tentative results provide warning-labelled coordinates and Google/Yandex buttons but deliberately omit Telegram's authoritative-looking native pin. True no-location outcomes remain point-free.
- **Up to 10 photos from one place:** send a Telegram album, then press **Locate / Определить**. Alternatively use `/photos`, send individual photos, and `/done`; `/cancel` clears the set. Use different directions from the same position. Each image is localized independently. Agreement among selected top results takes precedence; geographic candidate consensus is the fallback. Duplicate images cannot increase support. Conflicting sets return no point. Multi-photo estimates are experimental and tentative: an accuracy gain on real smartphone sessions has not yet been established.

Both experiences cover only supported Moscow street scenes and remain experimental.

The default frozen model contract is `configs/moscow_production_frozen.json`, SHA-256 `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`. It fixes SAGE ViT-B, source/checkpoint revisions, normalized 8,448-dimensional float32 descriptors, exact `IndexFlatIP`, K=30, the 20,487-image gallery, geographic aggregation, weighted medoid, 14-feature confidence model, threshold `0.9349250249145314`, disabled reranking, and disabled approximate retrieval. The opt-in cleaned-gallery runtime uses a separate configuration and always marks its outputs tentative; it has no calibrated confidence model.

## Production deployment

The commands below install the **default frozen release**. Requirements: Docker Engine/Compose v2 on Linux x86-64, a real domain for HTTPS, an approved production tile provider, and a Telegram token for live bot polling. The local 111,032-image candidate instead requires the external artifacts and overlay described in its [rollout record](docs/cleaned_sage_l_local_rollout_20260930.md); it is not a clean-clone deployment.

```bash
cp .env.example .env
# Fill domain, tile-provider contract, and TELEGRAM_BOT_TOKEN.

make provision-production
make production-up
make verify-production
```

The manifest at `configs/production_artifacts.json` downloads exact SAGE, checkpoint, index, metadata, gallery, smoke, and optimized reference-thumbnail artifacts from pinned official sources and the versioned GitHub artifact release. Provisioning verifies sizes, SHA-256 and archive tree hashes before atomic installation. The named volume survives restarts, and application requests never download model artifacts.

For a local topology smoke without a Telegram credential, set `TELEGRAM_VALIDATE_ONLY=true`; this checks bot configuration without contacting Telegram. See [production deployment](docs/deployment.md) for the complete clean-machine procedure and HTTPS setup.

## Development

Python 3.12 and Node 20+ are expected:

```bash
make setup
cd apps/frontend && npx playwright install chromium webkit && cd ../..
make test
```

Run the backend and frontend directly when local development artifacts are available:

```bash
make api-frozen
make frontend
```

`make api-frozen` requires the existing local frozen artifacts; a fresh clone should use the provisioned Docker deployment above. `make api` remains the configurable custom-gallery development entry point. `make bot` starts the local Telegram client against port 8000 using the token in `.env`; do not run it alongside another polling instance for that token.

The development Vite server proxies `/api` to `http://localhost:8000`. Development may use the documented OpenStreetMap fallback; production deployment requires explicit tile URL and attribution. The optional map coverage layer is an aggregate 20×20 grid derived from all 20,487 frozen-gallery references; it is coverage evidence for that historical gallery, not an accuracy claim or a map of the cleaned candidate. Research, ingestion, embedding, and historical evaluation targets remain in the `Makefile` but are not part of deployment. Product checks exclude artifact-dependent historical `ml/research/` snapshots, which retain their original hashes and isolated environments.

## Service contract

- `GET /health`: process liveness only.
- `GET /ready`: succeeds only when all dependencies of the selected localization runtime are loaded and valid; its runtime identity reports the actual index and gallery count.
- `POST /localize`: one multipart `image` (JPEG, PNG, or single-frame WebP); returns `ok`, `low_confidence`, or `out_of_coverage` for product outcomes.
- `POST /localize/multi`: 1–10 repeated multipart `images`, up to 40 MiB combined by default. Returns the same fields plus `multi_photo` evidence. See [multi-photo protocol](docs/multi_photo.md) for limits, consensus rules and examples.
- `GET /thumbnails/{reference_id}`: preview for a known opaque indexed reference ID when that runtime has a validated thumbnail bundle. The cleaned local candidate does not provide one.
- `GET /metrics`: internal Prometheus endpoint, denied by the public proxy.

For `low_confidence`, the API may retain the best candidate coordinates. Under the frozen default this means the candidate did not pass its acceptance policy; under the cleaned local candidate every result is tentative and numeric confidence is set to 0. `out_of_coverage`, and the defensive `low_confidence` case without a prediction, expose no point. Confidence must not be interpreted as a calibrated per-photo probability. Uploads are processed in memory, GPS EXIF is not used for localization, and photos are not permanently retained by default.

## Documentation

- [Documentation map: current versus historical](docs/README.md)
- [Project history and decisions](docs/history.md)
- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Cleaned SAGE-L local rollout and rollback](docs/cleaned_sage_l_local_rollout_20260930.md)
- [Operations](docs/operations.md)
- [Security and privacy](docs/security.md)
- [Telegram bot](docs/telegram_bot.md)
- [Multi-photo protocol](docs/multi_photo.md)
- [Research results and reproducibility](docs/research.md)
- [Contributing](CONTRIBUTING.md)
- [Production web interface](docs/frontend.md)
- [Frozen localization core](docs/final_localization_core.md)

The product UI is map-first on desktop and uses a bottom-sheet layout on mobile. Both clients preserve the backend's `ok` versus `low_confidence` decision while presenting them as distinct accepted and tentative tiers.

## Code, model and data licenses

Repository code is [MIT licensed](LICENSE). This does not relicense third-party weights or street photographs. Model revisions, distribution hashes and provenance for the default release are recorded in `configs/production_artifacts.json` and the frozen runtime config. Preserve provider attribution and follow the source-specific terms when redistributing references. MSLS remains research-only: it is absent from the frozen production gallery but present in the opt-in local candidate. Do not publish that gallery or its descriptors without a separate rights review. Downloaded photos, models, private configuration and full research caches are intentionally excluded from Git.
