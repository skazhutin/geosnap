# GeoSnap

GeoSnap is an experimental visual-geolocation product for supported Moscow street scenes. A user sends a street photo from the map-first website or bilingual Telegram bot; one shared FastAPI service retrieves frozen street-view references and returns either an accepted estimate, an explicitly tentative best guess, or no point when coverage is absent.

GeoSnap does not claim complete Moscow coverage. In the frozen final test it answered 127/1,499 queries (8.47%, bootstrap 95% CI 7.07–9.94%). Among those accepted answers, 96.85% were within 100 m (Wilson 95% CI 92.18–98.77%). That conditional result must always be presented together with answer rate and abstention—not as “96.85% accuracy in Moscow.”

## Production architecture

```text
Browser -> Caddy -> current static frontend
                  -> /api/* -> FastAPI -> frozen SAGE + exact FAISS + policy

Telegram -> Bot API -> lightweight bot -> internal FastAPI /localize
                                           |
                                           +-- persistent validated artifact volume
```

FastAPI is the only localization authority. The bot has no Torch, SAGE, or FAISS dependency. Production uses one CPU model worker, bounded inference concurrency and queueing, per-client rate limiting, structured logs, internal Prometheus metrics, strict readiness, same-origin browser routing, and Caddy TLS/security headers.

## Product flow

- **Website:** an `ok` response shows an accepted estimate. A `low_confidence` response that contains a candidate shows a visibly distinct tentative marker, coordinates, map links, and possible visual matches with an explicit warning. Coverage gaps and defensive no-prediction responses show no point.
- **Telegram:** accepted results retain a native location pin. Tentative results provide warning-labelled coordinates and Google/Yandex buttons but deliberately omit Telegram's authoritative-looking native pin. True no-location outcomes remain point-free.

Both experiences cover only supported Moscow street scenes and remain experimental.

The frozen model contract is `configs/moscow_production_frozen.json`, SHA-256 `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`. It fixes SAGE ViT-B, source/checkpoint revisions, normalized 8,448-dimensional float32 descriptors, exact `IndexFlatIP`, K=30, the production gallery, geographic aggregation, weighted medoid, 14-feature confidence model, threshold `0.9349250249145314`, disabled reranking, and disabled approximate retrieval.

## Production deployment

Requirements: Docker Engine/Compose v2 on Linux x86-64, a real domain for HTTPS, an approved production tile provider, and a Telegram token for live bot polling.

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
make test
```

Run the backend and frontend directly when local development artifacts are available:

```bash
make api
make frontend
```

The development Vite server proxies `/api` to `http://localhost:8000`. Development may use the documented OpenStreetMap fallback; production builds require explicit tile URL and attribution. The optional map coverage layer is an aggregate 20×20 grid derived from all 20,487 frozen gallery references; it is coverage evidence, not an accuracy claim. Research, ingestion, embedding, and historical evaluation targets remain in the `Makefile` but are not part of deployment.

## Service contract

- `GET /health`: process liveness only.
- `GET /ready`: succeeds only when all frozen localization dependencies are loaded and valid.
- `POST /localize`: one multipart `image` (JPEG, PNG, or single-frame WebP); returns `ok`, `low_confidence`, or `out_of_coverage` for product outcomes.
- `GET /thumbnails/{reference_id}`: preview for a known opaque indexed reference ID.
- `GET /metrics`: internal Prometheus endpoint, denied by the public proxy.

For `low_confidence`, the API may retain the best candidate coordinates even though that candidate did not pass the frozen acceptance policy. `out_of_coverage`, and the defensive `low_confidence` case without a prediction, expose no point. The confidence value is an evidence/policy score, not a calibrated per-photo probability. Uploads are processed in memory, GPS EXIF is not used for localization, and photos are not permanently retained by default.

## Documentation

- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Operations](docs/operations.md)
- [Security and privacy](docs/security.md)
- [Telegram bot](docs/telegram_bot.md)
- [Production web interface](docs/frontend.md)
- [Completed frontend handoff](docs/frontend_handoff.md)
- [Part 3 productionization report](docs/part3_productionization.md)
- [Final QA evidence](docs/final_qa.md)
- [Frozen localization core](docs/final_localization_core.md)

The product UI is map-first on desktop and uses a deliberate bottom-sheet layout on mobile. Both clients preserve the backend's frozen `ok` versus `low_confidence` decision while presenting them as clearly different accepted and tentative tiers.
