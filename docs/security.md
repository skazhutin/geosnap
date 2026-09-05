# Security and privacy controls

This document describes controls implemented in the Part 3 production topology. It is not a legal compliance certification.

## Trust boundaries and secrets

Caddy is the only public service. FastAPI and Prometheus metrics are reachable only on the internal Compose network; the bot calls FastAPI there rather than bypassing it. Caddy replaces the client forwarding header before FastAPI uses it for rate limiting. Browser requests are same-origin and production CORS defaults to no allowed cross-origin browser origins.

Secrets are environment-only. `.env` is ignored. `TELEGRAM_BOT_TOKEN`, acquisition credentials, and storage credentials are absent from images, Git, frontend arguments, logs, and examples. `VITE_*` and map-tile variables are public browser configuration and must not contain secrets. Containers run as unprivileged users with a read-only root filesystem, all capabilities dropped (Caddy retains only bind-service), `no-new-privileges`, bounded tmpfs, and rotated stdout logs.

## Upload and inference denial-of-service bounds

FastAPI accepts one multipart field named `image`, with JPEG, PNG, or single-frame WebP MIME/signature/decoded-format agreement. It rejects empty, malformed, corrupt, animated, undersized, oversized, MIME-mismatched, and extension-mismatched files. Pillow fully decodes the file; decompression-bomb warnings are errors. Production defaults are 10 MiB compressed bytes, 10,000 pixels per dimension, and 25 million decoded pixels. Caddy applies an outer request-body and upload timeout.

Decoded RGB pixels exist only in process memory. GeoSnap does not execute uploaded content, save user images, or use EXIF GPS as localization evidence. EXIF orientation can be applied for correct display geometry, but full EXIF and GPS are not logged. No upload directory or persistent user-image volume exists.

One FastAPI worker owns one model/index copy. A semaphore defaults to one active localization, with a queue of two and a five-second queue timeout. Inference has a 30-second client timeout; capacity remains occupied until timed-out background work actually ends. A bounded in-memory token bucket defaults to 12 requests/minute per proxy-supplied client address with a burst of three and returns HTTP 429 plus `Retry-After`. The Telegram client adds its own per-user cooldown, download bound, and concurrency bound but still passes through backend controls.

The limiter is process-local and intentionally sized for the single-worker MVP. A multi-replica deployment needs a shared limiter or equivalent enforcement at a trusted gateway; simply increasing Uvicorn workers would duplicate model/index memory and split limiter state.

## Artifact and model integrity

`configs/production_artifacts.json` pins HTTPS sources, versions, expected bytes, SHA-256, destination, provenance, and deterministic extracted-tree hashes. The provisioner rejects credentials in URLs, unsafe destinations, HTTP redirects, traversal members, links, devices, size mismatches, and hash mismatches. Files download to temporary names and become visible only through atomic replacement after validation. Valid persistent artifacts are reused.

Backend startup validates all required distribution artifacts, then the frozen config separately validates gallery/index/confidence compatibility. Missing or corrupt artifacts make `/ready` fail and never select a fallback. Requests never fetch artifacts.

SAGE code is loaded from a verified snapshot of exact revision `c7d6241c4885526d99d6c78c158024fc2a37097c`. The checkpoint is exact revision `2a2ea9964cdbdfd2211e7c625064a9d5e4678245`, SHA-256 `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e`. Checkpoint loading retains the restricted weights-only/safe-globals behavior and does not use unrestricted pickle deserialization. Production has no network fallback to repository HEAD.

## Reference thumbnails

Only the 20,487 indexed production reference IDs appear in the generated thumbnail manifest. Each ID maps to a SHA-256-derived filename under a fixed thumbnail root. The service validates the full mapping against loaded reference metadata and a strict relative filename pattern. The HTTP route accepts an opaque ID and asks the service for a known mapping; it never concatenates user input into a filesystem path and never fetches a user-supplied or metadata URL. This removes path-traversal and SSRF behavior. Unknown IDs fail with 404 and no storage path disclosure.

Provider attribution, license, and source links remain API fields. Thumbnail production strips metadata, bounds dimensions to 480x320, and uses deterministic WebP settings. Operators remain responsible for compliance with the recorded provider/license terms.

## HTTP and error safety

Caddy enables gzip/zstd, `nosniff`, strict-origin referrers, frame denial plus CSP `frame-ancestors 'none'`, a map-aware restrictive CSP, a restrictive permissions policy, and HSTS only on HTTPS. Hashed assets are immutable-cacheable, HTML is not cached, `/localize` is `no-store`, and thumbnails cache for one day. The public proxy denies `/api/metrics`.

Every request gets a generated UUID unless the incoming `X-Request-ID` matches the bounded 128-character safe pattern. IDs are returned in headers and structured logs. Error bodies use typed statuses and fixed safe messages; no exception, filesystem path, token, image bytes, or stack trace reaches a client. Server logs may contain an exception trace and category for operators but omit coordinates and image content.

## Known limitations

- TLS depends on correct DNS/firewall configuration and Caddy's ability to reach its certificate authority.
- The in-memory limiter is not a distributed abuse-control system.
- Application controls cannot prevent a client from copying an estimate or provider thumbnail after delivery.
- Telegram is an external processor in the bot flow and receives photos before GeoSnap.
- Container and dependency scanning is a point-in-time signal; rebuild and re-audit routinely.
- The model can be wrong. It answers only a small subset of evaluated queries and is not a safety-critical locator.
