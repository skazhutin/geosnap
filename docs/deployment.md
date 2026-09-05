# Production deployment

GeoSnap ships as three immutable services: a Caddy-served frontend/reverse proxy, one CPU FastAPI localization process, and a lightweight long-polling Telegram bot. The frozen model/index artifacts live in a named volume and are provisioned independently of images.

## Prerequisites

- A Linux x86-64 host with Docker Engine and Docker Compose v2.
- Enough free disk and memory for the values in `docs/operations.md`.
- A DNS name pointing at the host and inbound TCP 80/443 for public HTTPS, or localhost ports for a local smoke.
- A production-capable raster tile provider URL and its required attribution. A browser token is optional and public by definition.
- A Telegram bot token for live polling. It is not required for validation-only mode.

Only Caddy should be publicly reachable. FastAPI is on the internal Compose network, and its metrics endpoint is denied by the public proxy. The Telegram service reaches FastAPI internally and uses a separate egress-capable network for Telegram.

## Clean-machine install

```bash
git clone https://github.com/skazhutin/geosnap.git
cd geosnap
git checkout finalize-geosnap
cp .env.example .env
```

Fill `.env`. For a real deployment use:

```dotenv
GEOSNAP_SCHEME=https
GEOSNAP_DOMAIN=your-real-dns-name.example
GEOSNAP_HTTP_PORT=80
GEOSNAP_HTTPS_PORT=443
MAP_TILE_URL=https://your-tile-provider.example/tiles/{z}/{x}/{y}.png?key={token}
MAP_TILE_ORIGIN=https://your-tile-provider.example
MAP_ATTRIBUTION=Provider attribution required by contract | © OpenStreetMap contributors
MAP_TILE_PUBLIC_PARAMETER=public-browser-parameter-if-required
TELEGRAM_BOT_PUBLIC_URL=https://t.me/your_public_bot_name
TELEGRAM_BOT_TOKEN=server-side-secret
TELEGRAM_VALIDATE_ONLY=false
```

Do not quote braces in the tile template. `MAP_TILE_ORIGIN` is the scheme and host used by the CSP. `MAP_TILE_PUBLIC_PARAMETER` and optional `TELEGRAM_BOT_PUBLIC_URL` are compiled into browser JavaScript and therefore cannot be server secrets. The website hides its Telegram CTA when the public URL is unset. `TELEGRAM_BOT_TOKEN` remains server-side; never commit `.env`.

Provision, start, and verify:

```bash
make provision-production
make production-up
make verify-production
```

Provisioning downloads every declared artifact over HTTPS, verifies expected byte size and SHA-256, safely extracts archives, validates their deterministic tree hashes, and atomically installs them. Existing valid files are reused. Application startup never downloads artifacts, and ordinary `/localize` requests make no GitHub or Hugging Face calls.

`make production-up` does not provision implicitly: a missing/corrupt volume starts an alive but unready backend and prevents the dependent public services from becoming healthy. Fix the artifact condition and rerun provisioning; no mock fallback exists.

## Local production smoke without Telegram credentials

Use non-secret smoke tile values and validation-only bot mode:

```dotenv
GEOSNAP_SCHEME=http
GEOSNAP_DOMAIN=localhost
GEOSNAP_HTTP_PORT=8080
GEOSNAP_HTTPS_PORT=8443
MAP_TILE_URL=https://your-approved-test-provider.example/{z}/{x}/{y}.png
MAP_TILE_ORIGIN=https://your-approved-test-provider.example
MAP_ATTRIBUTION=Test provider attribution
TELEGRAM_BOT_TOKEN=
TELEGRAM_VALIDATE_ONLY=true
```

Then run the same three commands and open `http://localhost:8080`. Validation-only mode keeps an idle container after checking configuration and makes no Telegram network calls; it is not a substitute for a live-token bot smoke. CI and the production verifier use the one-shot `python -m apps.telegram_bot --validate-config` form.

## Artifact publication

`configs/production_artifacts.json` is the authoritative distribution manifest. SAGE source and checkpoint use their pinned official immutable revisions. GeoSnap-generated index, gallery, smoke, and thumbnail assets are versioned GitHub Release assets. To publish a future explicitly approved artifact version, first regenerate and independently hash it, then create a new immutable versioned release and update every manifest URL, size, hash, and provenance field. Never replace an asset in an existing production version.

Artifact publication does not release or merge application code. A future ML artifact version requires its own research/evaluation approval; infrastructure upgrades keep the current artifact manifest.

## HTTPS and network policy

Caddy obtains and renews certificates automatically when `GEOSNAP_SCHEME=https`, the configured real domain resolves to the host, and ports 80/443 are reachable. HSTS is applied only to HTTPS requests. Local HTTP intentionally receives no HSTS.

Place no second permissive proxy in front without preserving the client's address safely. The production Caddy overwrites `X-Forwarded-For`, and only that trusted internal hop is used by FastAPI's rate limiter. If a cloud load balancer is added, define and test an explicit trusted-proxy chain rather than accepting arbitrary forwarded headers.

## Stop, restart, and remove

```bash
make production-down
make production-up
make verify-production
```

The named artifact volume survives ordinary `down` and container replacement, so valid artifacts are not redownloaded. Do not add `-v` unless intentionally deleting artifacts and Caddy state. Images and services are stateless; uploaded photos are never written to the persistent volume.

## Upgrade and rollback

For an infrastructure/application upgrade:

1. Record the current commit/image identifiers and manifest version.
2. Build the new images while keeping the same frozen config and artifacts.
3. Deploy, wait for `/api/ready`, and run `make verify-production`.
4. If validation fails, redeploy the recorded image/commit and run the verifier again.

Rollback never regenerates ML artifacts. Pin deployed image digests in the hosting platform for repeatable rollback. Changing the frozen config or manifest is a separately reviewed release, not an environment override.
