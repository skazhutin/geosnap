# Production web interface

GeoSnap ships a map-first single-page interface. The map occupies the main viewport on desktop, with a bounded right-side task panel; on small screens the panel becomes a scrollable bottom sheet over a still-visible map. The initial camera is Moscow and the product never requests browser location.

## User flow

The explicit UI state machine is `idle → selected → processing → ok | low_confidence | out_of_coverage | error`. JPEG, PNG and WebP uploads are accepted up to 10 MiB. Selection, drag/drop, replacement and removal all use the same validation path. Choosing a new file aborts the active request and invalidates its sequence number, so a stale response cannot replace newer state. Blob preview URLs are revoked on replacement and unmount.

`ok` shows one estimated-location marker, coordinates rounded to four decimals, copy feedback, tested Google Maps and Yandex Maps links, the retained query preview, and the strongest three references with optional expansion. The evidence score is a ranking signal, never a probability. The backend uncertainty field is intentionally not visualized because it is not calibrated.

`low_confidence` and `out_of_coverage` retain the photo and provide a retry path, but show no marker, coordinates or external map links. Errors distinguish client validation, invalid backend input, readiness, capacity, rate limiting with `Retry-After`, timeout, network failure, malformed response and internal failure.

## Map and coverage

Production map configuration is compiled from public environment values:

- `MAP_TILE_URL` / `VITE_MAP_TILE_URL`: provider raster template;
- `MAP_ATTRIBUTION` / `VITE_MAP_ATTRIBUTION`: required attribution;
- `MAP_TILE_PUBLIC_PARAMETER` / `VITE_MAP_PUBLIC_PARAMETER`: optional public browser token;
- `MAP_TILE_ORIGIN`: CSP origin used by Caddy.

Production must not use the standard OpenStreetMap tile service as an unlimited backend. OpenStreetMap data attribution and provider-specific attribution must remain visible.

The optional coverage layer is lazy-loaded from `public/coverage/production-gallery-grid.json`. It contains 93 occupied cells of the existing 20×20 Moscow grid, aggregated from exactly 20,487 production references. It is bound to frozen gallery SHA-256 `ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da`. The 28,504-byte payload contains cell counts, relative sparse/medium/dense classes and provider diversity—never raw reference points. Coverage is off by default and can be enabled with the control or `?coverage=1`. The legend explicitly says coverage is not accuracy.

Regenerate it only from matching production reference metadata:

```bash
python tools/generate_frontend_coverage.py \
  --metadata /path/to/reference_metadata.jsonl \
  --gallery-sha256 ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da \
  --output apps/frontend/public/coverage/production-gallery-grid.json
```

## Public integrations and accessibility

Set `TELEGRAM_BOT_PUBLIC_URL=https://t.me/<bot-name>` to compile a Telegram CTA into the public site. It is hidden when unset or malformed; this public value must not be confused with `TELEGRAM_BOT_TOKEN`.

The interface uses semantic headings and controls, visible keyboard focus, live announcements, non-color status wording and reduced-motion camera behavior. Map tile failure leaves result coordinates and external actions usable.

## Verification

```bash
npm --prefix apps/frontend test -- --run
npm --prefix apps/frontend run build
npm --prefix apps/frontend run test:e2e
```

Playwright runs the map shell, accepted result, both abstentions and coverage in Chromium, WebKit and a mobile Chromium viewport.
