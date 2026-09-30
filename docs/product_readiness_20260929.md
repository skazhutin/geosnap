# GitHub and product readiness — 29 September 2026

The application now supports up to ten photos from the same place through Telegram and a new `/localize/multi` API. The website remains a single-photo client. The frozen SAGE model, gallery, acceptance policy and single-image response contract were retained.

## Delivered

- Explicit album completion, `/photos`, `/done`, `/cancel`, localized counters and messages, scoped collection ownership, expiry, duplicate handling, bounded downloads and inference, and tentative multi-photo results.
- Geographic consensus across independent per-image candidate hypotheses. No collage or averaged coordinate between distant hypotheses. Conflicting modes produce no point; scores are not calibrated probabilities.
- Backend and proxy limits for 40 MiB batches, extended batch timeouts, rate limiting and bounded capacity. Timed-out requests keep the capacity lease until their worker actually finishes.
- Mobile map correction: the selected location stays above the bottom sheet after viewport changes and with reduced-motion preferences enabled.
- README, deployment instructions, research-versus-production score definitions, multi-photo API examples and contributing instructions. `make api-frozen` and `make bot` simplify local use.
- Generated research environments, images, models and private local state excluded from Git and deployment build contexts. Historical research sources remain intact; they use separate environments and are excluded from product CI.

## Verification

| Check | Result |
|---|---|
| Python product/core suite | 462 passed, 1 skipped (opt-in official checkpoint download) |
| Frontend unit tests | 26 passed |
| Browser scenarios | 24 passed across Chromium, WebKit and mobile Chromium |
| Ruff / diff whitespace | Passed |
| Production frontend build | Passed; existing lazy MapLibre chunk produces a size warning |
| Backend, lightweight bot, Caddy images | Built for deployment; bot image contains neither Torch nor FAISS |
| Compose and Caddy configuration | Valid |
| Unprovisioned container | `/health` 200, `/ready` 503; no fake model fallback |
| Frozen real-model smoke cases | Expected `ok`, `low_confidence`, `low_confidence` |
| Real two-image API and bot-handler smoke | Consensus, 2/2 unique images; 2.18 s API wall time on the local CPU |
| Website with real model | Image upload, accepted marker, thumbnails and source attribution work; desktop/mobile inspected; no browser errors |
| Telegram read-only API | `getMe` valid for `geosnap_vbot`; no configured webhook |
| Artifact URLs | All 10 pinned distribution URLs returned HTTP 200 to HEAD checks |
| Publishable working-tree file scan | No matches for private-key, Hugging Face, GitHub or Telegram token patterns; no files above 5 MiB at inspection |

The Telegram transport in the multi-photo smoke was mocked; image download bytes, real HTTP backend calls and frozen-model inference were exercised. Read-only token validation does not substitute for sending an album in the deployed Telegram bot. No messages were sent to Telegram users during QA.

The two images in the smoke were nearby indexed reference images. This verifies wiring and cannot estimate generalization or the accuracy gain from multiple smartphone photos. No independent multi-photo accuracy is claimed.

## Integrity and remaining deployment work

The frozen runtime config SHA-256 remains `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`; artifact manifest remains `0d9bd64e53acea1cd2e97ae8d68718f6b5777405f0a54eb04ee3a28e541f9008`. The production verifier checked the actual gallery/index files and model contract. No tracked `ml/` or `configs/` source changed. Application source was intentionally changed for this requested feature; the older research guard covering 85 application files is preserved and is **not** claimed unchanged.

Publication and deployment are separate from this local implementation. Use a reviewed commit, approved tiles/domain and one polling instance per Telegram token. Existing `.env` files must pick up the new batch limits/timeouts described in [deployment](deployment.md). Then run the deployed bot's album flow end to end. No Git push or public deployment was performed here.

Research reports link to local artifacts that are deliberately not redistributed in Git. MIT covers repository code, not third-party model weights or street photographs; MSLS remains research-only. The working-tree pattern scan is not a complete historical secret audit.

## Repeat checks

```bash
make test
make production-config compose-config
make verify-moscow-production                 # needs local frozen artifacts
.venv/bin/pytest -q apps/backend/tests/test_multi_photo.py apps/telegram_bot/tests/test_multi_photo.py
# Deployment with provisioned artifacts and configured .env:
make provision-production
make production-up
make verify-production
```

Local QA logs, browser captures, hash receipts and the real-model smoke record are under `data/evaluation/product_readiness_20260929/` (ignored generated output). See [multi-photo protocol](multi_photo.md) for reproducible HTTP examples and the exact consensus rule.
