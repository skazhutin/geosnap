# Part 3 final QA

Evidence date: 5 September 2026. `PASS` means the named check produced a concrete successful result in the local final gate; limitations are stated inline rather than being silently treated as passes. The tentative-result usability correction began from expected clean HEAD `e6ad36b7cbb73dfce5ea23978d1dc15f0e7a2f4a`.

| Check | Result | Evidence |
| --- | --- | --- |
| Correct branch/start point | PASS | Work began on `finalize-geosnap` at exact expected `cbcfb0dd3caf1dcdc86c7f720b58da8a73f79d50`; initial tree was clean. |
| Frozen config | PASS | SHA-256 remains `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e`; verifier checks it before any smoke. |
| Clean backend Docker build | PASS | `docker compose build --no-cache backend telegram-bot` completed from pinned base and hash-locked dependencies. |
| Backend image size | PASS | 293,694,900-byte content image (1.39 GB local unpacked/deduplicated Docker disk), down from 17.5 GB local Docker disk/6.30 GB content. |
| Clean bot Docker build | PASS | 46,523,144-byte content image; import inspection found Telegram but no Torch or FAISS. |
| CPU-only runtime | PASS | Torch `2.13.0+cpu`, TorchVision `0.28.0+cpu`, `torch.version.cuda is None`, CUDA unavailable; no NVIDIA packages/runtime. |
| Artifact manifest/provisioning | PASS | Ten required objects, exact HTTPS sources, bytes/SHA-256/tree hashes, atomic install; total source bytes 1,384,949,790. |
| Existing-artifact reuse | PASS | Repeated `make provision-production` reported `already_valid` for all ten objects. |
| Missing artifact rejection | PASS | Isolated startup test returned liveness 200/readiness 503 and no fallback. |
| Corrupt artifact rejection | PASS | Isolated provision/startup tests reject modified bytes/tree hash and remain unready; real volume was not modified. |
| Backend startup | PASS | Real SAGE/index runtime-ready log observed 27.43 s after process start. |
| `/health` | PASS | HTTP 200 independently of readiness. |
| `/ready` | PASS | HTTP 200 only after all dependencies loaded; exposes exact non-secret frozen identity. |
| Real `/localize` | PASS | Clean Linux/amd64 CPU container, real indexed smoke, HTTP 200 `ok`, finite coordinates, 30 matches and provider attribution. |
| Restart | PASS | Named volume persisted, readiness returned in 29 s, real smoke remained `ok` with 30 matches. |
| Concurrency 1/2/4 | PASS | 1 and 2 completed; at 4 the bounded capacity accepted/queued three and rejected the excess request with immediate HTTP 503. |
| Rate limit | PASS | Default burst allowed three requests; fourth returned HTTP 429 with `Retry-After: 5`. |
| Upload validation | PASS | Tests cover empty, text-as-JPEG, corrupt JPEG, oversized bytes/dimensions/pixels, MIME/extension mismatch, valid JPEG/PNG/WebP, animation and EXIF behavior. |
| Request timeout/capacity release | PASS | Tests cover queue/processing timeout and ensure background inference retains its lease until completion. |
| Thumbnail safety | PASS | Known-ID manifest mapping only, hashed filenames, traversal/unknown-ID regression tests, no remote fetch/SSRF path, correct MIME/cache headers. |
| Map-first website load | PASS | Playwright loaded desktop and mobile production roots through Caddy with zero console warnings/errors; the Moscow map remains visible beside the desktop panel and behind the mobile bottom sheet. |
| Website `/api` routing | PASS | Browser request was same-origin `POST /api/localize` HTTP 200; compiled assets contain no backend localhost URL. |
| Website result smoke | PASS | Real `ok` rendered the accepted marker, four-decimal coordinates, tested Google/Yandex links and references. Three ordinary real `low_confidence` images each rendered an amber dashed tentative marker, warning, coordinates, links and three initially visible possible matches. |
| Website explicit states | PASS | Unit/browser tests cover idle, selected, processing, accepted `ok`, tentative `low_confidence`, defensive low-confidence-without-prediction, strict `out_of_coverage`, typed API errors, rate-limit `Retry-After`, network failure, oversized/unsupported inputs and stale-request cancellation. |
| Production coverage layer | PASS | Deterministic 28,504-byte aggregate contains 93 occupied 20×20 cells from exactly 20,487 references, is bound to gallery SHA `ff7cbc7e…c35da`, lazy loads, and says coverage is not accuracy. |
| Map configuration/attribution | PASS | Explicit test tile origin passed CSP; tiles loaded HTTP 200 and provider/OSM attribution rendered. Standard public OSM tile endpoint is rejected for production builds. |
| Security headers/cache | PASS | Caddy served CSP/frame restriction, `nosniff`, referrer/permissions policies and correct HTML/hashed asset/localize/thumbnail caching; HSTS is HTTPS-only. |
| CORS | PASS | Same-origin production default has no allowed cross-origin origins or credential wildcard. |
| Structured logs/request IDs | PASS | JSON logs and generated/bounded incoming IDs covered by tests and verifier; header propagation verified. |
| Metrics | PASS | Required Prometheus counters/histograms/gauges present internally; public `/api/metrics` returns 404. |
| Compose validation | PASS | `docker compose ... config --quiet` and Caddy validation passed. |
| Complete topology | PASS | Proxy, healthy real backend, and validation-only bot were simultaneously running; tokenless bot contacted no Telegram endpoint. |
| Telegram `/start`, `/language`, `/help` | PASS | Mocked handlers validate language-first Russian/English onboarding, language switching, localized help and photo-before-language continuation. |
| Telegram photo forwarding | PASS | Highest-resolution selection and multipart body/request-ID forwarding verified with mocked Telegram plus HTTP transport integration. |
| Telegram `ok` | PASS | Both languages send processing/result wording, four-decimal coordinates, native pin, tested Google/Yandex buttons and attribution; neither calls the score a probability. |
| Telegram tentative/no-location | PASS | RU and EN `low_confidence` with a candidate send warning-labelled coordinates plus Google/Yandex buttons but no native location. Low confidence without a prediction and `out_of_coverage` send no point or map buttons. |
| Telegram errors/rate/cooldown | PASS | Invalid/large input, backend 400/413/429/503, timeout, malformed response, unsupported input, cooldown and concurrency tests pass. |
| Telegram configuration smoke | PASS | Final bot image one-shot validation succeeded without a real token or network contact. A live Bot API smoke was not attempted because no token was provided. |
| Python/Ruff tests | PASS | Ruff clean; Pytest 441 passed with 1 intentional opt-in checkpoint-download test skipped. Telegram's isolated suite passed 36/36. |
| Frontend tests/build | PASS | Vitest 26/26, TypeScript/Vite production build, and 18/18 Playwright cases across Chromium, WebKit and mobile Chromium passed locally. Accepted/tentative marker semantics and mobile marker visibility are asserted. The browser suite includes the required 1440×900, 1280×800, 1024×768, 390×844 and 360×800 layout matrix. MapLibre remains isolated in a lazy chunk. |
| Dependency audit | PASS | npm production audit and bot `pip-audit`: no known vulnerabilities. Backend pinned packages: no known findings; audit could not identify direct CPU Torch/TorchVision URL wheels, documented as an audit limitation. |
| Production verifier | PASS | Frozen hash, all artifact hashes, runtime identity, real inference, frontend/cache/headers, private metrics and bot config all returned `status=ok`. |
| CI workflow definition | PASS | Workflow adds pinned Playwright browser installation and the 18-case browser matrix to Ruff/Python/bot tests, frontend unit/build, config/hash/Compose validation, three production image builds, bot-no-ML check and unready Docker smoke. Hosted result is recorded in PR #7 after push. |
| Secret/large-file/git hygiene | PASS | Final tracked-tree audit, `.gitignore`/Docker context checks and `git diff --check` are part of the final commit gate; no runtime artifact/cache/token is committed. |

## Real-photo tentative-result UX smoke

These are interface/wiring checks on existing real Moscow inputs, not a new model evaluation and not threshold-selection evidence.

| Input | Backend outcome | Website evidence |
| --- | --- | --- |
| Frozen easy gallery smoke `ffcd4860…` | strict `ok`, `55.8172, 37.3909` | “Accepted result · Strong evidence,” accepted marker, coordinates, actions, three visible references. |
| Frozen development smoke `fda6920d…` | tentative `low_confidence`, `55.6367, 37.3774` | Warning, amber dashed tentative marker, tentative coordinates, Google/Yandex links, 30 matches returned and three visible. |
| Development query `cb7481f3…` | tentative `low_confidence`, `55.6242, 37.5423` | Same tentative contract; three possible visual matches initially visible. |
| Development query `98df3066…` | tentative `low_confidence`, `55.6851, 37.4226` | Same tentative contract; three possible visual matches initially visible. |

A natural real-photo `out_of_coverage` example is not available under this frozen exact-index runtime: its configured rule is `no_retrieval_candidates`, while a valid query against a non-empty exact index ordinarily returns candidates. The defensive `low_confidence + prediction: null` state and strict `out_of_coverage` state therefore remain verified with API/unit and desktop/mobile browser fixtures rather than a fabricated real-photo claim.

## Measured limitation

Warm p95 was 2.529 seconds under Linux/amd64 emulation on Apple M1 Pro, so the optional sub-two-second target was not met in that non-native environment. Frozen ML behavior was not altered. Validate a native modern x86-64 host before defining the production latency SLO; the recommended starting allocation is 8 CPU cores and 8 GiB RAM.
