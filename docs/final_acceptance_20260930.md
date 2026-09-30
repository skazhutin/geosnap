# Final acceptance and product freeze — 30 September 2026

GeoSnap's final source handoff includes the website, Telegram client with up to ten photos per place, API, cleaned-gallery runtime, reproducible research code, and the preserved research history. The published `main` commit containing this report defines the final source version. No new localization experiment or accuracy estimate was produced during acceptance.

## Final corrections

Cancelling a single-photo HTTP request could release its inference slot while shielded background work was still running. The route now retains capacity until that work finishes, matching the multi-photo route. Two regression cases verify cancellation and subsequent capacity recovery for both endpoints. This changes request resource management; model selection and geographic scoring are unchanged.

Replacing a photo could briefly render the previous file's revoked object URL. The preview now renders only when its file identity matches the selected file. A regression test fails before this fix and passes after it, and checks that every created URL is released. The browser fixture was also a malformed PNG; it has been replaced with a decodable PNG, and every browser upload now verifies that the preview actually loads. The initial acceptance CI failure is preserved in [run 36699250410](https://github.com/skazhutin/geosnap/actions/runs/36699250410), rather than treated as a successful freeze.

## Verification

| Check | Evidence |
| --- | --- |
| Python product/core suite after the correction | 466 passed, 1 skipped; the skip explicitly opts out of downloading the MegaLoc checkpoint |
| Python lint, locked dependencies, diff whitespace | Passed |
| API and multi-photo targeted suite | 46 passed, including both cancellation cases |
| Frontend unit tests after the preview correction | 27 passed, including replacement/removal and revoked-URL regression coverage |
| Browser scenarios after the preview correction | 24 passed without retries across desktop Chromium, WebKit and mobile Chromium, including decoded previews, reduced motion and viewport changes |
| Local frontend production build | Passed; MapLibre's existing lazy-loaded map chunk still produces the bundle-size advisory |
| Frontend and three deployment container builds | Passed in [CI for `38f493d`](https://github.com/skazhutin/geosnap/actions/runs/36690099427) |
| Fixture image → embedding → FAISS → API and unprovisioned Docker smoke | Passed in the same CI; the latter returns healthy liveness and unready model status |
| Production and development Compose configurations | Passed |
| Frozen production artifact verification | Gallery/index bytes match the frozen contract: 20,487 references, 8,448 descriptor dimensions, 14 confidence features |
| Cleaned candidate | 111,032 unique IDs and matching reference rows; ordered mapping, artifact generation and sidecar hashes validated |
| Release distribution availability | All ten pinned artifact URLs returned HTTP 200 to HEAD; this checks availability, not fresh hashes of remote downloads |
| Repository Markdown links | No missing repository file targets; historical local research evidence remains external to Git |
| Telegram read-only checks | `getMe` succeeded for `geosnap_vbot`; no webhook is configured |
| Secret handling | Recent publication pattern scan found no real tokens/private keys; the one historical `.env` snapshot also has zero known token/private-key pattern matches. This is not an exhaustive historical secret audit |

Acceptance requires a successful CI check on the actual published commit containing the final correction and this report. Its result is recorded in that commit's [GitHub Actions checks](https://github.com/skazhutin/geosnap/actions/workflows/ci.yml), alongside container builds and browser/Python tests.

## Frozen identities

| Artifact | SHA-256 |
| --- | --- |
| Frozen production configuration | `9c0c38f93d4c4f76eff8ef821508da0aafdd104f04ddd932b48d2834bbd2984e` |
| Frozen production distribution manifest | `0d9bd64e53acea1cd2e97ae8d68718f6b5777405f0a54eb04ee3a28e541f9008` |
| Cleaned SAGE-L runtime configuration | `eb2ed5a8cbabeca6417b1f4656a7d79a23e439fd0b12868ca3c38defe026a5a8` |
| Cleaned gallery manifest | `fa24d16e387ea939536fe2760fe1615327797ad665602421d54e61b473ace39c` |
| Cleaned exact index | `20f0e27d8b0aa718799d19b5b62e378fbbf706e451f03f70fb4f7ec54919eb18` |
| Cleaned ID mapping | `920d5359c5027a948b8a7d13094a69857dd3dd7a983d227b9e6b7366d92be2fc` |
| Cleaned reference metadata | `db9a240b8133037ec6f974560adef37466130e420f0309594a527704fe6b064d` |
| SAGE-L checkpoint | `31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc` |
| SAGE context checkpoint | `b3b49e7aa0e7c848c4c57b2d79660c3654a9946fb9f423f6f0e91681afc414b2` |

The cleaned candidate contains exactly 33,102 Mapillary, 10,830 KartaView and 67,100 MSLS references. Fresh hash checks after reconnecting the external disk matched the existing build receipt, index metadata and rollout record. No reference was deleted or regenerated during acceptance.

Machine-readable URL, Telegram and artifact receipts are under `data/evaluation/product_final_acceptance_20260930/`. The successful disk check is `candidate_integrity_disk_connected.json`; `candidate_sidecars_receipt.json` verifies mapping/metadata correspondence. Earlier availability checks are retained as diagnostics: Docker Desktop paths first needed host-path normalization, then the external disk was reconnected. Local receipts and research images are intentionally ignored by Git.

## Product status and established limits

GeoSnap services are stopped on the original Mac. Other Docker projects are running. Acceptance did not restart GeoSnap, deploy a public server, or send Telegram messages. The read-only bot check verifies credentials and polling compatibility; live album delivery was not repeated. The preserved [six-photo HTTP check](cleaned_sage_l_local_rollout_20260930.md) selected a reference 21.80 m from the supplied panorama, taking 195 seconds. It remains a single wiring/example check, not a generalization result.

The source freeze retains two documented runtimes. A clean-machine install provisions the frozen SAGE-B release with 20,487 approved-source references. The [optional local overlay](cleaned_sage_l_local_rollout_20260930.md) enables SAGE-L with the complete cleaned 111,032-reference gallery and requires its external index/checkpoints. Those candidate bind paths must be supplied when activating it; merely starting the default stack selects the frozen release. Existing local frontend/bot images must be rebuilt to include the final source wording.

The cleaned candidate always returns tentative coordinates. Its confidence is uncalibrated, it has no matching thumbnail bundle, and the website's optional coverage overlay still describes the old frozen gallery. CPU inference is slow, especially for large photo sets. Ten-photo support is an input/API limit, not a guarantee of completion on every machine within the deadline. Multi-photo consensus has no independent smartphone accuracy measurement.

The research accuracy record remains: original-gallery SAGE-L **411/1,184 = 34.71%** RAW within 100 m; cleaned gallery **409/1,184 = 34.54%**. The **400/1,071 = 37.35%** result uses filtered queries and a different denominator. These reused development measurements are exploratory; see the [research synthesis](geosnap_project_final_report_20260929.md) for coverage ceilings and comparisons. Acceptance did not promote them to an independent final claim.

Photos, weights and full indices remain external artifacts. MSLS remains research-only in the source audit; source publication does not grant unrestricted rights to its imagery or descriptors. Public deployment still needs the documented host/domain, provider attribution and tile configuration. The final source is suitable for reproducible research and the documented experimental deployment with these limits.

## Final handoff

The product's functional scope is closed after the final commit passes CI. Preserve this commit and the artifact identities above when deploying or archiving the project. Use [deployment](deployment.md) and the [candidate activation/rollback procedure](cleaned_sage_l_local_rollout_20260930.md) for operations. Future model, gallery, threshold or confidence changes would constitute a new explicitly requested research/release cycle.
