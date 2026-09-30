# Cleaned SAGE-L local rollout (30 September 2026)

This is a **local research deployment candidate**, not a replacement for the frozen release. The default frozen config, old FAISS index, and production artifact manifest are unchanged. The candidate is enabled by `docker-compose.clean-v5.local.yml` and external read-only binds. Application code and `docker-compose.prod.yml` have changed to support multi-photo requests and this opt-in candidate; do not treat the old whole-repository hash guard as passed.

**Current operating state:** Docker Desktop was stopped at the user's request after the checks below. The local site, bot, backend, and unrelated Docker projects are offline until Docker is started again. Stopping containers does not remove their images, volumes, or the external candidate index.

After the HTTP check, frontend and bot wording was corrected to distinguish the uncalibrated candidate from the frozen release and to label the old coverage map. These source edits passed unit tests and a frontend build but **were not deployed to Docker**, which must remain stopped until requested. Rebuild `proxy` and `telegram-bot` images before the next local rollout; starting the old containers alone will show their previous wording.

## Identity and limits

- Gallery: 111,032 cleaned references: 33,102 Mapillary, 10,830 KartaView, and 67,100 MSLS. Gallery SHA-256: `fa24d16e387ea939536fe2760fe1615327797ad665602421d54e61b473ace39c`.
- Exact `IndexFlatIP` index SHA-256: `20f0e27d8b0aa718799d19b5b62e378fbbf706e451f03f70fb4f7ec54919eb18`; index ID `moscow_clean_v5_sage_l_111032`.
- SAGE ViT-L checkpoint SHA-256: `31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc`; context checkpoint SHA-256: `b3b49e7aa0e7c848c4c57b2d79660c3654a9946fb9f423f6f0e91681afc414b2`.
- Query 322+504, exact top-100 cosine retrieval, context rerank of top 30, top-reference coordinates. Six-photo consensus first tests agreement among independently selected top results, then falls back to geographic candidate consensus.
- Every output is tentative (`low_confidence`, numeric confidence `0.0`). No confidence model or independent accuracy estimate exists for this cleaned gallery. The previous 411/1,184 (34.71%) research result used a different gallery/query protocol and must not be advertised as this deployment's measured accuracy.
- MSLS is research-only with noncommercial/share-alike source restrictions. Public/commercial distribution or publication of these images and descriptors needs a separate rights review. Do not package the external gallery/index in a GitHub release by default.

## Local activation and rollback

Set `GEOSNAP_CLEAN_INDEX_HOST_DIR` to the verified index directory; set `GEOSNAP_SAGE_L_CHECKPOINT_HOST_FILE` and `GEOSNAP_SAGE_CONTEXT_HOST_FILE` to the verified checkpoint files. The external disk must remain mounted. Then:

```bash
docker compose -f docker-compose.prod.yml -f docker-compose.clean-v5.local.yml up -d --no-deps backend proxy telegram-bot
curl -fsS -H 'Host: localhost' http://127.0.0.1:8080/api/ready
```

Readiness must report `sage-vitl`, `gallery_count: 111032`, and `index_id: moscow_clean_v5_sage_l_111032`. The local Mac has 16 GiB RAM; Docker Desktop was raised from 8 to 12 GiB for this candidate. The backend used about 6.7 GiB after loading on Linux/amd64 under Rosetta. Cold start takes several minutes. The site single-image request timeout is 120 seconds; the candidate Compose overlay sets a 650-second proxy timeout and 620-second Telegram batch timeout, while backend batch work is limited to 600 seconds.

For the next rollout, run `docker compose -f docker-compose.prod.yml -f docker-compose.clean-v5.local.yml build proxy telegram-bot` with the same three candidate bind-path variables set before the `up` command above. This incorporates the wording fixes without changing the frozen release artifacts.

To restore the frozen release without removing candidate artifacts:

```bash
docker compose -f docker-compose.prod.yml up -d --no-deps backend proxy telegram-bot
```

## Verification on the six supplied views

The serving adapter matched all six frozen individual research top references. Through the local Caddy HTTP route, six uploads completed with HTTP 200 in **195.0 seconds**. Three independent top-1 predictions supported the selected existing reference at `55.762471812643, 37.634552130675`. Joining the user-provided panorama coordinate **after inference** gives **21.80 m** error. This is one user-supplied example, not a validation score. The HTTP response and separate GT analysis are saved under `data/evaluation/user_six_view_case_20260930/cleaned_sage_l_replay_v1/` with SHA-256 receipt `ce285cf7ee3285192aad94ea42348a6da3828e7bdbaa5c5ca2abcd6d7270004d` for `http_six_v1.json`.

The first HTTP attempt returned 504 after the old proxy's 185-second deadline, although backend inference later completed in 212 seconds. The longer candidate deadlines above were applied and the repeated request returned 200. A warm single-image request completed in 34.0 seconds. CPU latency remains a material product limitation, especially for ten photos. No claim is made that ten-photo batches always complete within the configured ceiling.

The website's optional static coverage overlay describes the **old frozen 20,487-reference gallery**, not this candidate. Its visible legend now says so. The cleaned candidate has no matching thumbnail bundle. Both differences remain product limitations when this overlay is used with the candidate.

The prior 85-file production guard reports 69 unchanged and 16 changed source files. The frozen config, artifact manifest, and every guarded file in the old index remain byte-identical. This scoped verification is the appropriate integrity statement for a cycle that intentionally edits serving code.
