# Initial repository audit

Audit date: 2026-08-15
Audited branch: `main` at `54f28c2`

This document records the state observed before the current repair. It is not a
claim about the final state of the repository.

## Evidence collected

- The working tree was initially clean.
- Commit `54f28c2` was the checked-out `main` revision.
- The previously referenced revisions `369b2b1` and `6c9b4af` were not objects
  in the local repository or its fetched references.
- Revision `71f94ac` existed only on unmerged remote Stage 3 branches and was
  not part of `main`.
- The checked-out repository contained the Stage 1 skeleton and Stage 2
  ingestion code, but no implemented cleaning, descriptor extraction, FAISS
  localization pipeline, evaluation system, or usable frontend.
- The API only exposed a database-coupled `/health` endpoint. There was no
  readiness contract or localization endpoint.
- The frontend rendered a single heading and had no upload, API integration,
  map, result state, or attribution UI.

## Commands and observed results

- The existing ingestion tests passed: 25 tests.
- The frontend installed from its lockfile and produced a Vite production
  build.
- The initial Python environment did not contain `pytest`; a project-local
  Python 3.12 environment and locked dependencies were therefore created.
- Docker and Docker Compose CLIs were installed, and the Compose file parsed,
  but the local Docker daemon was unavailable. Container startup could not be
  truthfully verified on this machine.
- The host had about 31 GiB free space, 16 GiB RAM, and an Apple M1 Pro. PyTorch
  MPS execution was verified.

## Data-source findings

- `MAPILLARY_ACCESS_TOKEN` was not configured. Live Mapillary acquisition is
  therefore an external-credential blocker; loaders and the rest of the
  pipeline remain testable using fixtures and local data.
- The legacy KartaView loader used parameters that the current endpoint did not
  accept. Its pagination assumptions also did not match the current response.
- A live KartaView request and a real image download were verified outside
  Moscow. The tested Moscow coverage tile was empty; a broader Moscow coverage
  probe is part of the repaired ingestion validation.
- The initial ingestion manifest omitted or inconsistently represented several
  required provenance fields, including sequence, heading, contributor,
  source URL, license, and normalized JSON metadata.

## ML findings

- The initial `ml/cleaning`, `ml/embeddings`, and `ml/index` directories were
  placeholders.
- Official MegaLoc and DINOv2+SALAD checkpoints were fetched into ignored local
  caches and executed on a real KartaView JPEG using MPS.
- Both produced finite, L2-normalized 8,448-dimensional descriptors. This is an
  execution smoke test, not a Moscow accuracy result and not evidence for
  choosing either model as the default.

## Known external constraints

1. Live Mapillary Moscow ingestion requires a user-provided access token.
2. KartaView may have no useful Moscow coverage; this must be reported from the
   live source rather than hidden with fabricated data.
3. Docker runtime verification requires an available Docker daemon.
4. Any Moscow quality number requires a real, leakage-controlled query/gallery
   set. No metric may be invented when that set cannot be acquired.

The full authoritative project prompt is preserved in `docs/project_sot.md`;
the original attachment remains the byte-authoritative source.
