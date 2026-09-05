You are RESUMING an interrupted implementation session for the GeoSnap visual geolocation project.

This is NOT a new implementation task and NOT permission to start over.

A large amount of work already exists in the CURRENT LOCAL WORKING TREE and may not have been committed or pushed.

Your responsibility is to:

1. preserve that work;
2. recover the exact state of the interrupted session;
3. finish every remaining requirement from the canonical SoT;
4. validate the system on REAL Moscow data;
5. make the product genuinely usable end-to-end;
6. commit and push the completed implementation so it is no longer local-only.

Do not stop at “tests pass”.
Do not stop at “the architecture is implemented”.
Do not stop at a one-image smoke test.
Do not stop at a mocked frontend result.
Do not stop at a tiny proxy benchmark.

The final deliverable must be a real, reproducible visual geolocation product.

---

# 0. CANONICAL SOURCE OF TRUTH

The complete project specification was stored during the previous session as:

`docs/project_sot.md`

and a SHA file was created:

`docs/project_sot.sha256`

The original SoT contains the full 69-section master prompt and remains authoritative.

FIRST:

- locate `docs/project_sot.md`;
- verify it against the original source if that source is still available;
- resolve the unfinished byte-level mismatch from the interrupted session;
- do NOT silently shorten, paraphrase or regenerate it;
- preserve the exact canonical SoT.

If the original attachment is still accessible, make the copy byte-identical.

If the original attachment is no longer accessible, do NOT pretend exact byte identity was verified; preserve the most complete available copy and report the limitation.

The SoT overrides older README statements and old PR descriptions.

This prompt is a completion directive, not a replacement for the SoT.

---

# 1. CRITICAL: PRESERVE THE CURRENT LOCAL WORKING TREE

The previous session reportedly modified approximately:

- 68 files;
- ~5,900 added lines;
- substantial backend, ML, frontend, evaluation and infrastructure code.

This work is NOT visible on GitHub `main`.

DO NOT:

- checkout `main`;
- reset --hard;
- clean the repository;
- discard uncommitted changes;
- re-clone over the current working directory;
- blindly merge old PRs over the current working tree.

Before touching implementation:

run and inspect:

`git status --short`
`git branch --show-current`
`git log --oneline --decorate -20`
`git diff --stat`
`git diff`

Understand what is currently local and what is committed.

The LOCAL WORKING TREE is likely much more advanced than remote GitHub and must be treated as valuable work.

---

# 2. SECRET SAFETY BEFORE ANY COMMIT

The remote repository currently tracks `.env`.

This must be fixed BEFORE making any safety commit.

DO NOT print environment secrets.

DO NOT run `git add -A` before checking `.env`.

Perform:

1. inspect whether `.env` contains any real token/secret;
2. do not display secret values in logs/output;
3. ensure `.env` is ignored;
4. remove `.env` from Git tracking while retaining the local file;
5. maintain `.env.example` containing placeholders only.

Conceptually:

`git rm --cached .env`

and add:

`.env`

to `.gitignore`.

Scan staged/uncommitted files for accidental:

- MAPILLARY_ACCESS_TOKEN;
- API tokens;
- Hugging Face tokens;
- credentials;
- passwords beyond harmless documented development defaults.

If a real secret was already committed/pushed, report it clearly and recommend rotation.

Never commit the Mapillary token.

---

# 3. CREATE A RECOVERY CHECKPOINT

After secrets are protected and BEFORE further large modifications:

create a safe checkpoint of the current implementation.

If the current branch is appropriate, commit there.

Otherwise create a dedicated continuation branch, e.g.:

`codex/finalize-geosnap`

Do not lose any useful local work.

Push the checkpoint branch to GitHub early so the interrupted work is no longer only local.

Do NOT merge it yet.

The point is disaster recovery.

---

# 4. REMOTE GITHUB REALITY

Remote repository:

`skazhutin/geosnap`

Remote `main` currently contains only the older implementation through Stage 2 ingestion.

There are old open Stage 3 PRs:

- PR #5
- PR #6

They contain older cleaning/H3 implementations but do NOT contain the complete local backend/ML work from the interrupted session.

Do NOT blindly merge PR #5 or PR #6.

Instead:

- compare their changes against the current local tree;
- preserve any useful fixes not already present;
- otherwise supersede them with the final branch.

At the end there should be ONE clear final PR representing the actual finished product.

Old obsolete PRs may be closed/superseded after comparison.

---

# 5. WHAT THE PREVIOUS SESSION ALREADY REPORTED

The interrupted session reportedly accomplished a significant amount of work.

Do not trust these claims blindly, but VERIFY them before rewriting anything.

Reported achievements include:

### Backend

- FastAPI backend substantially expanded;
- `/health`;
- `/ready`;
- `/localize`;
- typed API responses;
- CORS;
- error handling;
- readiness logic;
- startup loading;
- upload validation.

### ML

- model abstraction;
- MegaLoc adapter;
- DINOv2 + SALAD adapter;
- official model weights downloaded;
- real inference executed;
- Apple MPS tested;
- descriptor dimension reportedly 8448;
- FAISS implementation;
- localization logic;
- confidence;
- optional geometric verification.

### Data

- ingestion improvements;
- resumable state;
- source handling;
- bounded downloads;
- URL validation;
- SSRF protection;
- cleaning;
- H3;
- reports.

### Evaluation

- Wikimedia Commons Moscow proxy created;
- MegaLoc and SALAD benchmarked;
- robustness transforms;
- geometric verification ablation.

### Frontend

- upload UI;
- MapLibre map;
- confidence;
- radius;
- hypotheses;
- top matches;
- attribution;
- mobile/desktop browser QA.

### Tests

Previous run reportedly reached approximately:

- 176 Python tests passing;
- one opt-in real-model smoke skipped;
- 7 frontend tests passing;
- production frontend build passing.

Again:

VERIFY all of this in the current working tree.

Do not rewrite working modules unnecessarily.

---

# 6. IMPORTANT: WHAT IS NOT FINISHED

The following must NOT be considered finished merely because the previous session had a smoke test.

## A. Real Moscow reference gallery

A reported live test downloaded only ONE usable KartaView Moscow image.

That is not a product gallery.

A real useful reference gallery still needs to be acquired.

---

## B. Mapillary production ingestion

The project has a Mapillary loader and the user has previously created a Mapillary access token.

Use:

`MAPILLARY_ACCESS_TOKEN`

from the local environment.

Do not expose the token.

Actually execute Mapillary ingestion.

If the variable is missing, report the exact missing variable only after checking the local environment.

Do not ask the user for it if it already exists locally.

---

## C. Final Moscow evaluation

The previous evaluation reportedly used roughly:

- 20 Wikimedia Commons images;
- 4 landmarks;
- 12 gallery images;
- 8 independent query images.

This was useful as a pipeline proxy.

It is NOT sufficient as final evidence of product quality.

Do not report its 87.5% Recall@1 as “Moscow product accuracy”.

---

## D. Real successful frontend localization

The previous browser QA reportedly tested:

- a REAL low_confidence result using a one-image gallery;
- an `ok` frontend state using a mocked `/localize` response.

That proves UI behavior.

It does NOT prove successful real localization through the final backend.

Final QA must include at least one real successful:

real query
→ real model
→ real FAISS gallery
→ real localization
→ real `/localize`
→ real frontend map result

with NO mocked localization response.

---

## E. Full product data scale

A tiny gallery is only a smoke test.

The final product must contain enough real Moscow references to demonstrate meaningful coverage.

---

## F. Remote delivery

The advanced implementation appears not to have been pushed to GitHub.

The final work must not remain local-only.

---

# 7. FIRST EXECUTION AFTER RECOVERY

After preserving the local tree:

run the full current validation suite.

At minimum:

`make test`

or the canonical equivalent.

Verify:

- Ruff;
- Python tests;
- frontend tests;
- frontend production build.

Then run:

`make compose-config`

and inspect the resolved Compose configuration.

Run the current smoke path.

Record failures.

Only fix actual failures/gaps.

Do not refactor working areas without reason.

---

# 8. REAL DATA IS NOW THE HIGHEST PRIORITY

The next priority is NOT more architecture.

The next priority is REAL MOSCOW DATA.

The system already appears architecturally advanced enough.

Now prove it works.

---

# 9. MAPILLARY LIVE INGESTION

Use the current official Mapillary API.

Do not rely on obsolete assumptions.

Use the existing local loader if correct.

First run a SMALL live Moscow sample.

Verify:

- authentication works;
- pagination works;
- returned metadata is real;
- image URLs/download mechanism works;
- images actually decode;
- coordinates are Moscow coordinates;
- sequence IDs are retained;
- heading is retained when available;
- timestamps are retained;
- attribution is retained.

If current code is wrong, fix it.

After small success, progressively expand.

---

# 10. KARTAVIEW LIVE INGESTION

Repeat the same process for KartaView.

The earlier one-image result suggests either:

- poor coverage;
- overly restrictive query;
- API behavior mismatch;
- sampling limitations.

Determine which one is true.

Do not assume KartaView is useful just because it is in the architecture.

Measure actual Moscow coverage.

If KartaView contributes little useful data:

keep support implemented,
but do not allow it to dominate engineering time.

Mapillary may become the primary gallery source if real measurements support that.

---

# 11. BUILD A MEANINGFUL MOSCOW GALLERY

Do NOT immediately attempt an uncontrolled entire-Moscow download.

Use a staged strategy.

### Stage 1

Choose several geographically separated Moscow areas with good source coverage.

Prefer diversity such as:

- central streets;
- residential districts;
- newer residential areas;
- courtyards where available;
- major roads;
- mixed commercial/residential environments.

### Stage 2

Acquire enough reference images to produce meaningful dense retrieval.

Measure:

- number of references;
- unique locations/cells;
- spatial density;
- sequence diversity;
- heading diversity;
- source distribution.

### Stage 3

Expand toward broader Moscow coverage as disk/time allows.

Do not invent a fixed “minimum 200k” requirement.

Use measured coverage and retrieval quality.

If available resources make full Moscow impossible, explicitly define the actual supported coverage and do not falsely label it full-city coverage.

---

# 12. RESOURCE GUARDRAILS

Before large acquisition inspect:

- free disk;
- RAM;
- available GPU/MPS;
- estimated image storage;
- descriptor storage.

Do not fill the disk.

Use:

- resumable jobs;
- checkpoints;
- bounded concurrency;
- chunked processing;
- cache reuse.

Do not redownload valid files.

Do not recompute valid embeddings unnecessarily.

---

# 13. DATA CLEANING MUST BE CONSERVATIVE

Reuse the corrected Stage 3 logic where appropriate.

Ensure known issues remain fixed:

- no single-link geographic chain collapse;
- higher-quality visual duplicate survives;
- heading-diverse images are preserved;
- nearby but visually different viewpoints are not aggressively removed;
- no O(N²) global geo comparison;
- schemas survive empty results;
- metadata_json representation stays consistent;
- quality filtering remains conservative.

Do NOT optimize gallery size at the expense of retrieval coverage.

---

# 14. BUILD THE FINAL REAL REFERENCE MANIFEST

Generate a canonical processed manifest for the actual product gallery.

It must provide stable IDs connecting:

reference metadata
↔ image files
↔ embedding rows
↔ FAISS IDs
↔ API responses.

Run schema validation.

Generate the data report.

---

# 15. REAL MODEL BENCHMARK ON MOSCOW DATA

The previous Commons proxy is not sufficient to choose the final model.

Benchmark on the actual reference/query split.

At least compare:

- MegaLoc;
- DINOv2 + SALAD;

if both remain operational.

Use identical query/gallery data.

Measure:

- Recall@1;
- Recall@5;
- Recall@10;
- localization ≤25m;
- ≤50m;
- ≤100m;
- median error;
- p90 error;
- query embedding latency;
- FAISS latency;
- descriptor storage.

Do not declare a winner solely from the previous 8-query Commons proxy.

---

# 16. FINAL RETRIEVER SELECTION

The previous session tentatively chose MegaLoc because the tiny proxy suggested lower tail risk.

Treat that as provisional.

Re-evaluate using the real Moscow set.

Select the final default based on measured product behavior.

If MegaLoc remains superior:

keep it as default.

If SALAD clearly wins:

switch default.

If performance is effectively equal:

prefer the simpler/faster/more reliable operational option.

Do not create an ensemble unless it produces a measured meaningful improvement.

---

# 17. BUILD A REAL HELD-OUT MOSCOW QUERY SET

This is mandatory.

Do NOT use the same exact reference images as queries.

Where possible use:

- different sequences;
- different capture timestamps;
- distinct frames sufficiently separated from gallery duplicates;
- real geotagged Moscow images.

Avoid trivial adjacent-frame leakage.

Target a meaningful evaluation set.

Aim for at least ~100 independent real queries if source coverage allows.

Prefer more.

Spread them across multiple geographically distinct areas.

If the available licensed data cannot provide that many independent queries:

use the maximum defensible number,
state the limitation,
and DO NOT hide the sample size.

---

# 18. QUERY/GALLERY LEAKAGE AUDIT

Automatically check for leakage.

Reject:

- exact image identity;
- identical source image ID;
- exact hash duplicates;
- near-identical sequence neighbors where they make evaluation trivial.

Report leakage counts.

Final metrics must use leakage-clean queries.

---

# 19. ROBUSTNESS TESTING

Keep the real-image augmentation tests.

Generate variants of held-out REAL queries:

- crop;
- resize;
- JPEG compression;
- moderate blur;
- brightness change;
- color shift;
- slight perspective change;
- partial occlusion.

Report robustness separately from original-image metrics.

Do not mix augmented queries into the primary real-query metric without labeling them.

---

# 20. FAISS FINALIZATION

Build the production index from the selected final retriever.

Use exact search first if feasible.

Verify:

- descriptor normalization;
- index metadata;
- stable ID mapping;
- persisted sidecars;
- integrity hashes;
- save/reload equivalence;
- retriever/index compatibility.

The previous session reportedly added a schema-v2/integrity mechanism.

Verify it.

Do not rebuild indexes with mismatched descriptors silently.

---

# 21. LOCALIZATION LOGIC

Use real evaluation data to finalize:

- top-K;
- spatial grouping;
- coordinate estimator;
- confidence threshold.

Compare:

- top-1;
- weighted centroid;
- weighted medoid;
- other already-implemented estimators.

Never average unrelated geographic clusters.

Choose the method that gives the best real held-out performance.

---

# 22. CONFIDENCE / FAIL-SAFE

Calibrate confidence on the real Moscow evaluation set.

The system must be able to abstain.

Measure:

- answer rate;
- accuracy among answered queries;
- error by confidence bucket;
- false confident errors.

A wrong precise location with high confidence is worse than low_confidence.

Do not tune confidence on the same tiny Commons proxy and call it final.

---

# 23. GEOMETRIC VERIFICATION

The previous session reportedly evaluated:

- OpenCV SIFT/RANSAC;
- LightGlue;

and found no benefit on the small proxy, with substantial latency.

Therefore:

DO NOT spend excessive time on verification.

Retest verification on a reasonable subset of the real Moscow held-out data.

If it still does not provide meaningful metric improvement:

leave verification implemented but OFF by default.

Document the ablation.

If it clearly improves difficult cases enough to justify latency:

enable/configure appropriately.

Measurement decides.

---

# 24. OCR

OCR remains auxiliary and should not block completion.

Do not add complex OCR work unless the core product already satisfies Definition of Done.

If existing OCR integration works, keep it optional.

---

# 25. BACKEND FINAL ACCEPTANCE

Verify the actual backend.

Required:

`GET /health`
`GET /ready`
`POST /localize`

`/ready` must accurately report whether:

- model is loaded;
- index is loaded;
- gallery metadata is available.

`/localize` must execute the REAL selected model and REAL FAISS index.

No mocked localization inside production code.

Test:

- valid image;
- corrupt file;
- unsupported image;
- oversized upload;
- low confidence;
- successful localization;
- index/model incompatibility;
- unavailable resources.

Models/indexes should load once, not once per request.

---

# 26. FRONTEND FINAL ACCEPTANCE

The frontend already reportedly has:

- upload;
- map;
- confidence;
- uncertainty;
- hypotheses;
- reference matches;
- attribution.

Do NOT redesign it unnecessarily.

Finalize only what is required for real use.

Most importantly:

test the frontend against the REAL backend and REAL gallery.

At least one final successful browser QA must be:

REAL image
→ REAL HTTP request
→ REAL model
→ REAL FAISS result
→ REAL predicted coordinates
→ REAL map marker
→ REAL reference matches

NO mocked `/localize` route.

Also test:

- low-confidence response;
- invalid file;
- desktop;
- mobile.

No browser console errors.

---

# 27. ATTRIBUTION

Verify final reference matches show whatever attribution the source license requires.

Do not fabricate contributor identity.

Keep:

- source;
- source URL;
- attribution;
- license information

consistent through ingestion → manifest → API → frontend.

---

# 28. DATABASE REALITY

Do not pretend PostGIS is part of runtime if it is not actually used.

Inspect the implemented system.

If reference metadata retrieval currently works cleanly using Parquet/in-memory mappings:

it is acceptable for the school prototype.

Do not force unnecessary DB complexity just to match an old architecture diagram.

If PostGIS is already genuinely integrated and useful, verify it.

Architecture documentation must describe what ACTUALLY runs.

---

# 29. CLEAN-START REPRODUCIBILITY

Test from a reasonably clean environment.

Ensure a new developer can follow README.

Canonical flow should cover equivalents of:

setup
→ sample ingestion
→ data preparation
→ embedding
→ index build
→ evaluation
→ API
→ frontend
→ smoke test

Every documented command must actually run.

Do not document fictional commands.

---

# 30. MAKEFILE / COMMAND SURFACE

Verify commands such as:

`make setup`
`make test`
`make ingest-sample`
`make ingest-moscow`
`make prepare-data`
`make embed`
`make build-index`
`make eval`
`make api`
`make frontend`
`make smoke`

or their current equivalents.

Fix inconsistencies.

Commands must not silently succeed after a critical upstream failure.

For example:

if both imagery sources fail,
the pipeline must not produce an apparently valid empty product.

---

# 31. FULL TEST SUITE

After all final modifications run the COMPLETE suite.

At minimum:

Python lint/static checks.

Python tests.

Frontend tests.

Frontend production build.

Compose validation.

Integration smoke.

Real model smoke.

Real API smoke.

Do not omit a failing expensive test just to obtain green output.

Opt-in external/model tests may remain separately marked, but execute them at least once before final completion where resources permit.

---

# 32. FINAL REAL PRODUCT SMOKE

Create one automated command/script that proves:

real reference gallery exists
→ selected model loads
→ embeddings/index correspond
→ API starts
→ real query is localized
→ response contains real matches and coordinates.

This is different from unit tests.

It is the final product smoke test.

---

# 33. FINAL PRODUCT METRICS

Generate one canonical final report.

At minimum include:

### Dataset

Mapillary references discovered.

KartaView references discovered.

Downloaded valid images.

Final gallery size.

Supported geographic coverage.

### Retrieval

Recall@1.

Recall@5.

Recall@10.

### Localization

≤25 m.

≤50 m.

≤100 m.

Median error.

p90 error.

### Reliability

Answer rate.

Low-confidence rate.

Accuracy/error by confidence bucket.

### Performance

Model embedding latency.

FAISS latency.

Verification latency if enabled.

Total backend request latency.

### Sample counts

Number of:

gallery images;

query images;

geographic areas;

sources.

Never report a metric without its sample size.

---

# 34. DO NOT USE THE COMMONS PROXY AS FINAL PRODUCT CLAIM

Retain the previous Commons benchmark as:

pipeline/proxy benchmark

if useful.

Clearly label it.

Do not replace or overwrite its historical result deceptively.

But the final README/report must distinguish:

`proxy benchmark`

from

`actual Moscow product evaluation`.

---

# 35. COVERAGE HONESTY

The system can only localize places represented sufficiently in the gallery.

Generate a final coverage visualization.

If coverage is partial:

the UI/docs must not imply that every Moscow courtyard is supported.

State the actual coverage.

The product remains a valid Moscow visual-geolocation prototype even if open imagery coverage is incomplete.

But do not hide holes.

---

# 36. FINAL DOCUMENTATION

Update:

README.md
docs/architecture.md
docs/data_report.md
docs/evaluation_report.md
docs/licenses.md

and any relevant runbook.

Make them describe the ACTUAL final implementation.

Remove outdated claims inherited from the April README.

Document:

- current default retriever;
- actual gallery;
- data sources;
- real metrics;
- limitations;
- commands;
- hardware used for measured latency;
- coverage limitations;
- verification default state;
- confidence behavior.

---

# 37. SoT INTEGRITY MUST BE CLOSED

The previous session ended while discovering that:

`docs/project_sot.md`

was not byte-identical to the original attachment despite the stored SHA.

Do not leave this unresolved.

Find the exact difference.

Restore the copy.

Verify using a real command such as:

`cmp`
and
`shasum -a 256`

The final report must state the actual result.

Do not claim equality unless verified.

---

# 38. CLEAN GIT STATE / REMOTE DELIVERY

When implementation is finished:

inspect:

`git status`
`git diff --check`

Ensure:

- no secrets;
- no enormous downloaded imagery;
- no model cache;
- no embeddings/index binaries accidentally staged;
- no Playwright temporary artifacts;
- no `.env`.

Commit the final source code and documentation.

Push the branch.

Create or update ONE final PR.

The user must not depend on an unpushed local working tree.

---

# 39. OLD PR CLEANUP

Compare PR #5 and PR #6 against the final implementation.

If fully superseded:

mark them superseded/close them appropriately.

Do not merge obsolete Stage 3 implementations after the new final branch.

The final PR should contain the coherent product.

---

# 40. CI

Ensure the final PR runs CI.

CI should test the code that can run without private data/tokens.

Do not require Mapillary credentials for ordinary PR CI.

CI should include:

- Python lint;
- Python unit/integration tests using fixtures;
- frontend tests/build;
- backend startup or appropriate smoke.

Real-data/model evaluation may be a documented local/manual job if too large for CI.

---

# 41. DEFINITION OF DONE — HARD GATE

DO NOT declare the task complete until ALL applicable conditions below are true.

## Source safety

- current local work preserved;
- `.env` no longer tracked;
- no access tokens committed;
- final branch pushed.

## SoT

- canonical prompt preserved;
- byte-integrity issue resolved or honestly documented if original source is unavailable.

## Data

- actual real Moscow imagery downloaded;
- gallery contains substantially more than the one-image smoke test;
- canonical manifest built;
- coverage measured;
- ingestion resumable.

## Retrieval

- selected real VPR model loads;
- gallery embeddings generated;
- FAISS index built;
- save/reload verified;
- real held-out queries retrieve real references.

## Evaluation

- real Moscow held-out query set;
- no exact query/gallery leakage;
- Recall@1/5/10;
- ≤25/50/100m;
- median;
- p90;
- sample sizes recorded.

## Localization

- unrelated geographic modes not averaged;
- coordinate estimator selected empirically;
- confidence calibrated;
- low-confidence abstention works.

## Backend

- `/health`;
- `/ready`;
- `/localize`;
- real successful localization;
- real low-confidence localization;
- proper errors.

## Frontend

- real backend integration;
- real successful map result;
- top real references;
- attribution;
- mobile;
- desktop;
- no browser console errors.

## Testing

- full Python suite passes;
- frontend suite passes;
- frontend production build passes;
- Compose config passes;
- real product smoke passes.

## Documentation

- README accurate;
- architecture accurate;
- real metrics documented;
- real coverage documented;
- licenses documented;
- limitations documented.

## GitHub

- completed branch pushed;
- final PR exists;
- work is not local-only;
- CI passes or any external CI blocker is precisely reported.

---

# 42. PRIORITY RULE

Do not spend the remaining time making the architecture more sophisticated unless it improves a measured product requirement.

Priority from now on is:

1. preserve current work;
2. secure secrets;
3. push recovery checkpoint;
4. real Moscow gallery;
5. real embeddings/index;
6. real evaluation;
7. real localization;
8. real API success;
9. real frontend success;
10. tests;
11. documentation;
12. final push/PR.

Not:

more abstractions
more speculative modules
more proxy experiments
more mocked demos.

---

# 43. IF FULL MOSCOW DOWNLOAD IS IMPRACTICAL

If disk/time/open-source coverage makes entire Moscow impossible during this execution:

DO NOT stop.

Instead:

1. build the strongest reproducible multi-area Moscow gallery that resources allow;
2. select several geographically separated supported areas;
3. create real query/gallery splits there;
4. make the product genuinely work over that coverage;
5. document exact coverage;
6. keep ingestion scalable/resumable so coverage can later be expanded.

A real, honest, functioning partial-coverage product is preferable to an unfinished claim of city-wide coverage.

---

# 44. IF MAPILLARY TOKEN IS UNAVAILABLE

Only after checking the environment:

If `MAPILLARY_ACCESS_TOKEN` is genuinely absent:

- do not fake Mapillary results;
- continue with KartaView / existing licensed real data;
- complete all code/tests;
- report one precise external blocker;
- provide exact command to resume Mapillary ingestion.

But if the token already exists locally:

use it automatically without exposing it.

---

# 45. FINAL RESPONSE

When everything possible is complete, give ONE final report.

Include:

## Repository recovery

- original branch;
- initial git status;
- whether uncommitted prior work was preserved;
- recovery commit/branch.

## Remote state

- branch pushed;
- PR URL;
- CI result;
- status of old PR #5/#6.

## SoT

- exact integrity result.

## Data

- Mapillary count;
- KartaView count;
- valid downloaded count;
- final gallery count;
- supported coverage.

## Models

- MegaLoc actual metrics;
- SALAD actual metrics;
- chosen default and reason.

## Verification

- measured effect;
- whether enabled.

## Product metrics

- Recall@1/5/10;
- ≤25/50/100m;
- median error;
- p90;
- answer rate;
- latency;
- query count.

## Backend

- exact API endpoints verified.

## Frontend

- real successful browser flow verified;
- real low-confidence flow verified.

## Tests

Exact commands and exact pass/fail counts.

## Commands

Exact commands to:

- set up;
- ingest;
- prepare;
- embed;
- build index;
- evaluate;
- run API;
- run frontend;
- run smoke.

## Remaining limitations

Only REAL limitations.

Do not list work that you simply chose not to finish.

---

# FINAL INSTRUCTION

Do not respond after merely inspecting the repository.

Do not respond after merely fixing the SoT mismatch.

Do not respond after merely getting tests green.

Do not respond after merely downloading one real image.

Do not respond after merely running the Commons proxy.

Continue executing until the system is a genuinely working, tested, measured visual geolocation product over the strongest Moscow coverage achievable in the environment, and the completed source code is safely pushed to GitHub.

Use the full canonical `docs/project_sot.md` throughout the work.

Actual execution is the source of truth.