You are the lead ML/CV engineer, backend engineer, data engineer, and system architect responsible for taking an EXISTING partially implemented repository and turning it into a genuinely working, tested visual geolocation product.

You are not being asked to continue blindly from the current code.

You must:

1. inspect the repository;
2. determine its REAL current state;
3. verify previous implementation claims;
4. repair or replace broken components;
5. finish the missing backend/ML system;
6. acquire and prepare real reference data;
7. integrate and benchmark real VPR models;
8. build retrieval and localization;
9. expose the system through a working API;
10. test everything end-to-end on real Moscow imagery;
11. measure actual localization quality;
12. leave the repository in a reproducible, documented state.

DO NOT stop after writing a plan.

DO NOT merely create code that “should work”.

Run the system, run the tests, run the models, inspect actual outputs, fix failures, and continue until the Definition of Done is met or you hit a genuine external blocker such as unavailable credentials/network/storage.

When blocked by one external dependency, continue all other work rather than stopping the whole task.

Do not ask the user routine engineering questions. Make sensible engineering decisions yourself and document them.

---

# 1. PRODUCT DEFINITION

We are building a web-based visual geolocation service.

Initial production coverage:

MOSCOW ONLY.

The architecture must nevertheless be designed so additional cities/countries can later be added without rewriting the core retrieval system.

User input:

- one photograph of an outdoor urban environment.

Expected result:

- estimated latitude;
- estimated longitude;
- confidence score;
- uncertainty radius if defensible;
- top matching reference images;
- top location hypotheses where useful;
- metadata required to display results on a map;
- source attribution for reference imagery;
- a clear low-confidence / out-of-coverage result when the system cannot localize reliably.

The system MUST NOT use a neural network that directly regresses latitude/longitude as the main localization mechanism.

The main architecture is retrieval-based visual geolocation:

query image
→ validation / preprocessing
→ global visual descriptor
→ retrieval from geotagged reference gallery
→ optional local geometric verification
→ spatial candidate aggregation
→ coordinate estimation
→ confidence estimation
→ API response

The primary deliverable in this task is the BACKEND + DATA + ML system.

Frontend work is secondary and should only be done after the backend localization pipeline actually works.

---

# 2. IMPORTANT PROJECT CONTEXT

This is an educational non-commercial project.

A partially implemented repository already exists.

Previous coding agents claimed that the following stages were implemented.

Stage 1 supposedly included some combination of:

- project skeleton;
- FastAPI/backend scaffold;
- frontend scaffold;
- PostgreSQL/PostGIS;
- Docker configuration.

Stage 2 supposedly implemented:

- Mapillary ingestion;
- KartaView ingestion;
- Moscow bbox/grid generation;
- response parsers;
- normalized source manifests;
- image downloader;
- validation;
- preview tools;
- tests.

Commits previously mentioned:

369b2b1
6c9b4af

Stage 3 supposedly implemented:

- clean_images.py
- quality_filter.py
- deduplicate.py
- h3_assign.py
- build_final_manifest.py
- check_dataset.py
- reporting
- tests

A previously reviewed Stage 3 commit:

71f94ac029

These claims are NOT trustworthy evidence that the system works.

Treat the current repository as unverified legacy code.

Your first responsibility is to determine what is actually present and actually functional.

---

# 3. FIRST ACTION: FORENSIC REPOSITORY AUDIT

Before implementing new features:

Inspect:

- current branch;
- git status;
- git log;
- recent commits;
- repository tree;
- README;
- architecture docs;
- pyproject.toml / requirements files;
- Dockerfiles;
- docker-compose;
- environment configuration;
- .gitignore;
- database setup;
- frontend setup;
- backend setup;
- test configuration;
- CI configuration;
- ingestion scripts;
- cleaning scripts;
- manifests;
- sample data;
- current ML code;
- model-loading code;
- FAISS code if any;
- API endpoints if any.

Run existing tests.

Run existing documented commands.

Do not trust README instructions until you have actually executed them.

Identify:

- working components;
- partially working components;
- dead code;
- duplicate implementations;
- architectural mistakes;
- stale API integrations;
- incorrect assumptions;
- performance bottlenecks;
- missing dependencies;
- tests that are never executed;
- code paths that have never run against real data.

Create a concise internal audit summary before doing large changes.

Do NOT make broad stylistic refactors just because you prefer another style.

Preserve correct code.

Repair incorrect code.

Replace components when repair would leave an unnecessarily fragile design.

---

# 4. GIT SAFETY

Do not destroy existing work.

Do not:

- force-push;
- rewrite repository history;
- delete large parts of the repository without understanding them;
- delete existing datasets unless clearly invalid and reproducible.

Before risky changes, understand what the files contain.

Keep the working tree understandable.

Do not commit secrets or huge generated datasets/model caches.

If committing is part of the environment/workflow, make logical commits.

---

# 5. TARGET ARCHITECTURE

Use a MODULAR MONOLITH.

Do NOT introduce microservices unless there is a concrete technical reason.

Conceptually the repository should converge toward something similar to:

project/
  apps/
    backend/
      app/
        api/
        schemas/
        services/
        repositories/
        db/
        config/
      tests/

    frontend/
      ...

  ml/
    ingestion/
    cleaning/
    enrichment/
    retrieval/
    indexing/
    verification/
    localization/
    evaluation/

  data/
    raw/
    processed/
    indexes/
    evaluation/

  infra/

  docs/

Exact paths may differ if the existing repository has a coherent equivalent structure.

Separation of responsibilities matters more than exact folder names.

HTTP handlers must not contain ML implementation.

ML implementation must not directly depend on web request objects.

Offline dataset preparation must not be embedded inside the API server.

---

# 6. DATA SOURCES

The initial deployable Moscow reference gallery should be built from available street-level imagery sources such as:

- Mapillary;
- KartaView.

Do NOT blindly assume previous loaders still match current APIs.

Verify the CURRENT official APIs and current response formats before modifying the integration.

Use primary/official documentation for API behavior.

Do not build the product gallery from a source whose current terms do not permit the intended project use.

Preserve source attribution and license information.

Benchmark/training datasets and the deployable reference gallery are different concepts.

Datasets such as MSLS, GSV-Cities, Pitts, Tokyo24/7, OSV-5M, etc. may be useful for:

- training;
- benchmarking;
- model evaluation;
- robustness evaluation;

but they do not automatically replace a Moscow deployable reference gallery.

---

# 7. MAPILLARY

Inspect the existing Mapillary loader.

Use the current Mapillary API, not assumptions from old blog posts or old API versions.

Credentials must come from environment variables.

Expected environment variable:

MAPILLARY_ACCESS_TOKEN

Never hard-code the token.

Never commit it.

If the token is missing:

- fail with a clear setup message;
- continue testing the rest of the pipeline using fixtures/local data.

The real Mapillary ingestion must support as appropriate for the current API:

- geographic queries;
- pagination;
- required fields;
- coordinates;
- timestamps;
- heading/compass angle if available;
- sequence information if available;
- image identifier;
- usable image URL / retrieval mechanism;
- retries;
- timeouts;
- rate-limit handling;
- resumability;
- attribution.

Do not assume historical bbox constraints.

Read the current API documentation and implement whatever the current limits require.

---

# 8. KARTAVIEW

Audit the existing KartaView integration against the CURRENT official API.

Do not assume its response schema from the old implementation is still correct.

Retrieve:

- image/photo IDs;
- coordinates;
- timestamps;
- direction where available;
- sequence information where available;
- image retrieval information;
- source metadata.

Normalize KartaView records into the same canonical internal schema as Mapillary.

Support:

- retry;
- timeout;
- resumability;
- pagination/continuation if required;
- logging;
- dedup-safe repeated execution.

If authentication is optional or different for specific KartaView endpoints, follow the current official behavior rather than inventing credentials.

---

# 9. DATA ACQUISITION MUST ACTUALLY RUN

Do not stop after implementing loaders.

If network access and credentials are available, ACTUALLY EXECUTE ingestion.

Use this progression.

### Phase A — tiny live smoke test

Choose a small Moscow region.

Retrieve real Mapillary metadata.

Retrieve real KartaView metadata.

Download real images.

Confirm manually/programmatically that:

- images decode;
- coordinates are valid;
- imagery corresponds to Moscow;
- timestamps parse;
- identifiers are stable;
- attribution is stored;
- local file mappings are correct.

### Phase B — small end-to-end gallery

Build a small but meaningful real Moscow gallery.

Run:

ingestion
→ download
→ cleaning
→ dedup
→ final manifest
→ embedding extraction
→ FAISS
→ held-out query retrieval
→ localization

Fix all failures before starting a large ingestion.

### Phase C — larger Moscow acquisition

Once the small run works:

- estimate available disk;
- estimate expected dataset size;
- ingest a substantially larger Moscow gallery;
- keep the operation resumable.

Do NOT blindly download an arbitrary number such as “200,000 images”.

The useful amount depends on:

- actual coverage;
- viewpoint diversity;
- density;
- available storage;
- retrieval quality.

Measure coverage instead of choosing dataset size by intuition.

---

# 10. INGESTION RELIABILITY

Large downloads must be restart-safe.

Requirements:

- checkpoint progress;
- skip already validated files;
- retry transient failures;
- exponential backoff;
- sensible timeout;
- bounded concurrency;
- no uncontrolled thread/process explosion;
- log permanent failures;
- tolerate individual corrupt records;
- make repeated runs idempotent.

Avoid a design where interruption forces ingestion to restart from zero.

Produce statistics:

- source records discovered;
- metadata successfully normalized;
- images requested;
- images downloaded;
- already-existing images skipped;
- failed downloads;
- invalid images;
- final valid reference count.

---

# 11. CANONICAL REFERENCE MANIFEST

Define ONE canonical schema.

At minimum include:

id
city_id
source
source_image_id
sequence_id
image_path
lat
lon
captured_at
heading
quality_score
license
attribution
metadata_json

Include H3/geographic fields where useful.

Document the schema.

Validate it.

Every stage must preserve required fields.

Do not let columns silently disappear.

Use a stable unique reference ID independent of mutable DataFrame row numbers.

---

# 12. metadata_json CONSISTENCY

Audit existing metadata_json handling.

Do not mix:

Python dict
and
literal string "{}"

within the same logical representation.

Choose one consistent storage strategy.

If using JSON strings in Parquet:

- JSON-serialize every row consistently;
- parse explicitly when needed.

If using structured objects:

- confirm Parquet/Arrow schemas remain stable.

Add tests.

---

# 13. DATA CLEANING PHILOSOPHY

The objective is NOT to make the dataset as small as possible.

The objective is to remove genuinely harmful redundancy and unusable imagery while preserving useful VPR diversity.

Preserve useful variation in:

- heading;
- viewpoint;
- camera position;
- time;
- weather;
- season;
- illumination.

This diversity improves place recognition.

---

# 14. BASIC VALIDATION

Hard-remove only clearly invalid inputs such as:

- missing files;
- unreadable/corrupt files;
- invalid coordinates;
- zero-sized images;
- extremely low-resolution images that are unusable.

Preserve schema even if a processing stage produces zero rows.

If the gallery becomes empty, fail with a clear controlled error rather than creating malformed files.

---

# 15. QUALITY SCORING

Audit the current quality_score implementation.

It may use features such as:

- sharpness / blur;
- resolution;
- exposure.

Do not overfit an arbitrary formula.

Quality score should primarily help:

- analysis;
- duplicate selection;
- prioritization.

Do not aggressively delete useful:

- dusk;
- cloudy;
- winter;
- low-contrast;
- imperfect exposure

images if scene structure is still usable.

Hard rejection should be conservative.

Avoid slow pixel-wise Python loops.

Use vectorized NumPy/OpenCV/PIL-compatible efficient operations.

Benchmark if necessary.

---

# 16. DEDUPLICATION — CRITICAL

Previous implementations may be conceptually wrong.

Audit this carefully.

Do NOT treat all geographically close images as duplicates.

Images from the same approximate location may provide essential different views.

Deduplication should target primarily:

- exact identical files;
- exact visual duplicates;
- near-identical consecutive sequence frames;
- redundant captures with essentially identical viewpoint.

Use information such as:

- perceptual hash;
- sequence;
- capture time;
- geographic distance;
- heading;
- quality score.

---

# 17. FIX SINGLE-LINK GEO CHAINING

A previous implementation reportedly used graph connected-components where points within a geographic radius were connected.

This causes single-link chaining:

A near B
B near C
C near D

→ A/B/C/D all become one component

even if A and D are far apart.

On long street-view sequences this can collapse large geographic stretches into a single “duplicate cluster”.

DO NOT use this approach for per-location caps.

Replace it with conservative local grouping that cannot transitively collapse long trajectories.

Possible approaches include:

- spatial cells + local comparisons;
- bounded-radius neighborhoods around an anchor;
- sequence-aware thinning;
- spatial clustering with bounded cluster diameter.

Choose an efficient, correct approach and add a regression test reproducing the chaining problem.

---

# 18. HEADING-AWARE PRESERVATION

If heading/direction metadata exists:

Preserve materially different viewpoints.

Do not remove two geographically close images merely because they occupy the same location if their camera directions differ significantly.

Do NOT blindly hard-code “60 degrees” unless reasonable after inspection.

Use a documented threshold and test behavior.

Images looking in opposite directions are usually different reference information.

---

# 19. VISUAL DEDUP QUALITY ORDER

Visual duplicate suppression must prefer useful high-quality images.

If two images are visual near-duplicates:

do NOT simply preserve whichever appeared first in the manifest.

Prefer the candidate with better quality / useful metadata.

Add a regression test where:

low-quality image appears first
high-quality duplicate appears second

and confirm the high-quality image survives.

---

# 20. DEDUP PERFORMANCE

Do not do an O(N²) comparison across the full Moscow gallery.

Use spatial/sequence bucketing to reduce candidate comparisons.

Possible tools:

- H3;
- spatial grid;
- BallTree;
- KD-tree where appropriate;
- sequence grouping;
- database spatial queries.

Then perform exact geographic/visual comparisons only on plausible nearby candidates.

Benchmark before and after if the current implementation is large.

---

# 21. H3

Use H3 as an engineering geospatial indexing tool.

Good use cases:

- coverage analysis;
- geographic bucketing;
- future city/index sharding;
- statistics;
- spatial candidate grouping;
- train/test leakage diagnostics.

Do NOT attach inaccurate semantic names such as:

“H3 resolution X equals a district”
or
“H3 resolution Y equals a street”.

Choose resolutions based on actual physical cell scale and task requirements.

Do NOT add a hard H3 online pre-filter unless a coarse localization method has measured high recall.

For Moscow v1, searching a single Moscow FAISS index is acceptable if latency is reasonable.

Accuracy is more important than premature routing optimization.

---

# 22. COVERAGE ANALYSIS

Generate an actual Moscow coverage report.

Measure:

- total references;
- unique spatial cells;
- references per area;
- spatial density;
- nearest-reference distance statistics;
- Mapillary coverage;
- KartaView coverage;
- combined coverage.

Produce visualizations usable later in the project report:

- Moscow scatter/coverage map;
- density map;
- source comparison.

Do not claim “Moscow coverage” without measuring where data actually exists.

---

# 23. RETRIEVAL MODEL ABSTRACTION

Do not wire the entire codebase directly to one specific model.

Create a clean retriever interface.

Conceptually:

BaseRetriever

properties:
- model_name
- descriptor_dim
- device
- preprocessing/version metadata

methods:
- load()
- embed_batch(images)
- embed_query(image)

The rest of the retrieval pipeline must not care which specific model produced the descriptor.

---

# 24. MODEL CANDIDATES

At minimum investigate strong current VPR/retrieval candidates such as:

- MegaLoc;
- DINOv2 + SALAD.

EigenPlaces may also be evaluated if inexpensive to integrate.

Do NOT select SALAD only because an earlier conversation called it “best”.

Do NOT select MegaLoc only because it is newer.

Integrate candidates through the same interface and benchmark them on OUR actual Moscow data.

Before downloading model weights:

- verify official implementation;
- verify checkpoint source;
- verify current license;
- document model version/checkpoint.

This is a non-commercial educational project, so non-commercial/research models may be acceptable if their actual terms allow this use.

Still document all restrictions accurately.

---

# 25. MODEL WEIGHT ACQUISITION

If network access is available:

- automatically download required official checkpoints;
- cache them;
- verify load success;
- do not redownload unnecessarily.

If the environment cannot access the model host:

- provide an exact actionable blocker;
- keep integration code and fixture tests working;
- continue unrelated tasks.

Never silently fall back to random/uninitialized weights.

---

# 26. DEVICE SUPPORT

Support practical execution environments.

Preferred detection:

1. CUDA
2. Apple MPS if the chosen model/operators truly support it
3. CPU

Do not assume MPS support simply because PyTorch exposes MPS.

Run a real inference smoke test on the selected device.

If a model/operator breaks on MPS:

- fall back safely;
- log the fallback.

Use:

- torch.inference_mode();
- batching;
- model loaded once;
- no-grad inference;
- reasonable precision where supported.

---

# 27. REFERENCE EMBEDDING PIPELINE

Implement a robust offline embedding job.

Requirements:

- configurable batch size;
- progress logging;
- restart/checkpoint support;
- deterministic reference ID → descriptor row mapping;
- no model reload per batch;
- failed-image reporting;
- model/preprocessing metadata;
- normalization appropriate to the model.

Store:

descriptor matrix
reference-ID mapping
build metadata

Do not rely only on current DataFrame row position.

The mapping must remain explicit after index reload.

---

# 28. FAISS INDEXING

Use FAISS for reference descriptor search.

Start with a correctness-first index.

For a Moscow-sized gallery, first benchmark an exact search such as cosine-equivalent IndexFlatIP when descriptors are normalized.

Only introduce approximate ANN structures if actual measurements show:

- excessive memory;
- unacceptable latency.

Do NOT introduce IVF/PQ complexity prematurely.

Requirements:

- descriptor normalization where needed;
- persisted index;
- persisted ID mapping;
- index metadata;
- deterministic rebuild;
- successful reload;
- top-k retrieval with scores.

Index metadata should record:

- retriever;
- checkpoint/version;
- descriptor dimension;
- gallery size;
- normalization;
- FAISS index type;
- build timestamp.

Add test:

build index
→ query
→ save
→ reload
→ same nearest-neighbor results.

---

# 29. MOSCOW EVALUATION SET — CRITICAL

Build a REAL held-out Moscow evaluation set.

Do not use the exact gallery image as its own query.

Prevent trivial leakage.

Preferred query/reference separation:

1. different sequences when possible;
2. different capture times when possible;
3. exclude near-identical adjacent sequence frames;
4. ensure query itself does not exist in gallery.

Create multiple difficulty levels if useful.

For example:

A. Cross-sequence / cross-time

Strongest evaluation.

B. Held-out sequence frames

Useful when source coverage is limited.

C. Augmentation robustness

Derived from real held-out images.

Keep metrics for these levels separate.

Do not merge them into one misleading number.

---

# 30. TEST IMAGE GENERATION

You must generate automatic robustness test variants.

Start from REAL geotagged held-out images and generate variants such as:

- resize;
- center/random crop;
- JPEG compression;
- mild Gaussian blur;
- mild motion blur;
- brightness/exposure changes;
- color temperature/color changes;
- modest perspective transform;
- partial occlusion.

Ground-truth coordinates remain those of the original image.

Use these to test sensitivity to:

- camera quality;
- compression;
- crop;
- viewpoint perturbation;
- poor lighting.

AI-generated urban imagery may be created for:

- upload testing;
- invalid/OOD scenarios;
- frontend demonstrations;

but MUST NOT be used to claim geographic localization accuracy because it does not provide reliable real-world geolocation ground truth.

---

# 31. RETRIEVAL EVALUATION

For every candidate retriever calculate at least:

Recall@1
Recall@5
Recall@10

Define a retrieved result as positive using explicit geographic distance thresholds.

Report the definition.

Also measure:

- average query descriptor latency;
- gallery embedding throughput;
- descriptor dimension;
- gallery descriptor storage;
- retrieval latency.

Produce a comparison table.

Pick the default retriever based on actual Moscow results.

---

# 32. RETRIEVAL ENSEMBLE

Do not build an ensemble automatically.

First measure individual retrievers.

If two retrievers exhibit complementary errors, evaluate fusion.

Possible fusion:

- reciprocal rank fusion;
- calibrated normalized-score fusion.

Keep an ensemble only if it yields a meaningful accuracy gain that justifies:

- extra model memory;
- embedding storage;
- inference latency.

Otherwise use the best single retriever.

---

# 33. LOCAL GEOMETRIC VERIFICATION

After global retrieval works, evaluate second-stage local verification.

Use a strong maintained local feature pipeline compatible with the project, e.g. LightGlue plus a suitable extractor.

Verify current:

- implementation;
- model weights;
- compatibility;
- license.

Do not run local verification against the entire reference gallery.

Use it only on top-N candidates, configurable for example through:

VERIFY_TOP_K

For each candidate derive useful signals such as:

- raw match count;
- geometric inlier count;
- inlier ratio;
- geometric consistency.

Use robust geometric estimation such as RANSAC where appropriate.

Rerank candidates.

Benchmark:

global retrieval only

versus

global retrieval + verification

If verification does not improve actual Moscow localization metrics enough to justify its latency, keep it optional/off by default.

Do not keep complexity just because it sounds sophisticated.

---

# 34. OCR

OCR is NOT required for core localization.

Do not let it block the MVP.

Design an optional OCR service interface.

If implemented, use it as auxiliary evidence for:

- house numbers;
- street names;
- shop names;
- transport text;
- signs.

OCR evidence may increase/decrease candidate confidence.

Weak OCR must never override strong visual/geometric evidence.

---

# 35. ONLINE QUERY PREPROCESSING

The API must robustly validate uploads.

Handle:

- allowed image formats;
- MIME;
- corrupt files;
- decompression failures;
- EXIF orientation;
- RGB conversion;
- very large uploads;
- unreasonable dimensions.

Compute lightweight diagnostics:

- width/height;
- sharpness;
- exposure.

Reject only clearly unusable images.

Return meaningful user-facing status codes/messages.

---

# 36. ONLINE LOCALIZATION PIPELINE

The final online path should be conceptually:

upload
→ decode
→ orientation correction
→ model preprocessing
→ query global descriptor
→ FAISS top-K
→ reference metadata fetch
→ optional local verification
→ spatial grouping
→ coordinate estimation
→ confidence estimation
→ API result

Measure latency for each stage.

Avoid loading models/indexes on every request.

Load long-lived resources once during application startup.

---

# 37. SPATIAL CANDIDATE AGGREGATION

Do NOT simply average every top-k retrieval coordinate.

Urban appearance contains severe perceptual aliasing.

Top retrievals may represent visually similar locations kilometers apart.

Implement spatial grouping of top candidates.

The goal:

identify the dominant coherent geographic hypothesis.

Potential strategy:

- retrieve top K;
- group coordinates using a sensible local spatial threshold/clustering approach;
- calculate cluster evidence from retrieval scores and verification;
- rank clusters;
- choose strongest consistent cluster.

Tune relevant thresholds using the held-out evaluation data.

---

# 38. COORDINATE ESTIMATION

Compare simple coordinate estimators.

Candidate methods:

- top-1 reference coordinate;
- best verified reference;
- weighted centroid within winning compact cluster;
- weighted geographic medoid within winning cluster.

Never average candidates across different geographic modes.

Evaluate each estimator on the Moscow test set.

Use whichever performs best and remains interpretable.

---

# 39. CONFIDENCE

Confidence must correspond to actual reliability.

Possible signals:

- top-1 retrieval similarity;
- top1/top2 margin;
- score distribution;
- winning-cluster mass;
- distance to second geographic hypothesis;
- geographic compactness;
- verification inliers;
- query quality;
- local reference density;
- agreement between retrievers if ensemble exists.

Start with an interpretable heuristic.

Then calibrate thresholds using the Moscow held-out set.

Measure actual error by confidence bucket.

The system must be allowed to return:

low_confidence

rather than inventing a precise location.

---

# 40. UNCERTAINTY RADIUS

If returned, uncertainty_radius_m must not be arbitrary.

Derive it from measurable evidence such as:

- spatial spread of winning candidates;
- evaluation-calibrated error conditioned on confidence/features.

If the data is insufficient to produce defensible uncertainty, omit it temporarily rather than inventing a radius.

---

# 41. OUT-OF-COVERAGE / NOT-MOSCOW

Initial gallery coverage is Moscow.

Do not overbuild a global geolocation classifier now.

Implement practical out-of-coverage behavior using:

- low retrieval evidence;
- incoherent top candidates;
- low confidence;
- optional external coarse model only if justified.

Do not create a hard H3 router that can throw away the correct location without measured recall.

Architecture should allow adding future:

- city router;
- global model;
- multiple city indexes.

But Moscow v1 should stay simple.

---

# 42. GLOBAL EXPANSION READINESS

Avoid hard-coding Moscow into generic retrieval logic.

Use data/config concepts such as:

city_id
region_id
index_id

The current index may be:

moscow

Future indexes could be:

prague
london
world_region_X

Core retriever/localizer components must remain generic.

Moscow is current DATA SCOPE, not a fundamental algorithm assumption.

---

# 43. BACKEND

Backend/ML correctness is the main deliverable.

Standardize on FastAPI if the current backend is absent or unsuitable.

Required endpoints:

GET /health

Basic process health.

GET /ready

Verify:

- model loaded;
- FAISS loaded;
- metadata available;
- required dependencies ready.

POST /localize

Accept multipart image.

Return localization.

Optional:

GET /result/{id}

if persisted results are useful.

---

# 44. API RESPONSE CONTRACT

Use typed Pydantic models.

Example conceptual response:

{
  "status": "ok",
  "prediction": {
    "lat": 55.xxx,
    "lon": 37.xxx,
    "confidence": 0.xxx,
    "uncertainty_radius_m": 100
  },
  "hypotheses": [
    {
      "lat": ...,
      "lon": ...,
      "score": ...
    }
  ],
  "matches": [
    {
      "reference_id": "...",
      "source": "mapillary",
      "lat": ...,
      "lon": ...,
      "retrieval_score": ...,
      "verification_score": ...,
      "thumbnail_url": "...",
      "attribution": "..."
    }
  ],
  "diagnostics": {
    "retriever": "...",
    "query_ms": ...,
    "retrieval_ms": ...,
    "verification_ms": ...
  }
}

Do not expose:

- filesystem absolute paths;
- tokens;
- DB credentials;
- internal stack traces.

---

# 45. API ERROR STATES

Implement explicit behavior for:

invalid_image
unsupported_format
image_too_large
model_not_ready
index_not_ready
low_confidence
out_of_coverage
internal_error

Use appropriate HTTP behavior while keeping the product status explicit.

---

# 46. DATABASE / STORAGE

Do not overengineer storage.

If PostgreSQL/PostGIS is already working, use it for:

- reference metadata;
- geospatial queries;
- optional result persistence.

Canonical offline datasets may remain Parquet where convenient.

Images:

filesystem/object-storage abstraction.

Embeddings:

efficient binary arrays/files.

FAISS:

index file.

Maintain explicit IDs linking all artifacts.

Do NOT place huge embedding arrays into PostgreSQL just because PostgreSQL exists.

---

# 47. FRONTEND

Frontend is NOT the priority until localization works.

Do not spend significant time designing UI early.

If a frontend skeleton already exists, preserve it.

After backend acceptance tests pass, produce only the minimum useful demo:

- image upload;
- processing state;
- map;
- predicted marker;
- uncertainty radius if available;
- confidence;
- top matching reference images;
- attribution;
- error/low-confidence state.

Avoid unnecessary frontend complexity.

---

# 48. TEST FRAMEWORK

Audit how tests currently run.

Previous review suggested some tests may have been written in pytest style while the documented runner used unittest discovery.

Fix this.

Prefer one canonical runner:

pytest

unless the existing repository has a strong reason not to.

Provide ONE obvious command:

pytest -q

Ensure it discovers ALL tests.

Make README, CI and task runner agree.

---

# 49. UNIT TESTS

At minimum test:

Ingestion:
- bbox/grid handling;
- Mapillary parsing;
- KartaView parsing;
- pagination state;
- normalization;
- resume/skip behavior.

Data:
- manifest schema;
- invalid image handling;
- quality metrics;
- metadata_json representation.

Geo:
- Haversine;
- heading difference;
- spatial bucketing.

Dedup:
- exact duplicates;
- visual duplicates;
- different headings preserved;
- higher-quality duplicate retained;
- sequence thinning;
- no single-link trajectory collapse.

Embeddings:
- output shape;
- normalization where expected;
- deterministic ID mapping.

FAISS:
- expected nearest neighbor;
- save/reload equivalence.

Localization:
- cluster selection;
- unrelated clusters not averaged;
- low-confidence handling.

---

# 50. REGRESSION TESTS FOR PREVIOUS REVIEW COMMENTS

Explicitly reproduce and test these previously identified bugs.

### A. Single-link chaining

Construct:

A—B—C—D

where each adjacent pair is within radius but A and D are far apart.

Assert that the dedup algorithm does NOT treat the entire chain as one local duplicate group.

### B. Quality-order visual dedup

Low-quality duplicate first.

High-quality duplicate second.

Assert high-quality survives.

### C. Empty quality output

Make all images fail quality.

Assert:

- schema remains valid;
or
- pipeline produces a clear controlled “empty dataset” error.

No malformed schema-less Parquet.

### D. metadata_json

Assert type/encoding remains consistent across pipeline stages.

### E. Invalid configuration

Reject nonsensical values such as zero/negative “max keep” limits where those options exist.

### F. Visual hash threshold edge behavior

Define and test exact semantics for threshold zero.

---

# 51. PERFORMANCE FIXES

Audit actual performance.

Known suspect patterns include:

- O(N²) full-dataset geospatial loops;
- nested Python loops over image pixels;
- pandas apply(axis=1) for simple pure-Python transforms;
- serial network requests;
- model reloads;
- single-image model inference for large galleries.

Fix them when meaningful.

Use:

- spatial bucketing/indexes;
- vectorized operations;
- list comprehensions/itertuples where appropriate;
- batching;
- bounded concurrency;
- caching.

Do not introduce optimization complexity without measuring.

---

# 52. INTEGRATION TEST

Create a tiny local gallery fixture with geotagged images.

Run:

images
→ cleaning
→ embedding
→ index
→ query
→ retrieval
→ localization

Assert that the expected geographic candidate wins.

Use actual model inference in at least one integration/smoke test where runtime permits.

For fast default unit runs, lighter fixtures/mocks are acceptable.

---

# 53. API TESTS

Test:

GET /health

GET /ready

POST /localize with:

- valid image;
- corrupt file;
- unsupported format;
- oversized upload;
- low-confidence image;
- missing index;
- normal successful response.

Validate response schema.

---

# 54. REAL END-TO-END TEST

This is mandatory if network/model access permits.

Use real Moscow data.

Run:

real imagery download
→ cleaning
→ final reference manifest
→ model embedding
→ FAISS
→ held-out query
→ top references
→ coordinate estimate
→ API call

Record actual outputs.

Do not claim end-to-end success because unit tests passed.

---

# 55. EVALUATION METRICS

Produce actual evaluation.

Retrieval:

Recall@1
Recall@5
Recall@10

Localization:

percentage ≤ 25 m
percentage ≤ 50 m
percentage ≤ 100 m
median error in meters
p90 error in meters

Reliability:

coverage/answer rate
low-confidence rate
accuracy by confidence bucket

Performance:

query embedding latency
FAISS latency
verification latency
total API localization latency

Save machine-readable metrics.

Also produce a readable Markdown report.

---

# 56. ABLATIONS

Do not build dozens of research experiments.

Perform only useful product ablations.

At minimum where feasible:

Retriever A vs Retriever B.

Retrieval-only vs retrieval+verification.

Top-1 coordinate vs chosen spatial aggregation.

Optionally:

Mapillary-only vs KartaView-only vs combined gallery.

The purpose is to determine which components actually improve the product.

---

# 57. LICENSING / ATTRIBUTION AUDIT

Create:

docs/licenses.md

For every major component record:

- software;
- model/checkpoint;
- dataset;
- imagery source;
- license/terms;
- attribution requirement;
- relevant project-use restriction.

Verify CURRENT terms.

Do not assume:

repository code license
=
pretrained model license.

Preserve Mapillary/KartaView attribution according to their current requirements.

Do not expose third-party imagery without required attribution.

---

# 58. OBSERVABILITY

Replace scattered print debugging with structured logging where reasonable.

Offline jobs should log:

stage
input count
output count
reject count by reason
elapsed time
download retries
failures
throughput

Online requests should log:

request ID
preprocessing time
embedding time
retrieval time
verification time
total time
status

Never log secrets.

---

# 59. DATA REPORT

Produce an automatically generated report containing:

Mapillary metadata records discovered
KartaView metadata records discovered
download successes
download failures
invalid images
quality removals
duplicate removals
final reference gallery size
spatial coverage statistics
nearest-reference statistics
source proportions

Save:

JSON
and
human-readable Markdown

where convenient.

---

# 60. REPRODUCIBLE COMMANDS

Leave the repository with one obvious workflow.

Prefer a Makefile/task runner or equivalent.

Provide commands conceptually equivalent to:

make setup

make test

make ingest-sample

make ingest-moscow

make prepare-data

make embed

make build-index

make eval

make api

make smoke

Exact names may differ.

Every documented command must actually work.

Do not document commands you did not run.

---

# 61. ENVIRONMENT CONFIGURATION

Provide:

.env.example

Document:

MAPILLARY_ACCESS_TOKEN

database configuration where required

data root

model cache

retriever choice

batch size

verification enabled/disabled

top-k

confidence thresholds

Do not hard-code machine-specific paths.

---

# 62. .gitignore

Ensure generated material is not accidentally committed.

Typically ignore:

.env
access tokens
downloaded street imagery
model caches
large checkpoints where appropriate
embedding arrays
FAISS indexes
temporary manifests
runtime logs
Python caches
frontend build caches

Keep small fixtures and evaluation metadata if useful.

---

# 63. RESOURCE MANAGEMENT

Before a large Moscow run:

inspect:

- free disk space;
- available RAM;
- GPU availability.

Estimate likely storage requirements.

Do not exhaust the machine.

Use chunking.

If storage is limited:

prioritize geographically useful coverage rather than arbitrary random truncation.

Report constraints.

---

# 64. FAIL-SAFE BEHAVIOR

A product result must be honest.

Do not always return a Moscow coordinate.

If evidence is weak:

status = low_confidence

If gallery is unavailable:

status = index_not_ready

If image is invalid:

status = invalid_image

If query appears unsupported/out-of-coverage:

status = out_of_coverage

A confident wrong answer is worse than an explicit uncertain result.

---

# 65. WHAT NOT TO DO

Do NOT:

- replace retrieval with direct lat/lon regression;
- rely on generated AI images to claim accuracy;
- treat visual similarity alone as proof of exact location;
- average geographically unrelated retrievals;
- aggressively delete nearby reference views;
- implement microservices unnecessarily;
- build global geolocation before Moscow works;
- make H3 routing mandatory without measured recall;
- write giant notebooks as the production architecture;
- hide failures behind try/except pass;
- fake API responses;
- fake evaluation metrics;
- claim something was tested when it was not;
- stop once code compiles;
- stop once unit tests pass;
- spend most of the time polishing frontend before ML works.

---

# 66. IMPLEMENTATION ORDER

Follow this order unless repository reality clearly requires a small adjustment.

## Phase 1 — Audit

1. inspect repository;
2. inspect git history;
3. run current tests;
4. run current documented commands;
5. identify real state.

## Phase 2 — Repair existing infrastructure

6. repair dependency/environment setup;
7. repair tests;
8. repair Stage 2 ingestion;
9. repair Stage 3 cleaning/dedup/H3/reporting;
10. add regression tests for known review findings.

## Phase 3 — Real data validation

11. run tiny Mapillary Moscow ingestion;
12. run tiny KartaView Moscow ingestion;
13. validate downloaded real images;
14. run cleaning on real sample;
15. generate coverage/data report.

## Phase 4 — Baseline retrieval

16. implement retriever abstraction;
17. integrate first strong retriever;
18. create embeddings for sample gallery;
19. build FAISS;
20. retrieve held-out real queries;
21. verify ID/coordinate mapping.

## Phase 5 — Evaluation

22. build leakage-resistant Moscow held-out query set;
23. calculate Recall@K;
24. calculate initial localization metrics.

## Phase 6 — Model benchmark

25. integrate second candidate retriever;
26. benchmark both;
27. select default model empirically;
28. only implement ensemble if measurements justify it.

## Phase 7 — Localization

29. implement spatial grouping;
30. compare coordinate estimators;
31. choose best measured method;
32. implement low-confidence behavior.

## Phase 8 — Verification

33. integrate local geometric verification;
34. benchmark retrieval vs retrieval+verification;
35. keep verification enabled only if beneficial.

## Phase 9 — Full backend

36. implement startup model/index loading;
37. implement /health;
38. implement /ready;
39. implement /localize;
40. implement typed responses;
41. implement errors;
42. implement attribution.

## Phase 10 — Larger Moscow gallery

43. estimate storage/resources;
44. run resumable larger ingestion;
45. prepare final gallery;
46. embed final gallery;
47. build final index;
48. run final Moscow evaluation.

## Phase 11 — Robustness

49. generate real-query augmentation variants;
50. evaluate robustness;
51. adjust preprocessing/confidence only if evidence supports it.

## Phase 12 — Minimal frontend/demo

52. repair/create minimal frontend only now;
53. upload image;
54. call backend;
55. show predicted point;
56. show confidence;
57. show top reference images;
58. show attribution.

## Phase 13 — Final QA

59. run complete test suite;
60. run integration tests;
61. run API tests;
62. run real end-to-end smoke test;
63. run documented commands from clean-ish setup;
64. inspect git diff/status;
65. update README;
66. generate final reports.

---

# 67. DEFINITION OF DONE

The project is NOT done merely because Stage 4 was added.

It is done when the following applicable requirements are actually satisfied.

## Repository

- coherent modular structure;
- no obvious duplicate implementations;
- environment reproducible;
- secrets excluded;
- README matches reality.

## Data

- real Moscow imagery successfully ingested from available configured sources;
- normalized canonical manifest;
- conservative data cleaning;
- correct dedup;
- measured coverage;
- resumable ingestion.

## ML retrieval

- at least one real strong VPR model integrated;
- embeddings actually generated;
- FAISS index actually built;
- persisted mapping verified;
- held-out queries retrieve meaningful reference images.

## Localization

- top retrievals converted to geographic hypotheses correctly;
- unrelated clusters are not averaged;
- confidence exists;
- low-confidence output exists.

## Evaluation

- real held-out Moscow queries;
- Recall@1/5/10;
- ≤25m/50m/100m accuracy;
- median error;
- p90 error;
- robustness test results.

## Backend

- /health works;
- /ready works;
- /localize accepts real upload;
- response schema is typed;
- model and index load once;
- errors are handled.

## Testing

- one canonical test command;
- unit tests pass;
- regression tests pass;
- integration test passes;
- API tests pass;
- real smoke test passes when external resources are available.

## Performance

- no obvious avoidable O(N²) full-gallery bottleneck;
- no pixel-wise Python blur loops;
- embedding batched;
- network ingestion bounded/resumable;
- query latency measured.

## Documentation

- setup instructions;
- environment variables;
- ingestion commands;
- model/index build commands;
- evaluation commands;
- API instructions;
- architecture summary;
- data report;
- evaluation report;
- license report;
- known limitations.

---

# 68. FINAL DELIVERABLE REPORT

At the very end, give the user a concise but precise report containing:

### 1. Initial repository state

What actually existed and worked when you started.

### 2. Major problems found

Including which previous implementation assumptions were wrong.

### 3. What you changed

Organized by:

data
cleaning
retrieval
localization
backend
tests
performance
documentation

### 4. Final architecture

Describe the actual implemented path.

### 5. Data

Report actual:

Mapillary count
KartaView count
final gallery count
coverage statistics

If full ingestion could not run, explain exactly why.

### 6. Models

Which models were actually tested.

Show comparison metrics.

State which retriever became default and WHY.

### 7. Verification

State whether local verification improved results.

Include measured difference.

### 8. Final metrics

Report:

Recall@1
Recall@5
Recall@10

≤25 m
≤50 m
≤100 m

median error
p90 error

latency

Do not fabricate missing values.

### 9. Tests

Give exact executed test commands and results.

### 10. Run instructions

Exact commands required to:

setup
download sample
prepare gallery
embed
build index
evaluate
start API
run smoke test

### 11. Remaining limitations

Be explicit.

Examples may include:

coverage holes
seasonal mismatch
similar residential areas
open imagery availability
hardware latency

### 12. External blockers

List ONLY genuine remaining items requiring user action, such as:

missing access token
insufficient disk
environment network restriction

Do not list implementation work you could have done yourself.

---

# 69. CORE OPERATING PRINCIPLE

Your job is not to maximize the amount of code written.

Your job is to leave behind a real system where:

REAL geotagged Moscow imagery
→ clean reference gallery
→ real visual embeddings
→ FAISS retrieval
→ optional verified matching
→ geographic localization
→ confidence
→ FastAPI
→ real measured results

actually works.

Whenever there is a conflict between:

architectural sophistication
and
verified working behavior,

choose verified working behavior.

Whenever there is a conflict between:

assumption
and
measurement,

choose measurement.

Whenever previous code or documentation disagrees with actual execution,

actual execution is the source of truth.

Do not finish until the system is working, tested, measured, and documented, or until a genuine external blocker makes further execution impossible.