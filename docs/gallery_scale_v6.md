# Moscow gallery size and composition with fixed SAGE-L

This stage follows the user's 2026-09-08 scope. It does not resume the earlier
architecture/final-test workflow. Calibration, final, confidence, model training,
other encoders and production changes are out of scope.

- One unchanged development population: 1,184 v5 queries, SHA-256
  `6bcc0b4a618682e05e15305dc76b9f02af635e73573e06ea2567d6094ad3e7d9`.
- SAGE ViT-L No-Encoder, official checkpoint SHA-256
  `31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc`;
  official normalize-then-resize 322, float32 unit descriptors, MPS, batch 4,
  two torch threads, no device fallback. Existing compatible gallery and query
  descriptors are reused. New image descriptors have immutable chunk receipts.
- Exact cosine, deterministic manifest-order ties, top-1 reference coordinates.
  No confidence filter. Queries lacking nearby references stay in all denominators.
- Exact administrative relation R102269, polygon SHA-256
  `33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a`.

`configs/moscow_gallery_scale.json` is the single stage configuration.
`GEOSNAP_DATA_ROOT` overrides the previously recorded external research root.
Its `full_mapillary/` is the physical image store; gallery manifests contain
paths, never their own image copies. Legacy directories are symlinked after
verified storage migration so frozen manifests and production paths keep working.
The immutable frozen production descriptors, index, policies and source are kept.

## Controlled comparisons

G0 is the existing 31,084-reference research union. G1 random and smart add the
same number of references from one audited pool, targeting 60,000 total. The
random seed is 20260908. The single smart policy fills empty/low-density H3-9
cells, then prefers underrepresented provider sequences, 45-degree heading bins
and seasons. It has no query-coordinate or model-score argument. No spatial
proximity filter treats distinct viewpoints as duplicates.

G2 smart uses a prefix of the same fixed smart ordering, targeting 100,000, and
runs only when G1 smart's raw top-1 100 m point accuracy exceeds G1 random.
Paired geographic-bootstrap uncertainty is reported separately: this operational
gate is not a claim of statistical significance. Insufficient eligible images
produce actual smaller counts, not replacement filters or altered queries.

The optional, single panorama A/B requires at least 100 flagged, verified 2:1
equirectangular parents in the evaluated gallery, capped at 500. Four 90-degree
perspectives at relative yaw 0/90/180/270 replace the original panorama score.
Views use the same encoder, exist in memory only, and reduce to one maximum
similarity, identity and coordinate per parent. This does not add four geographic
votes. The stage stops after the A/B irrespective of the result.

## Provenance and leakage

Initial `full_mapillary` inventory contained 38,356 extracted JPGs from other
MSLS cities, 276 CSVs, nine ZIPs, a license and MD5 list. Moscow photos were still
in archives. The previously downloaded Moscow query ZIP was found in Downloads.
The verified official metadata provides 249,374 Moscow-package records; exact
R102269 retains 249,190 (225 panorama flags), before leakage and duplicate audits.

MSLS is a separately packaged, research-only source. Its original database/query
labels remain provenance, not GeoSnap evaluation assignments. They share some
provider sequences, so both packages use one normalized Mapillary sequence key.
The existing research-only license gate remains enabled. The bundled text calls
itself NC-SA but links to NC legalcode; that ambiguity is retained, and no
production distribution is authorized. G1/G2 containing MSLS are explicitly
research-only. Upstream SAGE training used MSLS; this development experiment
does not constitute an untouched-test generalization result.

New candidates exclude the existing deterministic query-role sequences. Identity,
SHA-256, decoded RGB hashes and pHash checks include the fixed development
population (all rotations and mirrors), plus pre-existing opaque historical
identity ledgers. Any development duplicate excludes its entire provider
sequence, including MSLS packaging aliases. Reference duplicates are audited
without geographic proximity suppression. Unknown historical-to-modern
Mapillary aliases cannot be certified absent; no held-out claim is made.
This stage does not read calibration/final manifests, images or results.

## Execution and resumption

Run from the repository with the existing environment:

```sh
.venv/bin/python -m ml.research.gallery_scale --phase run
```

The standalone controller runs phases in separate processes, freeing model and
scoring memory between galleries. It stops on failure and supports the same
command for resumption; completed audits, descriptor chunks and results are
reused. The descriptor pool holds row pointers to existing blocks and new
chunks, avoiding several 100k × 8448 matrices/indexes. External writable mmap
is not used. Reports and status are under `GEOSNAP_DATA_ROOT/gallery_scale_v6/`;
the small local pointer is `data/evaluation/moscow_gallery_scale_v6/storage.json`.

The controller holds an exclusive local lock. Check its PID and phase before
resuming; a saved `running` status alone does not mean the process is alive.
Completed migration and metadata preparation are skipped. Interrupted archive
copies resume only after byte-for-byte prefix verification; full checksums and
a decode smoke still precede removal of any internal source copy.

After the external-volume interruption, `chunk-000078.json` remained unreadable
and could not be renamed. A contract-bound `descriptor_recovery.json` can
explicitly exclude that receipt after the other chunks and new-file I/O have
been verified. The unreadable file and its vector file remain untouched. New
chunks use unused serials, and only images lacking a usable receipt are encoded.
Progress counts committed rows rather than inferring counts from serial numbers.
This recovery changes no model, gallery selection, or query population.

Final reporting includes every requested gallery/coverage/retrieval/raw metric,
full positive-rank quantiles, and the mutually exclusive error decomposition:
no reference within 100 m; a reference exists but ranks below 100; a reference
is ranked 2–100 but top-1 is wrong; top-1 is correct. No next research phase,
confidence tuning or production switch is launched after the report.
