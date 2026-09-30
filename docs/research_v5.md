# Moscow raw-localization research, v5

Status: architecture and calibration-selected threshold are frozen. The single
paired final evaluation is running on 1,177 primary and 158 recent-source queries.
Both cohorts were opened only after the full freeze. Production is unchanged;
no final comparison or promotion decision is available yet.

The preregistered protocol is
[`moscow_research_v5_protocol.json`](../configs/moscow_research_v5_protocol.json),
SHA-256 `eaad681fdb47b68666922dda370e30add26d271394e0bfe817002b0a752c069a`.
It prioritizes all-query coordinates and requires a >=5 percentage point raw
100 m improvement, positive paired geographic-bootstrap lower bound, no raw
catastrophic regression, and a useful >=90% conditional product point before
promotion. Calibration chooses a threshold only after architecture freeze.
A final failure keeps production. Historical tests are not selection data.

Production code/config/gallery/descriptors/index have a content-hash snapshot
at `data/evaluation/moscow_research_v5/baseline_snapshot.json`, SHA-256
`948f1b3b5faf7abac3b7859ca06a2c6d6a3c83b7ccd901b45a42018b880ba083`.
The originating Git commit is recorded in that snapshot. Verify each file against
the snapshot directly. Do not rerun the original inventory: its formerly unassigned
pool predates the sealed benchmark and must not be reassigned.

## Development findings so far

These are paired results on **751 historical v4 development queries**, with the
exact **20,487-image production gallery**. They are exploratory; they are not
new final-test evidence. This development split is heavily Mapillary dominated
(748 Mapillary, 3 KartaView), so its generalization limits are material.

| Measurement | Exact production pipeline |
|---|---:|
| Raw <=25 / <=50 / <=100 m | 14.11 / 22.50 / 29.69% |
| Raw median / p90 | 6,354 / 25,162 m |
| Raw >500 m | 65.11% |
| Retrieval R@1/5/10/20/50/100 within 100 m | 29.83 / 38.88 / 42.21 / 45.81 / 50.60 / 54.59% |
| First positive rank median / p75 / p90 | 43 / 1,184 / 5,082 |
| Frozen product answer rate | 70/751, 9.32% |
| Frozen product conditional <=100 m | 68/70, 97.14% |
| Accepted >500 m | 1/70, 1.43% |
| Accepted median / p90 | 20.64 / 56.96 m |

A five-fold H3 resolution-6 grouped experiment ranks geographic modes using
inference-available features. Labels are constructed **after** feature
extraction. Logistic mode selection gains only **+0.13 pp**, with a paired
geographic-bootstrap 95% interval **[-0.94, +1.43] pp**. Three boosted-tree
variants range from -0.40 to 0 pp. A top-50 mode oracle could reach 49.67%,
but that oracle has ground truth and is not a deployable candidate. These
results argue against spending this phase merely retuning abstention or
handwritten aggregation.

Query-only crop/orientation experiments also fail the material gate. Against
original SAGE top-1 at 29.83%, center crop reaches 28.36%, upper-image crop
28.89%, original/upper max 29.43%, original/center mean 28.63%, rotation max
18.64%, and three-neighbor query expansion 29.96%. Original preprocessing was
recomputed against the complete gallery and reproduces historical R@1 and
R@100. A visually inspected development failure was upside down, but the
all-query rotation experiment refuted the tempting general fix.

Reference diversity has a strong association with development difficulty:

| Nearest-positive stratum | Queries | Raw <=100 m | R@50 <=100 m |
|---|---:|---:|---:|
| One gallery reference within 100 m | 249 | 16.47% | 31.33% |
| At least five within 100 m | 177 | 43.50% | 73.45% |
| Heading difference <45 degrees | 389 | 48.33% | 74.29% |
| Heading difference >=90 degrees | 258 | 6.59% | 20.93% |

These associations do not establish causation; matched-gallery experiments are
still required. All old benchmark queries were required to have a positive,
which concealed the no-reference portion of the real task.

## New data and independence

An initial inventory found 3,634 downloaded images on previously unused
Mapillary sequences. Exact administrative-polygon filtering reduced that
physical pool to **23**: most were outside Moscow, rather than unused clean
in-city test photos. Metadata-only sources contain 4,331 unseen in-AOI
Mapillary sequences and 104 unseen KartaView sequences. We are therefore
performing fresh official Mapillary discovery across the entire administrative
polygon, with increased spatial quotas, deterministic seed 20260905, and
separate output/checkpoint files. Discovery completed with 625,405 candidate records and 40,000 spatially balanced hydrated records. The immutable discovery JSON has SHA-256 `7795df76e6807b52b10439c79235d93cccc79a7160946fa1b053844b7f0f4423`. These counts are neither downloaded
references nor independent test examples.

Prospective assignment reserves whole provider sequences for either reference
acquisition (60%) or query sampling (40%). Query splits use hashed H3-7 blocks,
explicit 250 m cross-split embargo, 30 m within-query spacing, one photo per
sequence, and a further 250 m exclusion around the historical v4 development
queries for new calibration/final. Assignment targets 1,200 development,
800 calibration and 1,200 final. No nearby-positive, model score, confidence,
or image-quality threshold selects query difficulty. Failed downloads and
exact/near duplicates still require audit before a benchmark can be sealed.
Reference acquisition cannot use private query coordinates or query sequences.

Provider imagery remains a proxy for user-uploaded photography. Sequence
holdout is not automatically photographer/session holdout; this must be audited
from available metadata, and missing provenance disclosed. The existing MSLS
Moscow archive is not a clean final-test shortcut: upstream model-training
contamination and its separate dataset license require clearance.

## Models and reproducibility

- [Official EDTformer](https://github.com/Tong-Jin01/EDTformer), source revision
  `d0b85da3475e776b7baf87fbab5d5bfa6b50d723`. Its official release checkpoint
  is 471,004,018 bytes, SHA-256
  `57406a40806e4effb9320b593f0eeda02e493cf3aaf3b37d9cb152112bf1154d`,
  agreeing with GitHub's asset digest. Official normalize-then-resize 322
  preprocessing, 4,096-D normalized descriptors. CPU smoke passed independent
  image/batch/order invariance (maximum component difference 5.82e-8), and
  seven CPU images took 1.90 s after load. Full-gallery MPS extraction completed on all 20,487 references, with zero inference failures. Code is MIT, bundled DINOv2 portions carry
  Apache notices, and the release states no separate checkpoint license:
  record that distinction before production selection. Loading keeps
  `weights_only=True` with a narrow NumPy compatibility allowlist.
- [BoQ](https://github.com/amaralibey/Bag-of-Queries) is audited as a further
  independent aggregation architecture; both development cohorts and full-gallery
  fusion/localization experiments are complete, with results below.
- [D2VPR](https://github.com/tony19980810/D2VPR) exposes a no-cross-image model,
  but the audited root did not expose a LICENSE file. Production rights need
  resolution; public weights alone are not sufficient.
- [XFeat](https://github.com/verlab/accelerated_features), official revision
  `e92685f57f8318b18725c5c8c0bd28c7fe188d9a`, Apache-2.0 code. Repository-distributed
  weights have SHA-256 `0f5187fd7bedd26c7fe6acc9685444493a165a35ecc087b33c2db3627f3ea10b`.
  All 751 development queries were verified against 20 SAGE candidates using
  up to 1,024 keypoints at a 640-pixel maximum image dimension, mutual cosine
  matching, and robust homography. Score blending and grouped learned reranking
  failed to improve raw accuracy. This does not establish that all local matching
  architectures fail: the candidate depth and sparse matcher remain limitations.
- Official SAGE ViT-L uses the same pinned official source and Hugging Face revision
  as production, with `SAGE_No-Encoder_Vit-L.pth` SHA-256
  `31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc`.
  Full original and expanded gallery extraction is complete, with results below.
- BoQ DINOv2 official release weights have SHA-256
  `d72ee0ce899e2790be6cf6d1d57f75dfad0c76475bd2198e6dc0c3f0221b3583` (380,916,283 bytes).
  The official source is pinned to `1a4965ea7dfd9bd0dd846adf7a0e430f68101d12`;
  its otherwise floating DINOv2 dependency is pinned to
  `7764ea0f912e53c92e82eb78a2a1631e92725fc8`. The full release state dict loads
  strictly, so a redundant pretrained-backbone download is avoided. Official
  tensor/bicubic/322/normalization preprocessing is retained. CPU independent
  image, batch and ordering smoke passed (maximum difference 1.01e-7; seven
  images in 1.37 s after load). Code is MIT; no separate release checkpoint
  license statement was located. Production clearance remains required.

## Reproduction and remaining work

Use the existing Python 3.12 `.venv`; current host has MPS, 16 GB unified RAM,
and roughly 15 GB free disk. Large artifacts remain ignored and separate.

```sh
.venv/bin/python -m ml.research.mode_experiment
.venv/bin/python -m ml.research.query_views
.venv/bin/python -m ml.research.edtformer
.venv/bin/python -m ml.research.embed_edtformer \
  --manifest data/evaluation/moscow_real_v4/gallery.parquet \
  --output data/embeddings/moscow_research_v5/edtformer
.venv/bin/pytest ml/research/tests -q
```

Pending: complete acquisition and provenance/duplicate audit; physically seal
new calibration/final; evaluate EDTformer and complementary descriptor/rank
fusion; learned local verification; expanded-gallery and matched-budget
sampling ablations; development-only adaptation if evidence warrants; objective
candidate selection; architecture freeze; new calibration; policy freeze; one
paired untouched final evaluation; production artifacts only after a real win.
The goal remains active.

## Additional completed development experiments (2026-09-06)

EDTformer alone reaches 24.63% raw top-1 versus SAGE's 29.83%. Cosine fusion
with 25% EDT reaches 30.23% (+0.40 pp, paired geographic interval [-1.01, 1.80]).
With the complete production geographic localizer, SAGE reproduces 29.69%
and its exact 70 accepted queries; that fusion reaches 29.96%, only +0.27 pp.
Reciprocal-rank fusion increases R@100 to 57.26% but reduces top-1 to 26.90%.
Retrieval recall improvement alone is therefore not a localization win.

Database augmentation uses reference descriptors only, with cubic cosine
weights and either unrestricted visual neighbors or a 100 m reference-GPS
constraint. Top-1 results are 29.96% (three visual neighbors), 29.43% (ten),
30.09% (three spatially constrained neighbors), and 29.96% (ten). Combining
visual augmentation with query expansion falls to 27.03% or 26.50%. None
meets the preregistered material-improvement gate. Twelve supervised query-to-positive linear descriptor adaptation settings also failed: the best reached 29.69% top-1, with a paired interval spanning zero improvement. Targets used training-fold queries only; evaluation used five geographically held-out folds.

Development product curves use grouped out-of-fold confidence predictions.
Their thresholds are exploratory, selected on those development outcomes,
not a valid replacement for new calibration or final performance. For example,
refitting baseline confidence can show 17.84% development coverage at 90.30%
conditional accuracy without changing a single raw coordinate. That is why
coverage and raw localization remain separate objectives.

New acquisition downloaded all 3,200 assigned queries and 10,964 of 10,965
planned reference-union images. The two reference arms each initially contain
6,000 candidates, with overlap; each will be independently deduplicated and
trimmed to the same surviving count. One reference returned HTTP 400. A prior
transient CDN outage was retried with the identical manifest and its original
failure evidence retained. These are not 625,000 downloaded photographs.

Storage is now the immediate resource constraint: approximately 6–8 GB free.
The user has been asked for 40–50 GB more space or an external destination;
no manual dataset download is needed for the current tranche. Models and
research descriptors remain separate from frozen production artifacts.

Further architecture work remains open: official SAGE full cross-image encoder
could be evaluated as a per-query reranker over retrieved references, with
strictly query-independent first-stage retrieval. It must not be treated as an
independent-image FAISS encoder or allowed to mix separate evaluation queries.
The official repository explicitly documents dependence on batch composition.
That contract limits deployment design but does not rule out a valid reranker.

Reclaimed 2.25 GiB from the completed XFeat per-image scratch feature cache.
All 751 per-query pairwise feature records, result summaries, source images,
checkpoint, and code remain; regeneration is possible. A cache-eviction receipt
is stored alongside the experimental results. No production artifacts were removed.

## External storage and new development evidence (2026-09-06, resumed)

The user supplied the external `mac os` folder. Its verified research destination
is recorded in the ignored `storage.json`; roughly 1.5 TiB is available. Writes,
reads and rename passed a byte-for-byte probe. Large gallery transfer verifies
all files before switching the original research path to a symlink. Existing
production and private held-out query storage are not moved. BoQ descriptors
are already being computed onto the external destination.

The benchmark seal is `b3acb88364fea263148e8e501735f9912a4464bdce43b17cffb961be6e26a78c`:
1,184 development, 792 calibration, 1,177 final queries. Query duplicate exclusions
include rotations and mirrors. The audit retained 10,804 available reference
images before within-arm duplicate removal. Both matched arms add exactly
5,692 images; the larger union is a separate deployment option, not a matched
budget comparison. Calibration and final ledgers are absent.

**Provenance limitation identified before new development inference:** 97.88%
of final queries share an attribution group with development, despite image,
sequence and geographic exclusions. Only 31/3,200 assigned query photos are
from attribution groups absent all historical sets. Thus the present sealed
cohort establishes new-photo/sequence/area generalization within these providers;
it does not establish unseen-photographer generalization. The protocol's
photographer-holdout aspiration has not been satisfied, and this must not be
hidden by calling the benchmark fully independent. Preserve the existing seal;
seek a separately assigned and sealed author-disjoint cohort before claiming
that stronger scope. Commons camera-geocoded photographs are under metadata-only
feasibility investigation; no Commons image has been downloaded or inferred.
Object-location coordinates alone cannot serve as camera-position ground truth.

On historical development, SAGE ViT-L raises top-1 <=100 m from 29.83% to 34.09%
(+4.26 pp, geographic-bootstrap interval [2.28, 6.28]). With the complete
geographic localizer, raw <=100 m rises from 29.69% to 34.22%, while >500 m
falls from 65.11% to 58.59%. This is a substantial architectural signal, although
it is below the preregistered +5 pp final promotion threshold on this development
comparison. New-development replication and complementary models remain necessary.

The new development queries deliberately include coverage failures. Baseline
SAGE top-1 <=25/50/100 m is 5.74/9.80/13.85%, median error 13,423 m, p90 36,306 m,
and >500 m 81.42%. R@1/5/10/20/50/100 is
13.85/18.75/20.02/22.47/26.27/29.39%. **537/1,184 (45.35%) have no reference
within 100 m.** Among covered queries, first-positive median rank is 64;
all-query median rank is 8,666 and upper quantiles are infinite. This exposes
a coverage failure hidden by the old positive-required query sampling.

Reference coverage on those identical public development queries:

| Gallery | Images | Queries with a reference within 100 m |
|---|---:|---:|
| Production | 20,487 | 54.65% |
| Matched random addition | 26,179 | 60.64% |
| Matched diversity addition | 26,179 | 72.80% |

These are geometric support measurements, not claimed localization accuracy.
The actual descriptor retrieval and geographic localization ablation is reported below.

## Completed gallery ablation and architecture checks (2026-09-06)

All results in this section are development results. Calibration and final remain
unopened; no architecture or threshold has been frozen and there is no production
promotion. The external gallery transfer verified all 10,972 content files before
removing its local duplicate. Writable memory mapping on the NTFS/FUSE volume was
too slow: embedding jobs now use the internal SSD for working matrices and copy
committed artifacts sequentially to the external volume with SHA-256 verification.
BoQ's 20,487 descriptors and SAGE-B's 10,804 new-reference descriptors were recovered
and verified with zero extraction failures. SAGE-L resumed from 1,008 verified
reference descriptors. The updated storage command preserves this arrangement.

The exact frozen FAISS index was also rerun on all 1,184 new development queries.
Compared with the earlier NumPy calculation, every top-1 identity agreed; seven
top-30 and thirty top-100 orderings differed due to floating-point differences
(maximum score difference 2.62e-6). Crucially, every final geographic coordinate
and error remained identical. Reports under `localized_exact_faiss_baseline_new`
use the actual production index, not an assumed equivalent replacement.

New development, unchanged SAGE-B and geographic aggregation:

| Gallery | Images | Raw <=25 m | <=50 m | <=100 m | Median m | p90 m | >500 m |
|---|---:|---:|---:|---:|---:|---:|---:|
| Production | 20,487 | 5.57% | 9.63% | 13.77% | 12,885 | 35,266 | 81.08% |
| Random addition | 26,179 | 6.76% | 11.91% | 16.89% | 10,770 | 33,152 | 77.20% |
| Diversity addition | 26,179 | 7.09% | 12.33% | 17.74% | 9,861 | 32,860 | 74.49% |
| Larger union | 31,084 | 8.02% | 14.02% | 20.02% | 9,127 | 32,979 | 72.55% |

The union adds 10,597 independently deduplicated references. Its raw <=100 m gain
is +6.25 pp, paired geographic-bootstrap 95% interval [4.55, 8.05] pp. Of 82 recovered
queries, 55 had no nearby reference in production and 27 were already covered;
eight previously correct queries regress. There remain 306/1,184 queries without
any reference within 100 m. Thus missing coverage and recognition both limit the
system. This is not a claim that simply adding arbitrary references always helps:
on historical positive-required development, the diversity arm drops from 29.69%
to 28.89%, random reaches 31.69%, and the larger union reaches 31.56%.

New-development retrieval recall, all-query denominator:

| Gallery | R@1 | R@5 | R@10 | R@20 | R@50 | R@100 |
|---|---:|---:|---:|---:|---:|---:|
| Production | 13.85% | 18.75% | 20.02% | 22.47% | 26.27% | 29.39% |
| Random | 17.23% | 23.23% | 25.17% | 27.03% | 30.91% | 33.61% |
| Diversity | 17.57% | 23.40% | 25.34% | 28.21% | 33.95% | 37.84% |
| Union | 20.35% | 27.28% | 29.73% | 32.18% | 37.42% | 40.79% |

Union first-positive bins (rank 1; 2–5; 6–10; 11–20; 21–50; 51–100; 101–500;
501–1,000; 1,001–31,084; absent) contain
241; 82; 29; 29; 62; 40; 100; 49; 246; 306 queries respectively. Covered-query
median rank is 49.5; all-query median is 556.5, with infinite upper quartile.
Complete row-level ranks and both conditional and unconditional quantiles are saved.

The unchanged production confidence policy answers 57/1,184 (4.81%) new queries,
but only 46/57 (80.70%) are within 100 m; accepted >500 m is 2/57 (3.51%), median
24.32 m and p90 171.52 m. It does not meet the requested product precision on this
distribution. Development OOF confidence with an exploratory threshold reaches
31/1,184 answers at 90.32% for baseline, versus 78/1,184 (6.59%) at 91.03% for the
union, accepted >500 m 1/78, median 22.99 m and p90 80.39 m. These optimized
development thresholds are not calibrated deployment operating points.

SAGE-L on the original gallery raises new-development full-localizer raw <=100 m
to 16.39% (+2.62 pp, paired interval [1.14, 4.32]). Equal cosine fusion of B and L
reaches 16.72% (+2.96 pp, interval [1.74, 4.44]). The factorial combination of L
and the added references is now complete; results follow below. Official full SAGE's contextual
encoder was evaluated in a separate per-query reranker experiment with independently
re-extracted pre-encoder features; completed results follow below. Among shared tensors, 42 differ from the independent
L checkpoint, so reusing L gallery descriptors would be invalid.

BoQ was evaluated on both open development cohorts. It reaches 25.83% historical
and 10.90% new raw top-1, below SAGE-B's 29.83% and 13.85%. Its best tested fusion
with B is still below L; full geographic fusion reaches at most 30.63% historical
and 14.61% new. Code is MIT; separate release-weight terms remain unresolved for
production. XFeat reranking all top 100 candidates also fails: the best grouped
learned reranker reaches 29.69% historical top-1 versus 29.83%, interval for the
gain [-0.82, 0.54] pp. Merely prioritizing geometrically verified inliers performs
substantially worse. All negative trials and their artifacts are retained.

A new reference-only metric-learning experiment uses 4,096 area-balanced anchors
from 4,831 eligible reference images, different-sequence positives within 25 m
(heading difference <=60 degrees when known), and eight visually hard negatives
at least 200 m away. A rank-64 residual adapter starts at the identity; no query
enters training or mining. Three training lengths and three residual strengths
were evaluated. Best new top-1 reaches 16.39% versus unadapted L's 16.05%
(+0.34 pp, exploratory interval [0.08, 0.69]), while that setting falls to 33.69%
from 34.09% historical. Stronger adaptation usually regresses. This does not yet
justify its deployment complexity. A native OpenMP conflict encountered in the
training-mining test was resolved by using exact BLAS cosine mining; production
FAISS was not changed.

Commons metadata discovery finished: 28,383 geographic candidates, a fixed
area-balanced cap of 10,000 hydrated records, no downloaded images or model
inference. The camera/EXIF/date/identity/license feasibility audit retains 900
records, excludes 544 from a known photographer already in Mapillary, and detects
89 camera-template/EXIF disagreements over 25 m. The 900 remain a candidate pool,
not a test: 523 share one author, further aliases and street content need audit,
and matching EXIF/template coordinates do not independently verify GPS accuracy.
The camera/object distinction follows the official
[Commons location documentation](https://commons.wikimedia.org/wiki/Template:Location);
the cross-provider alias is supported by the photographer's
[public Commons profile](https://commons.wikimedia.org/wiki/User:Svetlov_Artem).

Remaining: finish the CPU runtime preflight for the leading hybrid, resolve
benchmark scope limitations honestly; objectively choose one candidate; freeze
architecture, calibrate only its threshold, freeze both policies, and perform the
single paired untouched final evaluation. No winning final result exists yet.

Additional checks: the official SAGE descriptor concatenates a 256-dimensional
scene token followed by an 8,192-dimensional pooled branch. Measured scene-token
squared norm is 1/65. Seven branch weights on new development do not produce a
material gain over L: removing the token reaches 16.22% top-1 (+0.17 pp, interval
[-0.45, 0.73]); increasing its weight to 0.1 drops accuracy to 14.78%. The explicit
1/65 branch mixture reproduces the original result. The descriptor's 11 chunks of
768 used by the official context encoder are therefore not 11 semantic scene tokens.

Confidence-family evaluation now also separates an exploratory OOF threshold
envelope from a stricter nested operating-point evaluation. On B plus the union,
outer geographic folds use hyperparameters and thresholds selected exclusively
inside their training folds. Logistic with 14 features answers 93/1,184 at 89.25%
conditional accuracy; logistic with all features answers 79 at 88.61%; histogram
boosting with 14 features answers 60 at 85.00%; boosting with all features answers
61 (5.15%) at 90.16%, zero accepted >500 m, median 22.22 m and p90 81.04 m. These
are development estimates, not a replacement for the sealed calibration stage.
Both logistic and boosted models export to portable JSON; predictions match the
training implementation to floating-point precision, including decision-boundary
tests. Raw coordinates remain identical throughout this confidence comparison.

A deployment constraint was reproduced on this macOS environment: PyTorch 2.13
CPU operations followed by FAISS 1.15 search abort due to duplicate OpenMP runtimes,
even with one FAISS thread. The unsupported duplicate-library override was not
enabled. The exact baseline evaluation uses separate extraction and FAISS scoring
processes. A new deployment must pass a target-runtime smoke or use an explicitly
verified process separation/exact BLAS backend; current production files remain
unchanged. This constraint is independent of retrieval quality.

L plus the larger union reaches 23.73% raw <=100 m on all 1,184 new development
queries, versus production's 13.77%. Its paired geographic gain is +9.97 pp,
95% interval [7.57, 12.50]. Raw <=25/50 m is 9.80%/17.31%, median error 8,347 m,
p90 31,913 m and catastrophic >500 m 69.51%. L on matched random and diversity
arms reaches 19.43% and 20.95%; the same L/union combination improves historical
development from production's 29.69% to 37.02%. Thus both data and architecture
contribute, while most new queries still have large errors.

L/union retrieval R@1/5/10/20/50/100 is
23.82%/30.57%/33.19%/35.98%/40.12%/43.75%. First-positive bins use the same
boundaries as above: 282; 80; 31; 33; 49; 43; 91; 49; 220; 306 absent.
Covered-query median rank is 26 and all-query median is 424.

Equal B/L cosine fusion on this union with top-1 coordinates was the next
new-development raw leader: <=25/50/100 m 9.71%/17.48%/24.24%, catastrophic
>500 m 68.16%, median error 6,803 m and p90 31,965 m. Its paired gain over exact
production is +10.47 pp, geographic 95% interval [8.25, 12.75]. Retrieval
R@1/5/10/20/50/100 is 24.24%/31.08%/33.28%/36.66%/40.71%/44.76%; first-positive
bins are 287; 81; 26; 40; 48; 48; 93; 42; 213; 306 absent, with covered median
rank 23 and all-query median 294.5. Geographic aggregation reaches 23.82% on the same scores;
75%-L fusion reaches 23.99% top-1 and 23.90% geographic. These are still
exploratory winners, not a frozen production candidate or an untouched-test result.

Stricter nested confidence evaluation remains below the desired operating point
for this current raw leader: logistic with 14/all features accepts 115/122 queries
at 86.96%/88.52% conditional <=100 m; boosted models with 14/all features accept
99/136 at 88.89%/86.76%. Threshold selection on the outer validation labels is
forbidden. The independent calibration stage must select only the numeric threshold
after the architecture and confidence family have been frozen; final performance
may still fail the promotion gate.

The official SAGE paper states training on MSLS non-panoramic training imagery
and GSV-Cities ([implementation details](https://arxiv.org/html/2509.25723v2)).
New acquisition from Mapillary does not by itself establish unseen pretraining
content, and most primary v5 authors also appear in public development. The pinned
[checkpoint model card](https://huggingface.co/shunpeng/SAGE/raw/2a2ea9964cdbdfd2211e7c625064a9d5e4678245/README.md)
explicitly declares MIT; its local audited copy and hash are retained separately.
Foundation-model pretraining exposure remains unknown.

A separate 2024-and-later Commons camera-photo cohort is being prepared to stress
temporal/source transfer. The initial 10,000-record metadata snapshot yielded only
95 candidates after street-exterior metadata, author/day caps and the 250 m public
development embargo. These controls were retained; hydration was extended to the
already discovered 28,383-record population, preserving the original snapshots.
Selection uses neither model scores nor gallery coverage. This supplementary cohort
will have its own seal and separate result, the same single frozen architecture and
unchanged primary-calibration threshold. A joint cohort registry must be bound by
the architecture freeze before calibration; neither population can be exchanged
after opening. This is a procedural safeguard, not OS-level isolation.

The staged offline runtime now reproduces both exact-production and leading
fusion coordinates on every one of the 1,184 new development queries, with zero
error-distance difference. Encoding and FAISS scoring run in different processes.
The public-cache replay takes about 10 seconds for baseline search/localization
and 13 seconds for the candidate on this machine, excluding image encoding.
Fresh CPU inference with network connections disabled also passes batch/order
independence: eight images take 2.05 seconds for B and 5.32 seconds for L after
loading. CPU/MPS descriptor differences are reported explicitly: maximum L2
distance 4.77e-5 for B and 1.27e-5 for L. An initial per-component tolerance check
failed at B's 1.56e-5 component difference; the retained diagnostic establishes
that the unit-vector displacement is small. Within-device invariance and
cross-device descriptor displacement are checked separately. These are preflight
artifacts, not a frozen architecture or a production release.

The new fixed-policy evaluator retains failed encodings as raw failures, records
complete positive ranks and spatial-positive counts independently, and has no
final-set threshold-search branch. Both cohort seals must be registered before
architecture freeze. Calibration can change only the candidate's numeric
threshold; changing its retrieval, coordinate or confidence model blocks final
access. Thirty-six research tests currently pass, including joint-seal tampering,
one-shot opening, post-calibration architecture mutation and failed-query metric
denominators. Actual calibration and final ledgers remain absent.

The 31,084-reference union retains nonempty attribution and source URLs for every
row: 22,321 Mapillary and 8,763 KartaView references, all recorded as CC BY-SA 4.0.
The official [Mapillary license help](https://help.mapillary.com/hc/en-us/articles/115001770409-CC-BY-SA-license-for-open-data)
and [KartaView terms](https://kartaview.org/terms) support imagery reuse subject to
attribution/share-alike obligations; KartaView also requires its provider credit.
The [CC BY-SA 4.0 terms](https://creativecommons.org/licenses/by-sa/4.0/) require
credit, license links and modification notices for shared derivatives. SAGE's MIT
notice, pinned checkpoint model card, upstream DINOv2 Apache license, package
license metadata and imagery provenance checks are retained for the eventual
candidate-specific deployment audit.

The independent full-SAGE experiment is complete: all 20,487 original-gallery
references were extracted without failures using the actual full checkpoint.
Its pre-encoder raw top-1 is 26.10% historical and 12.58% new development; official
contextual reranking reaches at most 28.10% and 12.84%. Its best B fusion with
geographic localization reaches 31.82% historical and 14.61% new. This fails to
replace the stronger no-encoder L representation. A further 54-setting sweep of
ordinary B/L-union geographic aggregation peaks at 24.41%, only +0.17 pp over
top-1 with interval [-0.19, 0.60]; it does not explain a material additional gain.

A distinct hybrid does improve: feed no-encoder L descriptors into the full
checkpoint's contextual encoder, using one query plus the top 30 B/L-union
references, and blend its aligned scores equally with the original cosine scores.
This is explicitly an experimental transfer, not an official full-SAGE reproduction.
New-development raw <=25/50/100 m becomes 10.30%/18.07%/25.17%, median 6,474 m,
p90 31,317 m, catastrophic >500 m 66.64%. The gain over plain B/L union is
+0.93 pp, geographic interval [0.27, 1.61]. Historical development also increases
from 37.42% to 38.08%. Context depth 100 and other tested mixture weights do not
beat this new-development result; ordinary geographic aggregation is lower.

Context changes similarity statistics, so confidence was retrained for the actual
new coordinates. Keeping evidence both before and after reranking, plus four
inference-only agreement/movement features, gives 94 selected features. A further
auxiliary-training experiment uses the 751 historical development queries, while
excluding all auxiliary samples belonging to each outer and inner validation
geographic group. Hyperparameter comparisons and operating thresholds still use
only new-development validation outcomes. The best qualifying nested operating
point is boosted confidence on the combined features: 125/1,184 answers (10.56%),
113/125 within 100 m (90.40%), zero accepted >500 m, median 25.85 m and p90 90.11 m.
Its separate exploratory pooled-OOF threshold envelope is 154 answers (13.01%) at
90.26%, with two accepted >500 m. Neither threshold is a calibrated final policy.
Raw coordinates are unchanged by all confidence experiments. The auxiliary
geographic exclusion has a targeted leakage test; 36 research tests pass.

The hybrid passed an independent replay on all 1,184 development queries: every
coordinate/error matches the original development experiment, and all portable
confidence probabilities match exactly (maximum feature difference 4.48e-7).
Fresh offline MPS encoder smoke also passed. Full CPU query inference is now being
checked because the existing deployment targets a CPU server. CPU and MPS may
differ numerically; any outcome changes will be reported before freezing. The primary 792-query calibration and 1,177-query final manifests
remain sealed. Supplementary Commons metadata hydration resumed after a remote
connection interruption, using cached completed records and bounded retries.
No final winner or production promotion has been declared.

The final development selection is now recorded in
`data/evaluation/moscow_research_v5/candidate_reports/selection.json`, with an
index binding 85 experiment summaries (423 raw metric entries, including repeated
views of identical coordinate outcomes; this is not a count of independent trials).
The chosen CPU policy is `sage_bl_union_hybrid_context30_mix05`, policy SHA-256
`1aec1138689478ea58010380849f6c8f8f1703e0c164b69dfc6787eb89352354`.
Full fresh CPU encoding completed all 1,184 development images without failures.
Both baseline and candidate retain exactly every MPS coordinate/error. Small
similarity changes alter 9/18 complete positive ranks for baseline/candidate;
confidence is not bitwise invariant: 15 candidate probabilities change, maximum
absolute difference 0.05196, due to discrete tree decisions. Calibration and final
will therefore both use the fixed CPU runtime. No candidate product point is
claimed from its null-threshold preflight output.

The candidate CPU scoring process (exact retrieval, context, localization,
confidence, complete ranks and reporting) took 28.28 seconds for all 1,184 queries
and reached 3.23 GB maximum RSS on this macOS arm64 host. Encoding is separate:
SAGE-B took approximately 267 seconds and SAGE-L 610 seconds with batches of four
and two CPU threads. These batch totals are not user-facing latency or a Linux
server SLO. An eventual production adapter/container must pass target runtime
checks if the untouched final qualifies; the existing Docker daemon is stopped.
The candidate-specific licensing and deployment preflight receipts are complete.
Architecture freeze waits only for the separately acquired recent-camera cohort
and its joint registry; neither primary held-out split has been opened yet.

Supplementary discovery and provenance audit are complete: all 28,383 metadata
records were hydrated; 2,054 pass the original date/camera/EXIF/license/author
criteria. The fixed street-exterior/author/day/spacing/development-embargo plan
assigns 170 photos from 31 author groups. Original-file acquisition encountered
repeated HTTP 429 responses with Retry-After 600. Transport wrappers retain every
assigned identity, existing file and original image audit, respect each full
cooldown, and add a truthful project-contact User-Agent, a 2 MiB/s streaming cap
and ten-second minimum request interval. These implement the official
[Wikimedia media robot policy](https://wikitech.wikimedia.org/wiki/Robot_policy)
and [client identification policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy).
No browser impersonation or alternate host is used. The research source/runtime
and selected policy are unchanged; all lifecycle waiters were stopped before
restarting the downloader and reattached before any calibration/final opening.
The five earliest original JPEGs also passed an automatic actual-EXIF check: GPS
and DateTimeOriginal present, coordinates exactly equal to recorded metadata.
AppleDouble resource-fork files on the external volume are not photographs and
are excluded from progress counts. No final image has been displayed or inferred.

The supplementary cohort is now sealed at 158 queries. All 170 fixed originals
were attempted; 12 were excluded by the predeclared 20 MiB download cap (each has
a 20 MiB partial file and recorded ValueError). There were zero duplicate exclusions;
early progress counts alone must not be interpreted as duplicate counts. No
replacement sampling occurred. The supplementary seal is
`b9385289184992994ce2d8f3761b9994cb9d1ea14e80c1ce49d0c31af1210842`.
The joint registry is
`c339bc1488917d93c23e1a28bd869d3850078f64c044ffee0f3791b56aa6d6dd`.
Architecture freeze
`871f51abdc794ee76288645fd9cc234b165cacae1f249b28b4bca78366f0e64b`
binds 254 artifacts, including both concrete policies, code/runtime, weights,
reference descriptors, learned confidence parameters and selection/license/runtime
reports. Calibration was claimed at 2026-09-06T18:23:28Z. Only its numeric threshold
may now change; the single paired final transaction follows automatically after
full policy freeze. Production and both final query populations remain unchanged.

Calibration completed with threshold `0.6797796606749346`: 152/792 answers
(19.19%), 137/152 within 100 m (90.13%), four accepted >500 m (2.63%), accepted
median 31.45 m and p90 98.97 m. This is the maximum empirical calibration point
under the fixed rule, not a guarantee of final conditional accuracy. The full
policy freeze is
`b49eff7e591d70727308a4b0692e295aaf4aa50de6c61b2eb3c9f2f9b4ddc480`;
the concrete candidate policy is
`405b5ea9123e521d9c5c8716d2435c2ba0c50aabacd538a5045392b2985454fd`.
Primary final was claimed at 2026-09-06T18:35:07Z, supplementary final at
18:35:12Z, both before either final inference began. Their manifest hashes are
`53f591612710fbc646d1a0668cba51d3f517e98718d6c19c7bf7b89df52c29f7`
and `ac91a3136177f1bdae88767eb30739734bd9e019b0b0a01e8c11a1bb276f8e00`.
No architecture, confidence fitting, threshold rule or query-population change is
permitted after these claims. The final report will preserve all queries and
report the recent-source cohort separately at the unchanged primary threshold.
