# Part 3 handoff

Part 2 is complete. The localization core is frozen under Outcome B: **keep
the exact Part 2.5 policy on the v4 gallery/index**. The authoritative record is
`docs/final_localization_core.md`; the only production runtime configuration is
`configs/moscow_production_frozen.json` and its required SHA-256 sidecar.

## DO NOT CHANGE

- SAGE ViT-B retriever without cross-image encoder.
- Repository revision `c7d6241c4885526d99d6c78c158024fc2a37097c`.
- Checkpoint revision `2a2ea9964cdbdfd2211e7c625064a9d5e4678245`
  and checkpoint SHA-256
  `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e`.
- Official normalization/resize-322 preprocessing and normalized 8,448-D
  float32 descriptors.
- V4 gallery manifest and 20,487-reference exact `faiss.IndexFlatIP`.
- K=30, single-query retrieval, no reranker.
- Density-aware geographic-mode voting, sequence-deduplicated support,
  rank decay, compact-mode constraints and weighted-medoid coordinate.
- Part 2.5 14-feature standardized L2 logistic confidence artifact, SHA-256
  `5afee6b1a1abea5c2701bc77e05d08a00b7e0a0657985567146e1de82afe5f82`.
- Confidence threshold `0.9349250249145314`; no separate catastrophic veto,
  no explicit similarity/mass/count gates, no approximate tier.
- Status policy: `ok` only above the frozen score threshold;
  `low_confidence` for a retrieved but untrusted location;
  `out_of_coverage` only when no usable retrieval evidence exists.
- V4 final claim: 127/1,499 answers (8.47%, bootstrap 95% 7.07–9.94%),
  123/127 <=100 m (96.85%, Wilson 92.18–98.77%), 8.21% all-query success,
  4 accepted >100 m and 3 accepted >500 m.
- The sealed v4 test and all v1/v2/v3 tests. They are permanently opened and
  may not be rerun or used for tuning.

Part 3 must use `make verify-moscow-production` after artifact placement. Do
not substitute an experimental v4 confidence config, MegaLoc, SALAD,
SelaVPR++, CricaVPR, mock index, random weights, or silent CPU/model fallback.
Incompatibility is a readiness failure.

## PART 3 MAY CHANGE

- Deployment artifact download, storage and authenticated distribution.
- Container/image size, layer caching and clean image rebuild.
- Process packaging and service supervision.
- Reverse proxy, TLS and public routing.
- Rate limiting, abuse protection and request budgets.
- Monitoring, metrics, traces, operational logs and alerts.
- Production tile provider or self-hosted tiles with attribution.
- Secrets management and environment-specific deployment configuration that
  does not override the frozen localization contract.
- CI/CD and release/SBOM/license-notice generation.
- Privacy/security hardening, retention controls and incident procedures.
- Frontend polish and final real-browser accessibility/responsiveness QA.

The stale local `geosnap-backend:latest` image is about 17.5 GB and predates the
final runtime. The local Docker daemon was unavailable at handoff, so Part 3
must perform the clean functional build before slimming. Dockerfile dependency
declarations and the lockfile are ready; Compose statically resolves the
production config and v4 SAGE index.

Part 3 is deployment/productionization only. Future ML improvements are
post-MVP research and must not reopen the pre-production localization phase.
