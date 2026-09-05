# Moscow Wikimedia Commons evaluation proxy

`moscow_commons_landmarks.json` defines a small set of geotagged Wikimedia
Commons file page IDs around four Moscow landmarks. The first three page IDs
per landmark form the gallery and the final two form held-out queries.
`moscow_commons_snapshot_ledger.json` is the tracked, content-addressed snapshot
contract: for all 20 pages it pins the Commons source SHA-1, exact downloaded
JPEG SHA-256, coordinates, file-page URL, author/credit, and license.

Acquisition re-fetches metadata from the official Commons API and fails closed
if source identity, coordinates, or attribution/licensing differ from the
ledger. `--reuse-existing` reuses a cached JPEG only when its SHA-256 matches
the pin; a stale cache is downloaded again and the new bytes must still match
the pin before replacement. Updating this snapshot is therefore an explicit
reviewed ledger change, never a silent side effect of rerunning acquisition.

This is an **evaluation-only, tiny, hand-curated, landmark-biased proxy**. It is
not street-view imagery, not a representative Moscow sample, not a deployable
gallery, and not evidence of product coverage or city-wide accuracy. Images,
snapshot manifests, attributions, descriptors, and benchmark reports are
generated under `data/evaluation/generated/` and intentionally ignored by Git.

Generated manifests include `created_at`, so their file SHA-256 is intentionally
run-specific. `canonical_dataset_sha256` is instead computed from the tracked
split/config and pinned content/provenance fields; it excludes acquisition time,
output paths, and other machine-local details. A clean checkout can validate
the complete snapshot identity from the two tracked JSON files before any
ignored images exist.

Each generated manifest preserves the Commons file page URL, author and raw
author markup, exact license label and URL, attribution fields, coordinates,
original/upload timestamps when available, Commons SHA-1, and downloaded-byte
SHA-256. Reusers remain responsible for checking and complying with each file's
current license on its linked description page.

Official references used by the acquisition path:

- [MediaWiki Action API: Imageinfo](https://www.mediawiki.org/wiki/API:Imageinfo)
- [MediaWiki Action API: Geosearch](https://www.mediawiki.org/wiki/API:Geosearch)
- [Wikimedia Foundation User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy)
- [Commons reuse guidance](https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia)
