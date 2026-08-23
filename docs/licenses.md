# License and attribution audit

Verified: 2026-08-24. This is an engineering inventory, not legal advice. The
terms linked below remain authoritative and must be rechecked before a public
deployment or redistribution of images/model weights.

## Project code

GeoSnap source code is distributed under the repository's [MIT
license](../LICENSE). Third-party code, checkpoints, imagery, map data, and
rendered map tiles retain their own licenses; the repository license does not
replace them.

## Imagery and map data

| Source | Current terms used by this project | Required handling |
| --- | --- | --- |
| Mapillary imagery | [Mapillary's current help article](https://help.mapillary.com/hc/en-us/articles/115001770409-CC-BY-SA-license-for-open-data) says public user images are shared under CC BY-SA and gives the expected per-image attribution form. API and commercial use are also subject to the current [Mapillary Terms](https://www.mapillary.com/terms). | Preserve image ID, contributor username, original image/profile links, license, and attribution in every manifest stage. When GeoSnap serves a reference image, show the visible official Mapillary mark linked to that image, the author/profile link, and CC BY-SA 4.0 beside it. Keep the registered application token outside the repository. Do not bypass API limits or create an uncontrolled bulk downloader. |
| KartaView imagery | [KartaView Terms of Use](https://kartaview.org/terms), last modified 2025-06-17, state that street imagery is CC BY-SA 4.0. | Credit exactly `© Grab and KartaView Contributors`, preserve source/image links and license, and comply with CC BY-SA 4.0 when redistributing or adapting imagery. |
| Wikimedia Commons evaluation images | License and attribution are file-specific; the acquisition snapshot retains each file page, author, license label/URL and hashes. See the [Commons reuse guidance](https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia). | Evaluation-only. Recheck every linked file page before reuse; do not treat the proxy as a deployable street-view gallery or as city-wide evidence. |
| OpenStreetMap data | [OpenStreetMap copyright and license](https://www.openstreetmap.org/copyright): ODbL 1.0. | Show `© OpenStreetMap contributors` and link to the copyright page. Derived databases may trigger ODbL share-alike obligations. |
| OpenStreetMap standard tiles | Governed separately by the [tile usage policy](https://operations.osmfoundation.org/policies/tiles/). | Suitable only for a light local demo with visible attribution. A public/high-volume deployment needs a compliant tile provider or self-hosted tiles. |

Mapillary's Object Dataset and other benchmark datasets are **not** covered by
the street-imagery row above. Each separately packaged dataset must be audited
before use. No benchmark dataset is automatically a deployable Moscow gallery.

Mapillary rows without `creator.username` are quarantined instead of receiving
a synthetic generic attribution. The UI uses the green mark from Mapillary's
official [`mapillary/api-demo`](https://github.com/mapillary/api-demo/blob/main/logo_green.svg),
linked to the concrete image. GeoSnap never attempts to remove upstream privacy
blurring or re-identify people. If a displayed image is materially cropped,
recolored, or otherwise adapted, the UI/release must identify the modification
and retain CC BY-SA 4.0 for that adapted image. The current thumbnail proxy only
serves the validated local reference bytes without an image transformation.

## Models and checkpoints

| Component | Pinned source | License/status | Project consequence |
| --- | --- | --- | --- |
| MegaLoc code | [`gmberton/MegaLoc`](https://github.com/gmberton/MegaLoc) at `5fe0dd697c4a70ba3e23607f6716ab3c606b16db` | MIT | Copyright/license notice must accompany redistributed code. |
| MegaLoc checkpoint | [`gberton/MegaLoc`](https://huggingface.co/gberton/MegaLoc), `model.safetensors` at revision `37bb43d65dd6388d1578052de5eb0bcdceb497e7`, SHA-256 `d4f9f2bcb60018f91eb6a8e061ed054fd55654e10c2569cf13841ea986ffb4f8` | Model card declares MIT | Downloaded at runtime into an ignored cache; never committed. Revision and content hash are persisted in embedding/index metadata. |
| DINOv2 code and weights | [`facebookresearch/dinov2`](https://github.com/facebookresearch/dinov2) at `7764ea0f912e53c92e82eb78a2a1631e92725fc8` | Apache License 2.0 for code and model weights, as stated in the official README/license | Preserve Apache notices when redistributing covered material. SALAD's otherwise mutable nested Torch Hub request is rewritten to this exact revision. |
| SALAD code and v1.0.0 checkpoint | [`serizba/salad`](https://github.com/serizba/salad) at `6aede13a3f6c25750bf7fde10209c06cb73060bb`; [official v1.0.0 release](https://github.com/serizba/salad/releases/tag/v1.0.0), checkpoint SHA-256 `6b3f1720954293e83da6966c5cfcfc6713200d7fefadcca76fc51aeb80b3cada` | GPL-3.0 repository; the release does not state a separate checkpoint license | Treat the checkpoint/integration as GPL-3.0-covered unless the authors clarify otherwise. It is an optional benchmark candidate; distribution of a combined application needs GPL compatibility review. |
| LightGlue (optional verification) | [`cvg/LightGlue`](https://github.com/cvg/LightGlue), tested at `eb42fee2d71449efb0aa5c10549752b5d75384d8` | Apache License 2.0 for the repository code/weights | Not installed in the base environment: its tested upstream installation selected OpenCV 5, which conflicts with this project's pinned OpenCV 4 range. The SIFT adapter fails explicitly when absent. Use only an extractor whose own code and weights have compatible, explicitly verified terms. |

No adapter is allowed to fall back to random/uninitialized weights. Model
download failure is a visible readiness/blocker state.

## Major runtime components

| Component | License |
| --- | --- |
| PyTorch / torchvision | BSD-style licenses; see the upstream distribution notices |
| FAISS | MIT |
| H3 | Apache License 2.0 |
| FastAPI | MIT |
| React | MIT |
| MapLibre GL JS | BSD 3-Clause |
| PostgreSQL | PostgreSQL License |
| PostGIS | GPL-2.0-or-later |

Python and JavaScript lockfiles are the dependency inventory. A release should
generate a complete dependency/SBOM report from those locks and retain all
required notices.

## Attribution contract

Every reference row must carry `source`, `source_image_id`, `license`,
`attribution`, and source-link metadata. The API returns source, license and,
when available, contributor-profile links for each displayable match; it never
synthesizes a permissive default when upstream metadata is missing. The
frontend renders these links adjacent to reference thumbnails, renders the
official linked Mapillary mark on Mapillary cards, and renders map attribution
on the map itself. Outbound URLs are limited to HTTPS and known source/license
hosts before reaching the browser contract.
