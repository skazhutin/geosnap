# License and attribution audit

Verified: 2026-09-02. This is an engineering inventory, not legal advice. The
terms linked below remain authoritative and must be rechecked before a public
deployment or redistribution of images/model weights.

## Project code

GeoSnap source code is distributed under the repository's [MIT
license](../LICENSE). Third-party code, checkpoints, imagery, map data, and
rendered map tiles retain their own licenses; the repository license does not
replace them.

## Imagery and map data

The deployable Moscow reference gallery is restricted to physically preserved
**Mapillary** and **KartaView** imagery inside the pinned OSM administrative
boundary. The v2 approved-source corpus has 23,654 rows (17,143 Mapillary;
6,511 KartaView), while the frozen v3 production gallery/index contains 20,031
references (14,972 Mapillary; 5,059 KartaView). This is an engineering
publication boundary, not a statement that either source grants full-city
coverage or a blanket redistribution right.

| Source | Current terms used by this project | Required handling |
| --- | --- | --- |
| Mapillary imagery | [Mapillary's current help article](https://help.mapillary.com/hc/en-us/articles/115001770409-CC-BY-SA-license-for-open-data) says public user images are shared under CC BY-SA and gives the expected per-image attribution form. API and commercial use are also subject to the current [Mapillary Terms](https://www.mapillary.com/terms). | Preserve image ID, contributor username, original image/profile links, license, and attribution in every manifest stage. When GeoSnap serves a reference image, show the visible official Mapillary mark linked to that image, the author/profile link, and CC BY-SA 4.0 beside it. Keep the registered application token outside the repository. Do not bypass API limits or create an uncontrolled bulk downloader. |
| KartaView imagery | [KartaView Terms of Use](https://kartaview.org/terms), last modified 2025-06-17, state that street imagery is CC BY-SA 4.0. | Credit exactly `© Grab and KartaView Contributors`, preserve source/image links and license, and comply with CC BY-SA 4.0 when redistributing or adapting imagery. |
| Wikimedia Commons evaluation images | License and attribution are file-specific; the acquisition snapshot retains each file page, author, license label/URL and hashes. See the [Commons reuse guidance](https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia). | Evaluation-only. Recheck every linked file page before reuse; do not treat the proxy as a deployable street-view gallery or as city-wide evidence. |
| MSLS and other benchmark/training datasets | These are independently packaged datasets with their own terms, splits and possible redistribution restrictions. | Research/evaluation-only unless separately audited for a specific use. Never merge their images, descriptors or metadata into the Mapillary/KartaView production gallery or FAISS index. |
| OpenStreetMap data | [OpenStreetMap copyright and license](https://www.openstreetmap.org/copyright): ODbL 1.0. | Show `© OpenStreetMap contributors` and link to the copyright page. Derived databases may trigger ODbL share-alike obligations. |
| OpenStreetMap standard tiles | Governed separately by the [tile usage policy](https://operations.osmfoundation.org/policies/tiles/). | Suitable only for a light local demo with visible attribution. A public/high-volume deployment needs a compliant tile provider or self-hosted tiles. |

Mapillary's Object Dataset and other benchmark datasets are **not** covered by
the street-imagery row above. Each separately packaged dataset must be audited
before use. No benchmark dataset is automatically a deployable Moscow gallery.
The production publication gate accepts only `source=mapillary` and
`source=kartaview`; Commons/MSLS material is excluded before embedding and
indexing.

Mapillary rows without `creator.username` are quarantined instead of receiving
a synthetic generic attribution. The UI uses the green mark from Mapillary's
official [`mapillary/api-demo`](https://github.com/mapillary/api-demo/blob/main/logo_green.svg),
linked to the concrete image. GeoSnap never attempts to remove upstream privacy
blurring or re-identify people. If a displayed image is materially cropped,
recolored, or otherwise adapted, the UI/release must identify the modification
and retain CC BY-SA 4.0 for that adapted image. The current thumbnail proxy
technically resizes and re-encodes the validated local reference as JPEG; the UI
therefore labels every displayed proxy image as `Миниатюра уменьшена GeoSnap`.

## Models and checkpoints

| Component | Pinned source | License/status | Project consequence |
| --- | --- | --- | --- |
| MegaLoc code | [`gmberton/MegaLoc`](https://github.com/gmberton/MegaLoc) at `5fe0dd697c4a70ba3e23607f6716ab3c606b16db` | MIT | Copyright/license notice must accompany redistributed code. |
| MegaLoc checkpoint | [`gberton/MegaLoc`](https://huggingface.co/gberton/MegaLoc), `model.safetensors` at revision `37bb43d65dd6388d1578052de5eb0bcdceb497e7`, SHA-256 `d4f9f2bcb60018f91eb6a8e061ed054fd55654e10c2569cf13841ea986ffb4f8` | Model card declares MIT | Downloaded at runtime into an ignored cache; never committed. Revision and content hash are persisted in embedding/index metadata. |
| DINOv2 code and weights | [`facebookresearch/dinov2`](https://github.com/facebookresearch/dinov2) at `7764ea0f912e53c92e82eb78a2a1631e92725fc8` | Apache License 2.0 for code and model weights, as stated in the official README/license | Preserve Apache notices when redistributing covered material. SALAD's otherwise mutable nested Torch Hub request is rewritten to this exact revision. |
| SALAD code and v1.0.0 checkpoint | [`serizba/salad`](https://github.com/serizba/salad) at `6aede13a3f6c25750bf7fde10209c06cb73060bb`; [official v1.0.0 release](https://github.com/serizba/salad/releases/tag/v1.0.0), checkpoint SHA-256 `6b3f1720954293e83da6966c5cfcfc6713200d7fefadcca76fc51aeb80b3cada` | GPL-3.0 repository; the release does not state a separate checkpoint license | Treat the checkpoint/integration as GPL-3.0-covered unless the authors clarify otherwise. It is an optional benchmark candidate; distribution of a combined application needs GPL compatibility review. |
| SAGE code and ViT-B checkpoint | [`chenshunpeng/SAGE`](https://github.com/chenshunpeng/SAGE) at `c7d6241c4885526d99d6c78c158024fc2a37097c`; [`shunpeng/SAGE`](https://huggingface.co/shunpeng/SAGE) at `2a2ea9964cdbdfd2211e7c625064a9d5e4678245`, checkpoint SHA-256 `8cfed7d4e8bbcdee4c016b29211f64ffd015c3cbae538034b3352c858af6a27e` | Official repository and model card state MIT | Frozen v3 production retriever. Preserve MIT notice and recheck terms before deployment/redistribution; this is a technical audit, not legal advice. |
| SelaVPR++ code and checkpoints | [`Lu-Feng/SelaVPRplusplus`](https://github.com/Lu-Feng/SelaVPRplusplus) at `56bd921cbd3d53e9c5f91d0aafff147f95fb362a`; official release assets SHA-256 `b048490dbd1c27dee67fce6faaec7bec267d19a044c85877af94b8588e596a62` (base) and `da31138202b9a746916588ecd97499a56bae304e61444a50d6f34761377cdcbf` (rerank) | MIT repository; release assets state no separate contradictory terms | Evaluated deployable candidate, not selected. Preserve MIT notice and reconfirm checkpoint terms before redistribution. |
| CricaVPR code and checkpoint | [`Lu-Feng/CricaVPR`](https://github.com/Lu-Feng/CricaVPR) at `f53e941d34a559ca8432960bc2c29ef22f940c97`; official asset SHA-256 `e3102d28e07df60b9f82003e96b44feb3debe1827f7d90143180174c3ad8e046` | MIT repository; release asset states no separate contradictory terms | Licensing screen passed technically, but batch-dependent descriptors violate GeoSnap's independent gallery/query index contract, so full integration was rejected. |
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

For the Moscow AOI itself, GeoSnap retains the source relation link
([OSM relation 102269](https://www.openstreetmap.org/relation/102269)), its
GeoJSON SHA-256, and ODbL attribution. The AOI identifies where a reference is
accepted; it does not add an imagery license or imply that all of the polygon
is covered.
