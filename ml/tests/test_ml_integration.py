from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance

from ml.indexing import build_index_from_embedding_artifacts
from ml.localization import SpatialLocalizer, haversine_m
from ml.retrieval.embedding_job import EmbeddingJob, ReferenceImage
from ml.retrieval.testing import DeterministicFixtureRetriever


def _scene(path: Path, sky: tuple[int, int, int], building: tuple[int, int, int]) -> Image.Image:
    image = Image.new("RGB", (128, 96), sky)
    draw = ImageDraw.Draw(image)
    draw.rectangle((12, 35, 116, 95), fill=building)
    draw.rectangle((24, 48, 40, 72), fill=(240, 240, 180))
    draw.rectangle((55, 45, 75, 70), fill=(40, 40, 50))
    draw.polygon(((0, 95), (128, 70), (128, 95)), fill=(70, 70, 70))
    image.save(path)
    return image


def test_fixture_image_to_embedding_to_faiss_to_localization(tmp_path: Path) -> None:
    gallery_specs = [
        ("arbat", 55.7522, 37.5923, (90, 150, 220), (180, 90, 60)),
        ("north", 55.8500, 37.6000, (170, 180, 190), (90, 110, 130)),
        ("east", 55.7600, 37.7500, (240, 180, 100), (70, 100, 70)),
    ]
    records: list[ReferenceImage] = []
    gallery_images: dict[str, Image.Image] = {}
    for reference_id, lat, lon, sky, building in gallery_specs:
        path = tmp_path / f"{reference_id}.jpg"
        gallery_images[reference_id] = _scene(path, sky, building)
        records.append(
            ReferenceImage(
                reference_id,
                path,
                {
                    "id": reference_id,
                    "lat": lat,
                    "lon": lon,
                    "city_id": "moscow",
                    "source": "fixture",
                    "attribution": "Synthetic test fixture; not an accuracy benchmark",
                },
            )
        )

    retriever = DeterministicFixtureRetriever(allow_test_only=True)
    embedding_dir = tmp_path / "embeddings"
    EmbeddingJob(retriever, embedding_dir, batch_size=2).run(records)
    index = build_index_from_embedding_artifacts(
        embedding_dir,
        tmp_path / "index",
        index_id="moscow-fixture",
        city_id="moscow",
    )

    # Held-out query object: mild exposure change, not the gallery file itself.
    query = ImageEnhance.Brightness(gallery_images["arbat"]).enhance(0.92)
    query_descriptor = retriever.embed_query(query)
    matches = index.search_one(query_descriptor, k=3)
    assert matches[0].reference_id == "arbat"
    localization = SpatialLocalizer().localize(matches)
    # Three geographically unrelated singleton modes are deliberately not
    # promoted to an overconfident product answer, even when top-1 is correct.
    assert localization.status.value == "low_confidence"
    assert "winning_geographic_mode_has_insufficient_support" in localization.reasons
    assert localization.lat is not None and localization.lon is not None
    assert haversine_m(localization.lat, localization.lon, 55.7522, 37.5923) < 25
    assert localization.diagnostics["spatial_mode_count"] == 3
