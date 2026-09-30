from __future__ import annotations

import asyncio

import pytest
from app.main import create_app
from app.schemas.api import ApiStatus, Diagnostics, Hypothesis, LocalizeResponse, Prediction
from app.services.multi_photo import combine_views

from .test_api import ASGITestClient, FakeService, encoded_image, make_settings


def view(*locations: tuple[float, float], status=ApiStatus.LOW_CONFIDENCE):
    return LocalizeResponse(
        status=status,
        prediction=Prediction(lat=locations[0][0], lon=locations[0][1], confidence=0.8) if locations else None,
        hypotheses=[Hypothesis(lat=lat, lon=lon, score=0.8 - i * 0.1) for i, (lat, lon) in enumerate(locations)],
        diagnostics=Diagnostics(), request_id="test",
    )


def test_independent_views_can_resolve_different_first_choices():
    common = (55.75, 37.62)
    result = combine_views([view((55.6, 37.1), common), view((55.9, 37.9), common)], submitted=2, request_id="batch")
    assert result.prediction.lat == common[0]
    assert result.prediction.lon == common[1]
    assert result.multi_photo.supporting_images == 2
    assert result.multi_photo.agreement == "consensus"
    assert result.status == ApiStatus.LOW_CONFIDENCE
    assert "multi_photo_uncalibrated" in result.diagnostics.warnings


def test_three_independent_top_choices_win_over_shared_lower_rank_distractors():
    # Reproduces the shape of the six-view case without using its ground truth:
    # three independently selected the same reference, while every view has a
    # visually plausible but geographically distant second choice.
    anchor = (55.762471812643, 37.634552130675)
    distant = (55.771117200182, 37.678991449478)
    results = [
        view(anchor, distant), view(anchor, distant), view(distant, anchor),
        view((55.763523560017, 37.596632558852), distant),
        view(anchor, distant), view((55.7549961, 37.6257294), distant),
    ]
    result = combine_views(results, submitted=6, request_id="six")
    assert (result.prediction.lat, result.prediction.lon) == anchor
    assert result.multi_photo.supporting_images == 3
    assert result.multi_photo.method == "top1_then_geographic_consensus_v2"


def test_equal_support_for_distant_places_remains_ambiguous():
    candidates = ((55.7, 37.5), (55.9, 37.9))
    result = combine_views([view(*candidates), view(*reversed(candidates))], submitted=2, request_id="batch")
    assert result.prediction is None
    assert result.multi_photo.agreement == "ambiguous"


def test_one_photo_cannot_outvote_two_others_with_repeated_references():
    result = combine_views([
        view((55.7, 37.5), (55.7001, 37.5), (55.7002, 37.5)),
        view((55.9, 37.9)), view((55.9001, 37.9)),
    ], submitted=3, request_id="batch")
    assert result.prediction.lat >= 55.9
    assert result.multi_photo.supporting_images == 2


def test_no_candidates_and_single_unique_photo_keep_honest_semantics():
    unavailable = view(status=ApiStatus.OUT_OF_COVERAGE)
    result = combine_views([unavailable, unavailable], submitted=2, request_id="batch")
    assert result.status == ApiStatus.OUT_OF_COVERAGE
    assert result.prediction is None
    single = view((55.75, 37.62), status=ApiStatus.OK)
    result = combine_views([single], submitted=10, request_id="batch")
    assert result.prediction == single.prediction
    assert result.status == single.status
    assert result.multi_photo.duplicate_images == 9
    assert result.multi_photo.supporting_images == 1


def batch_body(images):
    boundary = "geosnap-batch-test"
    parts = []
    for image in images:
        mime = "image/png" if image.startswith(b"\x89PNG") else "image/jpeg"
        suffix = "png" if mime == "image/png" else "jpg"
        parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="images"; filename="view.{suffix}"\r\n'
                      f'Content-Type: {mime}\r\n\r\n').encode() + image + b"\r\n")
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    return body, {"content-type": f"multipart/form-data; boundary={boundary}", "content-length": str(len(body))}


def post_batch(client, images):
    body, headers = batch_body(images)
    return client.post("/localize/multi", body=body, headers=headers)


def test_api_ten_images_and_decoded_pixel_deduplication():
    service = FakeService()
    with ASGITestClient(create_app(settings=make_settings(), service_factory=lambda: service)) as client:
        images = [encoded_image(color=(i * 20, 120, 180)) for i in range(10)]
        response = post_batch(client, images)
        assert response.status_code == 200
        data = response.json()
        assert data["multi_photo"]["supporting_images"] == 10
        assert data["status"] == "low_confidence"
        assert response.headers["cache-control"] == "no-store"
        assert service.localize_calls == 10
        # PNG and JPEG with identical decoded black pixels cannot double the vote.
        response = post_batch(client, [encoded_image("PNG", color=(0, 0, 0)), encoded_image("JPEG", color=(0, 0, 0))])
        assert response.status_code == 200
        assert response.json()["multi_photo"]["unique_images"] == 1
        assert service.localize_calls == 11


@pytest.mark.parametrize("count", [0, 11])
def test_api_rejects_invalid_count_before_inference(count):
    service = FakeService()
    with ASGITestClient(create_app(settings=make_settings(), service_factory=lambda: service)) as client:
        response = post_batch(client, [encoded_image()] * count)
        assert response.status_code == 422
        assert service.localize_calls == 0


def test_bad_image_cannot_silently_disappear_from_batch():
    with ASGITestClient(create_app(settings=make_settings(), service_factory=FakeService)) as client:
        response = post_batch(client, [encoded_image(), b"corrupt"])
        assert response.status_code == 422
        assert response.json()["prediction"] is None


def test_batch_total_upload_limit_and_readiness():
    settings = make_settings(max_batch_upload_bytes=200, multipart_overhead_bytes=100)
    with ASGITestClient(create_app(settings=settings, service_factory=FakeService)) as client:
        assert post_batch(client, [encoded_image()] * 2).status_code == 413


def test_timed_out_batch_keeps_capacity_until_inference_finishes():
    class SlowService(FakeService):
        async def localize(self, query):
            await asyncio.sleep(0.08)
            return self.result

    app = create_app(settings=make_settings(batch_timeout_seconds=0.01, localization_queue_limit=0), service_factory=SlowService)
    with ASGITestClient(app) as client:
        first = post_batch(client, [encoded_image()])
        assert first.status_code == 504
        second = post_batch(client, [encoded_image()])
        assert second.status_code == 503
        client.runner.run(asyncio.sleep(0.12))
        lease = client.runner.run(app.state.localization_capacity.acquire())
        lease.release()
