import pytest

from ml.research.night_finish_queue import licensed_ready, ready


def test_finish_waits_for_audit_and_post_child_queue_validation(tmp_path):
    queue = tmp_path / "evaluation_queue"
    queue.mkdir()
    assert not ready(tmp_path)
    (tmp_path / "evaluation.done.json").touch()
    (tmp_path / "audit.done.json").touch()
    assert not ready(tmp_path)
    (queue / "done.json").touch()
    assert ready(tmp_path)
    (queue / "failure.json").touch()
    with pytest.raises(RuntimeError, match="failure review"):
        ready(tmp_path)


def test_cutoff_does_not_abandon_started_audit(tmp_path):
    queue = tmp_path / "evaluation_queue"
    queue.mkdir()
    (queue / "closed.json").touch()
    assert not ready(tmp_path)
    (tmp_path / "audit.done.json").touch()
    assert ready(tmp_path)


def test_licensed_transfer_must_finish_after_registration(tmp_path):
    assert licensed_ready(tmp_path)
    registration = tmp_path / "finish_queue"
    registration.mkdir()
    (registration / "licensed_third_extension.registration.json").touch()
    assert not licensed_ready(tmp_path)
    stage = tmp_path / "licensed_third"
    stage.mkdir()
    (stage / "intent.json").touch()
    assert not licensed_ready(tmp_path)
    (stage / "done.json").touch()
    assert licensed_ready(tmp_path)
    (stage / "failure.json").touch()
    with pytest.raises(RuntimeError, match="failure review"):
        licensed_ready(tmp_path)
