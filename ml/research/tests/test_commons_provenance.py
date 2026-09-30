import pytest

from ml.research.commons_provenance import author_keys, camera_position


def test_camera_coordinates_do_not_use_object_or_wikidata_position():
    assert camera_position("{{Object location|55.7|37.6}}") is None
    assert camera_position("{{Location|wikidata=Q649}}") is None
    assert camera_position("{{Object location|1|2}} {{Location|55.7|37.6}}") == (55.7, 37.6)


def test_camera_supports_decimal_named_and_dms_and_rejects_conflicts():
    assert camera_position("{{Location|1=55.7|2=37.6}}") == (55.7, 37.6)
    assert camera_position("{{Location|55|42|0|N|37|36|0|E}}") == pytest.approx((55.7, 37.6))
    assert camera_position("{{Location|55.7|37.6}}{{Location|55.8|37.6}}") is None
    assert camera_position("{{Location|NaN|37.6}}") is None
    assert camera_position("{{Location|55|80|0|N|37|36|0|E}}") is None


def test_author_identity_handles_redlinks_aliases_and_repeated_credits():
    direct = '<a href="//commons.wikimedia.org/wiki/User:Example_Name">Example</a>'
    redlink = '<a href="//commons.wikimedia.org/w/index.php?title=User:Example_Name&amp;action=edit">Example</a>'
    assert author_keys(direct) == author_keys(redlink) == ["commons:example name"]
    assert author_keys(direct + direct) == ["commons:example name"]
    assert author_keys('<a href="https://www.wikidata.org/wiki/Q110446558">Artyom Svetlov</a>') == [
        "known-mapillary:trolleway"
    ]
    assert author_keys("unknown photographer") == []
