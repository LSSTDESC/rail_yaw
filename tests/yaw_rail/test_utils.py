from __future__ import annotations

from pytest import mark

from rail.yaw_rail import utils


@mark.parametrize(
    "value,expect", [("/some/path", True), ("None", False), (None, False)]
)
def test_handle_has_path(value, expect):
    class DummyHandle:
        path = value

    dummy = DummyHandle()
    assert utils.handle_has_path(dummy) == expect
