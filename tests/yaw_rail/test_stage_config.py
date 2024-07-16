from __future__ import annotations

from pytest import raises
from yaw.config import ScalesConfig

from rail.yaw_rail import stage_config


def test_get_yaw_config_meta():
    with raises(AttributeError, match=".*no attribute.*"):
        stage_config.get_yaw_config_meta(ScalesConfig, "does_not_exist")
