from __future__ import annotations

from ceci.stage import StageParameter
from pandas import DataFrame
from pytest import raises
from rail.core.data import TableHandle
from rail.core.stage import RailStage

from rail.yaw_rail import stage


class TestStage(stage.YawRailStage, config_items=dict(test=StageParameter(dtype=int))):
    """@YawParameters"""

    inputs = [("input", TableHandle)]
    outputs = [("output", TableHandle)]


class StageMakerAliased:
    # cannot use a single stage instance here since handles are visible globally
    count = 0

    @classmethod
    def make_stage(cls) -> TestStage:
        cls.count += 1
        return TestStage.make_stage(
            test=0, name=f"stage{cls.count}", aliases=dict(input=f"input_{cls.count}")
        )


def make_test_handle() -> TableHandle:
    data = dict(a=[0])
    return TableHandle("test_tag", data=DataFrame(data))


class TestYawRailStage:
    def test_init_subclass(self):
        assert TestStage.name == TestStage.__name__
        assert set(TestStage.config_options) == (
            set(RailStage.config_options) | TestStage.algo_parameters | {"verbose"}
        )
        assert "test" in TestStage.__doc__
        assert "verbose" in TestStage.__doc__

    def test_get_algo_config_dict(self):
        test_stage = StageMakerAliased.make_stage()
        assert "test" in test_stage.get_algo_config_dict()
        assert test_stage.get_algo_config_dict()["test"] == 0
        assert len(test_stage.get_algo_config_dict(exclude=["test"])) == 0

    def test_get_optional_handle(self):
        test_stage = StageMakerAliased.make_stage()
        with raises(KeyError):
            test_stage.get_handle("input")
        assert test_stage.get_optional_handle("input") is None

        test_stage.add_handle("input", make_test_handle())
        test_stage.get_handle("input")

    def test_get_optional_data(self):
        test_stage = StageMakerAliased.make_stage()
        assert test_stage.get_optional_data("input") is None

        test_stage.add_data("input", make_test_handle())
        handle = test_stage.get_optional_data("input")
        assert isinstance(handle.data, DataFrame)

    def test_set_optional_data(self):
        test_stage = StageMakerAliased.make_stage()
        test_stage.set_optional_data("input", make_test_handle().data)
        handle = test_stage.get_handle("input")
        assert isinstance(handle.data, DataFrame)
