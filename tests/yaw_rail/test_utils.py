from __future__ import annotations

from ceci.stage import StageParameter
from pandas import DataFrame
from pytest import mark, raises
from rail.core.data import TableHandle
from rail.core.stage import RailStage

from rail.yaw_rail import utils


TEST_PARAM_DEFAULT = 0


# a dummy stage for testing with a single parameter
class StageTester(
    utils.YawRailStage,
    config_items=dict(test=StageParameter(dtype=int)),
):
    """__doc__"""

    inputs = [("input", TableHandle)]
    outputs = [("output", TableHandle)]

    def run(self):
        pass


class StageMakerAliased:
    # need to create aliases every time using a stage in different test
    count = 0  # incremented index for aliasing

    @classmethod
    def make_stage(cls) -> StageTester:
        cls.count += 1
        return StageTester.make_stage(
            test=TEST_PARAM_DEFAULT,
            name=f"stage{cls.count}",
            aliases=dict(input=f"input_{cls.count}"),
        )


def make_test_handle() -> TableHandle:
    data = dict(a=[0])
    return TableHandle("test_tag", data=DataFrame(data))


def make_stage_with_input() -> StageTester:
    test_stage = StageMakerAliased.make_stage()
    assert test_stage.get_optional_data("input") is None
    handle = make_test_handle()
    test_stage.add_data("input", handle.data)
    return test_stage


@mark.parametrize(
    "value,expect", [("/some/path", True), ("None", False), (None, False)]
)
def test_handle_has_path(value, expect):
    class DummyHandle:
        path = value

    dummy = DummyHandle()
    assert utils.handle_has_path(dummy) == expect


class TestYawRailStage:
    def test_init_subclass(self):
        assert StageTester.name == StageTester.__name__
        assert set(StageTester.config_options) == (
            set(RailStage.config_options) | StageTester.algo_parameters | {"verbose"}
        )

        # this is actually testing ceci code at the moment
        assert StageTester.__doc__.startswith("__doc__")
        assert "test" in StageTester.__doc__
        assert "verbose" in StageTester.__doc__

    def test_get_algo_config_dict(self):
        test_stage = StageMakerAliased.make_stage()

        assert "test" in test_stage.get_algo_config_dict()
        assert test_stage.get_algo_config_dict()["test"] == TEST_PARAM_DEFAULT

        assert len(test_stage.get_algo_config_dict(exclude=["test"])) == 0

    def test_get_optional_handle(self):
        test_stage = StageMakerAliased.make_stage()
        with raises(KeyError):
            test_stage.get_handle("input")
        assert test_stage.get_optional_handle("input") is None

        test_stage.add_handle("input", make_test_handle())
        test_stage.get_handle("input")

    def test_get_optional_data_memory(self):
        test_stage = make_stage_with_input()

        data = test_stage.get_optional_data("input")
        assert isinstance(data, DataFrame)

    def test_get_optional_data_file(self, tmp_path):
        stage = make_stage_with_input()
        # write the test data in order to read it back later
        path = str(tmp_path / "data.parquet")
        stage.get_optional_data("input").to_parquet(path)

        test_stage = StageMakerAliased.make_stage()
        test_stage.add_handle("input", path=path)
        data = test_stage.get_optional_data("input")
        assert isinstance(data.to_pandas(), DataFrame)

    def test_set_optional_data(self):
        test_stage = StageMakerAliased.make_stage()
        test_stage.set_optional_data("input", make_test_handle().data)
        handle = test_stage.get_handle("input")
        assert isinstance(handle.data, DataFrame)
