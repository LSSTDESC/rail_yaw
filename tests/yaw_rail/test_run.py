from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import numpy.testing as npt
from numpy.random import default_rng
from pytest import fixture
from yaw import UniformRandoms

from rail.core.stage import RailStage

from rail.yaw_rail import (
    YawAutoCorrelate,
    YawCacheCreate,
    YawCacheDrop,
    YawCrossCorrelate,
    YawSummarize,
)

if TYPE_CHECKING:
    from numpy.random import Generator
    from pandas import DataFrame
    from rail.core.data import DataStore


@fixture(name="data_store")
def fixture_data_store() -> DataStore:
    DS = RailStage.data_store
    DS.__class__.allow_overwrite = True
    return DS


@fixture(name="seed")
def fixture_seed() -> int:
    return 12345


@fixture(name="numpy_rng")
def fixture_numpy_rng(seed) -> Generator:
    return default_rng(seed)

@fixture(name="angular_rng")
def fixture_angular_rng(seed) -> UniformRandoms:
    return UniformRandoms(-5, 5, -5, 5, seed=seed)


@fixture(name="mock_data")
def fixture_mock_data(numpy_rng, angular_rng) -> DataFrame:
    N = 10_000
    z = numpy_rng.uniform(0.01, 1.0, size=N)
    return angular_rng.generate(N, draw_from=dict(z=z))


@fixture(name="mock_rand")
def fixture_mock_rand(numpy_rng, angular_rng) -> DataFrame:
    N = 20_000
    z = numpy_rng.uniform(0.01, 1.0, size=N)
    return angular_rng.generate(N, draw_from=dict(z=z))


def write_expect_wss(path: Path) -> Path:
    target = path / "wss_expect.txt"
    with open(target, "w") as f:
        f.write("""# correlation function estimate with symmetric 68% percentile confidence
#    z_low     z_high         nz     nz_err
 0.0100000  0.1090000 -0.0004704  0.0028912
 0.1090000  0.2080000 -0.0102598  0.0093342
 0.2080000  0.3070000  0.0153290  0.0086877
 0.3070000  0.4060000  0.0103880  0.0150123
 0.4060000  0.5050000 -0.0093514  0.0273241
 0.5050000  0.6040000 -0.0242266  0.0328208
 0.6040000  0.7030000 -0.0244370  0.0215342
 0.7030000  0.8020000 -0.0098123  0.0141603
 0.8020000  0.9010000  0.0199227  0.0315443
 0.9010000  1.0000000 -0.0454084  0.0397360
""")
    return target


def write_expect_wsp(path: Path) -> Path:
    target = path / "wss_expect.txt"
    with open(target, "w") as f:
        f.write("""# n(z) estimate with symmetric 68% percentile confidence
#    z_low     z_high         nz     nz_err
 0.0100000  0.1090000  0.0010139  0.0124166
 0.1090000  0.2080000  0.0232324  0.0208439
 0.2080000  0.3070000  0.0521142  0.0491161
 0.3070000  0.4060000  0.0604353  0.0688827
 0.4060000  0.5050000 -0.0215233  0.0534807
 0.5050000  0.6040000  0.0024020  0.0635777
 0.6040000  0.7030000 -0.0517819  0.0135965
 0.7030000  0.8020000 -0.0265981  0.0015099
 0.8020000  0.9010000  0.0496620  0.0772239
 0.9010000  1.0000000 -0.0059678  0.1281836
""")
    return target


def assert_cols_match(path_a: Path, path_b: Path, *, ignore_cols: list[int]) -> None:
    data_a = np.loadtxt(path_a).T
    data_b = np.loadtxt(path_b).T
    for i, (col_a, col_b) in enumerate(zip(data_a, data_b)):
        if i in ignore_cols:
            continue
        npt.assert_array_equal(col_a, col_b)


def test_run(data_store, tmp_path, mock_data, mock_rand) -> None:  # pylint: disable=W0613
    # data_store must be called at least once, which is done here implicitly
    cache_ref = YawCacheCreate.make_stage(
        name="cache_ref",
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=3,
    ).create(mock_data, mock_rand)

    cache_unk = YawCacheCreate.make_stage(
        name="cache_unk",
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patches=f"{tmp_path}/test_ref",
    ).create(mock_data, mock_rand)

    corr_config = dict(
        rmin=1000,
        rmax=10000,
        zmin=0.01,
        zmax=1.0,
        zbin_num=10,
    )

    w_ss = YawAutoCorrelate.make_stage(**corr_config).correlate(cache_ref)
    w_ss.data.sample().to_files(tmp_path / "wss")
    # compare to expected result
    expect = write_expect_wss(tmp_path)
    assert_cols_match(expect, tmp_path / "wss.dat", ignore_cols=[3])

    w_sp = YawCrossCorrelate.make_stage(**corr_config).correlate(cache_ref, cache_unk)
    res = YawSummarize.make_stage().summarize(w_sp)
    res["yaw_cc"].data.to_files(tmp_path / "wsp")
    # compare to expected result
    expect = write_expect_wsp(tmp_path)
    assert_cols_match(expect, tmp_path / "wsp.dat", ignore_cols=[3])

    YawCacheDrop.make_stage().drop(cache_unk)
    YawCacheDrop.make_stage().drop(cache_ref)
