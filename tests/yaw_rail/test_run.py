from __future__ import annotations

from pathlib import Path

import numpy as np
import numpy.testing as npt
from pytest import mark

from rail.yaw_rail import (
    YawAutoCorrelate,
    YawCacheCreate,
    YawCacheDrop,
    YawCrossCorrelate,
    YawSummarize,
)
from rail.yaw_rail.cache import stage_helper


def write_expect_wss(path: Path) -> Path:
    target = path / "wss_expect.txt"
    with open(target, "w") as f:
        f.write(
            """# correlation function estimate with symmetric 68% percentile confidence
#    z_low     z_high         nz     nz_err
 0.2002429  0.9998286  0.0115084  0.0039227
 0.9998286  1.7994142  0.0224274  0.0130083
"""
        )
    return target


def write_expect_wsp(path: Path) -> Path:
    target = path / "wsp_expect.txt"
    with open(target, "w") as f:
        f.write(
            """# correlation function estimate with symmetric 68% percentile confidence
#    z_low     z_high         nz     nz_err
 0.2002429  0.9998286  0.0060440  0.0046580
 0.9998286  1.7994142  0.0118823  0.0014990
"""
        )
    return target


def write_expect_ncc(path: Path) -> Path:
    target = path / "ncc_expect.txt"
    with open(target, "w") as f:
        f.write(
            """# n(z) estimate with symmetric 68% percentile confidence
#    z_low     z_high         nz     nz_err
 0.2002429  0.9998286  0.0704619  0.0454918
 0.9998286  1.7994142  0.0992310  0.0185709
"""
        )
    return target


def assert_cols_match(path_a: Path, path_b: Path, *, ignore_cols: list[int]) -> None:
    data_a = np.loadtxt(path_a).T
    data_b = np.loadtxt(path_b).T
    for i, (col_a, col_b) in enumerate(zip(data_a, data_b)):
        if i in ignore_cols:
            continue
        npt.assert_array_equal(col_a, col_b)


@mark.slow
def test_run(tmp_path, mock_data, mock_rand, zlim) -> None:
    cache_ref = YawCacheCreate.make_stage(
        name="ref",
        aliases=stage_helper("ref"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=3,
    ).create(mock_data, mock_rand)
    assert cache_ref.data.data.exists()
    assert cache_ref.data.rand.exists()

    cache_unk = YawCacheCreate.make_stage(
        name="unk",
        aliases=stage_helper("unk"),
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
        patches=f"{tmp_path}/test_ref",
    ).create(mock_data)
    assert cache_unk.data.data.exists()
    assert not cache_unk.data.rand.exists()

    corr_config = dict(
        rmin=500,
        rmax=1500,
        zmin=zlim[0],
        zmax=zlim[1],
        zbin_num=2,
    )

    w_ss = YawAutoCorrelate.make_stage(**corr_config).correlate(cache_ref)
    w_ss.data.sample().to_files(tmp_path / "wss")
    assert_cols_match(write_expect_wss(tmp_path), tmp_path / "wss.dat", ignore_cols=[3])

    w_sp = YawCrossCorrelate.make_stage(**corr_config).correlate(cache_ref, cache_unk)
    w_sp.data.sample().to_files(tmp_path / "wsp")
    assert w_sp.data.rr is None
    assert_cols_match(write_expect_wsp(tmp_path), tmp_path / "wsp.dat", ignore_cols=[3])

    ncc = YawSummarize.make_stage().summarize(w_sp, w_ss)["yaw_cc"]
    ncc.data.to_files(tmp_path / "ncc")
    assert_cols_match(write_expect_ncc(tmp_path), tmp_path / "ncc.dat", ignore_cols=[3])

    dropper = YawCacheDrop.make_stage()
    dropper.drop(cache_ref)
    dropper.drop(cache_unk)
