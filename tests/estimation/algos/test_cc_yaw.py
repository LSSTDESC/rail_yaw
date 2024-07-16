from __future__ import annotations

from pathlib import Path

import numpy as np
import numpy.testing as npt
from pytest import mark, raises, warns

from rail.estimation.algos.cc_yaw import (
    create_yaw_cache_alias,
    YawAutoCorrelate,
    YawCacheCreate,
    YawCrossCorrelate,
    YawSummarize,
)


def test_create_yaw_cache_alias():
    name = "test"
    aliases = create_yaw_cache_alias(name)
    assert all(alias == f"{key}_{name}" for key, alias in aliases.items())


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
        aliases=create_yaw_cache_alias("ref"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=3,
    ).create(data=mock_data, rand=mock_rand)
    assert cache_ref.data.data.exists()
    assert cache_ref.data.rand.exists()

    cache_unk = YawCacheCreate.make_stage(
        name="unk",
        aliases=create_yaw_cache_alias("unk"),
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
    ).create(data=mock_data, patch_source=cache_ref)
    assert cache_unk.data.data.exists()
    assert not cache_unk.data.rand.exists()

    corr_config = dict(
        rmin=500,
        rmax=1500,
        zmin=zlim[0],
        zmax=zlim[1],
        zbin_num=2,
    )

    w_ss = YawAutoCorrelate.make_stage(name="auto_corr", **corr_config).correlate(
        sample=cache_ref
    )
    w_ss.data.sample().to_files(tmp_path / "wss")
    assert_cols_match(write_expect_wss(tmp_path), tmp_path / "wss.dat", ignore_cols=[3])

    w_sp = YawCrossCorrelate.make_stage(name="cross_corr", **corr_config).correlate(
        reference=cache_ref, unknown=cache_unk
    )
    w_sp.data.sample().to_files(tmp_path / "wsp")
    assert w_sp.data.rr is None
    assert_cols_match(write_expect_wsp(tmp_path), tmp_path / "wsp.dat", ignore_cols=[3])

    ncc = YawSummarize.make_stage(name="summarize").summarize(
        cross_corr=w_sp, auto_corr_ref=w_ss
    )
    ncc.data.to_files(tmp_path / "ncc")
    assert_cols_match(write_expect_ncc(tmp_path), tmp_path / "ncc.dat", ignore_cols=[3])

    # cache cleaned up by pytest


@mark.slow
def test_missing_randoms(tmp_path, mock_data, zlim) -> None:
    cache_ref = YawCacheCreate.make_stage(
        name="ref_norand",
        aliases=create_yaw_cache_alias("ref_norand"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=3,
    ).create(data=mock_data)

    cache_unk = YawCacheCreate.make_stage(
        name="unk_norand",
        aliases=create_yaw_cache_alias("unk_norand"),
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
    ).create(data=mock_data, patch_source=cache_ref)

    with raises(ValueError, match=".*no randoms.*"):
        YawCrossCorrelate.make_stage(
            name="cross_corr_norand",
            rmin=500,
            rmax=1500,
            zmin=zlim[0],
            zmax=zlim[1],
            zbin_num=2,
        ).correlate(reference=cache_ref, unknown=cache_unk)


@mark.slow
def test_cache_args(tmp_path, mock_data, mock_rand) -> None:
    cache_ref = YawCacheCreate.make_stage(
        name="ref_n_patch",
        aliases=create_yaw_cache_alias("ref_n_patch"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=3,
    ).create(data=mock_data, rand=mock_rand)
    assert cache_ref.data.data.exists()
    assert cache_ref.data.n_patches() == 3
    np.savetxt(
        str(tmp_path / "coords"),
        cache_ref.data.get_patch_centers().values,
    )

    cache = YawCacheCreate.make_stage(
        name="ref_override",
        aliases=create_yaw_cache_alias("ref_override"),
        path=f"{tmp_path}/test_override",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=cache_ref.data.n_patches() + 1,
    ).create(data=mock_data, rand=mock_rand, patch_source=cache_ref)
    assert cache.data.n_patches() == cache_ref.data.n_patches()

    cache = YawCacheCreate.make_stage(
        name="ref_file",
        aliases=create_yaw_cache_alias("ref_file"),
        path=f"{tmp_path}/test_file",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patch_file=str(tmp_path / "coords"),
    ).create(data=mock_data, rand=mock_rand)
    npt.assert_almost_equal(
        cache.data.get_patch_centers().ra,
        cache_ref.data.get_patch_centers().ra,
    )
    npt.assert_almost_equal(
        cache.data.get_patch_centers().dec,
        cache_ref.data.get_patch_centers().dec,
    )

    with raises(ValueError, match=".*patch.*"):
        YawCacheCreate.make_stage(
            name="ref_no_method",
            aliases=create_yaw_cache_alias("ref_no_method"),
            path=f"{tmp_path}/test_no_method",
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
        ).create(data=mock_data, rand=mock_rand)


def test_warn_thread_num_deprecation():
    with warns(FutureWarning, match=".*thread_num.*"):
        YawCrossCorrelate.make_stage(
            name="cross_corr_thread_num",
            rmin=500,
            rmax=1500,
            zmin=0.1,
            zmax=0.2,
            zbin_num=2,
            thread_num=2,
        )
