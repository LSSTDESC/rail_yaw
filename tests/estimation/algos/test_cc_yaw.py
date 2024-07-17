from __future__ import annotations

import inspect
import pickle
from pathlib import Path
from subprocess import check_call

import numpy as np
import numpy.testing as npt
from pytest import mark, raises, warns

from rail.estimation.algos import cc_yaw


def test_create_yaw_cache_alias():
    name = "test"
    aliases = cc_yaw.create_yaw_cache_alias(name)
    assert all(alias == f"{key}_{name}" for key, alias in aliases.items())


@mark.slow
def test_missing_randoms(tmp_path, mock_data, zlim) -> None:
    cache_ref = cc_yaw.YawCacheCreate.make_stage(
        name="ref_norand",
        aliases=cc_yaw.create_yaw_cache_alias("ref_norand"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=3,
    ).create(data=mock_data)

    cache_unk = cc_yaw.YawCacheCreate.make_stage(
        name="unk_norand",
        aliases=cc_yaw.create_yaw_cache_alias("unk_norand"),
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
    ).create(data=mock_data, patch_source=cache_ref)

    with raises(ValueError, match=".*no randoms.*"):
        cc_yaw.YawCrossCorrelate.make_stage(
            name="cross_corr_norand",
            rmin=500,
            rmax=1500,
            zmin=zlim[0],
            zmax=zlim[1],
            zbin_num=2,
        ).correlate(reference=cache_ref, unknown=cache_unk)


@mark.slow
def test_cache_args(tmp_path, mock_data, mock_rand) -> None:
    cache_ref = cc_yaw.YawCacheCreate.make_stage(
        name="ref_n_patch",
        aliases=cc_yaw.create_yaw_cache_alias("ref_n_patch"),
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

    cache = cc_yaw.YawCacheCreate.make_stage(
        name="ref_override",
        aliases=cc_yaw.create_yaw_cache_alias("ref_override"),
        path=f"{tmp_path}/test_override",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        n_patches=cache_ref.data.n_patches() + 1,
    ).create(data=mock_data, rand=mock_rand, patch_source=cache_ref)
    assert cache.data.n_patches() == cache_ref.data.n_patches()

    cache = cc_yaw.YawCacheCreate.make_stage(
        name="ref_file",
        aliases=cc_yaw.create_yaw_cache_alias("ref_file"),
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
        cc_yaw.YawCacheCreate.make_stage(
            name="ref_no_method",
            aliases=cc_yaw.create_yaw_cache_alias("ref_no_method"),
            path=f"{tmp_path}/test_no_method",
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
        ).create(data=mock_data, rand=mock_rand)


def test_warn_thread_num_deprecation():
    with warns(FutureWarning, match=".*thread_num.*"):
        cc_yaw.YawCrossCorrelate.make_stage(
            name="cross_corr_thread_num",
            rmin=500,
            rmax=1500,
            zmin=0.1,
            zmax=0.2,
            zbin_num=2,
            thread_num=2,
        )


def write_expect_ncc(path: Path) -> Path:
    target = path / "ncc_expect.txt"
    with open(target, "w") as f:
        f.write(
            """# n(z) estimate with symmetric 68% percentile confidence
#    z_low     z_high         nz     nz_err
 0.2000000  0.4000000  0.1160194  0.0957173
 0.4000000  0.6000000  0.0898476  0.0616907
 0.6000000  0.8000000  0.1367271  0.0815823
 0.8000000  1.0000000  0.2435591  0.0549643
 1.0000000  1.2000000  0.1789916  0.0656216
 1.2000000  1.4000000  0.1954614  0.0690626
 1.4000000  1.6000000  0.1802148  0.0765422
 1.6000000  1.8000000  0.1872289  0.0729730
"""
        )
    return target


@mark.slow
def test_ceci_pipeline(tmp_path) -> None:
    from rail.pipelines.estimation import (  # pylint: disable=C0415
        build_pipeline as pipeline_build_scipt,
    )

    # build and run the pipeline
    DEBUG_LOG_PATH = "/dev/null"  # change to some non-temporary location
    build_script = inspect.getfile(pipeline_build_scipt)
    with open(DEBUG_LOG_PATH, "w") as f:
        redirect = dict(stdout=f, stderr=f)
        check_call(["python3", str(build_script), "--root", str(tmp_path)], **redirect)
        check_call(["ceci", str(tmp_path / "yaw_pipeline.yml")], **redirect)

    # convert output to YAW standard YAW text files
    with open(tmp_path / "data" / "output_summarize.pkl", "rb") as f:
        ncc = pickle.load(f)
        output_prefix = str(tmp_path / "output")
        ncc.to_files(output_prefix)

    # check results, ingore error column since patch centers are random
    expect_path = write_expect_ncc(tmp_path)
    expect_data = np.loadtxt(expect_path).T
    output_data = np.loadtxt(f"{output_prefix}.dat").T
    for i, (col_a, col_b) in enumerate(zip(expect_data, output_data)):
        if i == 3:
            break
        npt.assert_array_equal(col_a, col_b)
