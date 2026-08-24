from __future__ import annotations

import inspect
import pickle
from pathlib import Path
from subprocess import check_call

import numpy as np
import numpy.testing as npt
from pytest import fixture, mark, raises
from yaw import examples

from rail.estimation.algos import cc_yaw
from rail.yaw_rail.correlation import ScaledCorrFuncs
from rail.yaw_rail.cosmology import get_ccl_cosmology, growth_factor, matter_template


def test_create_yaw_cache_alias():
    name = "test"
    aliases = cc_yaw.create_yaw_cache_alias(name)
    assert all(alias == f"{key}_{name}" for key, alias in aliases.items())


# ----------------------------------------------------------------------------
# YawFitAmplitude / YawNzCombine
#
# These stages are driven with synthetic correlation amplitudes so that the
# fitted parameters are known analytically. `SyntheticCorrFuncs` reuses the
# example pair counts of *yet_another_wizz* for the binning and patch layout, but
# replaces the resampled amplitudes with a known signal. This keeps the tests
# fast and independent of the network.
# ----------------------------------------------------------------------------

NUM_SCALES = 8
SCALE_EDGES = np.logspace(np.log10(300.0), np.log10(30_000.0), NUM_SCALES + 1)

AMP_CROSS = 2.5
AMP_AUTO = 4.0
CONST_CROSS = 0.3


class SyntheticCorrFuncs(ScaledCorrFuncs):
    """A correlation measurement whose resampled amplitudes are injected."""

    def __init__(self, corrfunc, injected: np.ndarray) -> None:
        super().__init__(
            corrfuncs=[corrfunc] * NUM_SCALES,
            rmin=SCALE_EDGES[:-1],
            rmax=SCALE_EDGES[1:],
            unit="kpc",
        )
        self._injected = injected

    def samples(self) -> np.ndarray:
        return self._injected


def make_corrfuncs(corrfunc, amplitude, constant, noise=1e-3, seed=12345):
    """Build a measurement of `w = A * w_mm + C` with per-patch noise."""
    scales = ScaledCorrFuncs(
        corrfuncs=[corrfunc] * NUM_SCALES,
        rmin=SCALE_EDGES[:-1],
        rmax=SCALE_EDGES[1:],
        unit="kpc",
    )
    template = matter_template(scales, get_ccl_cosmology("Planck15"))

    signal = amplitude * template.T + constant  # (num_scales, num_zbins)
    num_patches = corrfunc.num_patches

    rng = np.random.default_rng(seed)
    samples = np.tile(signal, (num_patches, 1, 1))
    samples += rng.normal(0.0, noise * np.abs(signal).mean(), size=samples.shape)

    return SyntheticCorrFuncs(corrfunc, samples)


@fixture(name="cross_corr")
def fixture_cross_corr() -> SyntheticCorrFuncs:
    return make_corrfuncs(examples.cross, AMP_CROSS, CONST_CROSS)


@fixture(name="auto_corr")
def fixture_auto_corr() -> SyntheticCorrFuncs:
    return make_corrfuncs(examples.auto, AMP_AUTO, 0.0, seed=999)


@fixture(name="fit_stage_config")
def fixture_fit_stage_config() -> dict:
    return dict(
        fit_rmin=SCALE_EDGES[0],
        fit_rmax=SCALE_EDGES[-1],
        auto_fit_rmin=SCALE_EDGES[0],
        auto_fit_rmax=SCALE_EDGES[-1],
    )


class StageMaker:
    """Unique stage names are required because the RAIL data store is global."""

    count = 0

    @classmethod
    def unique(cls, prefix: str) -> str:
        cls.count += 1
        return f"{prefix}_{cls.count}"


def run_fit(cross_corr, auto_corr, **config):
    stage = cc_yaw.YawFitAmplitude.make_stage(name=StageMaker.unique("fit"), **config)
    return stage.fit(cross_corr=cross_corr, auto_corr_ref=auto_corr).data


def test_yaw_fit_amplitude(cross_corr, auto_corr, fit_stage_config):
    fit = run_fit(cross_corr, auto_corr, **fit_stage_config)

    num_patches = examples.cross.num_patches
    num_zbins = len(examples.cross.binning.mids)

    assert fit.amp_cross.shape == (num_zbins,)
    assert fit.amp_cross_samples.shape == (num_patches, num_zbins)
    assert fit.amp_auto_samples.shape == (num_patches, num_zbins)

    # the injected amplitudes and the constant are recovered
    npt.assert_allclose(fit.amp_cross, AMP_CROSS, rtol=1e-2)
    npt.assert_allclose(fit.amp_auto, AMP_AUTO, rtol=1e-2)
    npt.assert_allclose(fit.const_cross, CONST_CROSS, rtol=5e-2)

    assert np.all(fit.amp_cross_error > 0.0)
    npt.assert_array_equal(fit.redshift, examples.cross.binning.mids)


def test_yaw_fit_amplitude_without_constant(cross_corr, auto_corr, fit_stage_config):
    fit = run_fit(cross_corr, auto_corr, fit_constant=False, **fit_stage_config)

    assert fit.const_cross is None
    assert fit.const_cross_samples is None

    # not fitting the constant that is present in the data biases the amplitude
    assert not np.allclose(fit.amp_cross, AMP_CROSS, rtol=1e-2)


@mark.parametrize("use_jk_cov", [False, True])
def test_yaw_fit_amplitude_weighting(
    cross_corr, auto_corr, fit_stage_config, use_jk_cov
):
    fit = run_fit(
        cross_corr, auto_corr, use_jk_cov=use_jk_cov, alpha=-2.0, **fit_stage_config
    )
    npt.assert_allclose(fit.amp_cross, AMP_CROSS, rtol=5e-2)


def test_yaw_fit_amplitude_scale_cut(cross_corr, auto_corr, fit_stage_config):
    # a scale cut that excludes every radial bin must be reported
    config = fit_stage_config | dict(fit_rmin=1e6, fit_rmax=1e7)

    with raises(ValueError, match=".*exclude all.*"):
        run_fit(cross_corr, auto_corr, **config)


def test_yaw_fit_amplitude_requires_matching_scales(cross_corr, fit_stage_config):
    mismatched = ScaledCorrFuncs(
        corrfuncs=[examples.auto], rmin=[300.0], rmax=[30_000.0], unit="kpc"
    )

    with raises(ValueError, match=".*same.*radial bins.*"):
        run_fit(cross_corr, mismatched, **fit_stage_config)


def test_yaw_fit_amplitude_requires_matching_redshift_bins(
    cross_corr, auto_corr, fit_stage_config
):
    # the same matter model is fitted to both measurements, so a mismatch in the
    # redshift binning must be reported rather than silently misaligned
    rebinned = examples.auto.bins[:-1]
    mismatched = SyntheticCorrFuncs(rebinned, auto_corr.samples()[:, :, :-1])

    with raises(ValueError, match=".*same.*redshift bins.*"):
        run_fit(cross_corr, mismatched, **fit_stage_config)


def test_create_yaw_nz_combine():
    stage_class = cc_yaw.create_yaw_nz_combine(["bgs", "lrg"])

    assert stage_class.tracers == ["bgs", "lrg"]
    assert [tag for tag, _ in stage_class.inputs] == ["fit_bgs", "fit_lrg"]

    with raises(ValueError, match=".*at least one tracer.*"):
        cc_yaw.create_yaw_nz_combine([])

    with raises(ValueError, match=".*unique.*"):
        cc_yaw.create_yaw_nz_combine(["bgs", "bgs"])


def test_create_yaw_nz_combine_without_module_file():
    # In a notebook (or any interactive session) the caller's module is
    # `__main__`, which has no `__file__`. `ceci` reads
    # `sys.modules[cls.__module__].__file__` while creating the stage class, so
    # the factory must fall back to an importable module for this to work.
    import sys
    import types

    mod_name = "yaw_rail_fileless_module"
    sys.modules[mod_name] = types.ModuleType(mod_name)  # no __file__
    try:
        stage_class = cc_yaw.create_yaw_nz_combine(
            ["faketracera", "faketracerb"], module=mod_name
        )
        # not bound to the file-less module, but to an importable one
        assert stage_class.__module__ == cc_yaw.__name__
        # the original failure surfaced here, when ceci looked up __file__
        stage_class.make_stage(name=StageMaker.unique("combine"))
    finally:
        del sys.modules[mod_name]


def run_combine(tracers, fits, **config):
    stage = cc_yaw.create_yaw_nz_combine(tracers).make_stage(
        name=StageMaker.unique("combine"), **config
    )
    return stage.combine(**{f"fit_{t}": fit for t, fit in zip(tracers, fits)}).data


def test_yaw_nz_combine_single_tracer(cross_corr, auto_corr, fit_stage_config):
    fit = run_fit(cross_corr, auto_corr, **fit_stage_config)

    nz = run_combine(["bgs"], [fit], normalize=False, bias_mode="constant")

    # with a single tracer the combination reproduces n_cc = A_sp / sqrt(A_ss)
    npt.assert_allclose(nz.nz, fit.amp_cross / np.sqrt(fit.amp_auto))
    npt.assert_allclose(nz.redshift, fit.redshift)
    assert np.all(nz.nz_err > 0.0)
    assert list(nz.tracers) == ["bgs"]


def test_yaw_nz_combine_bias_mode(cross_corr, auto_corr, fit_stage_config):
    fit = run_fit(cross_corr, auto_corr, **fit_stage_config)

    constant = run_combine(["bgs"], [fit], normalize=False, bias_mode="constant")
    growth = run_combine(["bgs"], [fit], normalize=False, bias_mode="growth_factor")

    # dividing by b_p = 1 / D(z) suppresses the estimate towards high redshift
    ratio = growth.nz / constant.nz
    npt.assert_allclose(
        ratio, growth_factor(fit.redshift, get_ccl_cosmology("Planck15"))
    )
    assert np.all(np.diff(ratio) < 0.0)


def test_yaw_nz_combine_two_tracers(cross_corr, auto_corr, fit_stage_config):
    fit = run_fit(cross_corr, auto_corr, **fit_stage_config)

    combined = run_combine(["bgs", "lrg"], [fit, fit], normalize=False)
    single = run_combine(["bgs"], [fit], normalize=False)

    # combining a tracer with itself leaves the estimate unchanged but shrinks
    # its uncertainty by sqrt(2)
    npt.assert_allclose(combined.nz, single.nz)
    npt.assert_allclose(combined.nz_err, single.nz_err / np.sqrt(2.0))
    assert list(combined.tracers) == ["bgs", "lrg"]


def test_yaw_nz_combine_normalize(cross_corr, auto_corr, fit_stage_config):
    fit = run_fit(cross_corr, auto_corr, **fit_stage_config)

    raw = run_combine(["bgs"], [fit], normalize=False)
    normalized = run_combine(["bgs"], [fit], normalize=True)

    # normalisation rescales the estimate and its uncertainty by one factor
    scale = raw.nz / normalized.nz
    npt.assert_allclose(scale, scale[0])
    npt.assert_allclose(raw.nz_err / normalized.nz_err, scale[0])

    npt.assert_allclose(normalized.covariance, np.diag(normalized.nz_err**2))


@fixture(name="corr_config")
def fixture_corr_config(zlim):
    return dict(
        rmin=500,
        rmax=1500,
        zmin=zlim[0],
        zmax=zlim[1],
        num_bins=2,
        max_workers=1,
    )


@mark.slow
def test_multi_scale_correlate(tmp_path, mock_data, mock_rand, zlim) -> None:
    """The correlate stages must retain every radial bin, not just the first."""
    edges = np.logspace(np.log10(500.0), np.log10(3000.0), 4)

    cache_ref = cc_yaw.YawCacheCreate.make_stage(
        name="ref_scales",
        aliases=cc_yaw.create_yaw_cache_alias("ref_scales"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patch_num=5,
        max_workers=1,
    ).create(data=mock_data, rand=mock_rand)

    cache_unk = cc_yaw.YawCacheCreate.make_stage(
        name="unk_scales",
        aliases=cc_yaw.create_yaw_cache_alias("unk_scales"),
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
        max_workers=1,
    ).create(data=mock_data, patch_source=cache_ref)

    config = dict(
        rmin=edges[:-1].tolist(),
        rmax=edges[1:].tolist(),
        unit="kpc",
        zmin=zlim[0],
        zmax=zlim[1],
        num_bins=2,
        max_workers=1,
    )

    auto = cc_yaw.YawAutoCorrelate.make_stage(name="auto_scales", **config).correlate(
        sample=cache_ref
    )
    cross = cc_yaw.YawCrossCorrelate.make_stage(
        name="cross_scales", **config
    ).correlate(reference=cache_ref, unknown=cache_unk)

    for corr in (auto.data, cross.data):
        assert len(corr) == 3
        npt.assert_allclose(corr.rmin, edges[:-1])
        npt.assert_allclose(corr.rmax, edges[1:])
        assert corr.unit == "kpc"
        assert corr.samples().shape == (5, 3, 2)  # patches, scales, zbins

    # the radial bins must carry different measurements
    samples = cross.data.samples()
    assert not np.allclose(samples[:, 0, :], samples[:, 1, :])

    # the fit stage consumes the multi-scale measurement end to end
    fit = cc_yaw.YawFitAmplitude.make_stage(
        name="fit_scales",
        fit_rmin=edges[0],
        fit_rmax=edges[-1],
        auto_fit_rmin=edges[0],
        auto_fit_rmax=edges[-1],
        use_jk_cov=False,  # 5 patches cannot support a 3-scale covariance
    ).fit(cross_corr=cross, auto_corr_ref=auto)

    assert fit.data.amp_cross.shape == (2,)
    assert np.all(np.isfinite(fit.data.amp_cross))


@mark.slow
def test_missing_randoms(tmp_path, mock_data, corr_config) -> None:
    # create two caches without randoms and try running cross-correlations
    cache_ref = cc_yaw.YawCacheCreate.make_stage(
        name="ref_norand",
        aliases=cc_yaw.create_yaw_cache_alias("ref_norand"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patch_num=3,
        max_workers=1,
    ).create(data=mock_data)

    cache_unk = cc_yaw.YawCacheCreate.make_stage(
        name="unk_norand",
        aliases=cc_yaw.create_yaw_cache_alias("unk_norand"),
        path=f"{tmp_path}/test_unk",
        ra_name="ra",
        dec_name="dec",
    ).create(data=mock_data, patch_source=cache_ref)

    with raises(ValueError, match=".*no randoms.*"):
        cc_yaw.YawAutoCorrelate.make_stage(
            name="auto_corr_norand",
            **corr_config,
        ).correlate(sample=cache_ref)

    with raises(ValueError, match=".*no randoms.*"):
        cc_yaw.YawCrossCorrelate.make_stage(
            name="cross_corr_norand",
            **corr_config,
        ).correlate(reference=cache_ref, unknown=cache_unk)


@mark.slow
def test_cache_args(tmp_path, mock_data, mock_rand) -> None:
    # check that the patch_num parameter works
    cache_ref = cc_yaw.YawCacheCreate.make_stage(
        name="ref_n_patch",
        aliases=cc_yaw.create_yaw_cache_alias("ref_n_patch"),
        path=f"{tmp_path}/test_ref",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patch_num=3,
        max_workers=1,
    ).create(data=mock_data, rand=mock_rand)
    assert cache_ref.data.data.exists()
    assert cache_ref.data.num_patches == 3
    # save coordinates for later use
    np.savetxt(
        str(tmp_path / "coords"),
        cache_ref.data.get_patch_centers().data,
    )

    # check that patch_source stage input overwrites patch_num config parameter
    # (don't need to test other parameters explicitly)
    cache = cc_yaw.YawCacheCreate.make_stage(
        name="ref_override",
        aliases=cc_yaw.create_yaw_cache_alias("ref_override"),
        path=f"{tmp_path}/test_override",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patch_num=cache_ref.data.num_patches + 1,
        max_workers=1,
    ).create(data=mock_data, rand=mock_rand, patch_source=cache_ref)
    assert cache.data.num_patches == cache_ref.data.num_patches

    # check that patch_file config reproduces the original patch centers
    cache = cc_yaw.YawCacheCreate.make_stage(
        name="ref_file",
        aliases=cc_yaw.create_yaw_cache_alias("ref_file"),
        path=f"{tmp_path}/test_file",
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        patch_file=str(tmp_path / "coords"),
        max_workers=1,
    ).create(data=mock_data, rand=mock_rand)
    npt.assert_almost_equal(
        cache.data.get_patch_centers().ra,
        cache_ref.data.get_patch_centers().ra,
    )
    npt.assert_almost_equal(
        cache.data.get_patch_centers().dec,
        cache_ref.data.get_patch_centers().dec,
    )

    # check that an exception pointing to missing patch configuration is raised
    # if none of the methods is used
    with raises(ValueError, match=".*patch.*"):
        cc_yaw.YawCacheCreate.make_stage(
            name="ref_no_method",
            aliases=cc_yaw.create_yaw_cache_alias("ref_no_method"),
            path=f"{tmp_path}/test_no_method",
            ra_name="ra",
            dec_name="dec",
            redshift_name="z",
            max_workers=1,
        ).create(data=mock_data, rand=mock_rand)


def write_expect_ncc(path: Path) -> Path:
    # output that the example pipeline should produce
    # NOTE: need to update this after any changes to the algorithms
    target_path = path / "ncc_expect.txt"
    with open(target_path, "w") as f:
        f.write("""# n(z) estimate with symmetric 68% percentile confidence
#   (z_low    z_high]         nz     nz_err
 0.2000000  0.4000000  0.1160194  0.1174020
 0.4000000  0.6000000  0.0898476  0.0376813
 0.6000000  0.8000000  0.1367271  0.0636527
 0.8000000  1.0000000  0.2435591  0.0335718
 1.0000000  1.2000000  0.1789916  0.0823615
 1.2000000  1.4000000  0.1954614  0.0385094
 1.4000000  1.6000000  0.1802148  0.0822200
 1.6000000  1.8000000  0.1872289  0.0845762
""")
    return target_path


@mark.slow
def test_ceci_pipeline(tmp_path) -> None:
    # build and run the example pipeline in a temporary directory
    # NOTE: for debugging, change the DEBUG_LOG_PATH to avoid automatic removal
    # of the logs
    from rail.pipelines.estimation import (
        build_pipeline as pipeline_build_scipt,  # pylint: disable=C0415; should be a robust method to locate the pipeline generation script
    )

    build_script = inspect.getfile(pipeline_build_scipt)

    # build the pipeline config and run with ceci
    DEBUG_LOG_PATH = "/dev/null"
    with open(DEBUG_LOG_PATH, "w") as f:
        redirect = dict(stdout=f, stderr=f)
        check_call(["python3", str(build_script), "--root", str(tmp_path)], **redirect)
        check_call(["ceci", str(tmp_path / "yaw_pipeline.yml")], **redirect)

    # locate summarizer staage output and convert to YAW text file output
    with open(tmp_path / "data" / "output_summarize.pkl", "rb") as f:
        ncc = pickle.load(f)
        output_prefix = str(tmp_path / "output")
        ncc.to_files(output_prefix)

    # compare the output with the expected result after parsing both through
    # ASCII files to avoid potential numerical differences
    expect_path = write_expect_ncc(tmp_path)
    expect_data = np.loadtxt(expect_path).T
    output_data = np.loadtxt(f"{output_prefix}.dat").T
    for i, (col_a, col_b) in enumerate(zip(output_data, expect_data)):
        if i == 3:  # error column differs every time since using patch_num
            break
        npt.assert_array_equal(col_a, col_b)


@mark.slow
def test_ceci_pipeline_nz(tmp_path) -> None:
    # build and run the clustering redshift pipeline, which measures the
    # correlation amplitudes of two reference tracers in multiple radial bins,
    # fits them and combines them into a single redshift estimate
    from rail.pipelines.estimation import (  # pylint: disable=C0415
        build_pipeline_nz as pipeline_build_script,
    )

    build_script = inspect.getfile(pipeline_build_script)

    DEBUG_LOG_PATH = "/dev/null"
    with open(DEBUG_LOG_PATH, "w") as f:
        redirect = dict(stdout=f, stderr=f)
        check_call(["python3", str(build_script), "--root", str(tmp_path)], **redirect)
        check_call(["ceci", str(tmp_path / "yaw_nz_pipeline.yml")], **redirect)

    from rail.yaw_rail.handles import YawNzHandle  # pylint: disable=C0415

    nz = YawNzHandle(
        "nz", None, path=str(tmp_path / "data" / "output_combine.hdf5")
    ).read()

    tracers = pipeline_build_script.TRACERS
    assert list(nz.tracers) == list(tracers)

    # the estimate spans the union of the redshift ranges of both tracers, which
    # overlap in two bins that are combined into one
    z_lowz = nz.tracers["lowz"]["z"]
    z_highz = nz.tracers["highz"]["z"]
    shared = np.intersect1d(np.round(z_lowz, 6), np.round(z_highz, 6))
    assert len(shared) == 2
    assert len(nz.redshift) == len(z_lowz) + len(z_highz) - len(shared)
    npt.assert_allclose(nz.redshift, np.unique(np.r_[z_lowz, z_highz]))

    assert np.all(np.isfinite(nz.nz))
    assert np.all(nz.nz_err > 0.0)

    # the covariance must be symmetric and positive semi-definite
    npt.assert_allclose(nz.covariance, nz.covariance.T)
    assert np.linalg.eigvalsh(nz.covariance).min() >= 0.0
