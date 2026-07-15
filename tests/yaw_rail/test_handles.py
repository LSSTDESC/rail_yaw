from __future__ import annotations

import numpy as np
import numpy.testing as npt
from pytest import fixture
from yaw import examples

from rail.yaw_rail import cache, combine, correlation, fitting, handles


@fixture(name="corrfuncs")
def fixture_corrfuncs() -> correlation.ScaledCorrFuncs:
    edges = np.logspace(np.log10(30.0), np.log10(30_000.0), 4)
    return correlation.ScaledCorrFuncs(
        corrfuncs=[examples.cross, examples.auto, examples.cross],
        rmin=edges[:-1],
        rmax=edges[1:],
        unit="kpc",
    )


def test_YawCorrFuncHandle(tmp_path, corrfuncs):
    path = tmp_path / "test.hdf5"
    handle = handles.YawCorrFuncHandle("corr_func", corrfuncs, path=path)

    handle.write()  # ._write()
    f = handle.open()  # ._open()
    f.close()

    restored = handle.read(force=True)  # ._read()
    npt.assert_array_equal(restored.rmin, corrfuncs.rmin)
    npt.assert_array_equal(restored.rmax, corrfuncs.rmax)
    assert restored.unit == corrfuncs.unit
    assert len(restored) == len(corrfuncs)
    assert all(a == b for a, b in zip(restored.corrfuncs, corrfuncs.corrfuncs))


def test_YawFitHandle(tmp_path, corrfuncs):
    fit = fitting.YawAmplitudeFit(
        binning=corrfuncs.binning,
        amp_cross=np.array([1.0, 2.0]),
        amp_cross_samples=np.array([[0.9, 1.9], [1.1, 2.1]]),
        amp_auto=np.array([3.0, 4.0]),
        amp_auto_samples=np.array([[2.9, 3.9], [3.1, 4.1]]),
        const_cross=np.array([0.1, 0.2]),
        const_cross_samples=np.array([[0.0, 0.1], [0.2, 0.3]]),
    )
    path = tmp_path / "test.hdf5"
    handle = handles.YawFitHandle("fit", fit, path=path)

    handle.write()
    f = handle.open()
    f.close()

    restored = handle.read(force=True)
    npt.assert_array_equal(restored.amp_cross, fit.amp_cross)
    npt.assert_array_equal(restored.amp_auto_samples, fit.amp_auto_samples)
    npt.assert_array_equal(restored.const_cross, fit.const_cross)
    npt.assert_array_equal(restored.binning.edges, fit.binning.edges)


def test_YawFitHandle_without_constant(tmp_path, corrfuncs):
    fit = fitting.YawAmplitudeFit(
        binning=corrfuncs.binning,
        amp_cross=np.array([1.0, 2.0]),
        amp_cross_samples=np.array([[0.9, 1.9], [1.1, 2.1]]),
        amp_auto=np.array([3.0, 4.0]),
        amp_auto_samples=np.array([[2.9, 3.9], [3.1, 4.1]]),
    )
    handle = handles.YawFitHandle("fit_noconst", fit, path=tmp_path / "test.hdf5")
    handle.write()

    restored = handle.read(force=True)
    assert restored.const_cross is None
    assert restored.const_cross_samples is None


def test_YawNzHandle(tmp_path):
    # the tracer names are deliberately not in alphabetical order: HDF5 iterates
    # groups alphabetically, so their order must be preserved explicitly
    nz = combine.YawClusteringNz(
        redshift=np.array([0.1, 0.2]),
        nz=np.array([1.0, 2.0]),
        nz_err=np.array([0.1, 0.2]),
        covariance=np.diag([0.01, 0.04]),
        tracers={
            "lowz": dict(z=np.array([0.1, 0.2]), nz=np.array([1.0, 2.0])),
            "highz": dict(z=np.array([0.2, 0.3]), nz=np.array([3.0, 4.0])),
        },
    )
    path = tmp_path / "test.hdf5"
    handle = handles.YawNzHandle("nz", nz, path=path)

    handle.write()
    f = handle.open()
    f.close()

    restored = handle.read(force=True)
    npt.assert_array_equal(restored.redshift, nz.redshift)
    npt.assert_array_equal(restored.nz, nz.nz)
    npt.assert_array_equal(restored.covariance, nz.covariance)

    assert list(restored.tracers) == ["lowz", "highz"]
    npt.assert_array_equal(restored.tracers["lowz"]["nz"], nz.tracers["lowz"]["nz"])
    npt.assert_array_equal(restored.tracers["highz"]["z"], nz.tracers["highz"]["z"])


def test_TestYawCacheHandle(tmp_path):
    path = tmp_path / "cache.json"
    c = cache.YawCache.create(tmp_path / "cache")
    handle = handles.YawCacheHandle("cache", c, path=path)

    handle.write()  # ._write()
    assert handle.read(force=True).path == c.path  # ._open(), ._read()
