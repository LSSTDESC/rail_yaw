from __future__ import annotations

import numpy as np
import numpy.testing as npt
from pytest import fixture, mark, raises
from yaw import examples

from rail.yaw_rail import cosmology
from rail.yaw_rail.correlation import ScaledCorrFuncs


@fixture(name="cosmo", scope="module")
def fixture_cosmo():
    return cosmology.get_ccl_cosmology("Planck15")


@fixture(name="corrfuncs")
def fixture_corrfuncs() -> ScaledCorrFuncs:
    edges = np.logspace(np.log10(100.0), np.log10(10_000.0), 4)
    return ScaledCorrFuncs(
        corrfuncs=[examples.cross] * 3,
        rmin=edges[:-1],
        rmax=edges[1:],
        unit="kpc",
    )


@mark.parametrize("name", ["Planck15", "Planck18"])
def test_get_ccl_cosmology(name):
    cosmo = cosmology.get_ccl_cosmology(name)
    assert cosmo["h"] == cosmology.COSMOLOGY_PARAMS[name]["h"]

    # an existing cosmology is passed through unchanged
    assert cosmology.get_ccl_cosmology(cosmo) is cosmo


def test_get_ccl_cosmology_rejects_unknown():
    with raises(ValueError, match=".*unsupported cosmology.*"):
        cosmology.get_ccl_cosmology("Nonsense")


def test_growth_factor(cosmo):
    redshift = np.array([0.0, 0.5, 1.0, 2.0])
    growth = cosmology.growth_factor(redshift, cosmo)

    npt.assert_allclose(growth[0], 1.0, rtol=1e-3)
    assert np.all(np.diff(growth) < 0.0)  # structure grows towards low redshift


def test_scales_to_theta_deg_physical(cosmo):
    radii = np.array([100.0, 1000.0])

    theta = cosmology.scales_to_theta_deg(radii, "kpc", 0.5, cosmo)
    assert np.all(np.diff(theta) > 0.0)  # larger scales subtend larger angles

    # the same physical scale subtends a smaller angle at higher redshift
    far = cosmology.scales_to_theta_deg(radii, "kpc", 1.5, cosmo)
    assert np.all(far < theta)

    # kpc and Mpc must describe the same scale
    npt.assert_allclose(
        cosmology.scales_to_theta_deg(radii / 1000.0, "Mpc", 0.5, cosmo), theta
    )


@mark.parametrize(
    "unit,scale,expect",
    [("deg", 1.0, 1.0), ("arcmin", 60.0, 1.0), ("rad", np.pi, 180.0)],
)
def test_scales_to_theta_deg_angular(cosmo, unit, scale, expect):
    theta = cosmology.scales_to_theta_deg(np.array([scale]), unit, 0.5, cosmo)
    npt.assert_allclose(theta, [expect])


def test_scales_to_theta_deg_rejects_unknown_unit(cosmo):
    with raises(ValueError, match=".*unsupported unit.*"):
        cosmology.scales_to_theta_deg(np.array([1.0]), "parsec", 0.5, cosmo)


def test_matter_correlation_decreases_with_angle(cosmo):
    theta = np.array([0.01, 0.05, 0.2, 1.0])
    w_mm = cosmology.matter_correlation(theta, 0.4, 0.5, cosmo)

    assert np.all(w_mm > 0.0)
    assert np.all(np.diff(w_mm) < 0.0)


def test_matter_template(cosmo, corrfuncs):
    template = cosmology.matter_template(corrfuncs, cosmo)

    num_zbins = len(corrfuncs.binning.mids)
    assert template.shape == (num_zbins, len(corrfuncs))
    assert np.all(template > 0.0)

    # the correlation amplitude falls off with radius in every redshift bin
    assert np.all(np.diff(template, axis=1) < 0.0)
