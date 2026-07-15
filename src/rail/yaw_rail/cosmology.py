"""
This file implements the fiducial cosmology and the model for the angular matter
correlation function that the measured correlation amplitudes are fitted
against.

The same cosmology name is used by *yet_another_wizz* to convert physical scales
to angular separations and by CCL to model the matter correlation function, so
that both are computed consistently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyccl as ccl

if TYPE_CHECKING:
    from numpy.typing import ArrayLike, NDArray
    from yaw import Binning

    from rail.yaw_rail.correlation import ScaledCorrFuncs

__all__ = [
    "COSMOLOGY_PARAMS",
    "get_ccl_cosmology",
    "growth_factor",
    "matter_template",
    "scales_to_theta_deg",
]


COSMOLOGY_PARAMS: dict[str, dict[str, float]] = {
    "Planck15": dict(
        Omega_c=0.2589, Omega_b=0.0486, h=0.6774, n_s=0.9667, sigma8=0.8159
    ),
    "Planck18": dict(
        Omega_c=0.2607, Omega_b=0.04897, h=0.6766, n_s=0.9665, sigma8=0.8102
    ),
}
"""Parameters of the supported fiducial cosmologies. The names match those of the
astropy cosmologies that *yet_another_wizz* accepts."""

DEFAULT_TRANSFER_FUNCTION = "eisenstein_hu"
"""The default transfer function used to model the matter correlation function.

CCL defaults to `"boltzmann_camb"`, which requires `camb` to be installed. The
fitting approximation is used instead so that the matter model is available
without an external Boltzmann solver. It differs from a Boltzmann solution at the
per-cent level, which mostly cancels in the ratio of correlation amplitudes that
forms the clustering redshift estimate."""

_PHYSICAL_UNIT_TO_MPC = {"kpc": 1e-3, "Mpc": 1.0}
"""Conversion of the physical (transverse angular diameter) units to Mpc."""

_COMOVING_UNIT_TO_MPC = {"kpc/h": 1e-3, "Mpc/h": 1.0}
"""Conversion of the comoving (transverse) units to Mpc."""

_ANGULAR_UNIT_TO_DEG = {
    "deg": 1.0,
    "rad": 180.0 / np.pi,
    "arcmin": 1.0 / 60.0,
    "arcsec": 1.0 / 3600.0,
}
"""Conversion of the angular units to degrees."""


def get_ccl_cosmology(
    cosmology: str | ccl.Cosmology,
    transfer_function: str = DEFAULT_TRANSFER_FUNCTION,
) -> ccl.Cosmology:
    """
    Construct the CCL cosmology with the given name.

    Parameters
    ----------
    cosmology : str or pyccl.Cosmology
        Name of one of the cosmologies listed in `COSMOLOGY_PARAMS`. An existing
        CCL cosmology is passed through unchanged.
    transfer_function : str
        The transfer function used to model the matter power spectrum, see
        `DEFAULT_TRANSFER_FUNCTION`.

    Returns
    -------
    pyccl.Cosmology
        The fiducial cosmology.
    """
    if isinstance(cosmology, ccl.Cosmology):
        return cosmology

    try:
        params = COSMOLOGY_PARAMS[cosmology]
    except KeyError as err:
        supported = ", ".join(COSMOLOGY_PARAMS)
        raise ValueError(
            f"unsupported cosmology {cosmology!r}, must be one of: {supported}"
        ) from err

    return ccl.Cosmology(**params, transfer_function=transfer_function)


def growth_factor(redshift: ArrayLike, cosmology: ccl.Cosmology) -> NDArray:
    """
    Evaluate the linear growth factor `D(z)`, normalised to `D(0) = 1`.

    Parameters
    ----------
    redshift : ArrayLike
        The redshifts at which to evaluate the growth factor.
    cosmology : pyccl.Cosmology
        The fiducial cosmology.

    Returns
    -------
    NDArray
        The growth factor.
    """
    scale_factor = 1.0 / (1.0 + np.asarray(redshift, dtype=float))
    return ccl.growth_factor(cosmology, scale_factor)


def scales_to_theta_deg(
    scales: ArrayLike,
    unit: str,
    redshift: float,
    cosmology: ccl.Cosmology,
) -> NDArray:
    """
    Convert correlation scales to angular separations at a given redshift.

    Physical scales are converted with the angular diameter distance and comoving
    scales with the comoving radial distance, matching the conventions of
    *yet_another_wizz*. Scales that are already angular are only converted to
    degrees.

    Parameters
    ----------
    scales : ArrayLike
        The scales to convert, in `unit`.
    unit : str
        The unit of the scales, see `yaw.options.Unit`.
    redshift : float
        The redshift at which to evaluate the distances.
    cosmology : pyccl.Cosmology
        The fiducial cosmology.

    Returns
    -------
    NDArray
        The angular separations in degrees.

    Raises
    ------
    ValueError
        If the unit is not supported.
    """
    scales = np.asarray(scales, dtype=float)
    scale_factor = 1.0 / (1.0 + redshift)

    if unit in _ANGULAR_UNIT_TO_DEG:
        return scales * _ANGULAR_UNIT_TO_DEG[unit]

    if unit in _PHYSICAL_UNIT_TO_MPC:
        scales_mpc = scales * _PHYSICAL_UNIT_TO_MPC[unit]
        distance = ccl.angular_diameter_distance(cosmology, scale_factor)

    elif unit in _COMOVING_UNIT_TO_MPC:
        scales_mpc = scales * _COMOVING_UNIT_TO_MPC[unit]
        distance = ccl.comoving_radial_distance(cosmology, scale_factor)

    else:
        supported = ", ".join(
            (*_PHYSICAL_UNIT_TO_MPC, *_COMOVING_UNIT_TO_MPC, *_ANGULAR_UNIT_TO_DEG)
        )
        raise ValueError(f"unsupported unit {unit!r}, must be one of: {supported}")

    return np.degrees(scales_mpc / distance)


def matter_correlation(
    theta_deg: ArrayLike,
    zmin: float,
    zmax: float,
    cosmology: ccl.Cosmology,
    num_z: int = 256,
    num_ell: int = 512,
) -> NDArray:
    """
    Model the angular matter correlation function `w_mm(theta)` for a redshift
    bin.

    The model assumes a top-hat redshift distribution between `zmin` and `zmax`
    and unit galaxy bias, i.e. it traces the matter distribution.

    Parameters
    ----------
    theta_deg : ArrayLike
        The angular separations in degrees at which to evaluate the model.
    zmin : float
        Lower edge of the redshift bin.
    zmax : float
        Upper edge of the redshift bin.
    cosmology : pyccl.Cosmology
        The fiducial cosmology.
    num_z : int
        Number of samples of the top-hat redshift distribution.
    num_ell : int
        Number of multipoles used to compute the angular power spectrum.

    Returns
    -------
    NDArray
        The correlation amplitude at each angular separation.
    """
    redshifts = np.linspace(zmin, zmax, num_z)
    weights = np.ones_like(redshifts)
    weights /= np.trapezoid(weights, redshifts)

    tracer = ccl.NumberCountsTracer(
        cosmology,
        has_rsd=False,
        dndz=(redshifts, weights),
        bias=(redshifts, np.ones_like(redshifts)),
    )

    ell = np.geomspace(2, 2e4, num_ell)
    c_ell = ccl.angular_cl(cosmology, tracer, tracer, ell)

    return ccl.correlation(
        cosmo=cosmology, ell=ell, C_ell=c_ell, theta=theta_deg, type="NN"
    )


def matter_template(
    corrfuncs: ScaledCorrFuncs,
    cosmology: ccl.Cosmology,
) -> NDArray:
    """
    Model the matter correlation amplitude in every redshift and radial bin of a
    correlation measurement.

    Each redshift bin is modelled with a top-hat redshift distribution spanning
    the bin, evaluated at the geometric center of each radial bin.

    Parameters
    ----------
    corrfuncs : ScaledCorrFuncs
        The correlation measurement that defines the redshift and radial bins.
    cosmology : pyccl.Cosmology
        The fiducial cosmology.

    Returns
    -------
    NDArray
        The matter correlation amplitude with shape (num_zbins, num_scales).
    """
    binning: Binning = corrfuncs.binning
    r_center = corrfuncs.r_center

    template = []
    for zmin, zmax, redshift in zip(binning.left, binning.right, binning.mids):
        theta_deg = scales_to_theta_deg(r_center, corrfuncs.unit, redshift, cosmology)
        template.append(matter_correlation(theta_deg, zmin, zmax, cosmology))

    return np.asarray(template)
