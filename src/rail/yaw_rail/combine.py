"""
This file implements the conversion of the fitted correlation amplitudes to a
clustering redshift estimate and the combination of estimates obtained from
multiple reference tracers.

The clustering redshift estimate of a single tracer is

    n_cc(z) = A_sp(z) / sqrt(A_ss(z)) / b_p(z),

where `A_sp` and `A_ss` are the fitted amplitudes of the cross-correlation and of
the reference autocorrelation, which corrects for the evolving bias of the
reference sample, and `b_p` models the bias evolution of the unknown sample.

Estimates from tracers that cover different, possibly only partially overlapping
redshift ranges are combined by inverse-variance weighting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np
from scipy.interpolate import BSpline
from scipy.optimize import nnls

from rail.yaw_rail.cosmology import growth_factor

if TYPE_CHECKING:
    from h5py import Group
    from numpy.typing import ArrayLike, NDArray
    from pyccl import Cosmology

__all__ = [
    "BIAS_MODES",
    "YawClusteringNz",
    "bias_evolution",
    "combine_inverse_variance",
    "fit_nonneg_bspline",
    "ncc_from_amplitudes",
]

BIAS_MODES = ("growth_factor", "constant")
"""The supported models for the bias evolution of the unknown sample."""


def bias_evolution(
    redshift: ArrayLike,
    mode: str,
    cosmology: Cosmology,
) -> NDArray:
    """
    Model the galaxy bias evolution `b_p(z)` of the unknown sample.

    Parameters
    ----------
    redshift : ArrayLike
        The redshifts at which to evaluate the bias.
    mode : str
        Either `"growth_factor"` for `b_p = 1 / D(z)`, which keeps the product of
        bias and growth factor constant, or `"constant"` for `b_p = 1`.
    cosmology : pyccl.Cosmology
        The fiducial cosmology.

    Returns
    -------
    NDArray
        The bias at each redshift.

    Raises
    ------
    ValueError
        If the mode is not supported.
    """
    redshift = np.asarray(redshift, dtype=float)

    if mode == "growth_factor":
        return 1.0 / growth_factor(redshift, cosmology)
    if mode == "constant":
        return np.ones_like(redshift)

    supported = ", ".join(BIAS_MODES)
    raise ValueError(f"unsupported bias mode {mode!r}, must be one of: {supported}")


def _sample_covariance(samples: NDArray) -> NDArray:
    """Compute the jackknife covariance of samples with shape (num_samples,
    num_zbins)."""
    finite = np.all(np.isfinite(samples), axis=1)
    samples = samples[finite]

    num_samples = len(samples)
    if num_samples < 2:
        raise ValueError(
            "not enough finite jackknife samples to estimate a covariance matrix"
        )

    deviation = samples - samples.mean(axis=0)
    return (num_samples - 1) / num_samples * (deviation.T @ deviation)


def ncc_from_amplitudes(
    amp_cross: NDArray,
    amp_cross_samples: NDArray,
    amp_auto: NDArray,
    amp_auto_samples: NDArray,
    bias: NDArray,
    *,
    paired: bool = False,
) -> tuple[NDArray, NDArray, NDArray]:
    """
    Compute the clustering redshift estimate `n_cc = A_sp / sqrt(A_ss) / b_p`.

    Uncertainties are estimated in one of two ways. If the jackknife samples of
    the cross- and autocorrelation are `paired`, i.e. drawn from the same spatial
    patches, the ratio is formed sample by sample and its covariance estimated
    directly. Otherwise the covariances of numerator and denominator are
    estimated separately and propagated through the analytic Jacobian

        d n_cc / d A_sp = 1 / (sqrt(A_ss) * b_p)
        d n_cc / d A_ss = - n_cc / (2 * A_ss).

    Parameters
    ----------
    amp_cross : NDArray
        The fitted cross-correlation amplitude with shape (num_zbins,).
    amp_cross_samples : NDArray
        Its jackknife samples with shape (num_patches, num_zbins).
    amp_auto : NDArray
        The fitted autocorrelation amplitude of the reference sample.
    amp_auto_samples : NDArray
        Its jackknife samples.
    bias : NDArray
        The bias evolution of the unknown sample with shape (num_zbins,).
    paired : bool
        Whether the jackknife samples of the two measurements are paired.

    Returns
    -------
    tuple of NDArray
        The estimate, its uncertainty and its covariance matrix.
    """
    with np.errstate(invalid="ignore", divide="ignore"):
        ncc = amp_cross / np.sqrt(amp_auto) / bias

        if paired:
            covariance = _sample_covariance(
                amp_cross_samples / np.sqrt(amp_auto_samples) / bias[None, :]
            )

        else:
            cov_cross = _sample_covariance(amp_cross_samples)
            cov_auto = _sample_covariance(amp_auto_samples)

            jac_cross = 1.0 / (np.sqrt(amp_auto) * bias)
            jac_auto = -ncc / (2.0 * amp_auto)

            covariance = jac_cross[:, None] * cov_cross * jac_cross[None, :]
            covariance += jac_auto[:, None] * cov_auto * jac_auto[None, :]

    error = np.sqrt(np.clip(np.diag(covariance), 0.0, None))
    return ncc, error, covariance


def combine_inverse_variance(
    series: dict[str, dict[str, NDArray]],
    round_z_decimals: int = 6,
) -> tuple[NDArray, NDArray, NDArray]:
    """
    Combine the clustering redshift estimates of multiple tracers by
    inverse-variance weighting.

    Tracers may cover different redshift ranges. Their estimates are matched by
    redshift, rounded to `round_z_decimals`, so that bins shared by several
    tracers are combined and bins measured by a single tracer are passed through.
    This requires the redshift binning of the tracers to coincide where their
    ranges overlap.

    Parameters
    ----------
    series : dict
        Maps the name of each tracer to a dictionary with the keys `"z"`, `"nz"`
        and `"nz_err"`.
    round_z_decimals : int
        The number of decimals to which redshifts are rounded when matching bins
        of different tracers.

    Returns
    -------
    tuple of NDArray
        The redshifts, the combined estimate and its uncertainty, sorted by
        redshift.

    Raises
    ------
    ValueError
        If no tracer contributes any finite measurement, or if the redshift
        binning of tracers with overlapping ranges does not coincide.
    """
    buckets: dict[float, list[tuple[float, float, float]]] = {}
    # only the points that survive the filtering below can ever be combined, so
    # the binning is checked against those rather than the raw redshift ranges
    valid_z: dict[str, NDArray] = {}

    for name, tracer in series.items():
        redshift = np.asarray(tracer["z"], dtype=float)
        estimate = np.asarray(tracer["nz"], dtype=float)
        error = np.asarray(tracer["nz_err"], dtype=float)

        finite = (
            np.isfinite(redshift)
            & np.isfinite(estimate)
            & np.isfinite(error)
            & (error > 0.0)
        )
        valid_z[name] = redshift[finite]

        for z, nz, err in zip(redshift[finite], estimate[finite], error[finite]):
            buckets.setdefault(round(float(z), round_z_decimals), []).append(
                (float(z), float(nz), float(err))
            )

    if not buckets:
        raise ValueError("no tracer provides a finite clustering redshift estimate")

    _check_binning_overlaps(valid_z, buckets)

    redshift = np.empty(len(buckets))
    estimate = np.empty(len(buckets))
    error = np.empty(len(buckets))

    for i, entries in enumerate(buckets.values()):
        z, nz, err = np.transpose(entries)

        weight = 1.0 / err**2
        total = weight.sum()

        redshift[i] = np.sum(weight * z) / total
        estimate[i] = np.sum(weight * nz) / total
        error[i] = np.sqrt(1.0 / total)

    order = np.argsort(redshift)
    return redshift[order], estimate[order], error[order]


def _check_binning_overlaps(
    valid_z: dict[str, NDArray],
    buckets: dict[float, list],
) -> None:
    """
    Verify that tracers whose redshift ranges overlap share the redshift bins in
    the overlap, since otherwise they are never actually combined.
    """
    ranges = {
        name: (float(np.min(z)), float(np.max(z)))
        for name, z in valid_z.items()
        if len(z) > 0
    }
    shared = {z for z, entries in buckets.items() if len(entries) > 1}

    for i, (name_a, (zmin_a, zmax_a)) in enumerate(ranges.items()):
        for name_b, (zmin_b, zmax_b) in list(ranges.items())[i + 1 :]:
            low = max(zmin_a, zmin_b)
            high = min(zmax_a, zmax_b)
            if low > high:
                continue  # redshift ranges are disjoint, nothing to combine

            if not any(low <= z <= high for z in shared):
                raise ValueError(
                    f"the redshift ranges of tracers {name_a!r} and {name_b!r} "
                    f"overlap in [{low:.3f}, {high:.3f}], but they share no "
                    "redshift bin there, so they cannot be combined; their "
                    "redshift binning must coincide where they overlap"
                )


def fit_nonneg_bspline(
    redshift: NDArray,
    estimate: NDArray,
    error: NDArray | None = None,
    degree: int = 3,
    num_knots: int = 25,
) -> BSpline:
    """
    Fit a non-negative B-spline to a clustering redshift estimate.

    B-spline basis functions are non-negative, so constraining the coefficients
    to be non-negative, which is solved by non-negative least squares,
    guarantees a non-negative spline everywhere. Integrating this spline avoids
    the bias that a plain sum incurs when some measured values scatter below
    zero.

    Parameters
    ----------
    redshift : NDArray
        The redshifts of the estimate.
    estimate : NDArray
        The clustering redshift estimate.
    error : NDArray, optional
        The uncertainty of the estimate, used as inverse weights in the fit.
    degree : int
        The degree of the spline.
    num_knots : int
        The number of interior knots.

    Returns
    -------
    scipy.interpolate.BSpline
        The fitted spline.

    Raises
    ------
    ValueError
        If there are not enough finite points to constrain the spline.
    """
    finite = np.isfinite(redshift) & np.isfinite(estimate)
    if error is not None:
        finite &= np.isfinite(error) & (error > 0.0)

    redshift = redshift[finite]
    estimate = estimate[finite]
    error = error[finite] if error is not None else None

    order = np.argsort(redshift)
    redshift = redshift[order]
    estimate = estimate[order]
    error = error[order] if error is not None else None

    if redshift.size < degree + 2:
        raise ValueError("not enough finite points to fit a B-spline")

    num_interior = min(num_knots, max(0, redshift.size - degree - 1))
    interior = np.linspace(redshift[0], redshift[-1], num_interior + 2)[1:-1]
    knots = np.r_[[redshift[0]] * (degree + 1), interior, [redshift[-1]] * (degree + 1)]

    design = BSpline.design_matrix(redshift, knots, degree).toarray()
    if error is None:
        coeff, _ = nnls(design, estimate)
    else:
        weight = 1.0 / error
        coeff, _ = nnls(design * weight[:, None], estimate * weight)

    return BSpline(knots, coeff, degree)


@dataclass
class YawClusteringNz:
    """
    A clustering redshift estimate, combined from one or more reference tracers.

    This is not a probability density: the estimate is a ratio of correlation
    amplitudes and may scatter below zero. Further modelling of the output is
    required.

    Parameters
    ----------
    redshift : NDArray
        The redshifts of the estimate.
    nz : NDArray
        The combined clustering redshift estimate.
    nz_err : NDArray
        Its uncertainty.
    covariance : NDArray
        Its covariance matrix.
    tracers : dict
        The estimate of each individual tracer before the combination, mapping
        the tracer name to a dictionary with the keys `"z"`, `"nz"`, `"nz_err"`
        and `"covariance"`.
    """

    redshift: NDArray
    nz: NDArray
    nz_err: NDArray
    covariance: NDArray
    tracers: dict[str, dict[str, NDArray]] = field(default_factory=dict)

    def to_hdf(self, dest: Group) -> None:
        for name in ("redshift", "nz", "nz_err", "covariance"):
            dest.create_dataset(name, data=getattr(self, name))

        tracers = dest.create_group("tracers")
        # HDF5 iterates groups alphabetically, so the order of the tracers is
        # recorded separately to preserve it
        tracers.attrs["names"] = list(self.tracers)

        for name, tracer in self.tracers.items():
            group = tracers.create_group(name)
            for key, value in tracer.items():
                group.create_dataset(key, data=value)

    @classmethod
    def from_hdf(cls, source: Group) -> YawClusteringNz:
        source_tracers = source["tracers"]
        tracers = {
            str(name): {
                key: value[:] for key, value in source_tracers[str(name)].items()
            }
            for name in source_tracers.attrs["names"]
        }

        return cls(
            redshift=source["redshift"][:],
            nz=source["nz"][:],
            nz_err=source["nz_err"][:],
            covariance=source["covariance"][:],
            tracers=tracers,
        )
