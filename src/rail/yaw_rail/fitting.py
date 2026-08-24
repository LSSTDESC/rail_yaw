"""
This file implements the fit of the measured correlation amplitudes against a
model for the angular matter correlation function.

In every redshift bin, the correlation amplitude measured in a set of radial bins
is fitted as `w(r) ~ A * w_mm(r)`, optionally with an additive constant
`w(r) ~ A * w_mm(r) + C`. The fit is repeated for every jackknife sample of the
correlation measurement, which yields the uncertainty of the fitted parameters.

Two weighting schemes are available:

- Diagonal weighted least squares, weighting each radial bin by
  `r**alpha * dr`. Robust, but not minimum variance.
- Generalised least squares using the jackknife covariance across radial bins.
  Minimum variance, but the inverse covariance is estimated from as many samples
  as there are spatial patches, which is stabilised by shrinking the covariance
  towards its diagonal and correcting the bias of its inverse.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
from yaw import Binning

if TYPE_CHECKING:
    from h5py import Group
    from numpy.typing import NDArray

__all__ = [
    "YawAmplitudeFit",
    "fit_amplitude",
    "fit_amplitude_constant",
    "parse_shrinkage",
]

logger = logging.getLogger(__name__)


def parse_shrinkage(shrinkage: str | float | None) -> str | float:
    """
    Interpret the shrinkage intensity, which stages provide as a string.

    Returns either the string `"auto"`, selecting the Ledoit-Wolf optimal
    intensity, or a float in [0, 1].
    """
    if shrinkage is None or shrinkage == "auto":
        return "auto"

    try:
        value = float(shrinkage)
    except (TypeError, ValueError) as err:
        raise ValueError(
            f"invalid shrinkage {shrinkage!r}, must be 'auto' or a float in [0, 1]"
        ) from err

    if not 0.0 <= value <= 1.0:
        raise ValueError(f"shrinkage must be in [0, 1], got {value}")
    return value


def _shrink_cov(cov: NDArray, shrinkage: float) -> NDArray:
    """Shrink a covariance towards its diagonal.

    An intensity of 0 leaves the covariance unchanged, an intensity of 1 keeps
    only the per-bin variances, in which case the generalised least squares fit
    reduces to a diagonal one.
    """
    return (1.0 - shrinkage) * cov + shrinkage * np.diag(np.diag(cov))


def _ledoit_wolf_diag(samples: NDArray) -> tuple[NDArray, float]:
    """
    Estimate a covariance shrunk towards its diagonal with the Ledoit-Wolf
    optimal shrinkage intensity.

    Parameters
    ----------
    samples : NDArray
        The samples of the data vector with shape (num_samples, num_features).

    Returns
    -------
    tuple of NDArray and float
        The shrunk covariance and the shrinkage intensity used.
    """
    num_samples = samples.shape[0]
    deviation = samples - samples.mean(axis=0, keepdims=True)

    cov = (deviation.T @ deviation) / num_samples  # ddof=0, as required below
    target = np.diag(np.diag(cov))

    # sum of the asymptotic variances of the entries of the sample covariance
    deviation_sq = deviation**2
    pi_mat = (deviation_sq.T @ deviation_sq) / num_samples - cov**2
    pi_hat = pi_mat.sum()
    # the off-diagonal entries of the diagonal target are zero, so only its
    # diagonal contributes
    rho_hat = np.trace(pi_mat)
    gamma = np.sum((cov - target) ** 2)

    if gamma <= 0.0:
        intensity = 0.0
    else:
        intensity = float(np.clip((pi_hat - rho_hat) / (gamma * num_samples), 0.0, 1.0))

    shrunk = _shrink_cov(cov, intensity)
    shrunk *= num_samples / (num_samples - 1)  # restore the ddof=1 convention
    return shrunk, intensity


def _cov_inv(
    samples: NDArray,
    shrinkage: str | float,
    diag_eps: float,
    hartlap: bool,
) -> NDArray | None:
    """
    Build a stabilised inverse covariance from the jackknife samples of the data
    vector.

    Returns `None` if the inverse cannot be formed, in which case the caller
    falls back to a diagonal fit.
    """
    num_samples, num_features = samples.shape
    if num_samples < 2:
        return None

    if shrinkage == "auto":
        cov, _ = _ledoit_wolf_diag(samples)
    else:
        cov = np.atleast_2d(np.cov(samples, rowvar=False, ddof=1))
        cov = _shrink_cov(cov, shrinkage)

    cov = cov + diag_eps * np.eye(num_features)
    try:
        cov_inv = np.linalg.inv(cov)
    except np.linalg.LinAlgError:
        return None

    if hartlap:
        # The Hartlap correction for the bias of the inverse of an estimated
        # covariance is only defined while num_samples > num_features + 2.
        if num_samples <= num_features + 2:
            return None
        cov_inv *= (num_samples - num_features - 2.0) / (num_samples - 1.0)

    return cov_inv


def _jackknife_error(samples: NDArray, mean: NDArray) -> NDArray:
    """Compute the jackknife uncertainty of the fitted parameters."""
    num_samples = len(samples)
    if num_samples < 2:
        return np.zeros_like(mean)

    deviation = samples - mean[None, :]
    variance = (num_samples - 1) / num_samples * np.nansum(deviation**2, axis=0)
    return np.sqrt(variance)


class _Fitter:
    """
    Fits the correlation amplitude, and optionally an additive constant, in every
    redshift bin and for every jackknife sample.

    Parameters
    ----------
    samples : NDArray
        The jackknife samples of the correlation amplitude with shape
        (num_patches, num_scales, num_zbins).
    template : NDArray
        The model correlation amplitude with shape (num_zbins, num_scales).
    weights : NDArray
        The weight of each radial bin with shape (num_scales,), used by the
        diagonal fit.
    mask : NDArray
        Boolean mask over the radial bins that selects the fitted scales.
    """

    def __init__(
        self,
        samples: NDArray,
        template: NDArray,
        weights: NDArray,
        mask: NDArray,
    ) -> None:
        num_patches, num_scales, num_zbins = samples.shape
        if template.shape != (num_zbins, num_scales):
            raise ValueError(
                f"template shape {template.shape} does not match the correlation "
                f"measurement with {num_zbins} redshift and {num_scales} radial bins"
            )

        self.samples = samples[:, mask, :]
        self.template = template[:, mask]
        self.weights = weights[mask]
        self.num_patches = num_patches
        self.num_zbins = num_zbins

        self._cov_inv_cache: dict[tuple[int, bytes], NDArray | None] = {}

    def _get_cov_inv(
        self,
        zbin: int,
        valid: NDArray,
        shrinkage: str | float,
        diag_eps: float,
        hartlap: bool,
    ) -> NDArray | None:
        """Get the inverse covariance across the valid radial bins.

        The inverse is built from the sub-covariance of the valid radial bins,
        not as a sub-block of the full inverse, which would be a different
        quantity. Caching it avoids recomputing the inverse for every jackknife
        sample.
        """
        key = (zbin, valid.tobytes())
        if key not in self._cov_inv_cache:
            data = self.samples[:, valid, zbin]
            finite = np.all(np.isfinite(data), axis=1)
            self._cov_inv_cache[key] = _cov_inv(
                data[finite], shrinkage, diag_eps, hartlap
            )

        return self._cov_inv_cache[key]

    def run(
        self,
        fit_constant: bool,
        use_jk_cov: bool,
        shrinkage: str | float,
        diag_eps: float,
        hartlap: bool,
    ) -> tuple[NDArray, NDArray | None]:
        """
        Returns the jackknife samples of the amplitude, and of the constant if it
        is fitted, each with shape (num_patches, num_zbins).
        """
        amplitude = np.full((self.num_patches, self.num_zbins), np.nan)
        constant = np.full((self.num_patches, self.num_zbins), np.nan)
        degenerate = False

        for patch in range(self.num_patches):
            for zbin in range(self.num_zbins):
                data = self.samples[patch, :, zbin]
                model = self.template[zbin]

                valid = np.isfinite(data) & np.isfinite(model)
                if not valid.any():
                    continue

                cov_inv = None
                if use_jk_cov:
                    cov_inv = self._get_cov_inv(
                        zbin, valid, shrinkage, diag_eps, hartlap
                    )
                    degenerate |= cov_inv is None

                params = _solve(
                    data[valid],
                    model[valid],
                    self.weights[valid],
                    cov_inv,
                    fit_constant,
                )
                if params is None:
                    continue

                amplitude[patch, zbin] = params[0]
                if fit_constant:
                    constant[patch, zbin] = params[1]

        if degenerate:
            logger.warning(
                "jackknife covariance could not be inverted for at least one "
                "redshift bin, falling back to a diagonal fit there; this happens "
                "when the number of spatial patches does not exceed the number of "
                "fitted radial bins by more than two"
            )

        return amplitude, (constant if fit_constant else None)


def _solve(
    data: NDArray,
    model: NDArray,
    weights: NDArray,
    cov_inv: NDArray | None,
    fit_constant: bool,
) -> NDArray | None:
    """
    Solve the linear least squares problem for a single redshift bin and
    jackknife sample.

    If an inverse covariance is given, a generalised least squares fit is
    performed, in which the radial weights cancel analytically. Otherwise, the
    fit is a diagonal weighted least squares fit.

    Returns the fitted parameters, or `None` if the system is degenerate.
    """
    if fit_constant:
        design = np.vstack([model, np.ones_like(model)])
    else:
        design = model[None, :]

    if cov_inv is None:
        weighted = design * weights[None, :]
    else:
        weighted = design @ cov_inv

    normal = weighted @ design.T
    rhs = weighted @ data

    try:
        if normal.shape == (1, 1):
            if normal[0, 0] <= 0.0:
                return None
            return rhs / normal[0, 0]
        return np.linalg.solve(normal, rhs)
    except np.linalg.LinAlgError:
        return None


@dataclass
class YawAmplitudeFit:
    """
    The fitted correlation amplitudes of one unknown sample and one reference
    sample, i.e. the clustering redshift measurement of a single tomographic bin
    and tracer.

    Parameters
    ----------
    binning : Binning
        The redshift binning of the correlation measurements.
    amp_cross : NDArray
        The fitted amplitude of the cross-correlation with shape (num_zbins,).
    amp_cross_samples : NDArray
        Its jackknife samples with shape (num_patches, num_zbins).
    amp_auto : NDArray
        The fitted amplitude of the reference autocorrelation.
    amp_auto_samples : NDArray
        Its jackknife samples.
    const_cross : NDArray or None
        The fitted additive constant of the cross-correlation, if fitted.
    const_cross_samples : NDArray or None
        Its jackknife samples, if fitted.
    """

    binning: Binning
    amp_cross: NDArray
    amp_cross_samples: NDArray
    amp_auto: NDArray
    amp_auto_samples: NDArray
    const_cross: NDArray | None = None
    const_cross_samples: NDArray | None = None

    @property
    def redshift(self) -> NDArray:
        """The center of each redshift bin."""
        return np.asarray(self.binning.mids)

    @property
    def amp_cross_error(self) -> NDArray:
        """The jackknife uncertainty of the cross-correlation amplitude."""
        return _jackknife_error(self.amp_cross_samples, self.amp_cross)

    @property
    def amp_auto_error(self) -> NDArray:
        """The jackknife uncertainty of the autocorrelation amplitude."""
        return _jackknife_error(self.amp_auto_samples, self.amp_auto)

    def to_hdf(self, dest: Group) -> None:
        dest.create_dataset("edges", data=np.asarray(self.binning.edges))
        dest.attrs["closed"] = str(self.binning.closed)

        for name in ("amp_cross", "amp_cross_samples", "amp_auto", "amp_auto_samples"):
            dest.create_dataset(name, data=getattr(self, name))

        for name in ("const_cross", "const_cross_samples"):
            value = getattr(self, name)
            if value is not None:
                dest.create_dataset(name, data=value)

    @classmethod
    def from_hdf(cls, source: Group) -> YawAmplitudeFit:
        binning = Binning(source["edges"][:], closed=str(source.attrs["closed"]))

        def read(name: str) -> NDArray | None:
            return source[name][:] if name in source else None

        return cls(
            binning=binning,
            amp_cross=read("amp_cross"),
            amp_cross_samples=read("amp_cross_samples"),
            amp_auto=read("amp_auto"),
            amp_auto_samples=read("amp_auto_samples"),
            const_cross=read("const_cross"),
            const_cross_samples=read("const_cross_samples"),
        )


def fit_amplitude(
    samples: NDArray,
    template: NDArray,
    weights: NDArray,
    mask: NDArray,
    *,
    use_jk_cov: bool = True,
    shrinkage: str | float = "auto",
    diag_eps: float = 0.0,
    hartlap: bool = True,
) -> tuple[NDArray, NDArray]:
    """
    Fit the correlation amplitude `w(r) ~ A * w_mm(r)` in every redshift bin.

    Parameters
    ----------
    samples : NDArray
        The jackknife samples of the correlation amplitude with shape
        (num_patches, num_scales, num_zbins).
    template : NDArray
        The model correlation amplitude with shape (num_zbins, num_scales).
    weights : NDArray
        The weight of each radial bin with shape (num_scales,).
    mask : NDArray
        Boolean mask over the radial bins that selects the fitted scales.
    use_jk_cov : bool
        Whether to fit with generalised instead of diagonal least squares.
    shrinkage : str or float
        Shrinkage intensity of the jackknife covariance, `"auto"` selects the
        Ledoit-Wolf optimal intensity.
    diag_eps : float
        Value added to the diagonal of the covariance before inverting it.
    hartlap : bool
        Whether to correct the bias of the inverse covariance.

    Returns
    -------
    tuple of NDArray
        The fitted amplitude with shape (num_zbins,) and its jackknife samples
        with shape (num_patches, num_zbins).
    """
    fitter = _Fitter(samples, template, weights, mask)
    amplitude, _ = fitter.run(False, use_jk_cov, shrinkage, diag_eps, hartlap)
    return np.nanmean(amplitude, axis=0), amplitude


def fit_amplitude_constant(
    samples: NDArray,
    template: NDArray,
    weights: NDArray,
    mask: NDArray,
    *,
    use_jk_cov: bool = True,
    shrinkage: str | float = "auto",
    diag_eps: float = 0.0,
    hartlap: bool = True,
) -> tuple[NDArray, NDArray, NDArray, NDArray]:
    """
    Fit the correlation amplitude and an additive constant
    `w(r) ~ A * w_mm(r) + C` in every redshift bin.

    Takes the same parameters as `fit_amplitude`.

    Returns
    -------
    tuple of NDArray
        The fitted amplitude, its jackknife samples, the fitted constant and its
        jackknife samples.
    """
    fitter = _Fitter(samples, template, weights, mask)
    amplitude, constant = fitter.run(True, use_jk_cov, shrinkage, diag_eps, hartlap)
    return (
        np.nanmean(amplitude, axis=0),
        amplitude,
        np.nanmean(constant, axis=0),
        constant,
    )
