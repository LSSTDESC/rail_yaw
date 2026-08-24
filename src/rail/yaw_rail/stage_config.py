"""
This file implements the stage parameters and some automation tools to directly
derive them, including their default values and documentation, from
*yet_another_wizz*.
"""

from __future__ import annotations

import numpy as np
from ceci.config import StageParameter
from yaw import config
from yaw.options import NotSet

from rail.yaw_rail.cosmology import DEFAULT_TRANSFER_FUNCTION

__all__ = [
    "cache",
    "yaw_columns",
    "yaw_patches",
    "yaw_max_workers",
    "yaw_scales",
    "yaw_zbins",
    "yaw_verbose",
    "yaw_scale_idx",
    "yaw_cosmology",
    "yaw_ccl",
    "yaw_fit",
    "yaw_combine",
    "DEFAULT_SCALE_EDGES",
    "DEFAULT_RMIN",
    "DEFAULT_RMAX",
]


def create_rail_config(binning_cls: config.BaseConfig) -> dict[str, StageParameter]:
    """
    Create a dictionary of `rail.StageParameter` from a `yaw`configuration class.
    """
    params = dict()
    for name, param in binning_cls.get_paramspec().items():
        params[name] = StageParameter(
            msg=param.help,
            dtype=param.type,
            default=None if param.default is NotSet else param.default,
            required=param.default is NotSet,
        )

    return params


#### all stages ####

yaw_verbose = StageParameter(
    str,
    required=False,
    default="info",
    msg="lowest log level emitted by *yet_another_wizz*",
)
"""Stage parameter for the logging level."""


#### shared ####

yaw_max_workers = StageParameter(
    int,
    required=False,
    msg="configure a custom maximum number of parallel workers to use",
)
"""Stage parameter controlling the maximum number of parallel workers."""


#### YawCacheCreate ####

cache = dict(
    path=StageParameter(
        str, required=True, msg="path to cache directory, must not exist"
    ),
    overwrite=StageParameter(
        bool,
        required=False,
        msg="overwrite the path if it is an existing cache directory",
    ),
)
"""Stage parameters to specify the cache directory."""

yaw_columns = dict(
    ra_name=StageParameter(
        str,
        default="ra",
        msg="column name of right ascension (in degrees)",
    ),
    dec_name=StageParameter(
        str,
        default="dec",
        msg="column name of declination (in degrees)",
    ),
    weight_name=StageParameter(
        str,
        required=False,
        msg="column name of weight",
    ),
    redshift_name=StageParameter(
        str,
        required=False,
        msg="column name of redshift",
    ),
    degrees=StageParameter(
        bool,
        default=True,
        required=False,
        msg="Whether the input coordinates are in degrees or radian.",
    ),
)
"""Stage parameters to specify column names in the input data."""

yaw_patches = dict(
    patch_file=StageParameter(
        str,
        required=False,
        msg="path to ASCII file that lists patch centers (one per line) as "
        "pair of R.A./Dec. in radian, separated by a single space or tab",
    ),
    patch_name=StageParameter(
        str,
        required=False,
        msg="column name of patch index (starting from 0)",
    ),
    patch_num=StageParameter(
        int,
        required=False,
        msg="number of spatial patches to create using knn on coordinates of randoms",
    ),
    probe_size=StageParameter(
        int,
        default=-1,
        required=False,
        msg="The approximate number of objects to sample from the input file "
        "when generating patch centers.",
    ),
)
"""Optional stage parameters to specify the patch creation stragegy."""


#### YawAuto/CrossCorrelate ####

DEFAULT_SCALE_EDGES = np.logspace(np.log10(30.0), np.log10(30_000.0), 25)
"""Edges of the default radial binning in kpc, yielding 24 logarithmic scale bins."""

DEFAULT_RMIN: list[float] = DEFAULT_SCALE_EDGES[:-1].tolist()
"""Default lower limits of the 24 radial bins in kpc."""

DEFAULT_RMAX: list[float] = DEFAULT_SCALE_EDGES[1:].tolist()
"""Default upper limits of the 24 radial bins in kpc."""


yaw_scales = create_rail_config(config.ScalesConfig)
"""Stage parameters to configure the correlation measurements."""

# `ScalesConfig.get_paramspec()` reports `rmin`/`rmax` as `float`, but
# *yet_another_wizz* accepts a sequence of scale limits to measure the
# correlation amplitudes in multiple radial bins simultaneously. Using
# `dtype=None` disables the type check in `ceci` and permits both a scalar and a
# sequence of scale limits.
yaw_scales["rmin"] = StageParameter(
    dtype=None,
    default=DEFAULT_RMIN,
    required=False,
    msg="single or sequence of lower scale limits in given 'unit'",
)
yaw_scales["rmax"] = StageParameter(
    dtype=None,
    default=DEFAULT_RMAX,
    required=False,
    msg="single or sequence of upper scale limits in given 'unit'",
)

yaw_zbins = create_rail_config(config.BinningConfig)
"""Stage parameters to configure the redshift sampling of the redshift estimate."""

yaw_scale_idx = StageParameter(
    int,
    default=0,
    required=False,
    msg="index of the radial bin of the correlation measurement to use",
)
"""Stage parameter to select a single radial bin of a correlation measurement."""

yaw_cosmology = dict(
    cosmology=StageParameter(
        str,
        default="Planck15",
        required=False,
        msg="name of the fiducial cosmology, used by *yet_another_wizz* to "
        "convert physical scales to angles and by CCL to model the matter "
        "correlation function",
    ),
)
"""Stage parameter to select the fiducial cosmology."""

yaw_ccl = dict(
    transfer_function=StageParameter(
        str,
        default=DEFAULT_TRANSFER_FUNCTION,
        required=False,
        msg="transfer function used by CCL to model the matter correlation "
        "function; the default requires no external Boltzmann solver, use "
        "'boltzmann_camb' (CCL's own default) if 'camb' is installed",
    ),
)
"""Stage parameter to configure the CCL matter model."""


#### YawFitAmplitude ####

yaw_fit = dict(
    fit_rmin=StageParameter(
        float,
        default=1000.0,
        required=False,
        msg="lower scale limit of the cross-correlation amplitude fit, in the "
        "'unit' of the correlation measurement",
    ),
    fit_rmax=StageParameter(
        float,
        default=30_000.0,
        required=False,
        msg="upper scale limit of the cross-correlation amplitude fit",
    ),
    auto_fit_rmin=StageParameter(
        float,
        default=2000.0,
        required=False,
        msg="lower scale limit of the autocorrelation amplitude fit",
    ),
    auto_fit_rmax=StageParameter(
        float,
        default=30_000.0,
        required=False,
        msg="upper scale limit of the autocorrelation amplitude fit",
    ),
    alpha=StageParameter(
        float,
        default=0.0,
        required=False,
        msg="power-law exponent of the radial weight r**alpha * dr applied when "
        "fitting the amplitude with diagonal weighted least squares "
        "(no effect if 'use_jk_cov' is set, where the weights cancel)",
    ),
    fit_constant=StageParameter(
        bool,
        default=True,
        required=False,
        msg="whether to fit an additive constant alongside the amplitude of the "
        "cross-correlation, i.e. w ~ A * w_mm + C instead of w ~ A * w_mm",
    ),
    use_jk_cov=StageParameter(
        bool,
        default=True,
        required=False,
        msg="whether to fit with generalised least squares using the jackknife "
        "covariance across scales instead of diagonal weighted least squares",
    ),
    shrinkage=StageParameter(
        str,
        default="auto",
        required=False,
        msg="shrinkage intensity applied to the jackknife covariance to "
        "stabilise its inverse; either 'auto' for the Ledoit-Wolf optimal "
        "intensity or a float in [0, 1] given as string",
    ),
    hartlap=StageParameter(
        bool,
        default=True,
        required=False,
        msg="whether to apply the Hartlap correction for the bias of the inverse "
        "of an estimated covariance matrix",
    ),
    diag_eps=StageParameter(
        float,
        default=0.0,
        required=False,
        msg="small value added to the diagonal of the covariance before inversion",
    ),
)
"""Stage parameters to configure the correlation amplitude fit."""


#### YawNzCombine ####

yaw_combine = dict(
    bias_mode=StageParameter(
        str,
        default="growth_factor",
        required=False,
        msg="model for the galaxy bias evolution b_p(z) of the unknown sample; "
        "'growth_factor' uses b_p = 1 / D(z), 'constant' uses b_p = 1",
    ),
    paired=StageParameter(
        bool,
        default=False,
        required=False,
        msg="whether to treat the jackknife samples of the cross- and "
        "autocorrelation amplitudes as paired when propagating uncertainties "
        "into the clustering redshift estimate",
    ),
    normalize=StageParameter(
        bool,
        default=True,
        required=False,
        msg="whether to normalise the combined estimate to unit integral using a "
        "non-negative B-spline fit",
    ),
    z_norm_max=StageParameter(
        float,
        required=False,
        msg="upper redshift limit of the normalisation integral, defaults to the "
        "highest redshift of the combined estimate",
    ),
    round_z_decimals=StageParameter(
        int,
        default=6,
        required=False,
        msg="number of decimals to which redshifts are rounded when matching bins "
        "of different tracers for the inverse-variance combination",
    ),
)
"""Stage parameters to configure the combination of multiple tracers."""
