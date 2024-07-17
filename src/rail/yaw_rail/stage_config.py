"""
This file implements the stage parameters and some automation tools to directly
derive them, including their default values and documentation, from
*yet_another_wizz*.
"""

from __future__ import annotations

from dataclasses import fields
from typing import Any, Literal

from yaw import config

from ceci.config import StageParameter

__all__ = [
    "cache",
    "yaw_backend",
    "yaw_columns",
    "yaw_est",
    "yaw_patches",
    "yaw_resampling",
    "yaw_scales",
]


def get_yaw_config_meta(config_cls: Any, parname: str) -> dict[str, Any]:
    """Convert the parameter metadata, embedded in the *yet_another_wizz*
    configuration dataclasses, to a python dictionary."""
    for field in fields(config_cls):
        if field.name == parname:
            return {k[4:]: v for k, v in field.metadata.items()}
    raise AttributeError(f"{config_cls} has no attribute '{parname}'")


def create_param(
    category: Literal["backend", "binning", "scales", "resampling"],
    parname: str,
) -> StageParameter:
    """
    Hook into *yet_another_wizz* configuration and defaults to construct a
    `StageParameter` from a *yet_another_wizz* configuration class.

    Parameters
    ----------
    category : str
        Prefix of one of the *yet_another_wizz* configuration classes that
        defines the parameter of interest, e.g. `"scales"` for
        `yaw.config.ScalesConfig`.
    parname : str
        The name of the parameter of interest, e.g. `"rmin"` for
        `yaw.config.ScalesConfig.rmin`.

    Returns
    -------
    StageParameter
        Parameter metadata including `dtype`, `default`, `required` and `msg`
        values set.
    """
    category = category.lower().capitalize()

    metadata = get_yaw_config_meta(
        config_cls=getattr(config, f"{category}Config"),
        parname=parname,
    )

    config_default = getattr(config.DEFAULT, category)
    default = getattr(config_default, parname, None)

    return StageParameter(
        dtype=metadata.get("type"),
        default=default,
        required=metadata.get("required", False),
        msg=metadata.get("help"),
    )


#### all stages ####

yaw_verbose = StageParameter(
    str,
    required=False,
    default="info",
    msg="lowest log level emitted by *yet_another_wizz*",
)
"""Stage parameter for the logging level."""


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
    redshift_name=StageParameter(
        str,
        required=False,
        msg="column name of redshift",
    ),
    weight_name=StageParameter(
        str,
        required=False,
        msg="column name of weight",
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
    n_patches=StageParameter(
        int,
        required=False,
        msg="number of spatial patches to create using knn on coordinates of randoms",
    ),
)
"""Optional stage parameters to specify the patch creation stragegy."""


#### YawAuto/CrossCorrelate ####

yaw_scales = {
    p: create_param("scales", p) for p in ("rmin", "rmax", "rweight", "rbin_num")
}
"""Stage parameters to configure the correlation measurements."""

yaw_zbins = {
    p: create_param("binning", p)
    for p in ("zmin", "zmax", "zbin_num", "method", "zbins")
}
"""Stage parameters to configure the redshift sampling of the redshift estimate."""

# Since the current implementation does not support MPI, we need to implement
# the number of threads manually. The code uses multiprocessing and can only
# run on a single machine.
yaw_backend = {
    "thread_num": StageParameter(
        int,
        required=False,
        msg="the number of threads to use by the multiprocessing backend",
    )
}
"""Stage parameters to configure the computation."""


#### YawSummarize ####

# mapping from short-form name to full description of correlation functions
_key_to_cf_name = dict(
    cross="cross-correlation",
    ref="reference sample autocorrelation",
    unk="unknown sample autocorrelation",
)
yaw_est = {
    f"{key}_est": StageParameter(
        dtype=str, required=False, msg=f"Correlation estimator to use for {name}"
    )
    for key, name in _key_to_cf_name.items()
}
"""Stage parameters to specify estimators for each correlation function."""

yaw_resampling = {
    # resampling method: "method" (currently only "jackknife")
    # bootstrapping (not implemented in yet_another_wizz): "n_boot", "seed"
    # omitted: "global_norm"
    p: create_param("resampling", p)
    for p in ("crosspatch",)
}
"""Stage parameters to configure spatial resampling in `yet_another_wizz`."""
