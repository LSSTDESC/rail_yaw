"""
This file implements the stage parameters and some automation tools to directly
derive them, including their default values and documentation, from
*yet_another_wizz*.
"""

from __future__ import annotations

from ceci.config import StageParameter
from yaw import config
from yaw.options import NotSet

__all__ = [
    "cache",
    "yaw_columns",
    "yaw_patches",
    "yaw_max_workers",
    "yaw_scales",
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

yaw_scales = create_rail_config(config.ScalesConfig)
"""Stage parameters to configure the correlation measurements."""

yaw_zbins = create_rail_config(config.BinningConfig)
"""Stage parameters to configure the redshift sampling of the redshift estimate."""
