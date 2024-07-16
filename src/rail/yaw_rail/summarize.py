"""
This file implements RAIL stages that transform pair counts from the cross-
and/or autocorrelation function stages into an ensemble redshift estiamte. It is
the place where refined methods for moddeling clustering redshifts should be
defined.

NOTE: The current implementation is a very basic method to transform the
clustering redshift estimate into probability densities by clipping negative
correlation amplitudes and setting them to zero.
"""

from __future__ import annotations

import pickle
from typing import TextIO

from yaw import RedshiftData

from ceci.config import StageParameter
from rail.core.data import DataHandle
from rail.yaw_rail.stage import create_param

__all__ = [
    "YawRedshiftDataHandle",
]


_key_to_cf_name = dict(
    cross="cross-correlation",
    ref="reference sample autocorrelation",
    unk="unknown sample autocorrelation",
)

config_yaw_est = {
    f"{key}_est": StageParameter(
        dtype=str, required=False, msg=f"Correlation estimator to use for {name}"
    )
    for key, name in _key_to_cf_name.items()
}
config_yaw_resampling = {
    # resampling method: "method" (currently only "jackknife")
    # bootstrapping (not implemented in yet_another_wizz): "n_boot", "seed"
    # omitted: "global_norm"
    p: create_param("resampling", p)
    for p in ("crosspatch",)
}


class YawRedshiftDataHandle(DataHandle):
    """
    Class to act as a handle for a `yaw.RedshiftData` instance, associating
    it with a file and providing tools to read & write it to that file.

    Parameters
    ----------
    tag : str
        The tag under which this data handle can be found in the store.
    data : any or None
        The associated data.
    path : str or None
        The path to the associated file.
    creator : str or None
        The name of the stage that created this data handle.
    """

    data: RedshiftData
    suffix = "pkl"

    @classmethod
    def _open(cls, path: str, **kwargs) -> TextIO:
        kwargs["mode"] = "rb"
        return open(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> RedshiftData:
        with cls._open(path, **kwargs) as f:
            return pickle.load(f)

    @classmethod
    def _write(cls, data: RedshiftData, path: str, **kwargs) -> None:
        # cannot use native yaw I/O methods because they produce multiple files
        with open(path, mode="wb") as f:
            pickle.dump(data, f)
