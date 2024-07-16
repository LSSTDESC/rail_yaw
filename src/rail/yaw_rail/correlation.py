"""
This file implements utilities for the `YawAutoCorrelate` and `YawCrossCorrelate`
stages, which perform the pair counting for the correlation measurements. It
also implements the handle to wrap the pair counts in the stage output.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import h5py
from yaw import CorrFunc

from ceci.stage import StageParameter
from rail.core.data import DataHandle
from rail.yaw_rail.stage import create_param

if TYPE_CHECKING:  # pragma: no cover
    from rail.yaw_rail.cache import YawCache

__all__ = [
    "YawCorrFuncHandle",
]


config_yaw_scales = {
    p: create_param("scales", p) for p in ("rmin", "rmax", "rweight", "rbin_num")
}
"""Stage parameters to configure the correlation measurements."""

config_yaw_zbins = {
    p: create_param("binning", p)
    for p in ("zmin", "zmax", "zbin_num", "method", "zbins")
}
"""Stage parameters to configure the redshift sampling of the redshift estimate."""

# Since the current implementation does not support MPI, we need to implement
# the number of threads manually. The code uses multiprocessing and can only
# run on a single machine.
config_yaw_backend = {
    "thread_num": StageParameter(
        int,
        required=False,
        msg="the number of threads to use by the multiprocessing backend",
    )
}
"""Stage parameters to configure the computation."""


def warn_thread_num_deprecation(config: dict):
    """`thread_num` is deprecated when MPI backend is implemented."""
    if config.get("thread_num", None) is not None:
        warnings.warn(
            "The 'thread_num' stage parameter is deprecated and will be removed "
            "once the MPI parallelism is implemented.",
            FutureWarning,
            stacklevel=2
        )


class YawCorrFuncHandle(DataHandle):
    """
    Class to act as a handle for a `yaw.CorrFunc` instance, associating it
    with a file and providing tools to read and write the data.

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

    data: CorrFunc
    suffix = "hdf5"

    @classmethod
    def _open(cls, path: str, **kwargs) -> h5py.File:
        return h5py.File(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> CorrFunc:
        return CorrFunc.from_file(path)

    @classmethod
    def _write(cls, data: CorrFunc, path: str, **kwargs) -> None:
        data.to_file(path)
