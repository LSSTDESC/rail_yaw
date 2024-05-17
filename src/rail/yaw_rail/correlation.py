"""
This file implements stages for auto- and cross-correlation computions with
*yet_another_wizz*, which are essentially wrappers for the `yaw.autocorrelate`
and `yaw.crosscorrelate` functions. Additionally it defines a RAIL data handle
for *yet_another_wizz* pair count data, which are intermediate data products
from which the final correlation amplitudes are computed.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

import h5py
from yaw import Configuration, CorrFunc

from ceci.stage import StageParameter
from rail.core.data import DataHandle
from rail.yaw_rail.cache import YawCacheHandle
from rail.yaw_rail.stage import YawRailStage, create_param

if TYPE_CHECKING:  # pragma: no cover
    from rail.yaw_rail.cache import YawCache

__all__ = [
    "YawCorrFuncHandle",
]


config_yaw_scales = {
    p: create_param("scales", p) for p in ("rmin", "rmax", "rweight", "rbin_num")
}
config_yaw_zbins = {
    p: create_param("binning", p)
    for p in ("zmin", "zmax", "zbin_num", "method", "zbins")
}
config_yaw_backend = {p: create_param("backend", p) for p in ("crosspatch",)}
# Since the current implementation does not support MPI, we need to implement
# the number of threads manually. The code uses multiprocessing and can only
# run on a single machine.
config_yaw_backend["thread_num"] = StageParameter(
    int,
    required=False,
    msg="the number of threads to use by the multiprocessing backend (single machine, MPI not yet supported)",
)


class YawCorrFuncHandle(DataHandle):
    """
    Class to act as a handle for a `yaw.CorrFunc` instance, associating it
    with a file and providing tools to read & write it to that file.

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

    @classmethod
    def _open(cls, path: str, **kwargs) -> h5py.File:
        return h5py.File(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> CorrFunc:
        return CorrFunc.from_file(path)

    @classmethod
    def _write(cls, data: CorrFunc, path: str, **kwargs) -> None:
        data.to_file(path)


class YawBaseCorrelate(YawRailStage):
    """Base class for correlation measurement stages."""

    inputs: list[tuple[str, YawCacheHandle]]
    outputs: list[tuple[str, YawCorrFuncHandle]]

    def __init__(self, args, comm=None):
        super().__init__(args, comm=comm)
        self.yaw_config = Configuration.create(**self.get_algo_config_dict())

    @abstractmethod
    def correlate(self, *inputs: YawCache) -> YawCorrFuncHandle:
        pass  # pragma: no cover
