"""
This file implements all RAIL data handles used to pass data between the various
wrapper stages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import h5py
from yaw import CorrFunc

from rail.core.data import DataHandle
from rail.yaw_rail.cache import YawCache

if TYPE_CHECKING:
    from typing import TextIO

__all__ = [
    "YawCacheHandle",
    "YawCorrFuncHandle",
]


class YawCacheHandle(DataHandle):
    """
    Class to act as a handle for a `YawCache` instance, associating it with a
    file and providing tools to read & write it to that file.

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

    data: YawCache
    suffix = "path"

    @classmethod
    def _open(cls, path: str, **kwargs) -> TextIO:
        return open(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> YawCache:
        with cls._open(path, **kwargs) as f:
            path = f.read()
        return YawCache(path)

    @classmethod
    def _write(cls, data: YawCache, path: str, **kwargs) -> None:
        with cls._open(path, mode="w") as f:
            f.write(data.path)


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
