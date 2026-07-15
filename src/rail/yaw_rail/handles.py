"""
This file implements all RAIL data handles used to pass data between the various
wrapper stages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import h5py
from rail.core.data import DataHandle

from rail.yaw_rail.cache import YawCache
from rail.yaw_rail.combine import YawClusteringNz
from rail.yaw_rail.correlation import ScaledCorrFuncs
from rail.yaw_rail.fitting import YawAmplitudeFit

if TYPE_CHECKING:
    from typing import TextIO

__all__ = [
    "YawCacheHandle",
    "YawCorrFuncHandle",
    "YawFitHandle",
    "YawNzHandle",
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
    Class to act as a handle for a `ScaledCorrFuncs` instance, associating it
    with a file and providing tools to read and write the data.

    The correlation functions measured in each radial bin are stored in a single
    HDF5 file, one group per radial bin, together with the scale limits they were
    measured in.

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

    data: ScaledCorrFuncs
    suffix = "hdf5"

    @classmethod
    def _open(cls, path: str, **kwargs) -> h5py.File:
        return h5py.File(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> ScaledCorrFuncs:
        with h5py.File(path, mode="r") as f:
            return ScaledCorrFuncs.from_hdf(f)

    @classmethod
    def _write(cls, data: ScaledCorrFuncs, path: str, **kwargs) -> None:
        with h5py.File(path, mode="w") as f:
            data.to_hdf(f)


class YawFitHandle(DataHandle):
    """
    Class to act as a handle for a `YawAmplitudeFit` instance, associating it
    with a file and providing tools to read and write the data.

    Holds the fitted correlation amplitudes of a single tomographic bin and
    reference tracer.

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

    data: YawAmplitudeFit
    suffix = "hdf5"

    @classmethod
    def _open(cls, path: str, **kwargs) -> h5py.File:
        return h5py.File(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> YawAmplitudeFit:
        with h5py.File(path, mode="r") as f:
            return YawAmplitudeFit.from_hdf(f)

    @classmethod
    def _write(cls, data: YawAmplitudeFit, path: str, **kwargs) -> None:
        with h5py.File(path, mode="w") as f:
            data.to_hdf(f)


class YawNzHandle(DataHandle):
    """
    Class to act as a handle for a `YawClusteringNz` instance, associating it
    with a file and providing tools to read and write the data.

    Holds the clustering redshift estimate combined from one or more reference
    tracers, and the estimate of each tracer before the combination.

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

    data: YawClusteringNz
    suffix = "hdf5"

    @classmethod
    def _open(cls, path: str, **kwargs) -> h5py.File:
        return h5py.File(path, **kwargs)

    @classmethod
    def _read(cls, path: str, **kwargs) -> YawClusteringNz:
        with h5py.File(path, mode="r") as f:
            return YawClusteringNz.from_hdf(f)

    @classmethod
    def _write(cls, data: YawClusteringNz, path: str, **kwargs) -> None:
        with h5py.File(path, mode="w") as f:
            data.to_hdf(f)
