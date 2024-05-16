"""
This file implements stages for auto- and cross-correlation computions with
*yet_another_wizz*, which are essentially wrappers for the `yaw.autocorrelate`
and `yaw.crosscorrelate` functions. Additionally it defines a RAIL data handle
for *yet_another_wizz* pair count data, which are intermediate data products
from which the final correlation amplitudes are computed.
"""

from __future__ import annotations

import warnings
from abc import abstractmethod
from typing import TYPE_CHECKING, Literal

import h5py
from yaw import Configuration, CorrFunc, autocorrelate, crosscorrelate

from ceci.stage import StageParameter
from rail.core.data import DataHandle
from rail.yaw_rail.cache import YawCacheHandle
from rail.yaw_rail.logging import yaw_logged
from rail.yaw_rail.stage import YawRailStage, create_param

if TYPE_CHECKING:  # pragma: no cover
    from rail.yaw_rail.cache import YawCache
    from yaw.catalogs.scipy import ScipyCatalog

__all__ = [
    "YawAutoCorrelate",
    "YawCorrFuncHandle",
    "YawCrossCorrelate",
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


class YawAutoCorrelate(
    YawBaseCorrelate,
    config_items=dict(
        **config_yaw_scales,
        **config_yaw_zbins,
        **config_yaw_backend,
    ),
):
    """
    Wrapper stage for `yaw.autocorrelate` to compute a sample's angular
    autocorrelation amplitude.

    Generally used for the reference sample to compute an estimate for its
    galaxy sample as a function of redshift. Data is provided as a single cache
    directory that must have redshifts and randoms with redshift attached.
    """

    inputs = [
        ("sample", YawCacheHandle),
    ]
    outputs = [
        ("autocorr", YawCorrFuncHandle),
    ]

    def correlate(self, sample: YawCache) -> YawCorrFuncHandle:  # pylint: disable=W0221
        """
        Measure the angular autocorrelation amplitude in bins of redshift.

        Parameters
        ----------
        sample : YawCache
            Input cache which must have randoms attached and redshifts for both
            data set and randoms.

        Returns
        -------
        YawCorrFuncHandle
            A handle for the `yaw.CorrFunc` instance that holds the pair counts.
        """
        self.set_data("sample", sample)

        self.run()
        return self.get_handle("autocorr")

    @yaw_logged
    def run(self) -> None:
        cache_sample: YawCache = self.get_data("sample")
        data = cache_sample.data.get()
        rand = cache_sample.rand.get()

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            corr = autocorrelate(
                config=self.yaw_config,
                data=data,
                random=rand,
                compute_rr=True,
            )

        self.add_data("autocorr", corr)


class YawCrossCorrelate(
    YawBaseCorrelate,
    config_items=dict(
        **config_yaw_scales,
        **config_yaw_zbins,
        **config_yaw_backend,
    ),
):
    """
    Wrapper stage for `yaw.crosscorrelate` to compute the angular cross-
    correlation amplitude between the reference and the unknown sample.

    Generally used for the reference sample to compute an estimate for its
    galaxy sample as a function of redshift. Data sets are provided as cache
    directories. The reference sample must have redshifts and at least one
    cache must have randoms attached.
    """

    inputs = [
        ("reference", YawCacheHandle),
        ("unknown", YawCacheHandle),
    ]
    outputs = [
        ("crosscorr", YawCorrFuncHandle),
    ]

    def correlate(  # pylint: disable=W0221
        self, reference: YawCache, unknown: YawCache
    ) -> YawCorrFuncHandle:
        """
        Measure the angular cross-correlation amplitude in bins of redshift.

        Parameters
        ----------
        reference : YawCache
            Cache for the reference data, must have redshifts. If no randoms are
            attached, the unknown data cache must provide them.
        unknown : YawCache
            Cache for the unknown data. If no randoms are attached, the
            reference data cache must provide them.

        Returns
        -------
        YawCorrFuncHandle
            A handle for the `yaw.CorrFunc` instance that holds the pair counts.
        """
        self.set_data("reference", reference)
        self.set_data("unknown", unknown)

        self.run()
        return self.get_handle("crosscorr")

    def _get_catalogs(
        self,
        tag: Literal["reference", "unknown"],
    ) -> tuple[ScipyCatalog, ScipyCatalog | None]:
        """Get the catalog(s) from the given input cache handle"""
        cache: YawCache = self.get_data(tag)
        data = cache.data.get()
        try:
            return data, cache.rand.get()
        except FileNotFoundError:
            return data, None

    @yaw_logged
    def run(self) -> None:
        data_ref, rand_ref = self._get_catalogs("reference")
        data_unk, rand_unk = self._get_catalogs("unknown")
        if rand_ref is None and rand_unk is None:
            raise ValueError("no randoms provided")  # pragma: no cover

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            corr = crosscorrelate(
                config=self.yaw_config,
                reference=data_ref,
                unknown=data_unk,
                ref_rand=rand_ref,
                unk_rand=rand_unk,
            )

        self.add_data("crosscorr", corr)
