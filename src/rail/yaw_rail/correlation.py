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
from typing import TYPE_CHECKING

import h5py
from yaw import Configuration, CorrFunc, autocorrelate, crosscorrelate

from rail.core.data import DataHandle
from rail.yaw_rail.cache import YawCacheHandle
from rail.yaw_rail.logging import yaw_logged
from rail.yaw_rail.stage import YawRailStage, create_param

if TYPE_CHECKING:
    from rail.yaw_rail.cache import YawCache

__all__ = [
    "YawAutoCorrelate",
    "YawCorrFuncHandle",
    "YawCrossCorrelate",
]


yaw_config_scales = {
    p: create_param("scales", p) for p in ("rmin", "rmax", "rweight", "rbin_num")
}
yaw_config_zbins = {
    p: create_param("binning", p)
    for p in ("zmin", "zmax", "zbin_num", "method", "zbins")
}
yaw_config_backend = {
    p: create_param("backend", p) for p in ("crosspatch", "thread_num")
}


class YawCorrFuncHandle(DataHandle):
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
    """Base class for correlation measurement stages"""

    inputs: list[tuple[str, YawCacheHandle]]
    outputs = [
        ("corrfunc", YawCorrFuncHandle),
    ]

    def __init__(self, args, comm=None):
        super().__init__(args, comm=comm)
        self.yaw_config = Configuration.create(
            **self.get_stageparams(exclude=["verbose"]),
        )

    @abstractmethod
    def correlate(self, *inputs: YawCache) -> YawCorrFuncHandle:
        pass

    @abstractmethod
    def run(self) -> None:
        pass


class YawAutoCorrelate(
    YawBaseCorrelate,
    **yaw_config_scales,
    **yaw_config_zbins,
    **yaw_config_backend,
):
    """
    Measure the autocorrelation function amplitude for the give data sample.

    @YawParameters

    Returns
    -------
    corrfunc: YawCorrFuncHandle
        The measured correlation function stored as pair counts per spatial
        patch and redshift bin.
    """

    inputs = [
        ("sample", YawCacheHandle),
    ]

    def correlate(self, sample: YawCache) -> YawCorrFuncHandle:  # pylint: disable=W0221
        self.set_data("sample", sample)

        self.run()
        return self.get_handle("corrfunc")

    def run(self) -> None:
        with yaw_logged(self.config_options["verbose"].value):
            cache_sample: YawCache = self.get_data("sample")
            data = cache_sample.get_data()
            rand = cache_sample.get_rand()

            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                corr = autocorrelate(
                    config=self.yaw_config,
                    data=data,
                    random=rand,
                    compute_rr=True,
                )

        self.add_data("corrfunc", corr)


class YawCrossCorrelate(
    YawBaseCorrelate,
    **yaw_config_scales,
    **yaw_config_zbins,
    **yaw_config_backend,
):
    """
    Measure the cross-correlation function amplitude for the give reference
    and unknown sample.

    @YawParameters

    Returns
    -------
    corrfunc: YawCorrFuncHandle
        The measured correlation function stored as pair counts per spatial
        patch and redshift bin.
    """

    inputs = [
        ("reference", YawCacheHandle),
        ("unknown", YawCacheHandle),
    ]

    def correlate(  # pylint: disable=W0221
        self, reference: YawCache, unknown: YawCache
    ) -> YawCorrFuncHandle:
        self.set_data("reference", reference)
        self.set_data("unknown", unknown)

        self.run()
        return self.get_handle("corrfunc")

    def run(self) -> None:
        with yaw_logged(self.config_options["verbose"].value):
            cache_ref: YawCache = self.get_data("reference")
            data_ref = cache_ref.get_data()
            rand_ref = cache_ref.get_rand()

            cache_unk: YawCache = self.get_data("unknown")
            data_unk = cache_unk.get_data()
            try:
                rand_unk = cache_unk.get_rand()
            except FileNotFoundError:
                rand_unk = None

            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                corr = crosscorrelate(
                    config=self.yaw_config,
                    reference=data_ref,
                    unknown=data_unk,
                    ref_rand=rand_ref,
                    unk_rand=rand_unk,
                )

        self.add_data("corrfunc", corr)
