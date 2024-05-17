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
from functools import partial
from typing import TextIO

import numpy as np
import qp
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

clip_neg = partial(np.maximum, 0.0)
nan_inf_to_num = partial(np.nan_to_num, nan=0.0, posinf=0.0, neginf=0.0)


def clip_negative_values(nz: RedshiftData) -> RedshiftData:
    """Replace all non-finite and negative values in a `yaw.RedshiftData`
    instance with zeros."""
    return RedshiftData(
        binning=nz.get_binning(),
        data=clip_neg(nan_inf_to_num(nz.data)),
        samples=clip_neg(nan_inf_to_num(nz.samples)),
        method=nz.method,
        info=nz.info,
    )


def redshift_data_to_qp(nz: RedshiftData) -> qp.Ensemble:
    """Convert a `yaw.RedshiftData` instance to a `qp.Ensemble` by clipping
    negative values and normalising the spatial samples to PDFs."""
    samples = clip_negative_values(nz).samples
    for i, sample in enumerate(samples):
        samples[i] = sample / np.trapz(sample, x=nz.mids)
    return qp.Ensemble(qp.hist, data=dict(bins=nz.edges, pdfs=samples))


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
