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

import numpy as np
import qp
from yaw import CorrFunc, RedshiftData, ResamplingConfig

from ceci.config import StageParameter
from rail.core.data import DataHandle, QPHandle
from rail.yaw_rail.correlation import YawCorrFuncHandle
from rail.yaw_rail.logging import yaw_config_verbose, yaw_logged
from rail.yaw_rail.utils import YawRailStage, create_param


def _msg_fmt(name: str) -> str:
    return f"Correlation estimator to use for {name}"


key_to_cf_name = dict(
    cross="cross-correlation",
    ref="reference sample autocorrelation",
    unk="unknown sample autocorrelation",
)

yaw_config_est = {
    f"{key}_est": StageParameter(dtype=str, required=False, msg=_msg_fmt(name))
    for key, name in key_to_cf_name.items()
}
yaw_config_resampling = {
    # resampling method: "method" (currently only "jackknife")
    # bootstrapping (not implemented in yet_another_wizz): "n_boot", "seed"
    # omitted: "global_norm"
    p: create_param("resampling", p)
    for p in ("crosspatch",)
}


def clip_negative_values(nz: RedshiftData) -> RedshiftData:
    data = np.nan_to_num(nz.data, nan=0.0, posinf=0.0, neginf=0.0)
    samples = np.nan_to_num(nz.samples, nan=0.0, posinf=0.0, neginf=0.0)

    return RedshiftData(
        binning=nz.get_binning(),
        data=np.maximum(data, 0.0),
        samples=np.maximum(samples, 0.0),
        method=nz.method,
        info=nz.info,
    )


def redshift_data_to_qp(nz: RedshiftData) -> qp.Ensemble:
    nz_mids = nz.mids

    samples = nz.samples.copy()
    for i, sample in enumerate(samples):
        samples[i] = sample / np.trapz(sample, x=nz_mids)

    return qp.Ensemble(qp.hist, data=dict(bins=nz.edges, pdfs=samples))


class YawRedshiftDataHandle(DataHandle):
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
        # cannot use native I/O methods because the produce multiple files
        with open(path, mode="wb") as f:
            pickle.dump(data, f)


class YawSummarize(
    YawRailStage,
    **yaw_config_est,
    **yaw_config_resampling,
    verbose=yaw_config_verbose,
):
    """
    Convert the clustering redshift estimate to an QP ensemble by clipping
    negative values and substituting non-finite values.

    @Parameters

    Returns
    -------
    output: QPHandle
        The final QP ensemble.
    yaw_cc: YawRedshiftDataHandle
        A yet_another_wizz RedshiftData container containing all output data.
    """

    name = "YawEstimate"

    config_options = YawRailStage.config_options.copy()

    inputs = [
        ("cross_corr", YawCorrFuncHandle),
        ("ref_corr", YawCorrFuncHandle),
        ("unk_corr", YawCorrFuncHandle),
    ]
    outputs = [
        ("output", QPHandle),
        ("yaw_cc", YawRedshiftDataHandle),
    ]

    def __init__(self, args, comm=None):
        super().__init__(args, comm=comm)
        config = {p: self.config_options[p].value for p in yaw_config_resampling}
        self.yaw_config = ResamplingConfig.create(**config)

    def summarize(
        self,
        cross_corr: CorrFunc,
        ref_corr: CorrFunc | None = None,
        unk_corr: CorrFunc | None = None,
    ) -> dict:
        self.set_data("cross_corr", cross_corr)
        self.set_optional_data("ref_corr", ref_corr)
        self.set_optional_data("unk_corr", unk_corr)

        self.run()
        return {name: self.get_handle(name) for name, _ in self.outputs}

    def run(self) -> None:
        with yaw_logged(self.config_options["verbose"].value):
            cross_corr: CorrFunc = self.get_data("cross_corr")
            ref_corr: CorrFunc | None = self.get_optional_data("ref_corr")
            unk_corr: CorrFunc | None = self.get_optional_data("unk_corr")
            kwargs = {key: self.config_options[key].value for key in yaw_config_est}

            nz_cc = RedshiftData.from_corrfuncs(
                cross_corr=cross_corr,
                ref_corr=ref_corr,
                unk_corr=unk_corr,
                config=ResamplingConfig(),
                **kwargs,
            )

            nz_clipped = clip_negative_values(nz_cc)
            ensemble = redshift_data_to_qp(nz_clipped)

        self.add_data("output", ensemble)
        self.add_data("yaw_cc", nz_cc)
