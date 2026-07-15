"""
This file implements a container that pairs the correlation functions measured
by *yet_another_wizz* with the radial bins they were measured in.

*yet_another_wizz* measures the correlation amplitude in one or more radial bins
and returns one `yaw.CorrFunc` per bin. These instances do not record the scale
limits they belong to, which are only known to the `yaw.Configuration` used for
the measurement. `ScaledCorrFuncs` keeps both together so that later stages, in
particular the amplitude fit, can associate each measurement with its radius.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
from yaw import CorrFunc

if TYPE_CHECKING:
    from h5py import Group
    from numpy.typing import NDArray
    from yaw import Binning
    from yaw.config import ScalesConfig

__all__ = [
    "ScaledCorrFuncs",
]


@dataclass
class ScaledCorrFuncs:
    """
    The correlation functions measured in a set of radial bins.

    Parameters
    ----------
    corrfuncs : list of CorrFunc
        The pair counts, one per radial bin, ordered as `rmin`/`rmax`.
    rmin : NDArray
        Lower limit of each radial bin in `unit`.
    rmax : NDArray
        Upper limit of each radial bin in `unit`.
    unit : str
        The unit of the scale limits, see `yaw.options.Unit`.
    """

    corrfuncs: list[CorrFunc]
    rmin: NDArray
    rmax: NDArray
    unit: str

    def __post_init__(self) -> None:
        self.rmin = np.atleast_1d(np.asarray(self.rmin, dtype=float))
        self.rmax = np.atleast_1d(np.asarray(self.rmax, dtype=float))

        if not len(self.rmin) == len(self.rmax) == len(self.corrfuncs):
            raise ValueError(
                "number of scale limits does not match the number of "
                "correlation functions"
            )

    @classmethod
    def from_config(
        cls, corrfuncs: list[CorrFunc], scales: ScalesConfig
    ) -> ScaledCorrFuncs:
        """Pair the measured correlation functions with the scales they were
        measured in."""
        return cls(
            corrfuncs=list(corrfuncs),
            rmin=scales.rmin,
            rmax=scales.rmax,
            unit=scales.unit,
        )

    def __len__(self) -> int:
        return len(self.corrfuncs)

    def __getitem__(self, index: int) -> CorrFunc:
        return self.corrfuncs[index]

    @property
    def binning(self) -> Binning:
        """The redshift binning shared by all radial bins."""
        return self.corrfuncs[0].binning

    @property
    def num_patches(self) -> int:
        """The number of spatial patches, i.e. of jackknife samples."""
        return self.corrfuncs[0].num_patches

    @property
    def r_center(self) -> NDArray:
        """The geometric center of each radial bin."""
        return np.sqrt(self.rmin * self.rmax)

    @property
    def dr(self) -> NDArray:
        """The width of each radial bin."""
        return self.rmax - self.rmin

    def scale_mask(self, rmin: float, rmax: float) -> NDArray:
        """
        Select the radial bins that fall fully within the given scale limits.

        Parameters
        ----------
        rmin : float
            Lower scale limit in the unit of the measurement.
        rmax : float
            Upper scale limit in the unit of the measurement.

        Returns
        -------
        NDArray
            Boolean mask over the radial bins.

        Raises
        ------
        ValueError
            If the limits exclude all radial bins.
        """
        mask = (self.rmin >= rmin) & (self.rmax <= rmax)
        if not mask.any():
            raise ValueError(
                f"scale limits [{rmin}, {rmax}] {self.unit} exclude all "
                f"{len(self)} radial bins of the correlation measurement"
            )
        return mask

    def samples(self) -> NDArray:
        """
        Resample the correlation amplitudes in all radial bins.

        Returns
        -------
        NDArray
            The jackknife samples with shape (num_patches, num_scales,
            num_zbins).
        """
        return np.stack(
            [corr.sample().samples for corr in self.corrfuncs],
            axis=1,
        )

    def to_hdf(self, dest: Group) -> None:
        dest.attrs["rmin"] = self.rmin
        dest.attrs["rmax"] = self.rmax
        dest.attrs["unit"] = self.unit

        for i, corrfunc in enumerate(self.corrfuncs):
            corrfunc.to_hdf(dest.create_group(f"scale_{i}"))

    @classmethod
    def from_hdf(cls, source: Group) -> ScaledCorrFuncs:
        rmin = np.asarray(source.attrs["rmin"], dtype=float)
        corrfuncs = [CorrFunc.from_hdf(source[f"scale_{i}"]) for i in range(len(rmin))]

        return cls(
            corrfuncs=corrfuncs,
            rmin=rmin,
            rmax=np.asarray(source.attrs["rmax"], dtype=float),
            unit=str(source.attrs["unit"]),
        )
