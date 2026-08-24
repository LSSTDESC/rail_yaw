"""
This file implements all stages required to wrap *yet_another_wizz* in RAIL.
These are:

- YawCacheCreate:
  Preprocessing input data and arranging them in spatial patches for efficient
  acces.
- YawAutoCorrelate:
  Computing the autocorrelation by running the pair couting in spatial patches.
  Used for galaxy bias mitigation.
- YawCrossCorrelate:
  Computing the cross-correlation by running the pair couting in spatial
  patches. Represents a biased redshift estimte.
- YawSummarize:
  Transforming the correlation functin pair counts to a redshift estimate (not a
  PDF!).
- YawFitAmplitude:
  Fitting the measured correlation amplitudes against a model for the angular
  matter correlation function to obtain the clustering redshift measurement of a
  single tomographic bin and reference tracer.
- YawNzCombine:
  Combining the fitted amplitudes of one or more reference tracers into a
  clustering redshift estimate (not a PDF!).
"""

from __future__ import annotations

import sys
import warnings
from functools import cache
from itertools import chain
from types import new_class
from typing import TYPE_CHECKING

import numpy as np
from rail.core.data import ModelHandle, TableHandle
from yaw import Configuration, RedshiftData, autocorrelate, crosscorrelate

from rail.yaw_rail import stage_config
from rail.yaw_rail.cache import YawCache, patch_centers_from_file
from rail.yaw_rail.combine import (
    YawClusteringNz,
    bias_evolution,
    combine_inverse_variance,
    fit_nonneg_bspline,
    ncc_from_amplitudes,
)
from rail.yaw_rail.correlation import ScaledCorrFuncs
from rail.yaw_rail.cosmology import get_ccl_cosmology, matter_template
from rail.yaw_rail.fitting import (
    YawAmplitudeFit,
    fit_amplitude,
    fit_amplitude_constant,
    parse_shrinkage,
)
from rail.yaw_rail.handles import (
    YawCacheHandle,
    YawCorrFuncHandle,
    YawFitHandle,
    YawNzHandle,
)
from rail.yaw_rail.utils import YawRailStage, yaw_logged

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any, Literal

    from pandas import DataFrame
    from rail.core.data import DataHandle
    from yaw import Catalog

__all__ = [
    "YawCacheCreate",
    "YawAutoCorrelate",
    "YawCrossCorrelate",
    "YawSummarize",
    "YawFitAmplitude",
    "YawNzCombine",
    "create_yaw_cache_alias",
    "create_yaw_nz_combine",
]


def create_yaw_cache_alias(suffix: str) -> dict[str, Any]:
    """
    Create an alias mapping for all `YawCacheCreate` stage in- and outputs.

    Useful when creating a new stage with `make_stage`, e.g. by setting
    `aliases=create_yaw_cache_alias("suffix")`.

    Parameters
    ----------
    name : str
        The suffix to append to the in- and output tags, e.g. `"data_suffix"`.

    Returns
    -------
    dict
        Mapping from original to aliased in- and output tags.
    """
    keys_in = (key for key, _ in YawCacheCreate.inputs)
    keys_out = (key for key, _ in YawCacheCreate.outputs)
    return {key: f"{key}_{suffix}" for key in chain(keys_in, keys_out)}


def create_yaw_autocorrelate_alias(suffix: str) -> dict[str, Any]:
    """
    Create an alias mapping for all `YawAutoCorrelate` stage in- and outputs.

    Useful when creating a new stage with `make_stage`, e.g. by setting
    `aliases=create_yaw_cache_alias("suffix")`.

    Parameters
    ----------
    name : str
        The suffix to append to the in- and output tags, e.g. `"data_suffix"`.

    Returns
    -------
    dict
        Mapping from original to aliased in- and output tags.
    """
    keys_in = (key for key, _ in YawAutoCorrelate.inputs)
    keys_out = (key for key, _ in YawAutoCorrelate.outputs)
    return {key: f"{key}_{suffix}" for key in chain(keys_in, keys_out)}


class YawCacheCreate(
    YawRailStage,
    config_items=dict(
        **stage_config.cache,
        **stage_config.yaw_columns,
        **stage_config.yaw_patches,
        max_workers=stage_config.yaw_max_workers,
    ),
):
    """
    Create a new cache directory to hold a data set and optionally its matching
    random catalog.

    Both input data sets are split into consistent spatial patches that are
    required by *yet_another_wizz* for correlation function covariance
    estimates. Each patch is stored separately for efficient access.

    The cache can be constructed from input files or tabular data in memory.
    Column names for sky coordinates are required, redshifts and per-object
    weights are optional. One out of three patch create methods must be
    specified:

    #. Splitting the data into predefined patches (from ASCII file or an
       existing cache instance, linked as optional stage input).
    #. Splitting the data based on a column with patch indices.
    #. Generating approximately equal size patches using k-means clustering of
       objects positions (preferably randoms if provided).

    **Note:** The cache directory must be deleted manually when it is no longer
    needed. (The reference sample cache may be reused when operating on
    tomographic bins.)
    """

    entrypoint_function = "create"  # the user-facing science function for this class
    interactive_function = "yaw_cache_create"
    inputs = [
        ("data", TableHandle),
        # optional
        ("rand", TableHandle),
        ("patch_source", YawCacheHandle),
    ]
    outputs = [
        ("output", YawCacheHandle),
    ]

    def create(
        self,
        data: TableHandle | DataFrame,
        rand: TableHandle | DataFrame | None = None,
        patch_source: YawCacheHandle | YawCache | None = None,
        **kwargs,
    ) -> YawCacheHandle:
        """
        Create the new cache directory and split the input data into spatial
        patches.

        Parameters
        ----------
        data : DataFrame
            The data set to split into patches and cache.
        rand : DataFrame, optional
            The randoms to split into patches and cache, positions used to
            automatically generate patch centers if provided and stage is
            configured with `patch_num`.
            For interactive mode RAIL, set to the string "none" if not desired.
        patch_source : YawCache, optional
            An existing cache instance that provides the patch centers. Use to
            ensure consistent patch centers when running cross-correlations.
            Takes precedence over the any configuration parameters.
            For interactive mode RAIL, set to the string "none" if not desired.

        Returns
        -------
        YawCacheHandle
            A handle for the newly created cache directory.
        """
        self.set_data("data", data)
        self.set_optional_data("rand", rand)
        self.set_optional_data("patch_source", patch_source)

        self.run()
        return self.get_handle("output")

    @staticmethod
    def _get_path_or_data(handle: DataHandle) -> str | DataFrame:
        """
        Get a valid data source from a handle for the YAW catalog loader.

        The function assumes that either the data or path attributes are set.
        This function is necessary since pipelines provide only file paths,
        whereas notebook users may pass the data in memory.
        """
        # no cover justfication: nothing to actually test here
        if handle.data is None:  # ceci: no data set but have data path
            result = handle.path  # pragma: no cover
        else:  # notebook: have not path but actual data loaded
            result = handle.data  # pragma: no cover
        return result

    @yaw_logged
    def run(self) -> None:
        config = self.get_config_dict()

        try:  # stage input takes precedence over config options
            patch_centers = self.get_optional_data("patch_source").get_patch_centers()
        except AttributeError:  # patch_source not set, i.e. data is None
            if config["patch_file"] is None:
                patch_centers = None
            else:
                patch_centers = patch_centers_from_file(config["patch_file"])

        cache = YawCache.create(config["path"], overwrite=config["overwrite"])

        # randoms are also an optional input and may not be present
        handle_rand: TableHandle | None = self.get_optional_handle("rand")
        if handle_rand is not None:
            cache.rand.set(
                source=self._get_path_or_data(handle_rand),
                patch_centers=patch_centers,
                **self.get_algo_config_dict(),
            )

        handle_data: TableHandle = self.get_handle("data", allow_missing=True)
        cache.data.set(
            source=self._get_path_or_data(handle_data),
            patch_centers=patch_centers,
            **self.get_algo_config_dict(),
        )

        self.add_data("output", cache)


class YawCorrelateBase(YawRailStage):
    """Base class for the stages that measure a correlation amplitude."""

    def get_correlation_config(self) -> Configuration:
        """Build the *yet_another_wizz* configuration from the stage
        parameters."""
        config = self.get_algo_config_dict()

        # yaw subdivides each radial bin into `resolution` sub-bins to weight the
        # pairs by their separation, and fails with an opaque TypeError if the
        # resolution is not set
        if config["rweight"] is not None and config["resolution"] is None:
            raise ValueError("'resolution' is required when 'rweight' is set")

        return Configuration.create(**config)


class YawAutoCorrelate(
    YawCorrelateBase,
    config_items=dict(
        **stage_config.yaw_scales,
        **stage_config.yaw_zbins,
        **stage_config.yaw_cosmology,
        max_workers=stage_config.yaw_max_workers,
    ),
):
    """
    Wrapper stage for `yaw.autocorrelate` to compute a sample's angular
    autocorrelation amplitude.

    Generally used for the reference sample to compute an estimate for its
    galaxy sample as a function of redshift. Data is provided as a single cache
    directory that must have redshifts and randoms with redshift attached.

    The amplitude is measured in every radial bin configured through `rmin` and
    `rmax`, which accept either a single scale limit or a sequence of them.
    """

    entrypoint_function = "correlate"  # the user-facing science function for this class
    interactive_function = "yaw_auto_correlate"
    inputs = [
        ("sample", YawCacheHandle),
    ]
    outputs = [
        ("output", YawCorrFuncHandle),
    ]

    def correlate(
        self, sample: YawCacheHandle | YawCache, **kwargs
    ) -> YawCorrFuncHandle:
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
            A handle for the `ScaledCorrFuncs` instance that holds the pair
            counts of every radial bin.
        """
        self.set_data("sample", sample)

        self.run()
        return self.get_handle("output")

    @yaw_logged
    def run(self) -> None:
        config = self.get_correlation_config()

        max_workers = self.get_config_dict()["max_workers"]
        cache_sample: YawCache = self.get_data("sample", allow_missing=True)
        data = cache_sample.data.get(max_workers)
        try:
            rand = cache_sample.rand.get(max_workers)
        except FileNotFoundError as err:
            raise ValueError("no randoms provided") from err

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            corr = autocorrelate(
                config=config,
                data=data,
                random=rand,
                count_rr=True,
            )

        self.add_data("output", ScaledCorrFuncs.from_config(corr, config.scales))


class YawCrossCorrelate(
    YawCorrelateBase,
    config_items=dict(
        **stage_config.yaw_scales,
        **stage_config.yaw_zbins,
        **stage_config.yaw_cosmology,
        max_workers=stage_config.yaw_max_workers,
    ),
):
    """
    Wrapper stage for `yaw.crosscorrelate` to compute the angular cross-
    correlation amplitude between the reference and the unknown sample.

    Generally used for the reference sample to compute an estimate for its
    galaxy sample as a function of redshift. Data sets are provided as cache
    directories. The reference sample must have redshifts and at least one
    cache must have randoms attached.

    The amplitude is measured in every radial bin configured through `rmin` and
    `rmax`, which accept either a single scale limit or a sequence of them.
    """

    entrypoint_function = "correlate"  # the user-facing science function for this class
    interactive_function = "yaw_cross_correlate"
    inputs = [
        ("reference", YawCacheHandle),
        ("unknown", YawCacheHandle),
    ]
    outputs = [
        ("output", YawCorrFuncHandle),
    ]

    def correlate(
        self,
        reference: YawCacheHandle | YawCache,
        unknown: YawCacheHandle | YawCache,
        **kwargs,
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
            A handle for the `ScaledCorrFuncs` instance that holds the pair
            counts of every radial bin.
        """
        self.set_data("reference", reference)
        self.set_data("unknown", unknown)

        self.run()
        return self.get_handle("output")

    def _get_catalogs(
        self,
        tag: Literal["reference", "unknown"],
    ) -> tuple[Catalog, Catalog | None]:
        """Get the catalog(s) from the given input cache handle"""
        max_workers = self.get_config_dict()["max_workers"]
        cache: YawCache = self.get_data(tag, allow_missing=True)
        data = cache.data.get(max_workers)
        try:  # NOTE: randoms are optional inputs for YawCacheCreate
            rand = cache.rand.get(max_workers)
        except FileNotFoundError:
            rand = None
        return data, rand

    @yaw_logged
    def run(self) -> None:
        config = self.get_correlation_config()

        data_ref, rand_ref = self._get_catalogs("reference")
        data_unk, rand_unk = self._get_catalogs("unknown")
        if rand_ref is None and rand_unk is None:
            raise ValueError("no randoms provided")

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            corr = crosscorrelate(
                config=config,
                reference=data_ref,
                unknown=data_unk,
                ref_rand=rand_ref,
                unk_rand=rand_unk,
            )

        self.add_data("output", ScaledCorrFuncs.from_config(corr, config.scales))


class YawSummarize(
    YawRailStage,
    config_items=dict(scale_idx=stage_config.yaw_scale_idx),
):
    """
    A summarizer that computes a clustering redshift estimate from the measured
    correlation amplitudes.

    Evaluates the cross-correlation pair counts with the provided estimator.
    Additionally corrects for galaxy sample bias if autocorrelation measurements
    are provided as stage inputs.

    This summarizer operates on a single radial bin of the correlation
    measurement, selected with `scale_idx`. Use `YawFitAmplitude` and
    `YawNzCombine` to instead combine the amplitudes measured in all radial bins.

    **Note:** This summarizer does not produce a PDF, but a ratio of
    correlation functions, which may result in negative values. Further
    modelling of the output is required.
    """

    entrypoint_function = "summarize"  # the user-facing science function for this class
    interactive_function = "yaw_summarize"
    inputs = [
        ("cross_corr", YawCorrFuncHandle),
        ("auto_corr_ref", YawCorrFuncHandle),
        # optional
        ("auto_corr_unk", YawCorrFuncHandle),
    ]
    outputs = [
        ("output", ModelHandle),
    ]

    def summarize(
        self,
        cross_corr: YawCorrFuncHandle | ScaledCorrFuncs,
        auto_corr_ref: YawCorrFuncHandle | ScaledCorrFuncs | None = None,
        auto_corr_unk: YawCorrFuncHandle | ScaledCorrFuncs | None = None,
        **kwargs,
    ) -> dict[str, DataHandle]:
        """
        Compute a clustring redshift estimate and convert it to a PDF.

        Parameters
        ----------
        cross_corr : ScaledCorrFuncs
            Pair counts from the cross-correlation measurement, basis for the
            clustering redshift estimate.
        auto_corr_ref : ScaledCorrFuncs, optional
            Pair counts from the reference sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias.
        auto_corr_unk : ScaledCorrFuncs, optional
            Pair counts from the unknown sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias. Typically only
            availble when using simulated data sets.
            For interactive mode RAIL, set to the string "none" if not desired.

        Returns
        -------
        ModelHandle
            The clustering redshift estimate, spatial (jackknife) samples
            thereof, and its covariance matrix.
        """
        self.set_data("cross_corr", cross_corr)
        self.set_optional_data("auto_corr_ref", auto_corr_ref)
        self.set_optional_data("auto_corr_unk", auto_corr_unk)

        self.run()
        return self.get_handle("output")

    @yaw_logged
    def run(self) -> None:
        scale_idx = self.get_config_dict()["scale_idx"]

        def get_scale(tag: str, optional: bool = False) -> Any | None:
            corr = (
                self.get_optional_data(tag)
                if optional
                else self.get_data(tag, allow_missing=True)
            )
            return None if corr is None else corr[scale_idx]

        nz_cc = RedshiftData.from_corrfuncs(
            cross_corr=get_scale("cross_corr"),
            ref_corr=get_scale("auto_corr_ref", optional=True),
            unk_corr=get_scale("auto_corr_unk", optional=True),
        )

        self.add_data("output", nz_cc)


class YawFitAmplitude(
    YawRailStage,
    config_items=dict(
        **stage_config.yaw_fit,
        **stage_config.yaw_cosmology,
        **stage_config.yaw_ccl,
    ),
):
    """
    Fit the measured correlation amplitudes against a model for the angular
    matter correlation function.

    In every redshift bin, the amplitudes measured in the radial bins of the
    cross-correlation and of the reference autocorrelation are fitted as
    `w(r) ~ A * w_mm(r)`, where `w_mm` is modelled with CCL assuming unit galaxy
    bias and a top-hat redshift distribution spanning the bin. An additive
    constant may be fitted alongside the amplitude of the cross-correlation,
    which absorbs an additive systematic in the measurement.

    The fit is repeated for every jackknife sample of the correlation
    measurement, which yields the uncertainty of the fitted amplitudes.

    The output represents the clustering redshift measurement of a single
    tomographic bin and reference tracer, which `YawNzCombine` turns into a
    redshift estimate.
    """

    entrypoint_function = "fit"  # the user-facing science function for this class
    interactive_function = "yaw_fit_amplitude"
    inputs = [
        ("cross_corr", YawCorrFuncHandle),
        ("auto_corr_ref", YawCorrFuncHandle),
    ]
    outputs = [
        ("output", YawFitHandle),
    ]

    def fit(
        self,
        cross_corr: YawCorrFuncHandle | ScaledCorrFuncs,
        auto_corr_ref: YawCorrFuncHandle | ScaledCorrFuncs,
        **kwargs,
    ) -> YawFitHandle:
        """
        Fit the correlation amplitudes in every redshift bin.

        Parameters
        ----------
        cross_corr : ScaledCorrFuncs
            Pair counts from the cross-correlation measurement between the
            reference tracer and the unknown sample.
        auto_corr_ref : ScaledCorrFuncs
            Pair counts from the reference sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias.

        Returns
        -------
        YawFitHandle
            A handle for the `YawAmplitudeFit` instance that holds the fitted
            amplitudes and their jackknife samples.
        """
        self.set_data("cross_corr", cross_corr)
        self.set_data("auto_corr_ref", auto_corr_ref)

        self.run()
        return self.get_handle("output")

    @yaw_logged
    def run(self) -> None:
        config = self.get_config_dict()
        cosmology = get_ccl_cosmology(config["cosmology"], config["transfer_function"])

        cross: ScaledCorrFuncs = self.get_data("cross_corr", allow_missing=True)
        auto: ScaledCorrFuncs = self.get_data("auto_corr_ref", allow_missing=True)

        if (
            len(cross) != len(auto)
            or cross.unit != auto.unit
            or not np.array_equal(cross.rmin, auto.rmin)
            or not np.array_equal(cross.rmax, auto.rmax)
        ):
            raise ValueError(
                "the cross- and autocorrelation must be measured in the same "
                "radial bins"
            )

        # a single matter model is fitted to both, so they must share the binning
        if not np.array_equal(cross.binning.edges, auto.binning.edges):
            raise ValueError(
                "the cross- and autocorrelation must be measured in the same "
                "redshift bins"
            )

        fit_kwargs = dict(
            use_jk_cov=config["use_jk_cov"],
            shrinkage=parse_shrinkage(config["shrinkage"]),
            diag_eps=config["diag_eps"],
            hartlap=config["hartlap"],
        )
        # the radial weight cancels in the generalised least squares fit
        weights = cross.r_center ** config["alpha"] * cross.dr

        template = matter_template(cross, cosmology)

        amp_auto, amp_auto_samples = fit_amplitude(
            auto.samples(),
            template,
            weights,
            auto.scale_mask(config["auto_fit_rmin"], config["auto_fit_rmax"]),
            **fit_kwargs,
        )

        cross_mask = cross.scale_mask(config["fit_rmin"], config["fit_rmax"])
        const_cross = None
        const_cross_samples = None

        if config["fit_constant"]:
            (
                amp_cross,
                amp_cross_samples,
                const_cross,
                const_cross_samples,
            ) = fit_amplitude_constant(
                cross.samples(), template, weights, cross_mask, **fit_kwargs
            )
        else:
            amp_cross, amp_cross_samples = fit_amplitude(
                cross.samples(), template, weights, cross_mask, **fit_kwargs
            )

        self.add_data(
            "output",
            YawAmplitudeFit(
                binning=cross.binning,
                amp_cross=amp_cross,
                amp_cross_samples=amp_cross_samples,
                amp_auto=amp_auto,
                amp_auto_samples=amp_auto_samples,
                const_cross=const_cross,
                const_cross_samples=const_cross_samples,
            ),
        )


class YawNzCombine(
    YawRailStage,
    config_items=dict(
        **stage_config.yaw_combine,
        **stage_config.yaw_cosmology,
        **stage_config.yaw_ccl,
    ),
):
    """
    A summarizer that combines the fitted correlation amplitudes of one or more
    reference tracers into a clustering redshift estimate.

    The estimate of each tracer is `n_cc = A_sp / sqrt(A_ss) / b_p`, where
    `A_ss` corrects for the evolving bias of the reference sample and `b_p`
    models the bias evolution of the unknown sample. Tracers that cover
    different, possibly only partially overlapping redshift ranges are combined
    by inverse-variance weighting. This requires their redshift binning to
    coincide where their ranges overlap.

    Use `create_yaw_nz_combine` to create a stage for a given set of tracers.

    **Note:** This summarizer does not produce a PDF, but a ratio of correlation
    amplitudes, which may result in negative values. Further modelling of the
    output is required.
    """

    entrypoint_function = "combine"  # the user-facing science function for this class
    interactive_function = "yaw_nz_combine"
    tracers: list[str] = []
    inputs = []
    outputs = [
        ("output", YawNzHandle),
    ]

    def combine(self, **tracer_fits: YawFitHandle | YawAmplitudeFit) -> YawNzHandle:
        """
        Combine the fitted amplitudes of all tracers into a redshift estimate.

        Parameters
        ----------
        **tracer_fits : YawAmplitudeFit
            The fitted amplitudes of each tracer, given as keyword arguments
            named after the stage inputs, i.e. `fit_{tracer}`.

        Returns
        -------
        YawNzHandle
            A handle for the `YawClusteringNz` instance that holds the combined
            clustering redshift estimate.
        """
        for tag, fit in tracer_fits.items():
            self.set_data(tag, fit)

        self.run()
        return self.get_handle("output")

    @yaw_logged
    def run(self) -> None:
        config = self.get_config_dict()
        cosmology = get_ccl_cosmology(config["cosmology"], config["transfer_function"])

        series: dict[str, dict[str, Any]] = {}
        for tracer in self.tracers:
            fit: YawAmplitudeFit = self.get_data(f"fit_{tracer}", allow_missing=True)

            redshift = fit.redshift
            bias = bias_evolution(redshift, config["bias_mode"], cosmology)

            nz, nz_err, covariance = ncc_from_amplitudes(
                fit.amp_cross,
                fit.amp_cross_samples,
                fit.amp_auto,
                fit.amp_auto_samples,
                bias,
                paired=config["paired"],
            )
            series[tracer] = dict(
                z=redshift, nz=nz, nz_err=nz_err, covariance=covariance
            )

        redshift, nz, nz_err = combine_inverse_variance(
            series, round_z_decimals=config["round_z_decimals"]
        )

        if config["normalize"]:
            norm = self._normalisation(redshift, nz, nz_err, config["z_norm_max"])
            nz = nz / norm
            nz_err = nz_err / norm

        self.add_data(
            "output",
            YawClusteringNz(
                redshift=redshift,
                nz=nz,
                nz_err=nz_err,
                # the inverse-variance combination treats the redshift bins as
                # independent, matching how the estimate is consumed downstream;
                # the full covariance of each tracer is preserved in `tracers`
                covariance=np.diag(nz_err**2),
                tracers=series,
            ),
        )

    @staticmethod
    def _normalisation(
        redshift: np.ndarray,
        nz: np.ndarray,
        nz_err: np.ndarray,
        z_norm_max: float | None,
    ) -> float:
        """
        Integrate a non-negative B-spline fit of the estimate, which avoids the
        bias that a plain sum incurs when some measured values scatter below
        zero.
        """
        zmax = redshift[-1] if z_norm_max is None else min(z_norm_max, redshift[-1])

        spline = fit_nonneg_bspline(redshift, nz, nz_err)
        integral = spline.integrate(redshift[0], zmax, extrapolate=False)

        if not np.isfinite(integral) or integral <= 0.0:
            raise ValueError(
                "cannot normalise the clustering redshift estimate: the "
                "non-negative spline fit integrates to zero, which indicates "
                "that the estimate is consistent with zero everywhere"
            )
        return float(integral)


def create_yaw_nz_combine(
    tracers: Sequence[str], module: str | None = None
) -> type[YawNzCombine]:
    """
    Create a `YawNzCombine` stage that combines the given reference tracers.

    RAIL stages declare their inputs statically, so a stage class must be created
    for the set of tracers that it combines. Each tracer contributes one stage
    input, named `fit_{tracer}`, which takes the output of a `YawFitAmplitude`
    stage.

    The created class is named after its tracers, cached, and bound as an
    attribute of the module that requested it, in the same way that
    `collections.namedtuple` does. This is what makes the stage usable in a
    `ceci` pipeline: `ceci` runs every stage in a fresh process, where it looks
    the stage up by name after importing the module it was defined in. Call this
    at the top level of that module so that importing it recreates the class:

        MyCombine = create_yaw_nz_combine(["bgs", "lrg"])

    Parameters
    ----------
    tracers : sequence of str
        The names of the reference tracers to combine, e.g. `["bgs", "lrg"]`.
    module : str, optional
        The module to bind the created class to, defaults to the caller's module.

    Returns
    -------
    type
        A `YawNzCombine` subclass with one input per tracer.

    Examples
    --------
    >>> stage = create_yaw_nz_combine(["bgs", "lrg"]).make_stage()
    >>> stage.combine(fit_bgs=fit_bgs, fit_lrg=fit_lrg)
    """
    tracers = tuple(tracers)

    if not tracers:
        raise ValueError("at least one tracer is required")
    if len(set(tracers)) != len(tracers):
        raise ValueError(f"tracer names must be unique, got {list(tracers)}")

    if module is None:
        # the caller's module, so that `ceci` can recreate the class by importing
        # the module that the pipeline was defined in
        module = sys._getframe(1).f_globals.get("__name__", __name__)

    # `ceci` reads `sys.modules[cls.__module__].__file__` when the stage class is
    # created, which fails in an interactive session or notebook, where the
    # caller's module is `__main__` and has no `__file__`. Fall back to this
    # module, which is importable, so that the factory works interactively; a
    # `ceci` pipeline always passes an importable module with a file.
    caller = sys.modules.get(module)
    if caller is None or getattr(caller, "__file__", None) is None:
        module = __name__

    return _create_yaw_nz_combine(tracers, module)


@cache
def _create_yaw_nz_combine(tracers: tuple[str, ...], module: str) -> type[YawNzCombine]:
    """Create and cache the stage class for a given set of tracers.

    The class is built with `new_class` so that it is born with its final name.
    `YawRailStage` derives the stage name from the class name and `ceci`
    registers the stage under that name while the class is created, so neither
    can be assigned after the fact.
    """
    class_name = f"{YawNzCombine.__name__}_{'_'.join(tracers)}"

    namespace = dict(
        __doc__=YawNzCombine.__doc__,
        __module__=module,
        tracers=list(tracers),
        inputs=[(f"fit_{tracer}", YawFitHandle) for tracer in tracers],
    )

    stage_class = new_class(
        class_name,
        (YawNzCombine,),
        kwds=dict(
            config_items=dict(
                **stage_config.yaw_combine,
                **stage_config.yaw_cosmology,
                **stage_config.yaw_ccl,
            )
        ),
        exec_body=lambda ns: ns.update(namespace),
    )

    if module in sys.modules:
        setattr(sys.modules[module], class_name, stage_class)

    return stage_class
