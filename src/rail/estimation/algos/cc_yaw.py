from __future__ import annotations

import warnings
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal

from yaw import RedshiftData, ResamplingConfig, autocorrelate, crosscorrelate

from rail.core.data import DataHandle, QPHandle, TableHandle
from rail.yaw_rail.cache import (
    YawCache,
    YawCacheHandle,
    config_cache,
    config_yaw_columns,
    config_yaw_patches,
)
from rail.yaw_rail.correlation import (
    YawBaseCorrelate,
    YawCorrFuncHandle,
    config_yaw_backend,
    config_yaw_scales,
    config_yaw_zbins,
)
from rail.yaw_rail.summarize import (
    YawRedshiftDataHandle,
    config_yaw_est,
    config_yaw_resampling,
    redshift_data_to_qp,
)
from rail.yaw_rail.utils import handle_has_path
from rail.yaw_rail.logging import yaw_logged
from rail.yaw_rail.stage import YawRailStage

if TYPE_CHECKING:
    from pandas import DataFrame
    from yaw import CorrFunc
    from yaw.catalogs.scipy import ScipyCatalog


def stage_helper(suffix: str) -> dict[str, Any]:
    """
    Create an alias mapping for all `YawCacheCreate` stage in- and outputs.

    Useful when creating a new stage with `make_stage`, e.g. by setting
    `aliases=stage_helper("suffix")`.

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


class YawCacheCreate(
    YawRailStage,
    config_items=dict(
        **config_cache,
        **config_yaw_columns,
        **config_yaw_patches,
    ),
):
    """
    Create a new cache directory to hold a data set and optionally its matching
    random catalog.

    Both inputs are split into consistent spatial patches that are required by
    *yet_another_wizz* for correlation function covariance estimates. Each
    patch is cached separately for efficient access.

    The cache can be constructed from input files or tabular data in memory.
    Column names for sky coordinates are required, redshifts and per-object
    weights are optional. One out of three patch create methods must be
    specified:
    1. Splitting the data into predefined patches (e.g. form an existing cache
       instance).
    2. Splitting the data based on a column with patch indices.
    3. Generating approximately equal size patches using k-means clustering of
       objects positions (preferably randoms if provided).
    """

    inputs = [
        ("data", TableHandle),
        ("rand", TableHandle),
    ]
    outputs = [
        ("cache", YawCacheHandle),
    ]

    def create(self, data: DataFrame, rand: DataFrame | None = None) -> YawCacheHandle:
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
            configured with `n_patches`.

        Returns
        -------
        YawCacheHandle
            A handle for the newly created cache directory.
        """
        self.set_data("data", data)
        self.set_optional_data("rand", rand)

        self.run()
        return self.get_handle("cache")

    @yaw_logged
    def run(self) -> None:
        config = self.get_config_dict()

        if config["patches"] is not None:
            patch_centers = YawCache(config["patches"]).get_patch_centers()
        else:
            patch_centers = None

        cache = YawCache.create(config["path"], overwrite=config["overwrite"])

        rand: TableHandle | None = self.get_optional_handle("rand")
        if rand is not None:
            cache.rand.set(
                source=rand.path if handle_has_path(rand) else rand.read(),
                patch_centers=patch_centers,
                **self.get_algo_config_dict(),
            )

        data: TableHandle = self.get_handle("data")
        cache.data.set(
            source=data.path if handle_has_path(data) else data.read(),
            patch_centers=patch_centers,
            **self.get_algo_config_dict(),
        )

        self.add_data("cache", cache)


class YawCacheDrop(YawRailStage):
    """Utility stage to delete a *yet_another_wizz* cache directory."""

    inputs = [
        ("cache", YawCacheHandle),
    ]
    outputs = []

    def run(self) -> None:
        cache: YawCache = self.get_data("cache")
        cache.drop()

    def drop(self, cache: YawCache) -> None:
        """
        Delete a data cache.

        Parameters
        ----------
        cache : YawCache
            The cache to delete.
        """
        self.set_data("cache", cache)
        self.run()


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


class YawSummarize(
    YawRailStage,
    config_items=dict(
        **config_yaw_est,
        **config_yaw_resampling,
    ),
):
    """
    A simple summarizer that computes a clustering redshift estimate from the
    measured correlation amplitudes.

    Evaluates the cross-correlation pair counts with the provided estimator.
    Additionally corrects for galaxy sample bias if autocorrelation measurements
    are given.

    .. warning::
    This summarizer simply replaces all non-finite and negative values in the
    clustering redshift estimate to produce PDFs. This may have significant
    impacts on the recovered mean redshift.
    """

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
        config = {p: self.config_options[p].value for p in config_yaw_resampling}
        self.yaw_config = ResamplingConfig.create(**config)

    def summarize(
        self,
        cross_corr: CorrFunc,
        ref_corr: CorrFunc | None = None,
        unk_corr: CorrFunc | None = None,
    ) -> dict[str, DataHandle]:
        """
        Compute a clustring redshift estimate and convert it to a PDF.

        Parameters
        ----------
        cross_corr : CorrFunc
            Pair counts from the cross-correlation measurement, basis for the
            clustering redshift estimate.
        ref_corr : CorrFunc, optional
            Pair counts from the reference sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias.
        unk_corr : CorrFunc, optional
            Pair counts from the unknown sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias. Typically only
            availble when using simulated data sets.

        Returns
        -------
        dict
            Dictionary with keys `"output"` and `"yaw_cc"`:
            1. `QPHandle` containing PDFs of the estimated spatial samples.
            2. `YawRedshiftDataHandle` wrapping the direct output of
               *yet_another_wizz*; the clustering redshift estimate, spatial
               samples thereof, and its covariance matrix.
        """
        self.set_data("cross_corr", cross_corr)
        self.set_optional_data("ref_corr", ref_corr)
        self.set_optional_data("unk_corr", unk_corr)

        self.run()
        return {name: self.get_handle(name) for name, _ in self.outputs}

    @yaw_logged
    def run(self) -> None:
        cross_corr: CorrFunc = self.get_data("cross_corr")
        ref_corr: CorrFunc | None = self.get_optional_data("ref_corr")
        unk_corr: CorrFunc | None = self.get_optional_data("unk_corr")

        nz_cc = RedshiftData.from_corrfuncs(
            cross_corr=cross_corr,
            ref_corr=ref_corr,
            unk_corr=unk_corr,
            config=ResamplingConfig(),
            **self.get_algo_config_dict(exclude=config_yaw_resampling),
        )
        ensemble = redshift_data_to_qp(nz_cc)

        self.add_data("output", ensemble)
        self.add_data("yaw_cc", nz_cc)
