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
"""

from __future__ import annotations

import warnings
from itertools import chain
from typing import TYPE_CHECKING

from yaw import Configuration, RedshiftData, autocorrelate, crosscorrelate

from rail.core.data import ModelHandle, TableHandle
from rail.yaw_rail import stage_config
from rail.yaw_rail.cache import YawCache, patch_centers_from_file
from rail.yaw_rail.handles import YawCacheHandle, YawCorrFuncHandle
from rail.yaw_rail.utils import YawRailStage, yaw_logged

if TYPE_CHECKING:
    from typing import Any, Literal

    from pandas import DataFrame
    from yaw import Catalog, CorrFunc

    from rail.core.data import DataHandle

__all__ = [
    "YawCacheCreate",
    "YawAutoCorrelate",
    "YawCrossCorrelate",
    "YawSummarize",
    "create_yaw_cache_alias",
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
        patch_source : YawCache, optional
            An existing cache instance that provides the patch centers. Use to
            ensure consistent patch centers when running cross-correlations.
            Takes precedence over the any configuration parameters.

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


class YawAutoCorrelate(
    YawRailStage,
    config_items=dict(
        **stage_config.yaw_scales,
        **stage_config.yaw_zbins,
        max_workers=stage_config.yaw_max_workers,
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
        ("output", YawCorrFuncHandle),
    ]

    def correlate(self, sample: YawCacheHandle | YawCache) -> YawCorrFuncHandle:
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
        return self.get_handle("output")

    @yaw_logged
    def run(self) -> None:
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
                config=Configuration.create(**self.get_algo_config_dict()),
                data=data,
                random=rand,
                count_rr=True,
            )[0]

        self.add_data("output", corr)


class YawCrossCorrelate(
    YawRailStage,
    config_items=dict(
        **stage_config.yaw_scales,
        **stage_config.yaw_zbins,
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
    """

    inputs = [
        ("reference", YawCacheHandle),
        ("unknown", YawCacheHandle),
    ]
    outputs = [
        ("output", YawCorrFuncHandle),
    ]

    def correlate(
        self, reference: YawCacheHandle | YawCache, unknown: YawCacheHandle | YawCache
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
        data_ref, rand_ref = self._get_catalogs("reference")
        data_unk, rand_unk = self._get_catalogs("unknown")
        if rand_ref is None and rand_unk is None:
            raise ValueError("no randoms provided")

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            corr = crosscorrelate(
                config=Configuration.create(**self.get_algo_config_dict()),
                reference=data_ref,
                unknown=data_unk,
                ref_rand=rand_ref,
                unk_rand=rand_unk,
            )[0]

        self.add_data("output", corr)


class YawSummarize(YawRailStage):
    """
    A summarizer that computes a clustering redshift estimate from the measured
    correlation amplitudes.

    Evaluates the cross-correlation pair counts with the provided estimator.
    Additionally corrects for galaxy sample bias if autocorrelation measurements
    are provided as stage inputs.

    **Note:** This summarizer does not produce a PDF, but a ratio of
    correlation functions, which may result in negative values. Further
    modelling of the output is required.
    """

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
        cross_corr: YawCorrFuncHandle | CorrFunc,
        auto_corr_ref: YawCorrFuncHandle | CorrFunc | None = None,
        auto_corr_unk: YawCorrFuncHandle | CorrFunc | None = None,
    ) -> dict[str, DataHandle]:
        """
        Compute a clustring redshift estimate and convert it to a PDF.

        Parameters
        ----------
        cross_corr : CorrFunc
            Pair counts from the cross-correlation measurement, basis for the
            clustering redshift estimate.
        auto_corr_ref : CorrFunc, optional
            Pair counts from the reference sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias.
        auto_corr_unk : CorrFunc, optional
            Pair counts from the unknown sample autocorrelation measurement,
            used to correct for the reference sample galaxy bias. Typically only
            availble when using simulated data sets.

        Returns
        -------
        YawRedshiftDataHandle
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
        cross_corr: CorrFunc = self.get_data("cross_corr", allow_missing=True)
        ref_corr: CorrFunc | None = self.get_optional_data("auto_corr_ref")
        unk_corr: CorrFunc | None = self.get_optional_data("auto_corr_unk")

        nz_cc = RedshiftData.from_corrfuncs(
            cross_corr=cross_corr,
            ref_corr=ref_corr,
            unk_corr=unk_corr,
        )

        self.add_data("output", nz_cc)
