"""
This file implements a wrapper for a cache directory for *yet_another_wizz*
catalogs. The cache is designed to hold a pair of data and (optional) random
catalog. The patch center coordinates are enforced to be consistent.

The cache is wrapped by two RAIL stages, one to create a new cache from input
data tables, and one to delete the cache directory and its contents. They are
managed through a special data handle that allows passing the directory path
between RAIL stages to define inputs for the correlation function stages.
"""

from __future__ import annotations

import logging
import os
from itertools import chain
from shutil import rmtree
from typing import TYPE_CHECKING, Any, TextIO

from yaw import NewCatalog

from ceci.config import StageParameter
from rail.core.data import DataHandle, TableHandle
from rail.yaw_rail.logging import yaw_logged
from rail.yaw_rail.stage import YawRailStage

if TYPE_CHECKING:  # pragma: no cover
    from pandas import DataFrame
    from yaw.catalogs.scipy import ScipyCatalog
    from yaw.core.coordinates import Coordinate, CoordSky

__all__ = [
    "YawCache",
    "YawCacheCreate",
    "YawCacheDrop",
    "YawCacheHandle",
]


logger = logging.getLogger(__name__)

config_yaw_columns = dict(
    ra_name=StageParameter(
        str,
        default="ra",
        msg="column name of right ascension (in degrees)",
    ),
    dec_name=StageParameter(
        str,
        default="dec",
        msg="column name of declination (in degrees)",
    ),
    redshift_name=StageParameter(
        str,
        required=False,
        msg="column name of redshift",
    ),
    weight_name=StageParameter(
        str,
        required=False,
        msg="column name of weight",
    ),
)

config_yaw_patches = dict(
    patches=StageParameter(
        str,
        required=False,
        msg="path to cache which provides the patch centers to construct consistent datasets",
    ),
    patch_name=StageParameter(
        str,
        required=False,
        msg="column name of patch index (starting from 0)",
    ),
    n_patches=StageParameter(
        int,
        required=False,
        msg="number of spatial patches to create using knn on coordinates of randoms",
    ),
)

config_cache = dict(
    path=StageParameter(
        str, required=True, msg="path to cache directory, must not exist"
    ),
    overwrite=StageParameter(
        bool,
        required=False,
        msg="overwrite the path if it is an existing cache directory",
    ),
)


def normalise_path(path: str) -> str:
    """Substitute UNIX style home directories and environment variables in path
    names."""
    return os.path.expandvars(os.path.expanduser(path))


def get_patch_method(
    patch_centers: ScipyCatalog | Coordinate | None,
    patch_name: str | None,
    n_patches: int | None,
) -> ScipyCatalog | Coordinate | str | int:
    """
    Extract the preferred parameter value from the patch parameters, follow a
    hierarchy of preference.

    Parameters
    ----------
    patch_centers : ScipyCatalog, Coordinate or None
        A *yet_another_wizz* catalog or coordinates, or `None` if not set.
    patch_name : str or None
        The name of the column that list the patch indices or `None` if not set.
    n_patches: int or None
        The number of patches to generate using k-means clustering or `None` if
        not set.

    Returns
    -------
    ScipyCatalog, Coordinate, str, or int
        The preferred parameter value to configure the patch creation.

    Raises
    ------
    ValueError
        If all parameter values are set to `None`.
    """
    # preferred order, "create" should be the last resort
    if patch_centers is not None:  # deterministic and consistent
        return patch_centers
    if patch_name is not None:  # deterministic but assumes consistency
        return patch_name
    if n_patches is not None:  # non-determistic and never consistent
        return n_patches
    raise ValueError("no patch creation method specified")


class YawCatalog:
    """
    Wrapper around a *yet_another_wizz* catalog that is cached on disk in
    spatial patches.

    Parameters
    ----------
    path : str
        Path to the directory in which the data is cached.
    """

    path: str
    """Path to the directory in which the data is cached."""
    catalog: ScipyCatalog | None
    """Catalog instance or `None` if no data is cached yet."""

    def __init__(self, path: str) -> None:
        self.path = normalise_path(path)
        self.catalog = None
        self._patch_center_callback = None

    def set_patch_center_callback(self, cat: YawCatalog | None) -> None:
        """
        Register a different `YawCatalog` instance that defines the patch
        centers to use.

        If set, all patch configuration parameters in `set` are ignored and the
        patch centers of the linked catalog are used instead. Useful to ensure
        that two catalogs have consistent patch centers without explicitly
        setting them a priori.

        Parameters
        ----------
        cat : YawCatalog or None
            The catalog instance that acts are reference for the patch centers.
            If `None`, removes the callback.
        """
        if cat is None:
            self._patch_center_callback = None
        elif isinstance(cat, YawCatalog):
            self._patch_center_callback = lambda: cat.get().centers
        else:
            raise TypeError("referenced catalog is not a 'YawCatalog'")

    def exists(self) -> bool:
        """Whether the catalog's cache directory exists."""
        return os.path.exists(self.path)

    def get(self) -> ScipyCatalog:
        """
        Access the catalog instance without loading all data to memory.

        Retrieves the catalog metadata from disk if not in memory.

        Returns
        -------
        ScipyCatalog
            The cached catalog instance.

        Raises
        ------
        FileNotFoundError
            If not data is cached at the specifed path.
        """
        if not self.exists():
            raise FileNotFoundError(f"no catalog cached at {self.path}")
        if self.catalog is None:
            self.catalog = NewCatalog().from_cache(self.path)
        return self.catalog

    def set(
        self,
        source: DataFrame | str,
        ra_name: str,
        dec_name: str,
        *,
        patch_centers: ScipyCatalog | Coordinate | None = None,
        patch_name: str | None = None,
        n_patches: int | None = None,
        redshift_name: str | None = None,
        weight_name: str | None = None,
        overwrite: bool = False,
        **kwargs,  # pylint: disable=W0613; allows dict-unpacking of whole config
    ) -> ScipyCatalog:
        """
        Split a new data set in spatial patches and cache it.

        Parameters
        ----------
        source : DataFrame or str
            Data source, either a `DataFrame` or a FITS, Parquet, or HDF5 file.
        ra_name : str
            Column name of right ascension data in degrees.
        dec_name : str
            Column name of declination data in degrees.
        patch_centers : ScipyCatalog, Coordinate or None
            A *yet_another_wizz* catalog or coordinates, or `None` if not set.
        patch_name : str or None
            The name of the column that list the patch indices or `None` if not set.
        n_patches: int or None
            The number of patches to generate using k-means clustering or `None` if
            not set.
        redshift_name : str or None, optional
            Column name of redshifts.
        weight_name: str or None, optional
            Column name of per-object weigths.
        overwrite: bool, optional
            Whether to overwrite an existing, cached data set.

        Returns
        -------
        ScipyCatalog
            The cached catalog instance.

        Raises
        ------
        FileExistsError
            If there is already a data set cached and `overwrite` is not set.
        """
        if self.exists():
            if overwrite:
                rmtree(self.path)
            else:
                raise FileExistsError(self.path)
        os.makedirs(self.path)

        # check if any reference catalog is registered that overwrites the
        # provided patch centers
        try:
            patch_centers = self._patch_center_callback()
        except (TypeError, FileNotFoundError):
            pass

        if isinstance(source, str):  # dealing with a file
            patches = get_patch_method(
                patch_centers=patch_centers,
                patch_name=patch_name,
                n_patches=n_patches,
            )
            self.catalog = NewCatalog().from_file(
                filepath=source,
                patches=patches,
                ra=ra_name,
                dec=dec_name,
                redshift=redshift_name,
                weight=weight_name,
                cache_directory=self.path,
            )

        else:
            # ensure that patch_centers are always used if provided
            if patch_centers is not None:
                patch_name = None
                n_patches = None
            self.catalog = NewCatalog().from_dataframe(
                data=source,
                ra_name=ra_name,
                dec_name=dec_name,
                patch_centers=patch_centers,
                patch_name=patch_name,
                n_patches=n_patches,
                redshift_name=redshift_name,
                weight_name=weight_name,
                cache_directory=self.path,
            )

    def drop(self) -> None:
        """Delete the cached data from disk and unset the catalog instance."""
        if self.exists():
            rmtree(self.path)
        self.catalog = None


class YawCache:
    """
    A cache directory for *yet_another_wizz* to store a data and (optional)
    random catalogue.

    The data sets are split into consistent spatial patches used for spatial
    resampling and covariance estiation by *yet_another_wizz* and wrapped by
    `YawCatalog` instances. Once any data set is specifed, the other data set
    will inherit its patch centers.

    Create a new instance with the `create` method or open an existing cache.
    If an existing cache is used, the code checks if the provided directory is a
    valid cache. To interact with the data set and the randoms, directly access
    the methods of the `data` and `rand` attributes.

    Parameters
    ----------
    path : str
        Path at which the data and random catalogues are cached, must exist and
        has to be created with the `create` method.
    """

    _flag_path = ".yaw_cache"  # file to a cache directory, safeguard for .drop()
    path: str
    """Path at which the data and random catalogues are cached."""
    data: YawCatalog
    """Catalog instance for the data set."""
    rand: YawCatalog
    """Catalog instance for the randoms."""

    def __init__(self, path: str) -> None:
        self.path = normalise_path(path)

        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)
        if not self.is_valid(self.path):
            raise FileNotFoundError(f"not a valid cache directory: {self.path}")

        self.data = YawCatalog(os.path.join(self.path, "data"))
        self.rand = YawCatalog(os.path.join(self.path, "rand"))
        self.data.set_patch_center_callback(self.rand)
        self.rand.set_patch_center_callback(self.data)

    @classmethod
    def is_valid(cls, path: str) -> bool:
        """Whether the provided path is a valid cache."""
        indicator_path = os.path.join(path, cls._flag_path)
        return os.path.exists(indicator_path)

    @classmethod
    def create(cls, path: str, overwrite: bool = False) -> YawCache:
        """
        Create an empty cache directory at the specifed path.

        Parameters
        ----------
        path : str
            Path at which the data and random catalogues are cached.
        overwrite : bool, optional
            Whether to overwrite an existing cache directory.

        Returns
        -------
        YawCache
            The newly created cache instance.
        """
        normalised = normalise_path(path)

        if os.path.exists(normalised):
            if not overwrite:
                raise FileExistsError(normalised)
            # check if path is valid cache directry and *only* then delete it
            try:
                cls(path).drop()
            except FileNotFoundError as err:
                raise OSError("can only overwrite existing cache directories") from err

        logger.info("creating new cache directory '%s'", normalised)
        os.makedirs(normalised)
        with open(os.path.join(normalised, cls._flag_path), "w"):
            pass
        return cls(path)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(path='{self.path}')"

    def get_patch_centers(self) -> CoordSky:
        """
        Get the patch center coordinates.

        Returns
        -------
        CoordSky
            The patch center coordinates in degrees as
            `yaw.core.coordinates.CoordSky` instance.

        Raises
        ------
        FileNotFoundError
            If not data is cached yet.
        """
        if self.rand.exists():
            return self.rand.get().centers
        if self.data.exists():
            return self.data.get().centers
        raise FileNotFoundError("cache is empty")

    def n_patches(self) -> int:
        """
        Get the number of spatial patches.

        Returns
        -------
        int
            The number of patches.

        Raises
        ------
        FileNotFoundError
            If not data is cached yet.
        """
        return len(self.get_patch_centers())

    def drop(self) -> None:
        """Delete the entire cache directy."""
        logger.info("dropping cache directory '%s'", self.path)
        rmtree(self.path)


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


def handle_has_path(handle: DataHandle) -> bool:
    """This is a workaround for a potential bug in RAIL."""
    return handle.path is not None and handle.path != "None"


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
