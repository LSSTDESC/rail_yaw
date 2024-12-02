"""
This file implements a wrapper for a cache directory for *yet_another_wizz*
catalogs. The cache is designed to hold a pair of a data and an (optional)
random catalog. The patch center coordinates are enforced to be consistent
within a cache. These caches are created by `YawCacheCreate`, but must currently
be removed manually by the user when they are no longer needed.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING

import numpy as np
from yaw.catalog import Catalog
from yaw.coordinates import AngularCoordinates

if TYPE_CHECKING:
    from pandas import DataFrame

__all__ = [
    "YawCache",
]

logger = logging.getLogger(__name__)


def normalise_path(path: str) -> str:
    """Substitute UNIX style home directories and environment variables in path
    names."""
    return os.path.expandvars(os.path.expanduser(path))


def patch_centers_from_file(path: str) -> AngularCoordinates:
    """
    Load a list of patch centers from a file.

    Patch centers are expected to be listed line-by-line as pairs of R.A./Dec.
    in radian, separated by a single space or tab.

    Parameters
    ----------
    path: str
        Path to input file.

    Returns
    -------
    CoordSky
        List of patch centers read from file.
    """
    coords = np.loadtxt(path, ndmin=2)
    try:
        return AngularCoordinates(coords)
    except Exception as err:
        raise ValueError("invalid coordinate file format or schema") from err


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
    catalog: Catalog | None
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
            self._patch_center_callback = lambda: cat.get().get_centers()
        else:
            raise TypeError("referenced catalog is not a 'YawCatalog'")

    def exists(self) -> bool:
        """Whether the catalog's cache directory exists."""
        return os.path.exists(self.path)

    def get(self, max_workers: int | None = None) -> Catalog:
        """
        Access the catalog instance without loading all data to memory.

        Retrieves the catalog metadata from disk if not in memory.

        Parameters
        ----------
        max_workers: int, optional
            Number of parallel workers to use for processing the input data.

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
            self.catalog = Catalog(self.path, max_workers=max_workers)
        return self.catalog

    def set(
        self,
        source: DataFrame | str,
        ra_name: str,
        dec_name: str,
        *,
        patch_centers: Catalog | AngularCoordinates | None = None,
        patch_name: str | None = None,
        patch_num: int | None = None,
        redshift_name: str | None = None,
        weight_name: str | None = None,
        degrees: bool = True,
        overwrite: bool = False,
        max_workers: int | None = None,
        probe_size: int = -1,
        **kwargs,  # pylint: disable=W0613; allows dict-unpacking of whole config
    ) -> Catalog:
        """
        Split a new data set in spatial patches and cache it.

        Parameters
        ----------
        source : DataFrame or str
            Data source, either a `DataFrame` or a FITS, Parquet, or HDF5 file.
        ra_name : str
            Column name of right ascension data.
        dec_name : str
            Column name of declination data.
        weight_name: str or None, optional
            Column name of per-object weigths.
        redshift_name : str or None, optional
            Column name of redshifts.
        patch_centers : ScipyCatalog, Coordinate or None
            A *yet_another_wizz* catalog or coordinates, or `None` if not set.
        patch_name : str or None
            The name of the column that list the patch indices or `None` if not set.
        patch_num: int or None
            The number of patches to generate using k-means clustering or `None` if
            not set.
        degrees : bool, optional
            Whether the input coordinates are in degrees or radian.
        overwrite: bool, optional
            Whether to overwrite an existing, cached data set.
        max_workers: int, optional
            Number of parallel workers to use for processing the input data.
        probe_size : int, optional
            The approximate number of objects to sample from the input file when
            generating patch centers.

        Returns
        -------
        ScipyCatalog
            The cached catalog instance.

        Raises
        ------
        FileExistsError
            If there is already a data set cached and `overwrite` is not set.
        """
        if not overwrite and os.path.exists(self.path):
            raise FileExistsError(self.path)

        # check if any reference catalog is registered that overwrites the
        # provided patch centers
        try:
            patch_centers = self._patch_center_callback()
        except (TypeError, FileNotFoundError):
            pass

        if isinstance(source, (str, Path)):
            constructor = Catalog.from_file
        else:
            constructor = Catalog.from_dataframe

        self.catalog = constructor(
            self.path,
            source,
            ra_name=ra_name,
            dec_name=dec_name,
            weight_name=weight_name,
            redshift_name=redshift_name,
            patch_centers=patch_centers,
            patch_name=patch_name,
            patch_num=patch_num,
            degrees=degrees,
            overwrite=overwrite,
            max_workers=max_workers,
            probe_size=probe_size,
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

    _flag_path = ".yaw_cache"  # file to mark a valid cache directory
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
        # datasets should reference eachother to apply any existing patch centers
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
                tmp_cache = cls(path)
            except FileNotFoundError as err:
                raise OSError("can only overwrite existing cache directories") from err
            tmp_cache.drop()

        logger.info("creating new cache directory '%s'", normalised)
        os.makedirs(normalised)
        # create the flag file
        with open(os.path.join(normalised, cls._flag_path), "w"):
            pass
        return cls(path)

    def __str__(self) -> str:
        return f"{type(self).__name__}(path='{self.path}')"

    def get_patch_centers(self) -> AngularCoordinates:
        """
        Get the patch center coordinates.

        Returns
        -------
        CoordSky
            The patch center coordinates in radian as
            `yaw.core.coordinates.CoordSky` instance.

        Raises
        ------
        FileNotFoundError
            If not data is cached yet.
        """
        if self.rand.exists():
            return self.rand.get().get_centers()
        if self.data.exists():
            return self.data.get().get_centers()
        raise FileNotFoundError("no data set cached")

    @property
    def num_patches(self) -> int:
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
