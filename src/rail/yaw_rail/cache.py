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

import json
import os
import shutil
from typing import TYPE_CHECKING, Any, TextIO

from yaw import NewCatalog

from ceci.config import StageParameter
from rail.core.data import DataHandle, TableHandle
from rail.yaw_rail.logging import yaw_logged
from rail.yaw_rail.stage import YawRailStage

if TYPE_CHECKING:
    from pandas import DataFrame
    from yaw.catalogs.scipy import ScipyCatalog
    from yaw.core.coordinates import Coordinate, CoordSky

__all__ = [
    "YawCache",
    "YawCacheCreate",
    "YawCacheDrop",
    "YawCacheHandle",
]


yaw_config_columns = dict(
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

yaw_config_patches = dict(
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

config_cache_path = StageParameter(
    str, required=True, msg="path to cache directory, must not exist"
)


def normalise_path(path: str) -> str:
    """
    Substitute UNIX style home directories and environment variables in path
    names.
    """
    return os.path.expandvars(os.path.expanduser(path))


def get_patch_method(patch_centers, patch_name, n_patches) -> Any:
    """
    Select the preferred parameter value from the set of optional patch
    construction parameters. Raises a `ValueError` if none of the parameters is
    configured.
    """
    # preferred order, "create" should be the last resort
    if patch_centers is not None:  # deterministic and consistent
        return patch_centers
    if patch_name is not None:  # deterministic but assumes consistency
        return patch_name
    if n_patches is not None:  # non-determistic and never consistent
        return n_patches
    raise ValueError("no patch creation method specified")


def cache_dataset(
    cache_directory: str,
    overwrite: bool = False,
    *,
    source: DataFrame | str,
    ra_name: str,
    dec_name: str,
    patch_name: str | None = None,
    patch_centers: ScipyCatalog | Coordinate | None = None,
    n_patches: int | None = None,
    redshift_name: str | None = None,
    weight_name: str | None = None,
) -> None:
    """Split a data set, specified through a file path, into spatial patches and
    cache it in the specified directory. The directory created automatically,
    additional parameters define the relevant column names and patch creation
    method."""
    if os.path.exists(cache_directory):
        if overwrite:
            shutil.rmtree(cache_directory)
        else:
            raise FileExistsError(cache_directory)
    os.makedirs(cache_directory)

    if isinstance(source, str):
        patches = get_patch_method(
            patch_centers=patch_centers,
            patch_name=patch_name,
            n_patches=n_patches,
        )
        NewCatalog().from_file(
            filepath=source,
            patches=patches,
            ra=ra_name,
            dec=dec_name,
            redshift=redshift_name,
            weight=weight_name,
            cache_directory=cache_directory,
        )

    else:
        if patch_centers is not None:
            patch_name = None
            n_patches = None
        NewCatalog().from_dataframe(
            data=source,
            ra_name=ra_name,
            dec_name=dec_name,
            patch_centers=patch_centers,
            patch_name=patch_name,
            n_patches=n_patches,
            redshift_name=redshift_name,
            weight_name=weight_name,
            cache_directory=cache_directory,
        )


class YawCache:
    """
    A cache directory for yet_another_wizz to store a data and (optional)
    random catalogue that is split into spatial patches.

    Create a new instance with the `create()` method or open an existing cache
    (no checks are performed). Remove the cache using the `drop()` method.
    Additional methods exist to populte the cache or retrieve the cached data
    in the yet_another_wizz catalog format.
    """

    path: str

    def __init__(self, path: str) -> None:
        """Open an existing cache directory at the specified path."""
        normalised = normalise_path(path)
        if not os.path.exists(normalised):
            raise FileNotFoundError(normalised)
        self.path = normalised

    @classmethod
    def create(cls, path: str) -> YawCache:
        """Create an empty cache directory at the specifed path."""
        normalised = normalise_path(path)
        if os.path.exists(normalised):
            raise FileExistsError(normalised)

        os.makedirs(normalised)
        return cls(path)

    def __str__(self) -> str:
        return f"{self.__class__.__name__} @ {self.path}"

    @property
    def path_data(self) -> str:
        """Get the path at which the data sample is stored."""
        return os.path.join(self.path, "data")

    def has_data(self) -> bool:
        """Check if the data sample exists."""
        return os.path.exists(self.path_data)

    def get_data(self) -> ScipyCatalog:
        """Load the data sample as yet_another_wizz catalog."""
        if not self.has_data():
            raise FileNotFoundError("cache contains no data")
        return NewCatalog().from_cache(self.path_data)

    def store_data(
        self,
        source: DataFrame | str,
        ra_name: str,
        dec_name: str,
        *,
        patch_name: str | None = None,
        patch_centers: ScipyCatalog | Coordinate | None = None,
        n_patches: int | None = None,
        redshift_name: str | None = None,
        weight_name: str | None = None,
        overwrite: bool = False,
        **kwargs,  # pylint: disable=W0613; allows dict-unpacking of whole config
    ) -> None:
        """
        Split a data set, specified through a file path, into spatial patches
        and cache it. The additional parameters define the relevant column names
        and patch creation method. If the cache already contains randoms, it is
        used to define the patch centers.
        """
        try:
            # if randoms are cached, use their centers for internal consistency
            patch_centers = self.get_rand().centers
        except FileNotFoundError:
            pass
        cache_dataset(
            self.path_data,
            overwrite,
            source=source,
            ra_name=ra_name,
            dec_name=dec_name,
            patch_name=patch_name,
            patch_centers=patch_centers,
            n_patches=n_patches,
            redshift_name=redshift_name,
            weight_name=weight_name,
        )

    @property
    def path_rand(self) -> str:
        """Get the path at which the randoms are stored."""
        return os.path.join(self.path, "rand")

    def has_rand(self) -> bool:
        """Check if the randoms exist."""
        return os.path.exists(self.path_rand)

    def get_rand(self) -> ScipyCatalog:
        """Load the randoms as yet_another_wizz catalog."""
        if not self.has_rand():
            raise FileNotFoundError("cache contains no randoms")
        return NewCatalog().from_cache(self.path_rand)

    def store_rand(
        self,
        source: DataFrame | str,
        ra_name: str,
        dec_name: str,
        *,
        patch_name: str | None = None,
        patch_centers: ScipyCatalog | Coordinate | None = None,
        n_patches: int | None = None,
        redshift_name: str | None = None,
        weight_name: str | None = None,
        overwrite: bool = False,
        **kwargs,  # pylint: disable=W0613; allows dict-unpacking of whole config
    ) -> None:
        """
        Split a data set, specified through a file path, into spatial patches
        and cache it. The additional parameters define the relevant column names
        and patch creation method. If the cache already contains a data set, it
        is used to define the patch centers.
        """
        try:
            # if a data set is cached, use its centers for internal consistency
            patch_centers = self.get_data().centers
        except FileNotFoundError:
            pass
        cache_dataset(
            self.path_rand,
            overwrite,
            source=source,
            ra_name=ra_name,
            dec_name=dec_name,
            patch_name=patch_name,
            patch_centers=patch_centers,
            n_patches=n_patches,
            redshift_name=redshift_name,
            weight_name=weight_name,
        )

    def get_patch_centers(self) -> CoordSky:
        """Get the patch centers for a non-empty cache."""
        if self.has_rand():
            return self.get_rand().centers
        if self.has_data():
            return self.get_data().centers
        raise FileNotFoundError("cache is empty")

    def n_patches(self) -> int:
        """Get the number of spatial patches for a non-empty cache."""
        return len(self.get_patch_centers())

    def reset(self) -> None:
        """Delete all cached data."""
        if self.has_data():
            shutil.rmtree(self.path_data)
        if self.has_rand():
            shutil.rmtree(self.path_rand)

    def drop(self) -> None:
        """Delete the entire cache directy. Invalidates this instance."""
        if not os.path.exists(self.path):
            return
        self.reset()

        try:
            # safety: if any other data is present, something is wrong and we
            # don't want to delete the cache root directory and all its contents
            os.rmdir(self.path)
        except OSError as err:
            msg = "unaccounted cache directory contents, deletion failed"
            raise OSError(msg) from err


class YawCacheHandle(DataHandle):
    data: YawCache

    @classmethod
    def _open(cls, path: str, **kwargs) -> TextIO:
        return open(path)

    @classmethod
    def _read(cls, path: str, **kwargs) -> YawCache:
        with cls._open(path, **kwargs) as f:
            inst_args = json.load(f)
        return YawCache(**inst_args)

    @classmethod
    def _write(cls, data: YawCache, path: str, **kwargs) -> None:
        with cls._open(path, mode="w") as f:
            inst_args = dict(path=data.path)  # can restore cache from path alone
            json.dump(inst_args, f)


def handle_has_path(handle: DataHandle) -> bool:
    """This is a workaround for a potential bug in RAIL."""
    return handle.path is not None and handle.path != "None"


class YawCacheCreate(
    YawRailStage,
    path=config_cache_path,
    **yaw_config_columns,
    **yaw_config_patches,
):
    """
    Split a data and (optional) random data set into spatial patches and
    cache them on disk.

    @YawParameters

    Returns
    -------
    cache: YawCacheHandle
        Handle to the newly created cache directory.
    """

    inputs = [
        ("data", TableHandle),
        ("rand", TableHandle),
    ]
    outputs = [
        ("cache", YawCacheHandle),
    ]

    def create(
        self, data: TableHandle, rand: TableHandle | None = None
    ) -> YawCacheHandle:
        self.set_data("data", data)
        self.set_optional_data("rand", rand)

        self.run()
        return self.get_handle("cache")

    def run(self) -> None:
        with yaw_logged(self.config_options["verbose"].value):
            patch_path = self.config_options["patches"].value
            if patch_path is not None:
                patch_centers = YawCache(patch_path).get_patch_centers()
            else:
                patch_centers = None

            cache = YawCache.create(self.config_options["path"].value)

            rand: TableHandle | None = self.get_optional_handle("rand")
            if rand is not None:
                cache.store_rand(
                    source=rand.path if handle_has_path(rand) else rand.read(),
                    patch_centers=patch_centers,
                    **self.get_stageparams(),
                )

            data: TableHandle = self.get_handle("data")
            cache.store_data(
                source=data.path if handle_has_path(data) else data.read(),
                patch_centers=patch_centers,
                **self.get_stageparams(),
            )

        self.add_data("cache", cache)


class YawCacheDrop(YawRailStage):
    """
    Delete an existing cache.
    """

    inputs = [
        ("cache", YawCacheHandle),
    ]
    outputs = []

    def run(self) -> None:
        cache: YawCache = self.get_data("cache")
        cache.drop()

    def drop(self, cache: YawCache) -> None:
        self.set_data("cache", cache)
        self.run()
