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

config_cache = dict(
    path=StageParameter(
        str, required=True, msg="path to cache directory, must not exist"
    ),
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


class YawCatalog:
    """
    Wrapper around a *yet_another_wizz* catalog.
    """

    path: str
    catalog: ScipyCatalog | None

    def __init__(self, path: str) -> None:
        self.path = normalise_path(path)
        self.catalog = None
        self._patch_center_callback = None

    def set_patch_center_callback(self, cat: YawCatalog) -> None:
        self._patch_center_callback = lambda: cat.get().centers

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def get(self) -> ScipyCatalog:
        if not self.exists():
            raise FileNotFoundError(f"no catalog cached at {self.path}")
        self.catalog = NewCatalog().from_cache(self.path)
        return self.catalog

    def set(
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
    ) -> ScipyCatalog:
        if self.exists():
            if overwrite:
                shutil.rmtree(self.path)
            else:
                raise FileExistsError(self.path)
        os.makedirs(self.path)

        try:
            patch_centers = self._patch_center_callback()
        except (TypeError, FileNotFoundError):
            pass

        if isinstance(source, str):
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
        if self.exists():
            shutil.rmtree(self.path)
        self.catalog = None


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
    data: YawCatalog
    rand: YawCatalog

    def __init__(self, path: str) -> None:
        """Open an existing cache directory at the specified path."""
        normalised = normalise_path(path)
        if not os.path.exists(normalised):
            raise FileNotFoundError(normalised)
        self.path = normalised

        self.data = YawCatalog(os.path.join(self.path, "data"))
        self.rand = YawCatalog(os.path.join(self.path, "rand"))
        self.data.set_patch_center_callback(self.rand)
        self.rand.set_patch_center_callback(self.data)

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

    def get_patch_centers(self) -> CoordSky:
        """Get the patch centers for a non-empty cache."""
        if self.rand.exists():
            return self.rand.get().centers
        if self.data.exists():
            return self.data.get().centers
        raise FileNotFoundError("cache is empty")

    def n_patches(self) -> int:
        """Get the number of spatial patches for a non-empty cache."""
        return len(self.get_patch_centers())

    def drop(self) -> None:
        """Delete the entire cache directy. Invalidates this instance."""
        if not os.path.exists(self.path):
            return
        self.data.drop()
        self.rand.drop()

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
    config_items=dict(
        **config_cache,
        **yaw_config_columns,
        **yaw_config_patches,
    ),
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
        config = self.get_config_dict()

        with yaw_logged(config["verbose"]):
            if config["patches"] is not None:
                patch_centers = YawCache(config["patches"]).get_patch_centers()
            else:
                patch_centers = None

            cache = YawCache.create(config["path"])

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
