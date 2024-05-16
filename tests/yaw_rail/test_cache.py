from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from numpy.testing import assert_array_equal
from pytest import fixture, mark, raises
from yaw.core.coordinates import CoordSky

from rail.yaw_rail import cache

if TYPE_CHECKING:
    from numpy.typing import NDArray
    from pandas import DataFrame
    from yaw.catalogs.scipy import ScipyCatalog


def test_get_patch_method():
    # test the parameter hierarchy
    kwargs = dict(
        patch_centers=CoordSky([1.0], [1.0]),
        patch_name="patch",
        n_patches=1,
    )
    assert cache.get_patch_method(**kwargs) == kwargs["patch_centers"]

    kwargs["patch_centers"] = None
    assert cache.get_patch_method(**kwargs) == kwargs["patch_name"]

    kwargs["patch_name"] = None
    assert cache.get_patch_method(**kwargs) == kwargs["n_patches"]

    # no values given
    kwargs["n_patches"] = None
    with raises(ValueError):
        cache.get_patch_method(**kwargs)


@fixture(name="column_kwargs")
def fixture_column_kwargs() -> dict[str, str]:
    return dict(
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        weight_name="index",
    )


@fixture(name="mock_data_indexed")
def fixture_mock_data_indexed(mock_data_small: DataFrame, column_kwargs) -> DataFrame:
    mock = mock_data_small.copy()
    col = column_kwargs["weight_name"]
    mock[col] = np.arange(len(mock_data_small))  # useful to restore original order
    mock["patch"] = np.arange(len(mock_data_small)) % 2
    return mock


def write_and_get_path(path: str, data: DataFrame) -> str:
    data.to_parquet(path)
    return str(path)


def get_redshifts_ordered(cat: ScipyCatalog) -> NDArray:
    # use the weight column to get original order (see fixture_mock_data_indexed)
    order = np.argsort(cat.weights)
    return cat.redshifts[order]


class TestYawCatalog:
    def test_filesystem(self, tmp_path, mock_data_indexed, column_kwargs):
        # should not perform any I/O
        inst = cache.YawCatalog(tmp_path / "cat")
        assert inst.path == cache.normalise_path(tmp_path / "cat")
        assert not inst.exists()

        # add data and create a new instance
        inst.set(mock_data_indexed, **column_kwargs, n_patches=2)
        inst = cache.YawCatalog(tmp_path / "cat")
        assert inst.exists()
        assert inst.get()

        # check that drop removes the cache directory
        inst.drop()
        assert inst.catalog is None
        assert not inst.exists()

    def test_patch_center_callback(
        self, tmp_path, column_kwargs, mock_data_indexed
    ):  # pylint: disable=W0212
        ref = cache.YawCatalog(tmp_path / "ref")
        ref.set(mock_data_indexed, **column_kwargs, n_patches=2)

        # check that the returned center coordinates match the registered reference
        inst = cache.YawCatalog(tmp_path / "cat")
        inst.set_patch_center_callback(ref)
        assert_array_equal(
            inst._patch_center_callback().ra,
            ref.get().centers.ra,
        )
        assert_array_equal(
            inst._patch_center_callback().dec,
            ref.get().centers.dec,
        )

        # set callback
        inst.set_patch_center_callback(None)
        assert inst._patch_center_callback is None

        with raises(TypeError):
            inst.set_patch_center_callback("wrong type")

    def test_set_errors(self, tmp_path, column_kwargs, mock_data_indexed):
        inst = cache.YawCatalog(tmp_path / "cat")

        with raises(FileNotFoundError):
            inst.get()

        inst.set(mock_data_indexed, n_patches=2, **column_kwargs)
        with raises(FileExistsError):
            inst.set(mock_data_indexed, n_patches=2, **column_kwargs)

    def test_set_n_patches(self, tmp_path, column_kwargs, mock_data_indexed):
        inst = cache.YawCatalog(tmp_path / "cat")
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        for source in [mock_data_indexed, path]:
            inst.set(source, n_patches=2, **column_kwargs, overwrite=True)
            z = get_redshifts_ordered(inst.get())
            assert_array_equal(z, mock_data_indexed["z"])
            assert inst.get().n_patches == 2

    def test_set_patch_name(self, tmp_path, mock_data_indexed, column_kwargs):
        inst = cache.YawCatalog(tmp_path / "cat")
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        for source in [mock_data_indexed, path]:
            inst.set(source, patch_name="patch", **column_kwargs, overwrite=True)
            for i in (0, 1):
                patch = inst.get()[i]
                patch.load()
                # see fixture_mock_data_indexed
                assert_array_equal(patch.redshifts, mock_data_indexed["z"][i::2])

    def test_set_patch_center(self, tmp_path, mock_data_indexed, column_kwargs):
        inst = cache.YawCatalog(tmp_path / "cat")
        inst.set(mock_data_indexed, patch_name="patch", **column_kwargs)
        centers = inst.get().centers
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        for source in [mock_data_indexed, path]:
            inst.set(source, patch_centers=centers, **column_kwargs, overwrite=True)
            assert_array_equal(inst.get().centers.ra, centers.ra)

    def test_set_with_callback(self, tmp_path, mock_data_indexed, column_kwargs):
        n_patches = 3
        ref = cache.YawCatalog(tmp_path / "ref")
        ref.set(mock_data_indexed, n_patches=n_patches, **column_kwargs)

        inst = cache.YawCatalog(tmp_path / "cat")
        inst.set_patch_center_callback(ref)

        for key, value in zip(
            ["n_patches", "patch_name", "patch_centers"],
            [2, "patch", ref.get().centers[:2]],
        ):
            patch_conf = {key: value}
            inst.set(mock_data_indexed, **patch_conf, **column_kwargs, overwrite=True)
            assert len(inst.get().centers) == n_patches


class TestYawCache:
    def test_init(self, tmp_path):
        with raises(FileNotFoundError):
            cache.YawCache(tmp_path / "not_existing")

        # cache indicator file does not exist
        with raises(FileNotFoundError):
            cache.YawCache(tmp_path)

    def test_create(self, tmp_path):
        inst = cache.YawCache.create(tmp_path / "not_existing")
        assert cache.YawCache.is_valid(inst.path)

        with raises(FileExistsError):
            cache.YawCache.create(tmp_path)

    def test_overwrite(self, tmp_path):
        # create a cache with some file inside
        path = tmp_path / "cache"
        cache.YawCache.create(path)
        dummy_path = tmp_path / "cache" / "dummy.file"
        with open(dummy_path, "w"):
            pass

        # overwrite the directory
        cache.YawCache.create(path, overwrite=True)
        assert not dummy_path.exists()

        # do not allow overwriting any normal directory
        path = tmp_path / "my_precious_data"
        path.mkdir()
        with raises(OSError):
            cache.YawCache.create(path, overwrite=True)

    def test_patch_centers(self, tmp_path, mock_data_indexed, column_kwargs):
        inst = cache.YawCache.create(tmp_path / "cache")
        with raises(FileNotFoundError):
            inst.get_patch_centers()

        # check that random centers are used
        inst = cache.YawCache.create(tmp_path / "cache1")
        inst.rand.set(mock_data_indexed, patch_name="patch", **column_kwargs)
        assert len(inst.get_patch_centers()) == inst.n_patches()

        inst.data.set(mock_data_indexed, n_patches=3, **column_kwargs)
        assert len(inst.data.get().centers) == 2
        assert_array_equal(inst.rand.get().centers.ra, inst.data.get().centers.ra)

        # check that data centers are used
        inst = cache.YawCache.create(tmp_path / "cache2")
        inst.data.set(mock_data_indexed, patch_name="patch", **column_kwargs)
        assert len(inst.get_patch_centers()) == inst.n_patches()

        inst.rand.set(mock_data_indexed, n_patches=3, **column_kwargs)
        assert len(inst.rand.get().centers) == 2
        assert_array_equal(inst.rand.get().centers.ra, inst.data.get().centers.ra)

    def test_drop(self, tmp_path):
        path = tmp_path / "cache"
        inst = cache.YawCache.create(path)
        assert str(path) in str(inst)  # test __str__()
        inst.drop()
        assert not path.exists()


def test_TestYawCacheHandle(tmp_path):
    path = tmp_path / "cache.json"
    c = cache.YawCache.create(tmp_path / "cache")
    handle = cache.YawCacheHandle("cache", c, path=path)

    handle.write()  # ._write()
    assert handle.read(force=True).path == c.path  # ._open(), ._read()


@mark.parametrize(
    "value,expect", [("/some/path", True), ("None", False), (None, False)]
)
def test_handle_has_path(value, expect):
    class DummyHandle:
        path = value

    dummy = DummyHandle()
    assert cache.handle_has_path(dummy) == expect


def test_stage_helper():
    name = "test"
    aliases = cache.stage_helper(name)
    assert all(alias == f"{key}_{name}" for key, alias in aliases.items())
