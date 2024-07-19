from __future__ import annotations

import os
from typing import TYPE_CHECKING

import numpy as np
from numpy.testing import assert_array_equal
from pytest import fixture, raises
from yaw.core.coordinates import CoordSky

from rail.yaw_rail import cache

if TYPE_CHECKING:
    from numpy.typing import NDArray
    from pandas import DataFrame
    from yaw.catalogs.scipy import ScipyCatalog


def test_patch_centers_from_file(tmp_path):
    # create a small test dataset with patch centers (RA/Dec in radians)
    ra = np.linspace(1.0, 2.0)
    dec = np.linspace(-1.0, 1.0)
    path = str(tmp_path / "coords")
    np.savetxt(path, np.transpose([ra, dec]))

    # load back and check the data
    coords = cache.patch_centers_from_file(path)
    assert_array_equal(coords.ra, ra)
    assert_array_equal(coords.dec, dec)

    # check exception thrown with invalid input data
    with raises(ValueError, match="invalid.*"):
        np.savetxt(path, np.transpose([ra, dec, dec]))
        cache.patch_centers_from_file(path)


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


N_PATCHES_COLUMN = 2


@fixture(name="mock_data_indexed")
def fixture_mock_data_indexed(mock_data_small: DataFrame, column_kwargs) -> DataFrame:
    # take the mock data and use the unused weight column to store indices for
    # the original order of the data points
    mock = mock_data_small.copy()
    col = column_kwargs["weight_name"]
    mock[col] = np.arange(len(mock_data_small))
    # assign objects to predicatble patch (indices)
    mock["patch"] = np.arange(len(mock_data_small)) % N_PATCHES_COLUMN
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

        # write the testdata to the cache directory and try to open the cache
        inst.set(mock_data_indexed, **column_kwargs, n_patches=2)
        inst = cache.YawCatalog(tmp_path / "cat")
        assert inst.exists()
        assert inst.get()  # check that testdata is loaded

        # check that drop removes the cache directory
        inst.drop()
        assert inst.catalog is None
        assert not inst.exists()

    def test_patch_center_callback(
        self, tmp_path, column_kwargs, mock_data_indexed
    ):  # pylint: disable=W0212
        # create a cache and add testdata, which will be the patch reference
        ref = cache.YawCatalog(tmp_path / "ref")
        ref.set(mock_data_indexed, **column_kwargs, n_patches=2)

        # check that the returned center coordinates match those of the testdata
        inst = cache.YawCatalog(tmp_path / "cat")
        inst.set_patch_center_callback(ref)  # link testdata to find patch centers
        fetched_centers = inst._patch_center_callback()
        assert_array_equal(fetched_centers.ra, ref.get().centers.ra)
        assert_array_equal(fetched_centers.dec, ref.get().centers.dec)

        # remove the link to the test data
        inst.set_patch_center_callback(None)
        assert inst._patch_center_callback is None

        # link an invalid type of data
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
        # create a copy of the data set by writing to a file
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        inst = cache.YawCatalog(tmp_path / "cache")
        for data_source in [mock_data_indexed, path]:
            inst.set(data_source, n_patches=2, **column_kwargs, overwrite=True)
            assert inst.get().n_patches == 2

    def test_set_patch_name(self, tmp_path, mock_data_indexed, column_kwargs):
        # create a copy of the data set by writing to a file
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        # use the weight column to verify that the objects land in the correct
        # patch when using the assignment based on the patch index column
        for data_source in (mock_data_indexed, path):
            inst = cache.YawCatalog(tmp_path / "cache")
            inst.set(data_source, patch_name="patch", **column_kwargs, overwrite=True)
            assert inst.get().n_patches == N_PATCHES_COLUMN

            yaw_catalog = inst.get()
            yaw_catalog.load()
            for i, patch in enumerate(yaw_catalog):
                assert np.all(patch.weights % N_PATCHES_COLUMN == i)

    def test_set_patch_center(self, tmp_path, mock_data_indexed, column_kwargs):
        # create a copy of the data set by writing to a file
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        # create a reference set of patch centers
        inst = cache.YawCatalog(tmp_path / "cache")
        inst.set(mock_data_indexed, n_patches=4, **column_kwargs)
        ref_centers = inst.get().centers

        # check that the patch centers remain fixed when constructing patches
        # with the reference centers
        for source in [mock_data_indexed, path]:
            inst.set(source, patch_centers=ref_centers, **column_kwargs, overwrite=True)
            assert_array_equal(inst.get().centers.ra, ref_centers.ra)
            assert_array_equal(inst.get().centers.dec, ref_centers.dec)

    def test_set_with_callback(self, tmp_path, mock_data_indexed, column_kwargs):
        n_patches = 3
        ref = cache.YawCatalog(tmp_path / "ref")
        ref.set(mock_data_indexed, n_patches=n_patches, **column_kwargs)

        # create cache that links to reference cache to determine patch centers
        inst = cache.YawCatalog(tmp_path / "cache")
        inst.set_patch_center_callback(ref)

        # try adding data with only two patch centers and verify that in every
        # case the three centers from the reference are used
        for key, value in zip(
            ["n_patches", "patch_name", "patch_centers"],
            [2, "patch", ref.get().centers[:2]],
        ):
            patch_conf = {key: value}
            inst.set(mock_data_indexed, **patch_conf, **column_kwargs, overwrite=True)
            assert len(inst.get().centers) == n_patches
            assert_array_equal(ref.get().centers.ra, inst.get().centers.ra)
            assert_array_equal(ref.get().centers.dec, inst.get().centers.dec)


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

        # verify that overwriting is permitted when the special flag file exists,
        # i.e. the directory has been created with the .create() method
        assert cache.YawCache._flag_path in set(  # pylint: disable=W0212
            os.listdir(path)
        )
        cache.YawCache.create(path, overwrite=True)
        # the dummy file should be removed now
        assert not dummy_path.exists()

        # verify that any regular directory cannot be overwriten
        path = tmp_path / "my_precious_data"
        path.mkdir()
        with raises(OSError):
            cache.YawCache.create(path, overwrite=True)

    def test_patch_centers(self, tmp_path, mock_data_indexed, column_kwargs):
        inst = cache.YawCache.create(tmp_path / "cache")
        with raises(FileNotFoundError):
            inst.get_patch_centers()
        inst.drop()

        # check that the centers from the dataset, registered first, is applied
        # to the second
        def create_fill_check_cache(use_first, use_second):
            inst = cache.YawCache.create(tmp_path / "cache")
            getattr(inst, use_first).set(
                mock_data_indexed, patch_name="patch", **column_kwargs
            )
            assert len(inst.get_patch_centers()) == inst.n_patches()

            getattr(inst, use_second).set(
                mock_data_indexed, n_patches=3, **column_kwargs
            )
            assert len(inst.data.get().centers) == 2
            assert_array_equal(inst.rand.get().centers.ra, inst.data.get().centers.ra)

            inst.drop()  # clean up for reuse

        # test both ways
        create_fill_check_cache("data", "rand")
        create_fill_check_cache("rand", "data")

    def test_drop(self, tmp_path):
        path = tmp_path / "cache"
        inst = cache.YawCache.create(path)
        assert path.exists()

        assert str(path) in str(inst)  # test __str__()
        inst.drop()
        assert not path.exists()
