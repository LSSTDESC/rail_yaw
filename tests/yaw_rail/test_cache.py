from __future__ import annotations

import os

import numpy as np
from pandas import DataFrame
from numpy.testing import assert_array_equal
from numpy.typing import NDArray
from pytest import fixture, raises, mark
from yaw.catalog import Catalog
from yaw.coordinates import AngularCoordinates

from rail.yaw_rail import cache


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


@fixture(name="column_kwargs")
def fixture_column_kwargs() -> dict[str, str]:
    return dict(
        ra_name="ra",
        dec_name="dec",
        redshift_name="z",
        weight_name="index",
        max_workers=1,  # disable multiprocessing
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


def get_redshifts_ordered(cat: Catalog) -> NDArray:
    # use the weight column to get original order (see fixture_mock_data_indexed)
    order = np.argsort(cat.weights)
    return cat.redshifts[order]


def assert_coords_equal(coord1: AngularCoordinates, coord2: AngularCoordinates) -> None:
    assert_array_equal(coord1.ra, coord2.ra)
    assert_array_equal(coord1.dec, coord2.dec)


class TestYawCatalog:
    def test_filesystem(self, tmp_path, mock_data_indexed, column_kwargs):
        # should not perform any I/O
        inst = cache.YawCatalog(tmp_path / "cat")
        assert inst.path == cache.normalise_path(tmp_path / "cat")
        assert not inst.exists()

        # write the testdata to the cache directory and try to open the cache
        inst.set(mock_data_indexed, **column_kwargs, patch_num=2)
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
        # create a cache and add testdata, which will be the patch reference
        ref = cache.YawCatalog(tmp_path / "ref")
        ref.set(mock_data_indexed, **column_kwargs, patch_num=2)

        # check that the returned center coordinates match those of the testdata
        inst = cache.YawCatalog(tmp_path / "cat")
        inst.set_patch_center_callback(ref)  # link testdata to find patch centers
        fetched_centers = inst._patch_center_callback()
        assert_coords_equal(fetched_centers, ref.get().get_centers())

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

        inst.set(mock_data_indexed, patch_num=2, **column_kwargs)
        with raises(FileExistsError):
            inst.set(mock_data_indexed, patch_num=2, overwrite=False, **column_kwargs)

    def test_set_num_patches(self, tmp_path, column_kwargs, mock_data_indexed):
        # create a copy of the data set by writing to a file
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        inst = cache.YawCatalog(tmp_path / "cache")
        for data_source in [mock_data_indexed, path]:
            inst.set(data_source, patch_num=2, **column_kwargs, overwrite=True)
            assert inst.get().num_patches == 2

    def test_set_patch_name(self, tmp_path, mock_data_indexed, column_kwargs):
        # create a copy of the data set by writing to a file
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        # use the weight column to verify that the objects land in the correct
        # patch when using the assignment based on the patch index column
        for data_source in (mock_data_indexed, path):
            inst = cache.YawCatalog(tmp_path / "cache")
            inst.set(data_source, patch_name="patch", **column_kwargs, overwrite=True)
            assert inst.get().num_patches == N_PATCHES_COLUMN

            yaw_catalog = inst.get()
            for i, patch in enumerate(yaw_catalog.values()):
                assert np.all(patch.weights % N_PATCHES_COLUMN == i)

    def test_set_patch_center(self, tmp_path, mock_data_indexed, column_kwargs):
        # create a copy of the data set by writing to a file
        path = write_and_get_path(tmp_path / "data.pqt", mock_data_indexed)

        # create a reference set of patch centers
        inst = cache.YawCatalog(tmp_path / "cache")
        inst.set(mock_data_indexed, patch_num=4, **column_kwargs)
        ref_centers = inst.get().get_centers()

        # check that the patch centers remain fixed when constructing patches
        # with the reference centers
        for source in [mock_data_indexed, path]:
            inst.set(source, patch_centers=ref_centers, **column_kwargs, overwrite=True)
            assert_coords_equal(inst.get().get_centers(), ref_centers)

    @mark.parametrize(
        "patch_param, value",
        [("patch_num", 2), ("patch_name", "patch"), ("patch_centers", None)],
    )
    def test_set_with_callback(
        self, tmp_path, mock_data_indexed, column_kwargs, patch_param, value
    ):
        ref = cache.YawCatalog(tmp_path / "ref")
        ref.set(mock_data_indexed, patch_num=3, **column_kwargs)

        # create cache that links to reference cache to determine patch centers
        inst = cache.YawCatalog(tmp_path / "cache")
        inst.set_patch_center_callback(ref)

        # try adding data with only two patch centers and verify that in every
        # case the three centers from the reference are used
        if value is None:
            value = ref.get().get_centers()[:2]

        patch_conf = {patch_param: value}
        inst.set(mock_data_indexed, **patch_conf, **column_kwargs, overwrite=True)
        assert inst.get().num_patches == ref.get().num_patches
        assert_coords_equal(ref.get().get_centers(), inst.get().get_centers())


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

    @mark.parametrize(
        "create_first, create_second", [("data", "rand"), ("rand", "data")]
    )
    def test_patch_centers2(
        self, tmp_path, mock_data_indexed, column_kwargs, create_first, create_second
    ):
        inst = cache.YawCache.create(tmp_path / "cache")
        with raises(FileNotFoundError):
            inst.get_patch_centers()
        inst.drop()

        inst = cache.YawCache.create(tmp_path / "cache")
        getattr(inst, create_first).set(
            mock_data_indexed, patch_name="patch", **column_kwargs
        )
        assert len(inst.get_patch_centers()) == inst.num_patches

        getattr(inst, create_second).set(
            mock_data_indexed, patch_num=3, **column_kwargs
        )
        assert len(inst.data.get().get_centers()) == 2
        assert_coords_equal(
            inst.rand.get().get_centers(), inst.data.get().get_centers()
        )

    def test_drop(self, tmp_path):
        path = tmp_path / "cache"
        inst = cache.YawCache.create(path)
        assert path.exists()

        assert str(path) in str(inst)  # test __str__()
        inst.drop()
        assert not path.exists()
