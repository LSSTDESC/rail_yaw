from __future__ import annotations

import os
from typing import TYPE_CHECKING

from pandas import DataFrame, read_parquet
from pytest import fixture
from yaw import UniformRandoms

from rail.core.stage import RailStage
from rail import yaw_rail

if TYPE_CHECKING:
    from rail.core.data import DataStore


@fixture(name="data_store", scope="session", autouse=True)
def fixture_data_store() -> DataStore:
    data_store = RailStage.data_store
    data_store.__class__.allow_overwrite = True
    return data_store


@fixture(name="seed", scope="session")
def fixture_seed() -> int:
    return 12345


@fixture(name="mock_data", scope="session")
def fixture_mock_data(seed) -> DataFrame:
    repo_root, _ = yaw_rail.__file__.rsplit("/src", 1)
    path = os.path.join(repo_root, "examples", "example_data.pqt")
    return read_parquet(path).sample(20000, random_state=seed)


@fixture(name="zlim", scope="session")
def fixture_zlim(mock_data):
    redshifts = mock_data["z"].to_numpy()
    return (redshifts.min(), redshifts.max())


@fixture(name="mock_rand", scope="session")
def fixture_mock_rand(mock_data, seed) -> DataFrame:
    n_data = len(mock_data)
    redshifts = mock_data["z"].to_numpy()
    angular_rng = UniformRandoms(
        mock_data["ra"].min(),
        mock_data["ra"].max(),
        mock_data["dec"].min(),
        mock_data["dec"].max(),
        seed=seed,
    )
    return angular_rng.generate(2 * n_data, draw_from=dict(z=redshifts))
