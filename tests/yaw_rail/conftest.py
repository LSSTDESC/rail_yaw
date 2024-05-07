from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import fixture

from rail.core.stage import RailStage

if TYPE_CHECKING:
    from rail.core.data import DataStore


@fixture(name="data_store", scope="session", autouse=True)
def fixture_data_store() -> DataStore:
    data_store = RailStage.data_store
    data_store.__class__.allow_overwrite = True
    return data_store
