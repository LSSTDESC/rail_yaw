from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from pandas import read_parquet

if TYPE_CHECKING:  # pragma: no cover
    from pandas import DataFrame

__all__ = [
    "get_dc2_test_data",
]


@lru_cache
def get_dc2_test_data() -> DataFrame:
    return read_parquet("https://portal.nersc.gov/cfs/lsst/PZ/test_dc2_rail_yaw.pqt")
