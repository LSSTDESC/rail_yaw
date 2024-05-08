from __future__ import annotations

import os
import subprocess
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from pandas import read_parquet

if TYPE_CHECKING:
    from pandas import DataFrame

__all__ = [
    "get_dc2_test_data",
]


def get_dc2_test_data() -> DataFrame:
    link = (
        "https://drive.usercontent.google.com/"
        "download?id=1n9_vfbTlUrlyR6eUwHxJLZ35OjYf8Uyf&export=download"
    )
    with TemporaryDirectory() as tmp_path:
        path = os.path.join(tmp_path, "test_dc2_rail_yaw.pqt")
        try:
            subprocess.run(
                ["curl", link, "-o", path],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            data = read_parquet(path)
        except Exception as err:
            raise OSError("downloading test data failed") from err
    return data
