from __future__ import annotations

from pytest import fixture

from yaw import RedshiftData
from yaw.examples import w_sp

from rail.yaw_rail import summarize


@fixture(name="redshift_data")
def fixture_redshift_data() -> RedshiftData:
    return w_sp.sample()


def test_YawRedshiftDataHandle(tmp_path, redshift_data):
    path = tmp_path / "test.pkl"
    handle = summarize.YawRedshiftDataHandle("redshift_data", redshift_data, path=path)

    handle.write()  # ._write()
    assert handle.read(force=True) == redshift_data  # ._open(), ._read()
