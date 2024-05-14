from __future__ import annotations

from yaw.examples import w_sp

from rail.yaw_rail import correlation


def test_YawCorrFuncHandle(tmp_path):
    path = tmp_path / "test.pkl"
    handle = correlation.YawCorrFuncHandle("corr_func", w_sp, path=path)

    handle.write()  # ._write()
    f = handle.open()  # ._open()
    f.close()
    assert handle.read(force=True) == w_sp  # ._read()
