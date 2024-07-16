from __future__ import annotations

from yaw.examples import w_sp

from rail.yaw_rail import cache, handles


def test_YawCorrFuncHandle(tmp_path):
    path = tmp_path / "test.pkl"
    handle = handles.YawCorrFuncHandle("corr_func", w_sp, path=path)

    handle.write()  # ._write()
    f = handle.open()  # ._open()
    f.close()
    assert handle.read(force=True) == w_sp  # ._read()


def test_TestYawCacheHandle(tmp_path):
    path = tmp_path / "cache.json"
    c = cache.YawCache.create(tmp_path / "cache")
    handle = handles.YawCacheHandle("cache", c, path=path)

    handle.write()  # ._write()
    assert handle.read(force=True).path == c.path  # ._open(), ._read()
