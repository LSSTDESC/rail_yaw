from __future__ import annotations

from yaw import examples

from rail.yaw_rail import cache, handles


def test_YawCorrFuncHandle(tmp_path):
    path = tmp_path / "test.pkl"
    handle = handles.YawCorrFuncHandle("corr_func", examples.w_sp, path=path)

    handle.write()  # ._write()
    f = handle.open()  # ._open()
    f.close()
    assert handle.read(force=True) == examples.w_sp  # ._read()


def test_TestYawCacheHandle(tmp_path):
    path = tmp_path / "cache.json"
    c = cache.YawCache.create(tmp_path / "cache")
    handle = handles.YawCacheHandle("cache", c, path=path)

    handle.write()  # ._write()
    assert handle.read(force=True).path == c.path  # ._open(), ._read()
