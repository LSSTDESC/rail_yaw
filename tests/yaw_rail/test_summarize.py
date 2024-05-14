from __future__ import annotations

import numpy as np
from numpy.testing import assert_array_almost_equal
from pytest import fixture

from yaw import RedshiftData
from yaw.examples import w_sp

from rail.yaw_rail import summarize


@fixture(name="redshift_data")
def fixture_redshift_data() -> RedshiftData:
    return w_sp.sample()


@fixture(name="redshift_data_nfin")
def fixture_redshift_data_nfin() -> RedshiftData:
    nz = w_sp.sample()
    # insert values that are not finite
    data = nz.data
    data[0] = np.nan
    data[-1] = np.inf

    samples = nz.samples
    samples[:, 0] = np.nan
    samples[:, -1] = np.inf

    return RedshiftData(
        binning=nz.get_binning(),
        data=data,
        samples=samples,
        method=nz.method,
        info=nz.info,
    )


def test_clip_negative_values(redshift_data_nfin):
    clipped = summarize.clip_negative_values(redshift_data_nfin)

    data_bad = (redshift_data_nfin.data < 0.0) | ~np.isfinite(redshift_data_nfin.data)
    assert np.all(clipped.data[data_bad] == 0.0)

    samples_bad = (redshift_data_nfin.samples < 0.0) | ~np.isfinite(
        redshift_data_nfin.samples
    )
    assert np.all(clipped.samples[samples_bad] == 0.0)


def test_redshift_data_to_qp(redshift_data_nfin):
    ensemble = summarize.redshift_data_to_qp(redshift_data_nfin)
    pdfs = ensemble.pdf(redshift_data_nfin.mids)

    nz_clipped = summarize.clip_negative_values(redshift_data_nfin)
    nz_mids = nz_clipped.mids
    for pdf, sample in zip(pdfs, nz_clipped.samples):
        norm = np.trapz(sample, nz_mids)
        assert_array_almost_equal(pdf, sample / norm)


def test_YawRedshiftDataHandle(tmp_path, redshift_data):
    path = tmp_path / "test.pkl"
    handle = summarize.YawRedshiftDataHandle("redshift_data", redshift_data, path=path)

    handle.write()  # ._write()
    assert handle.read(force=True) == redshift_data  # ._open(), ._read()
