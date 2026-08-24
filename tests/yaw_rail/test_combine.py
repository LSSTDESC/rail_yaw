from __future__ import annotations

import numpy as np
import numpy.testing as npt
from pytest import fixture, mark, raises

from rail.yaw_rail import combine, cosmology


@fixture(name="cosmo", scope="module")
def fixture_cosmo():
    return cosmology.get_ccl_cosmology("Planck15")


def make_samples(mean: np.ndarray, scatter: float, seed: int = 12345) -> np.ndarray:
    """Build jackknife samples scattered around a mean."""
    rng = np.random.default_rng(seed)
    return mean[None, :] + rng.normal(0.0, scatter, size=(20, len(mean)))


def test_bias_evolution_constant(cosmo):
    redshift = np.array([0.1, 0.5, 1.0])
    npt.assert_array_equal(
        combine.bias_evolution(redshift, "constant", cosmo), np.ones(3)
    )


def test_bias_evolution_growth_factor(cosmo):
    redshift = np.array([0.1, 0.5, 1.0])

    bias = combine.bias_evolution(redshift, "growth_factor", cosmo)
    npt.assert_allclose(bias, 1.0 / cosmology.growth_factor(redshift, cosmo))

    assert np.all(np.diff(bias) > 0.0)  # bias grows towards high redshift


def test_bias_evolution_rejects_unknown_mode(cosmo):
    with raises(ValueError, match=".*unsupported bias mode.*"):
        combine.bias_evolution(np.array([0.1]), "nonsense", cosmo)


def test_ncc_from_amplitudes():
    """With A_sp = 4, A_ss = 4 and unit bias, n_cc = 4 / sqrt(4) = 2."""
    amp_cross = np.full(3, 4.0)
    amp_auto = np.full(3, 4.0)
    bias = np.ones(3)

    ncc, error, covariance = combine.ncc_from_amplitudes(
        amp_cross,
        make_samples(amp_cross, 0.1),
        amp_auto,
        make_samples(amp_auto, 0.1, seed=999),
        bias,
    )

    npt.assert_allclose(ncc, 2.0)
    assert np.all(error > 0.0)
    npt.assert_allclose(covariance, covariance.T)


def test_ncc_from_amplitudes_applies_bias():
    amp_cross = np.full(3, 4.0)
    amp_auto = np.full(3, 4.0)
    cross_samples = make_samples(amp_cross, 0.1)
    auto_samples = make_samples(amp_auto, 0.1, seed=999)

    bias = np.array([1.0, 2.0, 4.0])
    ncc, error, _ = combine.ncc_from_amplitudes(
        amp_cross, cross_samples, amp_auto, auto_samples, bias
    )
    npt.assert_allclose(ncc, 2.0 / bias)

    # both the estimate and its uncertainty scale as 1 / b_p
    _, unbiased_error, _ = combine.ncc_from_amplitudes(
        amp_cross, cross_samples, amp_auto, auto_samples, np.ones(3)
    )
    npt.assert_allclose(error, unbiased_error / bias)


def test_ncc_error_propagation_matches_jacobian():
    """Check the propagated uncertainty against the analytic Jacobian."""
    amp_cross = np.array([4.0])
    amp_auto = np.array([4.0])
    bias = np.array([1.0])

    cross_samples = make_samples(amp_cross, 0.2)
    auto_samples = make_samples(amp_auto, 0.3, seed=999)

    _, error, _ = combine.ncc_from_amplitudes(
        amp_cross, cross_samples, amp_auto, auto_samples, bias
    )

    var_cross = combine._sample_covariance(cross_samples)[0, 0]
    var_auto = combine._sample_covariance(auto_samples)[0, 0]

    # d ncc / d A_sp = 1 / (sqrt(A_ss) * b_p) = 1/2
    # d ncc / d A_ss = - ncc / (2 * A_ss)     = -1/4
    expect = np.sqrt(0.5**2 * var_cross + 0.25**2 * var_auto)
    npt.assert_allclose(error, [expect])


def test_ncc_paired_differs_from_propagated():
    amp_cross = np.full(3, 4.0)
    amp_auto = np.full(3, 4.0)
    bias = np.ones(3)
    cross_samples = make_samples(amp_cross, 0.1)
    auto_samples = make_samples(amp_auto, 0.1, seed=999)

    _, propagated, _ = combine.ncc_from_amplitudes(
        amp_cross, cross_samples, amp_auto, auto_samples, bias, paired=False
    )
    _, paired, _ = combine.ncc_from_amplitudes(
        amp_cross, cross_samples, amp_auto, auto_samples, bias, paired=True
    )

    assert np.all(paired > 0.0)
    assert not np.allclose(paired, propagated)


def test_combine_inverse_variance_weights_by_variance():
    """Two tracers measuring 1 +/- 1 and 3 +/- 1 in the same bin combine to
    2 +/- 1/sqrt(2)."""
    series = dict(
        a=dict(z=np.array([0.5]), nz=np.array([1.0]), nz_err=np.array([1.0])),
        b=dict(z=np.array([0.5]), nz=np.array([3.0]), nz_err=np.array([1.0])),
    )

    redshift, nz, nz_err = combine.combine_inverse_variance(series)

    npt.assert_allclose(redshift, [0.5])
    npt.assert_allclose(nz, [2.0])
    npt.assert_allclose(nz_err, [1.0 / np.sqrt(2.0)])


def test_combine_inverse_variance_favours_precise_tracer():
    series = dict(
        a=dict(z=np.array([0.5]), nz=np.array([1.0]), nz_err=np.array([1.0])),
        b=dict(z=np.array([0.5]), nz=np.array([3.0]), nz_err=np.array([0.5])),
    )

    _, nz, nz_err = combine.combine_inverse_variance(series)

    # weights of 1 and 4 give (1 * 1 + 4 * 3) / 5 = 2.6
    npt.assert_allclose(nz, [2.6])
    npt.assert_allclose(nz_err, [np.sqrt(1.0 / 5.0)])


def test_combine_inverse_variance_partial_overlap():
    """Tracers covering different redshift ranges yield the union of their bins;
    bins measured by a single tracer are passed through unchanged."""
    series = dict(
        a=dict(
            z=np.array([0.1, 0.2, 0.3]),
            nz=np.array([1.0, 1.0, 1.0]),
            nz_err=np.array([1.0, 1.0, 1.0]),
        ),
        b=dict(
            z=np.array([0.3, 0.4]),
            nz=np.array([3.0, 5.0]),
            nz_err=np.array([1.0, 1.0]),
        ),
    )

    redshift, nz, nz_err = combine.combine_inverse_variance(series)

    npt.assert_allclose(redshift, [0.1, 0.2, 0.3, 0.4])
    npt.assert_allclose(nz, [1.0, 1.0, 2.0, 5.0])  # only z=0.3 is combined
    npt.assert_allclose(nz_err, [1.0, 1.0, 1.0 / np.sqrt(2.0), 1.0])


def test_combine_inverse_variance_drops_invalid_points():
    series = dict(
        a=dict(
            z=np.array([0.1, 0.2]),
            nz=np.array([1.0, np.nan]),
            nz_err=np.array([1.0, 1.0]),
        ),
        b=dict(z=np.array([0.2]), nz=np.array([4.0]), nz_err=np.array([0.0])),
    )

    redshift, nz, _ = combine.combine_inverse_variance(series)

    # the NaN estimate and the zero uncertainty are both discarded
    npt.assert_allclose(redshift, [0.1])
    npt.assert_allclose(nz, [1.0])


def test_combine_inverse_variance_rejects_incompatible_binning():
    """Tracers whose ranges overlap but whose bins never coincide cannot be
    combined, which must be reported rather than silently passed through."""
    series = dict(
        a=dict(
            z=np.array([0.1, 0.2, 0.3]),
            nz=np.ones(3),
            nz_err=np.ones(3),
        ),
        b=dict(
            z=np.array([0.15, 0.25]),  # offset by half a bin
            nz=np.ones(2),
            nz_err=np.ones(2),
        ),
    )

    with raises(ValueError, match=".*share no redshift bin.*"):
        combine.combine_inverse_variance(series)


def test_combine_inverse_variance_allows_disjoint_tracers():
    series = dict(
        a=dict(z=np.array([0.1]), nz=np.array([1.0]), nz_err=np.array([1.0])),
        b=dict(z=np.array([0.9]), nz=np.array([2.0]), nz_err=np.array([1.0])),
    )

    redshift, nz, _ = combine.combine_inverse_variance(series)

    npt.assert_allclose(redshift, [0.1, 0.9])
    npt.assert_allclose(nz, [1.0, 2.0])


def test_combine_inverse_variance_requires_data():
    series = dict(
        a=dict(z=np.array([0.1]), nz=np.array([np.nan]), nz_err=np.array([1.0]))
    )

    with raises(ValueError, match=".*no tracer.*"):
        combine.combine_inverse_variance(series)


def test_fit_nonneg_bspline_is_nonnegative():
    """A spline fitted to data that scatters below zero must stay non-negative,
    so that its integral is not biased low."""
    redshift = np.linspace(0.1, 1.0, 20)
    nz = np.exp(-(((redshift - 0.5) / 0.2) ** 2))
    nz[:3] = -0.2  # the measurement scatters below zero at the edges

    spline = combine.fit_nonneg_bspline(redshift, nz, np.full(20, 0.1))

    fine = np.linspace(redshift[0], redshift[-1], 200)
    assert np.all(spline(fine) >= -1e-12)
    assert spline.integrate(redshift[0], redshift[-1]) > 0.0


def test_fit_nonneg_bspline_requires_enough_points():
    with raises(ValueError, match=".*not enough finite points.*"):
        combine.fit_nonneg_bspline(np.array([0.1, 0.2]), np.array([1.0, 1.0]))


@mark.parametrize("paired", [False, True])
def test_sample_covariance_requires_samples(paired):
    with raises(ValueError, match=".*not enough finite jackknife samples.*"):
        combine._sample_covariance(np.array([[1.0, 2.0]]))
