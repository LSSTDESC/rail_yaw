from __future__ import annotations

import numpy as np
import numpy.testing as npt
from pytest import fixture, mark, raises

from rail.yaw_rail import fitting

NUM_PATCHES = 20
NUM_SCALES = 6
NUM_ZBINS = 3

TRUE_AMPLITUDE = 3.7
TRUE_CONSTANT = 2.1


@fixture(name="template")
def fixture_template() -> np.ndarray:
    """A matter template that decreases with radius, one row per redshift bin."""
    radius = np.logspace(0.0, 3.0, NUM_SCALES)
    return np.stack([radius ** -(0.8 + 0.1 * i) for i in range(NUM_ZBINS)])


@fixture(name="weights")
def fixture_weights() -> np.ndarray:
    return np.ones(NUM_SCALES)


@fixture(name="mask")
def fixture_mask() -> np.ndarray:
    return np.ones(NUM_SCALES, dtype=bool)


def make_samples(
    template: np.ndarray,
    amplitude: float,
    constant: float,
    noise: float,
    seed: int = 12345,
) -> np.ndarray:
    """Build jackknife samples of `w = A * w_mm + C` with per-patch noise.

    Some noise is required, otherwise the jackknife covariance is singular and
    the generalised least squares fit cannot be formed.
    """
    rng = np.random.default_rng(seed)
    signal = amplitude * template.T + constant  # (num_scales, num_zbins)

    samples = np.tile(signal, (NUM_PATCHES, 1, 1))
    return samples + rng.normal(0.0, noise, size=samples.shape)


def test_fit_amplitude_recovers_input(template, weights, mask):
    samples = make_samples(template, TRUE_AMPLITUDE, 0.0, noise=1e-4)

    amplitude, amp_samples = fitting.fit_amplitude(
        samples, template, weights, mask, use_jk_cov=False
    )

    assert amp_samples.shape == (NUM_PATCHES, NUM_ZBINS)
    npt.assert_allclose(amplitude, TRUE_AMPLITUDE, rtol=1e-3)


@mark.parametrize("use_jk_cov", [False, True])
def test_fit_amplitude_constant_recovers_input(template, weights, mask, use_jk_cov):
    samples = make_samples(template, TRUE_AMPLITUDE, TRUE_CONSTANT, noise=1e-4)

    amplitude, _, constant, const_samples = fitting.fit_amplitude_constant(
        samples, template, weights, mask, use_jk_cov=use_jk_cov
    )

    assert const_samples.shape == (NUM_PATCHES, NUM_ZBINS)
    npt.assert_allclose(amplitude, TRUE_AMPLITUDE, rtol=1e-2)
    npt.assert_allclose(constant, TRUE_CONSTANT, rtol=1e-2)


def test_fit_amplitude_ignores_constant_offset(template, weights, mask):
    """Without a fitted constant, an additive offset biases the amplitude, which
    is exactly why the constant is offered as an option."""
    samples = make_samples(template, TRUE_AMPLITUDE, TRUE_CONSTANT, noise=1e-4)

    amplitude, _ = fitting.fit_amplitude(
        samples, template, weights, mask, use_jk_cov=False
    )

    assert not np.allclose(amplitude, TRUE_AMPLITUDE, rtol=1e-2)


def test_jackknife_error_shrinks_with_noise(template, weights, mask):
    errors = []
    for noise in (1e-2, 1e-3):
        samples = make_samples(template, TRUE_AMPLITUDE, 0.0, noise=noise)
        amplitude, amp_samples = fitting.fit_amplitude(
            samples, template, weights, mask, use_jk_cov=False
        )
        errors.append(fitting._jackknife_error(amp_samples, amplitude))

    assert np.all(errors[0] > 0.0)
    assert np.all(errors[1] < errors[0])


def test_diagonal_weights_change_the_fit(template, mask):
    """The radial weights only act on the diagonal fit; in the generalised fit
    they cancel analytically."""
    samples = make_samples(template, TRUE_AMPLITUDE, TRUE_CONSTANT, noise=0.1)

    radius = np.logspace(0.0, 3.0, NUM_SCALES)
    flat = np.ones(NUM_SCALES)
    steep = radius**-2.0

    def fit(weights, use_jk_cov):
        return fitting.fit_amplitude(
            samples, template, weights, mask, use_jk_cov=use_jk_cov
        )[0]

    assert not np.allclose(fit(flat, False), fit(steep, False))
    npt.assert_allclose(fit(flat, True), fit(steep, True))


def test_scale_mask_selects_scales(template, weights):
    samples = make_samples(template, TRUE_AMPLITUDE, 0.0, noise=1e-4)

    mask = np.zeros(NUM_SCALES, dtype=bool)
    mask[2:] = True

    amplitude, _ = fitting.fit_amplitude(
        samples, template, weights, mask, use_jk_cov=False
    )
    npt.assert_allclose(amplitude, TRUE_AMPLITUDE, rtol=1e-3)


def test_template_shape_is_validated(template, weights, mask):
    samples = make_samples(template, TRUE_AMPLITUDE, 0.0, noise=1e-4)

    with raises(ValueError, match=".*template shape.*"):
        fitting.fit_amplitude(samples, template.T, weights, mask)


def test_cov_inv_requires_enough_samples():
    rng = np.random.default_rng(12345)
    num_features = 5

    # the Hartlap correction is only defined while num_samples > num_features + 2
    too_few = rng.normal(size=(num_features + 2, num_features))
    assert fitting._cov_inv(too_few, "auto", 0.0, hartlap=True) is None

    enough = rng.normal(size=(num_features + 3, num_features))
    assert fitting._cov_inv(enough, "auto", 0.0, hartlap=True) is not None

    # without the correction the inverse only needs the covariance to be regular
    assert fitting._cov_inv(too_few, "auto", 0.0, hartlap=False) is not None
    assert fitting._cov_inv(too_few[:1], "auto", 0.0, hartlap=False) is None


def test_fit_falls_back_to_diagonal_when_covariance_is_degenerate(template, weights):
    """With more fitted scales than patches the jackknife covariance cannot be
    inverted, and the fit must fall back to the diagonal solution rather than
    fail."""
    mask = np.ones(NUM_SCALES, dtype=bool)
    samples = make_samples(template, TRUE_AMPLITUDE, 0.0, noise=1e-4)[: NUM_SCALES + 1]

    amplitude, _ = fitting.fit_amplitude(
        samples, template, weights, mask, use_jk_cov=True
    )
    npt.assert_allclose(amplitude, TRUE_AMPLITUDE, rtol=1e-3)


def test_ledoit_wolf_intensity_is_bounded():
    rng = np.random.default_rng(12345)
    samples = rng.normal(size=(30, 5))

    cov, intensity = fitting._ledoit_wolf_diag(samples)

    assert 0.0 <= intensity <= 1.0
    assert cov.shape == (5, 5)
    npt.assert_allclose(cov, cov.T)


def test_shrink_cov_interpolates_to_the_diagonal():
    cov = np.array([[2.0, 1.0], [1.0, 3.0]])

    npt.assert_allclose(fitting._shrink_cov(cov, 0.0), cov)
    npt.assert_allclose(fitting._shrink_cov(cov, 1.0), np.diag([2.0, 3.0]))


@mark.parametrize(
    "value,expect", [("auto", "auto"), (None, "auto"), ("0.5", 0.5), (0.25, 0.25)]
)
def test_parse_shrinkage(value, expect):
    assert fitting.parse_shrinkage(value) == expect


@mark.parametrize("value", ["nonsense", "-0.5", "1.5"])
def test_parse_shrinkage_rejects_invalid(value):
    with raises(ValueError, match=".*shrinkage.*"):
        fitting.parse_shrinkage(value)
