from __future__ import annotations

import random

import pytest

from edgesentinel.resilience.backoff import compute_delay


def test_fixed_backoff_never_grows() -> None:
    delays = [
        compute_delay("fixed", n, initial_delay=2.0, max_delay=100.0, jitter=False)
        for n in range(1, 5)
    ]
    assert delays == [2.0, 2.0, 2.0, 2.0]


def test_linear_backoff_grows_by_a_constant_step() -> None:
    delays = [
        compute_delay("linear", n, initial_delay=1.0, max_delay=100.0, jitter=False)
        for n in range(1, 5)
    ]
    assert delays == [1.0, 2.0, 3.0, 4.0]


def test_exponential_backoff_doubles_each_attempt() -> None:
    delays = [
        compute_delay("exponential", n, initial_delay=1.0, max_delay=1000.0, jitter=False)
        for n in range(1, 5)
    ]
    assert delays == [1.0, 2.0, 4.0, 8.0]


@pytest.mark.parametrize("algorithm", ["linear", "exponential"])
def test_delay_is_capped_at_max_delay(algorithm: str) -> None:
    delay = compute_delay(algorithm, attempt=20, initial_delay=1.0, max_delay=5.0, jitter=False)  # type: ignore[arg-type]
    assert delay == 5.0


def test_fixed_backoff_is_also_capped_if_initial_delay_exceeds_max_delay() -> None:
    delay = compute_delay("fixed", 1, initial_delay=10.0, max_delay=5.0, jitter=False)
    assert delay == 5.0


def test_jitter_returns_a_value_between_zero_and_base_delay() -> None:
    rng = random.Random(42)
    delay = compute_delay("fixed", 1, initial_delay=10.0, max_delay=10.0, jitter=True, rng=rng)
    assert 0.0 <= delay <= 10.0


def test_jitter_is_deterministic_given_a_seeded_rng() -> None:
    delay_a = compute_delay(
        "fixed", 1, initial_delay=10.0, max_delay=10.0, jitter=True, rng=random.Random(7)
    )
    delay_b = compute_delay(
        "fixed", 1, initial_delay=10.0, max_delay=10.0, jitter=True, rng=random.Random(7)
    )
    assert delay_a == delay_b


def test_jitter_of_zero_base_delay_is_zero() -> None:
    delay = compute_delay("fixed", 1, initial_delay=0.0, max_delay=0.0, jitter=True)
    assert delay == 0.0


def test_attempt_below_one_is_rejected() -> None:
    with pytest.raises(ValueError):
        compute_delay("fixed", 0, initial_delay=1.0, max_delay=10.0, jitter=False)
