"""Backoff algorithms for spacing out retry attempts.

Each algorithm is a pure function of the attempt number -- no hidden state,
no sleeping -- so it can be unit tested without a clock or an event loop.
:func:`compute_delay` applies the chosen algorithm and, optionally, jitter.

Jitter (enabled by default on :class:`~edgesentinel.resilience.retry.RetryPolicy`)
matters more than it looks: without it, every client retrying the same
downstream failure backs off on the exact same schedule and then hammers the
dependency again in lockstep the moment it (maybe) recovers. That's a retry
storm, and it's the opposite of what a "reliability" layer should do.
"""

from __future__ import annotations

import random
from typing import Literal

BackoffAlgorithm = Literal["fixed", "linear", "exponential"]


def _fixed(attempt: int, initial_delay: float, max_delay: float) -> float:
    return min(initial_delay, max_delay)


def _linear(attempt: int, initial_delay: float, max_delay: float) -> float:
    return min(initial_delay * attempt, max_delay)


def _exponential(attempt: int, initial_delay: float, max_delay: float) -> float:
    # 2.0 (not 2) keeps this a float** float operation -- mypy types int**int
    # as Any because a negative int exponent would return a float, which
    # would otherwise leak an Any through this function's return type.
    return min(initial_delay * (2.0 ** (attempt - 1)), max_delay)


_ALGORITHMS = {
    "fixed": _fixed,
    "linear": _linear,
    "exponential": _exponential,
}


def compute_delay(
    algorithm: BackoffAlgorithm,
    attempt: int,
    *,
    initial_delay: float,
    max_delay: float,
    jitter: bool,
    rng: random.Random | None = None,
) -> float:
    """Return the delay in seconds before retry attempt number ``attempt``.

    Args:
        algorithm: One of ``"fixed"``, ``"linear"``, ``"exponential"``.
        attempt: The 1-indexed attempt that just failed (i.e. the delay
            returned is how long to wait before attempt ``attempt + 1``).
        initial_delay: Base delay in seconds.
        max_delay: Upper bound on the delay, applied before jitter.
        jitter: If true, apply "full jitter" -- return a uniformly random
            value between 0 and the computed base delay -- instead of the
            base delay itself.
        rng: Source of randomness for jitter. Defaults to a fresh
            :class:`random.Random`; pass a seeded instance for deterministic
            tests.
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    base = _ALGORITHMS[algorithm](attempt, initial_delay, max_delay)
    if not jitter:
        return base
    if base <= 0:
        return 0.0
    rng = rng if rng is not None else random.Random()
    return rng.uniform(0, base)
