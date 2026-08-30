"""Retry policy: run an operation, retrying on failure with backoff.

``RetryPolicy`` is a plain, immutable configuration object -- constructing
one has no side effects and doesn't touch a clock or an event loop, so it's
trivial to unit test and safe to share between concurrent calls. All the
actual work happens in :meth:`RetryPolicy.run`.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from edgeguard.resilience.backoff import BackoffAlgorithm, compute_delay

T = TypeVar("T")

Sleep = Callable[[float], Awaitable[None]]


@dataclasses.dataclass(frozen=True, slots=True)
class RetryAttempt:
    """Reports a single failed attempt before the next retry.

    Attributes:
        attempt: The 1-indexed attempt number that just failed.
        delay: How long :meth:`RetryPolicy.run` will wait before retrying.
        exception: The exception raised by the failed attempt.
    """

    attempt: int
    delay: float
    exception: BaseException


OnRetry = Callable[[RetryAttempt], Awaitable[None]]


@dataclasses.dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Configuration for retrying a fallible async operation.

    Example:
        >>> policy = RetryPolicy(max_attempts=5, backoff="exponential",
        ...                      initial_delay=1, max_delay=300, jitter=True)
        >>> result = await policy.run(lambda: upload(data))

    Args:
        max_attempts: Total number of attempts, including the first one
            (i.e. ``max_attempts=3`` means up to 2 retries). Must be >= 1.
        backoff: Backoff algorithm; see :mod:`edgeguard.resilience.backoff`.
        initial_delay: Base delay in seconds passed to the backoff algorithm.
        max_delay: Upper bound on the computed delay, in seconds.
        jitter: Randomize each delay to avoid synchronized retry storms
            across multiple clients/devices. Strongly recommended; only
            disable for tests that need exact, deterministic delays.
        retry_on: Exception types that should trigger a retry. Anything
            else propagates immediately on the first attempt. Defaults to
            ``(Exception,)`` -- override this for operations where retrying
            certain failures (e.g. validation errors) would be unsafe or
            pointless.
    """

    max_attempts: int = 3
    backoff: BackoffAlgorithm = "exponential"
    initial_delay: float = 1.0
    max_delay: float = 300.0
    jitter: bool = True
    retry_on: tuple[type[Exception], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.initial_delay < 0:
            raise ValueError(f"initial_delay must be >= 0, got {self.initial_delay}")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if not self.retry_on:
            raise ValueError("retry_on must contain at least one exception type")

    async def run(
        self,
        func: Callable[[], Awaitable[T]],
        *,
        on_retry: OnRetry | None = None,
        sleep: Sleep = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> T:
        """Call ``func`` (a zero-argument async callable), retrying on failure.

        The original exception from the final attempt propagates unchanged
        (with its original traceback) if every attempt fails, so callers can
        still handle specific exception types the same way they would
        without retrying.

        Args:
            func: Zero-argument async callable to invoke. Wrap arguments
                with a lambda or ``functools.partial``.
            on_retry: Optional async callback invoked after each failed
                attempt except the last, before sleeping.
            sleep: Injectable sleep function, for deterministic tests.
            rng: Injectable randomness source for jitter, for deterministic
                tests.
        """
        for attempt in range(1, self.max_attempts + 1):
            try:
                return await func()
            except self.retry_on as exc:
                if attempt == self.max_attempts:
                    raise
                delay = compute_delay(
                    self.backoff,
                    attempt,
                    initial_delay=self.initial_delay,
                    max_delay=self.max_delay,
                    jitter=self.jitter,
                    rng=rng,
                )
                if on_retry is not None:
                    await on_retry(RetryAttempt(attempt=attempt, delay=delay, exception=exc))
                await sleep(delay)
        # Unreachable: the loop above always either returns or raises on the
        # final iteration.
        raise AssertionError("unreachable")
