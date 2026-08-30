from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from edgeguard.resilience.retry import RetryAttempt, RetryPolicy


class FlakyError(Exception):
    pass


class UnrelatedError(Exception):
    pass


def test_invalid_max_attempts_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


def test_invalid_delay_bounds_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(initial_delay=-1)
    with pytest.raises(ValueError):
        RetryPolicy(initial_delay=10, max_delay=5)


def test_empty_retry_on_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(retry_on=())


async def test_succeeds_on_first_attempt_without_sleeping() -> None:
    policy = RetryPolicy(max_attempts=3, jitter=False)
    sleeps: list[float] = []

    async def op() -> str:
        return "ok"

    result = await policy.run(op, sleep=_record(sleeps))
    assert result == "ok"
    assert sleeps == []


async def test_succeeds_after_transient_failures() -> None:
    policy = RetryPolicy(max_attempts=3, backoff="fixed", initial_delay=0.01, jitter=False)
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise FlakyError("not yet")
        return "ok"

    sleeps: list[float] = []
    result = await policy.run(op, sleep=_record(sleeps))

    assert result == "ok"
    assert calls == 3
    assert sleeps == [0.01, 0.01]


async def test_reraises_original_exception_after_exhausting_attempts() -> None:
    policy = RetryPolicy(max_attempts=3, backoff="fixed", initial_delay=0.01, jitter=False)

    async def op() -> str:
        raise FlakyError("still broken")

    with pytest.raises(FlakyError, match="still broken"):
        await policy.run(op, sleep=_record([]))


async def test_does_not_retry_exceptions_outside_retry_on() -> None:
    policy = RetryPolicy(max_attempts=5, retry_on=(FlakyError,))
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise UnrelatedError("not retryable")

    with pytest.raises(UnrelatedError):
        await policy.run(op, sleep=_record([]))
    assert calls == 1


async def test_on_retry_callback_receives_attempt_details() -> None:
    policy = RetryPolicy(max_attempts=3, backoff="fixed", initial_delay=5.0, jitter=False)
    attempts: list[RetryAttempt] = []
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise FlakyError(f"fail-{calls}")
        return "ok"

    async def on_retry(info: RetryAttempt) -> None:
        attempts.append(info)

    await policy.run(op, on_retry=on_retry, sleep=_record([]))

    assert [a.attempt for a in attempts] == [1, 2]
    assert [a.delay for a in attempts] == [5.0, 5.0]
    assert str(attempts[0].exception) == "fail-1"


async def test_never_sleeps_after_the_final_attempt() -> None:
    policy = RetryPolicy(max_attempts=2, backoff="fixed", initial_delay=1.0, jitter=False)
    sleeps: list[float] = []

    async def op() -> str:
        raise FlakyError("always fails")

    with pytest.raises(FlakyError):
        await policy.run(op, sleep=_record(sleeps))

    # 2 attempts means exactly 1 sleep in between, never a sleep after the
    # last (doomed) attempt.
    assert sleeps == [1.0]


def _record(calls: list[float]) -> Callable[[float], Awaitable[None]]:
    async def sleep(delay: float) -> None:
        calls.append(delay)

    return sleep
