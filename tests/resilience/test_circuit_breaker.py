from __future__ import annotations

import asyncio
from typing import cast

import pytest

from edgeguard.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)


class FailingBackend(Exception):
    pass


async def _ok() -> str:
    return "ok"


async def _fail() -> str:
    raise FailingBackend("backend down")


def test_invalid_failure_threshold_rejected() -> None:
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)


def test_invalid_recovery_timeout_rejected() -> None:
    with pytest.raises(ValueError):
        CircuitBreaker(recovery_timeout=-1)


async def test_starts_closed() -> None:
    cb = CircuitBreaker()
    assert cb.state is CircuitState.CLOSED


async def test_successful_calls_keep_it_closed() -> None:
    cb = CircuitBreaker(failure_threshold=2)
    for _ in range(5):
        assert await cb.call(_ok) == "ok"
    assert cb.state is CircuitState.CLOSED
    assert cb.failure_count == 0


async def test_trips_open_after_failure_threshold_consecutive_failures() -> None:
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
    for _ in range(3):
        with pytest.raises(FailingBackend):
            await cb.call(_fail)
    assert cb.state is CircuitState.OPEN


async def test_open_circuit_rejects_calls_without_invoking_the_function() -> None:
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=60)
    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    assert cb.state is CircuitState.OPEN

    calls = 0

    async def should_not_run() -> str:
        nonlocal calls
        calls += 1
        return "unreachable"

    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(should_not_run)
    assert calls == 0


async def test_success_resets_the_failure_count() -> None:
    cb = CircuitBreaker(failure_threshold=3)
    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    assert cb.failure_count == 2

    await cb.call(_ok)
    assert cb.failure_count == 0
    assert cb.state is CircuitState.CLOSED


async def test_transitions_to_half_open_after_recovery_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_now = [1000.0]
    monkeypatch.setattr("time.monotonic", lambda: fake_now[0])

    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30)
    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    assert cb.state is CircuitState.OPEN

    # Not enough time has passed yet.
    fake_now[0] += 10
    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(_ok)
    assert cb.state is CircuitState.OPEN

    # Recovery timeout has now elapsed: the next call is a HALF_OPEN trial.
    fake_now[0] += 30
    assert await cb.call(_ok) == "ok"
    # mypy narrows `cb.state` to Literal[OPEN] from the assert above and
    # doesn't invalidate that across the intervening await; cast back to the
    # full enum so this genuinely new read can be checked against CLOSED.
    assert cast(CircuitState, cb.state) is CircuitState.CLOSED


async def test_half_open_failure_reopens_the_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_now = [1000.0]
    monkeypatch.setattr("time.monotonic", lambda: fake_now[0])

    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30)
    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    fake_now[0] += 31

    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    assert cb.state is CircuitState.OPEN


async def test_half_open_allows_only_one_concurrent_trial_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_now = [1000.0]
    monkeypatch.setattr("time.monotonic", lambda: fake_now[0])

    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30)
    with pytest.raises(FailingBackend):
        await cb.call(_fail)
    fake_now[0] += 31
    assert cb.state is CircuitState.OPEN

    release = asyncio.Event()
    entered = asyncio.Event()

    async def slow_trial() -> str:
        entered.set()
        await release.wait()
        return "ok"

    trial_task = asyncio.create_task(cb.call(slow_trial))
    await entered.wait()

    # A second caller arriving while the trial is in flight must be
    # rejected immediately, not queued behind it.
    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(_ok)

    release.set()
    assert await trial_task == "ok"
    # See the cast note above -- same false-narrowing across an await.
    assert cast(CircuitState, cb.state) is CircuitState.CLOSED


async def test_concurrent_failures_trip_exactly_once_at_threshold() -> None:
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

    results = await asyncio.gather(*(cb.call(_fail) for _ in range(10)), return_exceptions=True)

    assert all(isinstance(r, (FailingBackend, CircuitBreakerOpenError)) for r in results)
    assert cb.state is CircuitState.OPEN


class FakeStore:
    def __init__(self) -> None:
        self.records: dict[str, tuple[str, int, float | None]] = {}

    async def save_circuit_breaker_state(
        self, name: str, *, state: str, failure_count: int, opened_at: float | None
    ) -> None:
        self.records[name] = (state, failure_count, opened_at)

    async def load_circuit_breaker_state(self, name: str) -> tuple[str, int, float | None] | None:
        return self.records.get(name)


async def test_persists_state_to_the_store_on_every_transition() -> None:
    store = FakeStore()
    cb = CircuitBreaker(failure_threshold=1, name="cloud-api", store=store)

    with pytest.raises(FailingBackend):
        await cb.call(_fail)

    state, failure_count, opened_at = store.records["cloud-api"]
    assert state == "open"
    assert failure_count == 1
    assert opened_at is not None


async def test_restore_reopens_a_still_recovering_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_now = [2000.0]
    monkeypatch.setattr("time.monotonic", lambda: fake_now[0])
    monkeypatch.setattr("time.time", lambda: fake_now[0])

    store = FakeStore()
    await store.save_circuit_breaker_state(
        "cloud-api", state="open", failure_count=3, opened_at=fake_now[0] - 10
    )

    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=60, name="cloud-api", store=store)
    await cb.restore()

    assert cb.state is CircuitState.OPEN
    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(_ok)


async def test_restore_moves_to_half_open_if_recovery_window_already_passed() -> None:
    store = FakeStore()
    await store.save_circuit_breaker_state(
        "cloud-api", state="open", failure_count=3, opened_at=0.0
    )

    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=5, name="cloud-api", store=store)
    await cb.restore()

    assert cb.state is CircuitState.HALF_OPEN
    assert await cb.call(_ok) == "ok"
    # See the cast note above -- same false-narrowing across an await.
    assert cast(CircuitState, cb.state) is CircuitState.CLOSED


async def test_restore_with_no_prior_state_is_a_no_op() -> None:
    store = FakeStore()
    cb = CircuitBreaker(name="never-seen-before", store=store)
    await cb.restore()
    assert cb.state is CircuitState.CLOSED


async def test_restore_without_a_store_is_a_no_op() -> None:
    cb = CircuitBreaker()
    await cb.restore()
    assert cb.state is CircuitState.CLOSED
