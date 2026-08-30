from __future__ import annotations

import asyncio

import pytest

from edgesentinel.core.events import Event, EventBus
from edgesentinel.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from edgesentinel.resilience.policy import build_reliable_decorator
from edgesentinel.resilience.retry import RetryPolicy
from edgesentinel.resilience.timeout import OperationTimeoutError


class FlakyError(Exception):
    pass


async def test_bare_decorator_succeeds_on_first_try() -> None:
    decorator = build_reliable_decorator()

    @decorator
    async def op(x: int) -> int:
        return x * 2

    assert await op(21) == 42


async def test_retries_are_applied_with_no_real_delay_needed_for_zero_initial_delay() -> None:
    decorator = build_reliable_decorator(retries=3, initial_delay=0.0, jitter=False)
    calls = 0

    @decorator
    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise FlakyError("not yet")
        return "ok"

    assert await op() == "ok"
    assert calls == 3


async def test_per_attempt_timeout_is_enforced() -> None:
    decorator = build_reliable_decorator(retries=1, timeout=0.01)

    @decorator
    async def op() -> str:
        await asyncio.sleep(10)
        return "too slow"

    with pytest.raises(OperationTimeoutError):
        await op()


async def test_circuit_breaker_true_creates_a_private_breaker_per_function() -> None:
    decorator_a = build_reliable_decorator(
        retries=1, circuit_breaker=True, name="a", initial_delay=0.0
    )
    decorator_b = build_reliable_decorator(
        retries=1, circuit_breaker=True, name="b", initial_delay=0.0
    )

    @decorator_a
    async def a() -> str:
        raise FlakyError("a broken")

    @decorator_b
    async def b() -> str:
        return "b fine"

    with pytest.raises(FlakyError):
        await a()
    # b's breaker is independent of a's -- a tripping must not affect b.
    assert await b() == "b fine"


async def test_shared_circuit_breaker_trips_across_functions() -> None:
    shared = CircuitBreaker(failure_threshold=1, recovery_timeout=60)
    decorator_a = build_reliable_decorator(
        retries=1, circuit_breaker=shared, name="a", initial_delay=0.0
    )
    decorator_b = build_reliable_decorator(
        retries=1, circuit_breaker=shared, name="b", initial_delay=0.0
    )

    @decorator_a
    async def a() -> str:
        raise FlakyError("dependency down")

    @decorator_b
    async def b() -> str:
        return "should not run"

    with pytest.raises(FlakyError):
        await a()
    with pytest.raises(CircuitBreakerOpenError):
        await b()


async def test_circuit_breaker_wraps_the_whole_retried_operation_not_each_attempt() -> None:
    # failure_threshold=1 but retries=3: if the breaker saw every attempt,
    # it would trip on attempt 1 and reject attempts 2-3. Since it wraps the
    # whole retried call, it should only see one net failure after retries
    # are exhausted, or one net success if a retry succeeds.
    shared = CircuitBreaker(failure_threshold=1, recovery_timeout=60)
    decorator = build_reliable_decorator(
        retries=3, circuit_breaker=shared, initial_delay=0.0, jitter=False
    )
    calls = 0

    @decorator
    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise FlakyError("transient")
        return "ok"

    assert await op() == "ok"
    assert calls == 3
    assert shared.state.value == "closed"


async def test_open_breaker_short_circuits_before_any_retry_attempt() -> None:
    shared = CircuitBreaker(failure_threshold=1, recovery_timeout=60)
    decorator = build_reliable_decorator(retries=5, circuit_breaker=shared, initial_delay=0.0)
    calls = 0

    @decorator
    async def op() -> str:
        nonlocal calls
        calls += 1
        raise FlakyError("down")

    with pytest.raises(FlakyError):
        await op()
    assert calls == 5  # exhausted retries once, tripping the breaker

    calls = 0
    with pytest.raises(CircuitBreakerOpenError):
        await op()
    assert calls == 0  # breaker rejected before any attempt was made


async def test_events_are_published_for_retries_and_final_outcome() -> None:
    bus = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    bus.subscribe(collect)

    decorator = build_reliable_decorator(
        retries=3, initial_delay=0.0, jitter=False, events=bus, component="test"
    )
    calls = 0

    @decorator
    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise FlakyError("first try fails")
        return "ok"

    assert await op() == "ok"

    types = [e.type for e in seen]
    assert types == ["retry_attempt", "operation_succeeded"]


async def test_failure_event_published_when_retries_exhausted() -> None:
    bus = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    bus.subscribe(collect)

    decorator = build_reliable_decorator(
        retries=2, initial_delay=0.0, jitter=False, events=bus, component="test"
    )

    @decorator
    async def op() -> str:
        raise FlakyError("always fails")

    with pytest.raises(FlakyError):
        await op()

    types = [e.type for e in seen]
    assert types == ["retry_attempt", "operation_failed"]


async def test_explicit_retry_policy_overrides_shorthand_kwargs() -> None:
    policy = RetryPolicy(max_attempts=5, initial_delay=0.0, jitter=False)
    decorator = build_reliable_decorator(retries=1, retry=policy)
    calls = 0

    @decorator
    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 5:
            raise FlakyError("not yet")
        return "ok"

    assert await op() == "ok"
    assert calls == 5
