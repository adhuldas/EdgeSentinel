from __future__ import annotations

import asyncio

import pytest

from edgesentinel.core.events import Event, EventBus
from edgesentinel.core.exceptions import InvalidStateTransitionError
from edgesentinel.core.state import RuntimeState
from edgesentinel.process.watchdog import UnknownWatchTargetError, Watchdog


async def _instant_sleep(_: float) -> None:
    await asyncio.sleep(0)


class _FakeClock:
    """A settable clock, so tests can advance time explicitly instead of
    guessing how many times :class:`Watchdog` calls ``now()`` internally."""

    def __init__(self, start: float = 0.0) -> None:
        self.time = start

    def __call__(self) -> float:
        return self.time


async def test_non_positive_poll_interval_is_rejected() -> None:
    with pytest.raises(ValueError):
        Watchdog(poll_interval=0)


async def test_non_positive_timeout_is_rejected() -> None:
    watchdog = Watchdog()
    with pytest.raises(ValueError):
        watchdog.register("t", timeout=0)


async def test_set_state_without_get_state_is_rejected() -> None:
    async def set_state(_: RuntimeState) -> None:
        pass

    with pytest.raises(ValueError):
        Watchdog(set_state=set_state)


async def test_heartbeat_for_an_unregistered_name_raises() -> None:
    watchdog = Watchdog()
    with pytest.raises(UnknownWatchTargetError):
        watchdog.heartbeat("does-not-exist")


async def test_a_target_within_its_timeout_is_not_stale() -> None:
    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock)
    watchdog.register("t", timeout=10.0)

    clock.time = 5.0
    assert await watchdog.check_once() == ()
    assert watchdog.stale == ()


async def test_a_target_that_never_heartbeats_goes_stale() -> None:
    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock)
    watchdog.register("t", timeout=10.0)

    clock.time = 20.0
    newly_stale = await watchdog.check_once()

    assert newly_stale == ("t",)
    assert watchdog.stale == ("t",)


async def test_heartbeat_clears_staleness() -> None:
    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock)
    watchdog.register("t", timeout=10.0)

    clock.time = 20.0
    await watchdog.check_once()
    assert watchdog.stale == ("t",)

    clock.time = 21.0
    watchdog.heartbeat("t")

    clock.time = 25.0
    assert await watchdog.check_once() == ()  # within 10s of the 21.0 heartbeat


async def test_events_published_only_on_transitions() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock, events=events, component="proc")
    watchdog.register("t", timeout=10.0)

    clock.time = 20.0
    await watchdog.check_once()  # newly stale

    clock.time = 21.0
    watchdog.heartbeat("t")
    clock.time = 22.0
    await watchdog.check_once()  # recovered

    clock.time = 40.0
    await watchdog.check_once()  # stale again

    types = [e.type for e in seen]
    assert types == [
        "watchdog_target_stale",
        "watchdog_target_recovered",
        "watchdog_target_stale",
    ]
    assert all(e.component == "proc" for e in seen)


async def test_unregister_stops_tracking() -> None:
    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock)
    watchdog.register("t", timeout=10.0)
    watchdog.unregister("t")

    clock.time = 20.0
    assert await watchdog.check_once() == ()
    assert watchdog.stale == ()


async def test_start_runs_an_initial_check_and_stop_cancels_cleanly() -> None:
    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock, sleep=_instant_sleep)
    watchdog.register("t", timeout=10.0)
    clock.time = 20.0

    await watchdog.start()
    try:
        assert watchdog.stale == ("t",)
    finally:
        await watchdog.stop()
        await watchdog.stop()  # idempotent


async def test_escalate_moves_runtime_to_failed() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock, set_state=set_state, get_state=lambda: state)
    watchdog.register("t", timeout=10.0)

    clock.time = 20.0
    await watchdog.check_once()

    assert state is RuntimeState.FAILED


async def test_escalate_never_touches_stopping_or_stopped() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while STOPPING")

    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock, set_state=set_state, get_state=lambda: RuntimeState.STOPPING)
    watchdog.register("t", timeout=10.0)

    clock.time = 20.0
    assert await watchdog.check_once() == ("t",)  # still detects staleness


async def test_escalate_swallows_an_illegal_transition() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise InvalidStateTransitionError(RuntimeState.STOPPING, target)

    clock = _FakeClock(0.0)
    watchdog = Watchdog(now=clock, set_state=set_state, get_state=lambda: RuntimeState.HEALTHY)
    watchdog.register("t", timeout=10.0)

    clock.time = 20.0
    assert await watchdog.check_once() == ("t",)
