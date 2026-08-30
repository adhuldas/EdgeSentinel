from __future__ import annotations

import pytest

from edgesentinel.core.events import Event, EventBus, StateChangeEvent
from edgesentinel.core.exceptions import InvalidStateTransitionError
from edgesentinel.core.lifecycle import LifecycleManager
from edgesentinel.core.state import RuntimeState, StateMachine


def make_manager() -> tuple[LifecycleManager, StateMachine, EventBus, list[StateChangeEvent]]:
    sm = StateMachine(RuntimeState.BOOTING)
    events = EventBus()
    handlers: list[StateChangeEvent] = []

    async def record(change: StateChangeEvent) -> None:
        handlers.append(change)

    manager = LifecycleManager(sm, events, [record])
    return manager, sm, events, handlers


async def test_boot_reaches_healthy_and_runs_on_init() -> None:
    manager, sm, _, handlers = make_manager()
    init_called = False

    async def on_init() -> None:
        nonlocal init_called
        init_called = True
        # by the time on_init runs, we must already be INITIALIZING
        assert sm.current is RuntimeState.INITIALIZING

    await manager.boot(on_init=on_init)

    assert init_called
    assert sm.current is RuntimeState.HEALTHY
    assert [h.current for h in handlers] == [
        RuntimeState.INITIALIZING,
        RuntimeState.HEALTHY,
    ]


async def test_boot_transitions_to_failed_when_on_init_raises() -> None:
    manager, sm, _, handlers = make_manager()

    async def on_init() -> None:
        raise RuntimeError("disk full")

    with pytest.raises(RuntimeError, match="disk full"):
        await manager.boot(on_init=on_init)

    assert sm.current is RuntimeState.FAILED
    assert [h.current for h in handlers] == [
        RuntimeState.INITIALIZING,
        RuntimeState.FAILED,
    ]


async def test_boot_publishes_state_change_events_to_the_bus() -> None:
    manager, _sm, events, _ = make_manager()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    async def on_init() -> None:
        return None

    await manager.boot(on_init=on_init)

    assert [e.type for e in seen] == ["state_change", "state_change"]
    assert seen[-1].metadata == {"previous": "initializing", "current": "healthy"}


async def test_shutdown_reaches_stopped_and_runs_on_stop_even_from_degraded() -> None:
    sm = StateMachine(RuntimeState.DEGRADED)
    events = EventBus()
    manager = LifecycleManager(sm, events, [])
    stop_called = False

    async def on_stop() -> None:
        nonlocal stop_called
        stop_called = True

    await manager.shutdown(on_stop=on_stop)

    assert stop_called
    assert sm.current is RuntimeState.STOPPED


async def test_shutdown_reaches_stopped_even_if_on_stop_raises() -> None:
    sm = StateMachine(RuntimeState.HEALTHY)
    manager = LifecycleManager(sm, EventBus(), [])

    async def on_stop() -> None:
        raise RuntimeError("teardown failed")

    with pytest.raises(RuntimeError, match="teardown failed"):
        await manager.shutdown(on_stop=on_stop)

    assert sm.current is RuntimeState.STOPPED


async def test_shutdown_from_stopped_is_rejected() -> None:
    sm = StateMachine(RuntimeState.STOPPED)
    manager = LifecycleManager(sm, EventBus(), [])

    async def on_stop() -> None:
        return None

    with pytest.raises(InvalidStateTransitionError):
        await manager.shutdown(on_stop=on_stop)
