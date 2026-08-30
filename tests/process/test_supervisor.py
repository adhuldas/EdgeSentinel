from __future__ import annotations

import asyncio
import itertools
from collections.abc import Callable

import pytest

from edgeguard.core.events import Event, EventBus
from edgeguard.core.exceptions import InvalidStateTransitionError
from edgeguard.core.state import RuntimeState
from edgeguard.process.supervisor import Supervisor


async def _instant_sleep(_: float) -> None:
    await asyncio.sleep(0)


async def _pump(condition: Callable[[], bool] | None = None, iterations: int = 200) -> None:
    for _ in range(iterations):
        await asyncio.sleep(0)
        if condition is not None and condition():
            return


async def test_max_crashes_below_one_is_rejected() -> None:
    async def noop() -> None:
        pass

    with pytest.raises(ValueError):
        Supervisor(noop, name="t", max_crashes=0)


async def test_non_positive_window_is_rejected() -> None:
    async def noop() -> None:
        pass

    with pytest.raises(ValueError):
        Supervisor(noop, name="t", window=0)


async def test_set_state_without_get_state_is_rejected() -> None:
    async def noop() -> None:
        pass

    async def set_state(_: RuntimeState) -> None:
        pass

    with pytest.raises(ValueError):
        Supervisor(noop, name="t", set_state=set_state)


async def test_start_is_idempotent_and_stop_cancels_cleanly() -> None:
    calls = 0

    async def loop_forever() -> None:
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()  # runs "forever" until cancelled

    supervisor = Supervisor(loop_forever, name="t", sleep=_instant_sleep)
    await supervisor.start()
    await supervisor.start()  # no-op, must not start a second task
    await asyncio.sleep(0)
    assert supervisor.is_running is True
    assert calls == 1

    await supervisor.stop()
    await supervisor.stop()  # idempotent
    assert supervisor.is_running is False


async def test_a_clean_return_is_restarted_and_published_as_task_exited() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    calls = 0

    async def returns_cleanly() -> None:
        nonlocal calls
        calls += 1

    supervisor = Supervisor(
        returns_cleanly,
        name="t",
        max_crashes=100,
        events=events,
        component="proc",
        sleep=_instant_sleep,
    )
    await supervisor.start()
    await _pump(lambda: calls >= 2)
    await supervisor.stop()

    assert calls >= 2
    assert any(e.type == "task_exited" for e in seen)
    assert not any(e.type == "task_crashed" for e in seen)
    assert all(e.component == "proc" for e in seen)


async def test_crash_loop_detected_after_max_crashes_within_window() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    async def always_fails() -> None:
        raise RuntimeError("boom")

    clock = iter([0.0, 1.0, 2.0])
    supervisor = Supervisor(
        always_fails,
        name="t",
        max_crashes=3,
        window=60.0,
        events=events,
        sleep=_instant_sleep,
        now=lambda: next(clock),
    )
    await supervisor.start()
    await _pump(lambda: supervisor.is_crashed)

    assert supervisor.is_crashed is True
    assert supervisor.is_running is False
    types = [e.type for e in seen]
    assert types.count("task_crashed") == 3
    assert "task_crash_loop_detected" in types


async def test_crashes_spaced_beyond_the_window_do_not_accumulate() -> None:
    async def always_fails() -> None:
        raise RuntimeError("boom")

    counter = itertools.count(0, 20)
    supervisor = Supervisor(
        always_fails,
        name="t",
        max_crashes=2,
        window=10.0,
        sleep=_instant_sleep,
        now=lambda: next(counter),
    )
    await supervisor.start()
    await _pump(iterations=10)
    assert supervisor.is_crashed is False
    assert supervisor.is_running is True
    await supervisor.stop()


async def test_is_crashed_resets_on_restart() -> None:
    async def always_fails() -> None:
        raise RuntimeError("boom")

    supervisor = Supervisor(always_fails, name="t", max_crashes=1, sleep=_instant_sleep)
    await supervisor.start()
    await _pump(lambda: supervisor.is_crashed)
    assert supervisor.is_crashed is True

    await supervisor.start()
    assert supervisor.is_crashed is False
    assert supervisor.is_running is True
    await supervisor.stop()


async def test_escalate_moves_runtime_to_failed() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    async def always_fails() -> None:
        raise RuntimeError("boom")

    supervisor = Supervisor(
        always_fails,
        name="t",
        max_crashes=1,
        set_state=set_state,
        get_state=lambda: state,
        sleep=_instant_sleep,
    )
    await supervisor.start()
    await _pump(lambda: supervisor.is_crashed)

    assert state is RuntimeState.FAILED


async def test_escalate_never_touches_stopping_or_stopped() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while STOPPING")

    async def always_fails() -> None:
        raise RuntimeError("boom")

    supervisor = Supervisor(
        always_fails,
        name="t",
        max_crashes=1,
        set_state=set_state,
        get_state=lambda: RuntimeState.STOPPING,
        sleep=_instant_sleep,
    )
    await supervisor.start()
    await _pump(lambda: supervisor.is_crashed)

    assert supervisor.is_crashed is True  # still gives up, just doesn't escalate


async def test_escalate_swallows_an_illegal_transition() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise InvalidStateTransitionError(RuntimeState.STOPPING, target)

    async def always_fails() -> None:
        raise RuntimeError("boom")

    supervisor = Supervisor(
        always_fails,
        name="t",
        max_crashes=1,
        set_state=set_state,
        get_state=lambda: RuntimeState.HEALTHY,
        sleep=_instant_sleep,
    )
    await supervisor.start()
    await _pump(lambda: supervisor.is_crashed)

    assert supervisor.is_crashed is True
