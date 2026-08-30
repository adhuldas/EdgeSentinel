from __future__ import annotations

import asyncio

import pytest

from edgeguard.core.events import Event, EventBus
from edgeguard.core.exceptions import InvalidStateTransitionError
from edgeguard.core.state import RuntimeState
from edgeguard.storage.checks import StorageStatus
from edgeguard.storage.monitor import StorageMonitor


class _FakeDisk:
    """A settable usage source, so tests can drive free space explicitly."""

    def __init__(self, free_bytes: int, total_bytes: int = 1000) -> None:
        self.free_bytes = free_bytes
        self.total_bytes = total_bytes

    def __call__(self, path: str) -> StorageStatus:
        return StorageStatus(free_bytes=self.free_bytes, total_bytes=self.total_bytes)


async def test_non_positive_low_water_bytes_is_rejected() -> None:
    with pytest.raises(ValueError):
        StorageMonitor("/tmp", low_water_bytes=0)


async def test_non_positive_low_water_inodes_is_rejected() -> None:
    with pytest.raises(ValueError):
        StorageMonitor("/tmp", low_water_bytes=100, low_water_inodes=0)


async def test_non_positive_interval_is_rejected() -> None:
    with pytest.raises(ValueError):
        StorageMonitor("/tmp", low_water_bytes=100, interval=0)


async def test_set_state_without_get_state_is_rejected() -> None:
    async def set_state(_: RuntimeState) -> None:
        pass

    with pytest.raises(ValueError):
        StorageMonitor("/tmp", low_water_bytes=100, set_state=set_state)


async def test_usage_above_the_low_water_mark_is_not_low() -> None:
    disk = _FakeDisk(free_bytes=500)
    monitor = StorageMonitor("/tmp", low_water_bytes=100, usage_check=disk)

    status = await monitor.check_once()

    assert status.free_bytes == 500
    assert monitor.is_low is False


async def test_usage_below_the_low_water_mark_is_low() -> None:
    disk = _FakeDisk(free_bytes=50)
    monitor = StorageMonitor("/tmp", low_water_bytes=100, usage_check=disk)

    await monitor.check_once()

    assert monitor.is_low is True


async def test_low_water_inodes_also_triggers_low() -> None:
    def usage(path: str) -> StorageStatus:
        return StorageStatus(free_bytes=1000, total_bytes=1000, free_inodes=5, total_inodes=1000)

    monitor = StorageMonitor("/tmp", low_water_bytes=100, low_water_inodes=10, usage_check=usage)

    await monitor.check_once()

    assert monitor.is_low is True


async def test_cleanup_runs_in_order_and_stops_once_no_longer_low() -> None:
    disk = _FakeDisk(free_bytes=50)
    calls: list[str] = []

    async def free_a_little() -> None:
        calls.append("a")
        disk.free_bytes = 80  # still below the 100 threshold

    async def free_a_lot() -> None:
        calls.append("b")
        disk.free_bytes = 500  # now above the threshold

    async def never_called() -> None:
        calls.append("c")

    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        cleanup=[free_a_little, free_a_lot, never_called],
        usage_check=disk,
    )

    status = await monitor.check_once()

    assert calls == ["a", "b"]
    assert status.free_bytes == 500
    assert monitor.is_low is False


async def test_a_raising_cleanup_action_is_logged_not_propagated() -> None:
    disk = _FakeDisk(free_bytes=50)

    async def broken() -> None:
        raise RuntimeError("boom")

    async def fixes_it() -> None:
        disk.free_bytes = 500

    monitor = StorageMonitor(
        "/tmp", low_water_bytes=100, cleanup=[broken, fixes_it], usage_check=disk
    )

    await monitor.check_once()  # must not raise

    assert monitor.is_low is False


async def test_events_published_only_on_transitions() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    disk = _FakeDisk(free_bytes=500)
    monitor = StorageMonitor(
        "/tmp", low_water_bytes=100, events=events, component="disk", usage_check=disk
    )

    await monitor.check_once()  # healthy, no event
    disk.free_bytes = 50
    await monitor.check_once()  # newly low
    await monitor.check_once()  # still low, no new event
    disk.free_bytes = 500
    await monitor.check_once()  # recovered

    types = [e.type for e in seen]
    assert types == ["storage_low", "storage_recovered"]
    assert all(e.component == "disk" for e in seen)


async def test_cleanup_exhausted_publishes_a_critical_event() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    disk = _FakeDisk(free_bytes=50)

    async def useless() -> None:
        pass

    monitor = StorageMonitor(
        "/tmp", low_water_bytes=100, cleanup=[useless], events=events, usage_check=disk
    )

    await monitor.check_once()

    types = [e.type for e in seen]
    assert types == ["storage_low", "storage_cleanup_exhausted"]
    assert monitor.is_low is True


async def test_apply_to_runtime_moves_healthy_to_degraded_and_back() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    disk = _FakeDisk(free_bytes=500)
    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        set_state=set_state,
        get_state=lambda: state,
        usage_check=disk,
    )

    await monitor.check_once()
    assert state is RuntimeState.HEALTHY

    disk.free_bytes = 50
    await monitor.check_once()
    # mypy narrows `state` from its initial value and can't see across the
    # `nonlocal` reassignment inside `set_state`, called indirectly via the
    # awaited check_once() above.
    assert state is RuntimeState.DEGRADED  # type: ignore[comparison-overlap]

    disk.free_bytes = 500
    await monitor.check_once()
    assert state is RuntimeState.HEALTHY


async def test_cleanup_exhausted_escalates_to_failed() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    async def useless() -> None:
        pass

    disk = _FakeDisk(free_bytes=50)
    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        cleanup=[useless],
        set_state=set_state,
        get_state=lambda: state,
        usage_check=disk,
    )

    await monitor.check_once()

    assert state is RuntimeState.FAILED


async def test_no_cleanup_configured_stays_degraded_without_escalating() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    disk = _FakeDisk(free_bytes=50)
    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        set_state=set_state,
        get_state=lambda: state,
        usage_check=disk,
    )

    await monitor.check_once()

    assert state is RuntimeState.DEGRADED


async def test_escalate_never_touches_stopping_or_stopped() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while STOPPING")

    disk = _FakeDisk(free_bytes=50)
    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        set_state=set_state,
        get_state=lambda: RuntimeState.STOPPING,
        usage_check=disk,
    )

    await monitor.check_once()  # must not raise

    assert monitor.is_low is True


async def test_apply_to_runtime_swallows_an_illegal_transition() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise InvalidStateTransitionError(RuntimeState.HEALTHY, target)

    disk = _FakeDisk(free_bytes=50)
    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        set_state=set_state,
        get_state=lambda: RuntimeState.HEALTHY,
        usage_check=disk,
    )

    await monitor.check_once()  # must not raise


async def test_apply_to_runtime_never_touches_unmanaged_states() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while BOOTING")

    disk = _FakeDisk(free_bytes=50)
    monitor = StorageMonitor(
        "/tmp",
        low_water_bytes=100,
        set_state=set_state,
        get_state=lambda: RuntimeState.BOOTING,
        usage_check=disk,
    )

    await monitor.check_once()  # must not raise


async def test_start_runs_an_initial_check_immediately() -> None:
    disk = _FakeDisk(free_bytes=500)
    monitor = StorageMonitor("/tmp", low_water_bytes=100, interval=999, usage_check=disk)

    await monitor.start()
    try:
        assert monitor.status is not None
    finally:
        await monitor.stop()


async def test_start_is_idempotent_and_stop_cancels_cleanly() -> None:
    disk = _FakeDisk(free_bytes=500)
    calls = 0

    def usage(path: str) -> StorageStatus:
        nonlocal calls
        calls += 1
        return disk(path)

    async def instant_yield(_: float) -> None:
        await asyncio.sleep(0)

    monitor = StorageMonitor(
        "/tmp", low_water_bytes=100, interval=999, usage_check=usage, sleep=instant_yield
    )
    await monitor.start()
    await monitor.start()  # no-op, must not start a second poll loop
    for _ in range(5):
        await asyncio.sleep(0)
    await monitor.stop()
    await monitor.stop()  # idempotent

    assert calls >= 2  # the initial check plus at least one poll iteration
