from __future__ import annotations

import asyncio

import pytest

from edgeguard.core.events import Event, EventBus
from edgeguard.core.exceptions import InvalidStateTransitionError
from edgeguard.core.state import RuntimeState
from edgeguard.metrics.checks import HardwareStatus
from edgeguard.metrics.monitor import MetricsMonitor


class _FakeMetrics:
    """A settable metrics source, so tests can drive readings explicitly."""

    def __init__(
        self,
        *,
        cpu_load_ratio: float = 0.1,
        memory_used_ratio: float = 0.1,
        temperature_celsius: float | None = None,
    ) -> None:
        self.cpu_load_ratio = cpu_load_ratio
        self.memory_used_ratio = memory_used_ratio
        self.temperature_celsius = temperature_celsius

    def __call__(self) -> HardwareStatus:
        return HardwareStatus(
            cpu_load_ratio=self.cpu_load_ratio,
            memory_used_ratio=self.memory_used_ratio,
            temperature_celsius=self.temperature_celsius,
        )


async def test_no_thresholds_configured_is_rejected() -> None:
    with pytest.raises(ValueError):
        MetricsMonitor()


async def test_non_positive_cpu_high_is_rejected() -> None:
    with pytest.raises(ValueError):
        MetricsMonitor(cpu_high=0)


async def test_non_positive_memory_high_is_rejected() -> None:
    with pytest.raises(ValueError):
        MetricsMonitor(memory_high=0)


async def test_non_positive_temperature_high_is_rejected() -> None:
    with pytest.raises(ValueError):
        MetricsMonitor(temperature_high_celsius=0)


async def test_non_positive_interval_is_rejected() -> None:
    with pytest.raises(ValueError):
        MetricsMonitor(cpu_high=0.9, interval=0)


async def test_set_state_without_get_state_is_rejected() -> None:
    async def set_state(_: RuntimeState) -> None:
        pass

    with pytest.raises(ValueError):
        MetricsMonitor(cpu_high=0.9, set_state=set_state)


async def test_usage_below_every_threshold_is_not_high() -> None:
    metrics = _FakeMetrics(cpu_load_ratio=0.2, memory_used_ratio=0.2)
    monitor = MetricsMonitor(cpu_high=0.9, memory_high=0.9, metrics_check=metrics)

    status = await monitor.check_once()

    assert status.cpu_load_ratio == 0.2
    assert monitor.is_high is False


async def test_cpu_above_threshold_is_high() -> None:
    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    monitor = MetricsMonitor(cpu_high=0.9, metrics_check=metrics)

    await monitor.check_once()

    assert monitor.is_high is True


async def test_memory_above_threshold_is_high() -> None:
    metrics = _FakeMetrics(memory_used_ratio=0.95)
    monitor = MetricsMonitor(memory_high=0.9, metrics_check=metrics)

    await monitor.check_once()

    assert monitor.is_high is True


async def test_temperature_above_threshold_is_high() -> None:
    metrics = _FakeMetrics(temperature_celsius=85.0)
    monitor = MetricsMonitor(temperature_high_celsius=80.0, metrics_check=metrics)

    await monitor.check_once()

    assert monitor.is_high is True


async def test_temperature_threshold_is_ignored_when_reading_is_none() -> None:
    metrics = _FakeMetrics(temperature_celsius=None)
    monitor = MetricsMonitor(temperature_high_celsius=80.0, metrics_check=metrics)

    await monitor.check_once()

    assert monitor.is_high is False


async def test_mitigations_run_in_order_and_stop_once_no_longer_high() -> None:
    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    calls: list[str] = []

    async def mitigate_a_little() -> None:
        calls.append("a")
        metrics.cpu_load_ratio = 0.85  # still above the 0.5 threshold

    async def mitigate_a_lot() -> None:
        calls.append("b")
        metrics.cpu_load_ratio = 0.1  # now below the threshold

    async def never_called() -> None:
        calls.append("c")

    monitor = MetricsMonitor(
        cpu_high=0.5,
        mitigations=[mitigate_a_little, mitigate_a_lot, never_called],
        metrics_check=metrics,
    )

    status = await monitor.check_once()

    assert calls == ["a", "b"]
    assert status.cpu_load_ratio == 0.1
    assert monitor.is_high is False


async def test_a_raising_mitigation_action_is_logged_not_propagated() -> None:
    metrics = _FakeMetrics(cpu_load_ratio=0.95)

    async def broken() -> None:
        raise RuntimeError("boom")

    async def fixes_it() -> None:
        metrics.cpu_load_ratio = 0.1

    monitor = MetricsMonitor(cpu_high=0.5, mitigations=[broken, fixes_it], metrics_check=metrics)

    await monitor.check_once()  # must not raise

    assert monitor.is_high is False


async def test_events_published_only_on_transitions() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    metrics = _FakeMetrics(cpu_load_ratio=0.1)
    monitor = MetricsMonitor(cpu_high=0.5, events=events, component="device", metrics_check=metrics)

    await monitor.check_once()  # healthy, no event
    metrics.cpu_load_ratio = 0.9
    await monitor.check_once()  # newly high
    await monitor.check_once()  # still high, no new event
    metrics.cpu_load_ratio = 0.1
    await monitor.check_once()  # recovered

    types = [e.type for e in seen]
    assert types == ["hardware_metrics_high", "hardware_metrics_recovered"]
    assert all(e.component == "device" for e in seen)


async def test_mitigations_exhausted_publishes_a_critical_event() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    metrics = _FakeMetrics(cpu_load_ratio=0.95)

    async def useless() -> None:
        pass

    monitor = MetricsMonitor(
        cpu_high=0.5, mitigations=[useless], events=events, metrics_check=metrics
    )

    await monitor.check_once()

    types = [e.type for e in seen]
    assert types == ["hardware_metrics_high", "hardware_metrics_mitigation_exhausted"]
    assert monitor.is_high is True


async def test_apply_to_runtime_moves_healthy_to_degraded_and_back() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    metrics = _FakeMetrics(cpu_load_ratio=0.1)
    monitor = MetricsMonitor(
        cpu_high=0.5, set_state=set_state, get_state=lambda: state, metrics_check=metrics
    )

    await monitor.check_once()
    assert state is RuntimeState.HEALTHY

    metrics.cpu_load_ratio = 0.9
    await monitor.check_once()
    # mypy narrows `state` from its initial value and can't see across the
    # `nonlocal` reassignment inside `set_state`, called indirectly via the
    # awaited check_once() above.
    assert state is RuntimeState.DEGRADED  # type: ignore[comparison-overlap]

    metrics.cpu_load_ratio = 0.1
    await monitor.check_once()
    assert state is RuntimeState.HEALTHY


async def test_mitigations_exhausted_escalates_to_failed() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    async def useless() -> None:
        pass

    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    monitor = MetricsMonitor(
        cpu_high=0.5,
        mitigations=[useless],
        set_state=set_state,
        get_state=lambda: state,
        metrics_check=metrics,
    )

    await monitor.check_once()

    assert state is RuntimeState.FAILED


async def test_no_mitigations_configured_stays_degraded_without_escalating() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    monitor = MetricsMonitor(
        cpu_high=0.5, set_state=set_state, get_state=lambda: state, metrics_check=metrics
    )

    await monitor.check_once()

    assert state is RuntimeState.DEGRADED


async def test_escalate_never_touches_stopping_or_stopped() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while STOPPING")

    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    monitor = MetricsMonitor(
        cpu_high=0.5,
        set_state=set_state,
        get_state=lambda: RuntimeState.STOPPING,
        metrics_check=metrics,
    )

    await monitor.check_once()  # must not raise

    assert monitor.is_high is True


async def test_apply_to_runtime_swallows_an_illegal_transition() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise InvalidStateTransitionError(RuntimeState.HEALTHY, target)

    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    monitor = MetricsMonitor(
        cpu_high=0.5,
        set_state=set_state,
        get_state=lambda: RuntimeState.HEALTHY,
        metrics_check=metrics,
    )

    await monitor.check_once()  # must not raise


async def test_apply_to_runtime_never_touches_unmanaged_states() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while BOOTING")

    metrics = _FakeMetrics(cpu_load_ratio=0.95)
    monitor = MetricsMonitor(
        cpu_high=0.5,
        set_state=set_state,
        get_state=lambda: RuntimeState.BOOTING,
        metrics_check=metrics,
    )

    await monitor.check_once()  # must not raise


async def test_start_runs_an_initial_check_immediately() -> None:
    metrics = _FakeMetrics(cpu_load_ratio=0.1)
    monitor = MetricsMonitor(cpu_high=0.9, interval=999, metrics_check=metrics)

    await monitor.start()
    try:
        assert monitor.status is not None
    finally:
        await monitor.stop()


async def test_start_is_idempotent_and_stop_cancels_cleanly() -> None:
    metrics = _FakeMetrics(cpu_load_ratio=0.1)
    calls = 0

    def check() -> HardwareStatus:
        nonlocal calls
        calls += 1
        return metrics()

    async def instant_yield(_: float) -> None:
        await asyncio.sleep(0)

    monitor = MetricsMonitor(cpu_high=0.9, interval=999, metrics_check=check, sleep=instant_yield)
    await monitor.start()
    await monitor.start()  # no-op, must not start a second poll loop
    for _ in range(5):
        await asyncio.sleep(0)
    await monitor.stop()
    await monitor.stop()  # idempotent

    assert calls >= 2  # the initial check plus at least one poll iteration
