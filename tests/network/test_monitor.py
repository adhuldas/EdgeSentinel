from __future__ import annotations

import asyncio

import pytest

from edgeguard.core.events import Event, EventBus
from edgeguard.core.exceptions import InvalidStateTransitionError
from edgeguard.core.state import RuntimeState
from edgeguard.network.monitor import LayerCheck, NetworkLayer, NetworkMonitor


def _check(result: bool) -> LayerCheck:
    async def check() -> bool:
        return result

    return check


async def test_check_once_stops_at_the_first_failing_layer() -> None:
    monitor = NetworkMonitor(
        {
            NetworkLayer.LINK: _check(True),
            NetworkLayer.GATEWAY: _check(True),
            NetworkLayer.DNS: _check(False),
            NetworkLayer.INTERNET: _check(True),
        }
    )

    status = await monitor.check_once()

    assert status.reachable == (NetworkLayer.LINK, NetworkLayer.GATEWAY)
    assert status.highest_layer is NetworkLayer.GATEWAY
    assert status.is_reachable(NetworkLayer.DNS) is False
    assert monitor.is_fully_connected is False


async def test_check_once_all_layers_reachable() -> None:
    monitor = NetworkMonitor(
        {layer: _check(True) for layer in NetworkLayer},
    )

    status = await monitor.check_once()

    assert status.reachable == tuple(NetworkLayer)
    assert monitor.is_fully_connected is True


async def test_unconfigured_layers_are_skipped_not_treated_as_failures() -> None:
    monitor = NetworkMonitor(
        {NetworkLayer.DNS: _check(True), NetworkLayer.INTERNET: _check(True)},
    )

    status = await monitor.check_once()

    assert status.reachable == (NetworkLayer.DNS, NetworkLayer.INTERNET)
    assert monitor.is_fully_connected is True


async def test_a_raising_check_is_treated_as_a_failure_not_propagated() -> None:
    async def broken() -> bool:
        raise RuntimeError("boom")

    monitor = NetworkMonitor({NetworkLayer.LINK: broken, NetworkLayer.GATEWAY: _check(True)})

    status = await monitor.check_once()

    assert status.reachable == ()


async def test_empty_checks_is_rejected() -> None:
    with pytest.raises(ValueError):
        NetworkMonitor({})


async def test_non_positive_interval_is_rejected() -> None:
    with pytest.raises(ValueError):
        NetworkMonitor({NetworkLayer.LINK: _check(True)}, interval=0)


async def test_set_state_without_get_state_is_rejected() -> None:
    with pytest.raises(ValueError):
        NetworkMonitor(
            {NetworkLayer.LINK: _check(True)},
            set_state=lambda _: _unreachable(),
        )


async def _unreachable() -> None:
    raise AssertionError("should never be called")


async def test_event_published_only_when_the_highest_layer_changes() -> None:
    events = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    events.subscribe(collect)

    calls = 0

    async def flaky_dns() -> bool:
        nonlocal calls
        calls += 1
        return calls > 1  # fails once, then succeeds forever after

    monitor = NetworkMonitor({NetworkLayer.DNS: flaky_dns}, events=events, component="net")

    await monitor.check_once()  # DNS fails -> highest_layer None (first ever status)
    await monitor.check_once()  # DNS succeeds -> highest_layer changes to DNS
    await monitor.check_once()  # DNS still succeeds -> no change, no new event

    assert [e.type for e in seen] == ["network_status_changed", "network_status_changed"]
    assert all(e.component == "net" for e in seen)


async def test_start_runs_an_initial_check_immediately() -> None:
    calls = 0

    async def check() -> bool:
        nonlocal calls
        calls += 1
        return True

    monitor = NetworkMonitor({NetworkLayer.LINK: check}, interval=999)
    await monitor.start()
    try:
        assert calls == 1
        assert monitor.status is not None
    finally:
        await monitor.stop()


async def test_start_is_idempotent_and_stop_cancels_cleanly() -> None:
    calls = 0

    async def check() -> bool:
        nonlocal calls
        calls += 1
        return True

    async def instant_yield(_: float) -> None:
        await asyncio.sleep(0)

    monitor = NetworkMonitor({NetworkLayer.LINK: check}, interval=999, sleep=instant_yield)
    await monitor.start()
    await monitor.start()  # no-op, must not start a second poll loop
    for _ in range(5):
        await asyncio.sleep(0)
    await monitor.stop()
    await monitor.stop()  # idempotent

    assert calls >= 2  # the initial check plus at least one poll iteration


async def test_apply_to_runtime_moves_healthy_to_offline_and_back() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    calls = 0

    async def flaky() -> bool:
        nonlocal calls
        calls += 1
        return calls != 2  # up, then down once, then up again

    monitor = NetworkMonitor(
        {NetworkLayer.INTERNET: flaky},
        set_state=set_state,
        get_state=lambda: state,
    )

    await monitor.check_once()  # up
    assert state is RuntimeState.HEALTHY

    await monitor.check_once()  # down -> OFFLINE (no lower layer configured)
    # mypy narrows `state` from its initial value and can't see across the
    # `nonlocal` reassignment inside `set_state`, called indirectly via the
    # awaited check_once() above.
    assert state is RuntimeState.OFFLINE  # type: ignore[comparison-overlap]

    await monitor.check_once()  # up again -> OFFLINE cannot jump straight to
    # HEALTHY, so the monitor must route through DEGRADED first
    assert state is RuntimeState.HEALTHY


async def test_apply_to_runtime_uses_degraded_for_a_partial_outage() -> None:
    state: RuntimeState = RuntimeState.HEALTHY

    async def set_state(target: RuntimeState) -> None:
        nonlocal state
        state = target

    monitor = NetworkMonitor(
        {NetworkLayer.LINK: _check(True), NetworkLayer.INTERNET: _check(False)},
        set_state=set_state,
        get_state=lambda: state,
    )

    await monitor.check_once()

    assert state is RuntimeState.DEGRADED


async def test_apply_to_runtime_never_touches_unmanaged_states() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise AssertionError("must not be called while BOOTING")

    monitor = NetworkMonitor(
        {NetworkLayer.LINK: _check(False)},
        set_state=set_state,
        get_state=lambda: RuntimeState.BOOTING,
    )

    await monitor.check_once()  # must not raise


async def test_apply_to_runtime_swallows_an_illegal_transition() -> None:
    async def set_state(target: RuntimeState) -> None:
        raise InvalidStateTransitionError(RuntimeState.DEGRADED, target)

    monitor = NetworkMonitor(
        {NetworkLayer.LINK: _check(True)},
        set_state=set_state,
        get_state=lambda: RuntimeState.DEGRADED,
    )

    await monitor.check_once()  # target is HEALTHY, set_state raises -- must not raise here
