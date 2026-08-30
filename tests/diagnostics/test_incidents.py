from __future__ import annotations

import datetime as dt

import pytest

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.core.state import RuntimeState
from edgesentinel.diagnostics.incidents import IncidentTracker, build_incidents


def _state_change(
    previous: RuntimeState, current: RuntimeState, *, timestamp: dt.datetime
) -> Event:
    return Event(
        type="state_change",
        component="runtime",
        severity=Severity.INFO,
        timestamp=timestamp,
        metadata={"previous": previous.value, "current": current.value},
    )


def _other_event(*, timestamp: dt.datetime) -> Event:
    return Event(
        type="network_lost",
        component="network",
        severity=Severity.WARNING,
        timestamp=timestamp,
    )


_T0 = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)


def _at(seconds: int) -> dt.datetime:
    return _T0 + dt.timedelta(seconds=seconds)


def test_max_history_rejects_non_positive() -> None:
    with pytest.raises(ValueError, match="max_history"):
        IncidentTracker(max_history=0)


async def test_normal_startup_and_healthy_produce_no_incident() -> None:
    tracker = IncidentTracker()
    bus = EventBus()
    tracker.attach(bus)

    await bus.publish(
        _state_change(RuntimeState.BOOTING, RuntimeState.INITIALIZING, timestamp=_at(0))
    )
    await bus.publish(
        _state_change(RuntimeState.INITIALIZING, RuntimeState.HEALTHY, timestamp=_at(1))
    )

    assert tracker.open_incident is None
    assert tracker.incidents == ()


async def test_degraded_opens_an_incident() -> None:
    tracker = IncidentTracker()
    bus = EventBus()
    tracker.attach(bus)

    await bus.publish(_state_change(RuntimeState.HEALTHY, RuntimeState.DEGRADED, timestamp=_at(0)))

    incident = tracker.open_incident
    assert incident is not None
    assert incident.is_open
    assert incident.started_at == _at(0)
    assert incident.states == (RuntimeState.DEGRADED,)
    assert tracker.incidents == ()


async def test_recovery_closes_the_incident_and_captures_events_in_between() -> None:
    tracker = IncidentTracker()
    bus = EventBus()
    tracker.attach(bus)

    await bus.publish(_state_change(RuntimeState.HEALTHY, RuntimeState.DEGRADED, timestamp=_at(0)))
    await bus.publish(_other_event(timestamp=_at(1)))
    await bus.publish(_state_change(RuntimeState.DEGRADED, RuntimeState.HEALTHY, timestamp=_at(2)))

    assert tracker.open_incident is None
    incidents = tracker.incidents
    assert len(incidents) == 1
    incident = incidents[0]
    assert not incident.is_open
    assert incident.started_at == _at(0)
    assert incident.ended_at == _at(2)
    assert incident.duration == dt.timedelta(seconds=2)
    assert incident.states == (RuntimeState.DEGRADED,)
    assert [e.type for e in incident.events] == ["state_change", "network_lost", "state_change"]


async def test_multiple_state_visits_within_one_incident_are_all_recorded() -> None:
    tracker = IncidentTracker()
    bus = EventBus()
    tracker.attach(bus)

    await bus.publish(_state_change(RuntimeState.HEALTHY, RuntimeState.OFFLINE, timestamp=_at(0)))
    await bus.publish(
        _state_change(RuntimeState.OFFLINE, RuntimeState.RECOVERING, timestamp=_at(1))
    )
    await bus.publish(
        _state_change(RuntimeState.RECOVERING, RuntimeState.HEALTHY, timestamp=_at(2))
    )

    incidents = tracker.incidents
    assert len(incidents) == 1
    assert incidents[0].states == (RuntimeState.OFFLINE, RuntimeState.RECOVERING)


async def test_detach_stops_tracking() -> None:
    tracker = IncidentTracker()
    bus = EventBus()
    tracker.attach(bus)
    tracker.detach(bus)

    await bus.publish(_state_change(RuntimeState.HEALTHY, RuntimeState.DEGRADED, timestamp=_at(0)))

    assert tracker.open_incident is None


async def test_attach_and_detach_are_idempotent() -> None:
    tracker = IncidentTracker()
    bus = EventBus()
    tracker.detach(bus)  # never attached
    tracker.attach(bus)
    tracker.attach(bus)  # already attached

    await bus.publish(_state_change(RuntimeState.HEALTHY, RuntimeState.DEGRADED, timestamp=_at(0)))
    await bus.publish(_state_change(RuntimeState.DEGRADED, RuntimeState.HEALTHY, timestamp=_at(1)))

    assert len(tracker.incidents) == 1
    tracker.detach(bus)
    tracker.detach(bus)  # already detached


async def test_max_history_bounds_closed_incidents_in_memory() -> None:
    tracker = IncidentTracker(max_history=2)
    bus = EventBus()
    tracker.attach(bus)

    for i in range(3):
        base = i * 10
        await bus.publish(
            _state_change(RuntimeState.HEALTHY, RuntimeState.DEGRADED, timestamp=_at(base))
        )
        await bus.publish(
            _state_change(RuntimeState.DEGRADED, RuntimeState.HEALTHY, timestamp=_at(base + 1))
        )

    incidents = tracker.incidents
    assert len(incidents) == 2
    # Oldest incident (started at _at(0)) must have been dropped, not the newest.
    assert [incident.started_at for incident in incidents] == [_at(10), _at(20)]
    # The underlying list itself, not just what `incidents` returns, must be
    # bounded -- this is what makes it safe for a long-running process.
    assert len(tracker._builder.closed) == 2


def test_build_incidents_reconstructs_closed_incidents_from_a_sequence() -> None:
    events = [
        _state_change(RuntimeState.BOOTING, RuntimeState.INITIALIZING, timestamp=_at(0)),
        _state_change(RuntimeState.INITIALIZING, RuntimeState.HEALTHY, timestamp=_at(1)),
        _state_change(RuntimeState.HEALTHY, RuntimeState.DEGRADED, timestamp=_at(2)),
        _other_event(timestamp=_at(3)),
        _state_change(RuntimeState.DEGRADED, RuntimeState.HEALTHY, timestamp=_at(4)),
    ]

    incidents = build_incidents(events)

    assert len(incidents) == 1
    assert not incidents[0].is_open
    assert incidents[0].started_at == _at(2)
    assert incidents[0].ended_at == _at(4)


def test_build_incidents_leaves_the_last_incident_open_if_unresolved() -> None:
    events = [
        _state_change(RuntimeState.HEALTHY, RuntimeState.OFFLINE, timestamp=_at(0)),
    ]

    incidents = build_incidents(events)

    assert len(incidents) == 1
    assert incidents[0].is_open
    assert incidents[0].ended_at is None


def test_build_incidents_on_empty_sequence_returns_no_incidents() -> None:
    assert build_incidents([]) == []
