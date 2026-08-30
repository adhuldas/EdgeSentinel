from __future__ import annotations

import datetime as dt

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.diagnostics.timeline import EventLog
from edgesentinel.persistence.database import Database


def _event(
    *,
    type: str = "tick",
    component: str = "test",
    severity: Severity = Severity.INFO,
    timestamp: dt.datetime | None = None,
    metadata: dict[str, object] | None = None,
) -> Event:
    return Event(
        type=type,
        component=component,
        severity=severity,
        timestamp=timestamp or dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        metadata=metadata or {},
    )


async def test_attach_persists_published_events(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)

    await bus.publish(_event(type="state_change", component="runtime"))

    events = await log.query()
    assert len(events) == 1
    assert events[0].type == "state_change"
    assert events[0].component == "runtime"


async def test_detach_stops_recording(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    log.detach(bus)

    await bus.publish(_event())

    assert await log.query() == []


async def test_attach_is_idempotent(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    log.attach(bus)  # must not double-subscribe

    await bus.publish(_event())

    assert len(await log.query()) == 1


async def test_detach_is_idempotent_and_safe_without_attach(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.detach(bus)  # never attached; must not raise
    log.attach(bus)
    log.detach(bus)
    log.detach(bus)  # already detached; must not raise


async def test_query_filters_by_component(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(component="network"))
    await bus.publish(_event(component="storage"))

    events = await log.query(component="network")
    assert [e.component for e in events] == ["network"]


async def test_query_filters_by_type(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(type="network_lost"))
    await bus.publish(_event(type="network_restored"))

    events = await log.query(type="network_lost")
    assert [e.type for e in events] == ["network_lost"]


async def test_query_filters_by_min_severity(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(severity=Severity.INFO))
    await bus.publish(_event(severity=Severity.WARNING))
    await bus.publish(_event(severity=Severity.CRITICAL))

    events = await log.query(min_severity=Severity.WARNING)
    severities = {e.severity for e in events}
    assert severities == {Severity.WARNING, Severity.CRITICAL}


async def test_query_filters_by_since_and_until(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(timestamp=dt.datetime(2024, 1, 1, tzinfo=dt.UTC)))
    await bus.publish(_event(timestamp=dt.datetime(2024, 1, 2, tzinfo=dt.UTC)))
    await bus.publish(_event(timestamp=dt.datetime(2024, 1, 3, tzinfo=dt.UTC)))

    events = await log.query(
        since=dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
        until=dt.datetime(2024, 1, 2, 12, tzinfo=dt.UTC),
    )
    assert len(events) == 1
    assert events[0].timestamp == dt.datetime(2024, 1, 2, tzinfo=dt.UTC)


async def test_query_respects_limit(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    for _ in range(5):
        await bus.publish(_event())

    assert len(await log.query(limit=2)) == 2


async def test_query_newest_first_toggle(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(component="first"))
    await bus.publish(_event(component="second"))

    newest_first = await log.query()
    assert [e.component for e in newest_first] == ["second", "first"]

    oldest_first = await log.query(newest_first=False)
    assert [e.component for e in oldest_first] == ["first", "second"]


async def test_prune_older_than_deletes_old_events(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(component="old", timestamp=dt.datetime(2000, 1, 1, tzinfo=dt.UTC)))
    await bus.publish(_event(component="new", timestamp=dt.datetime(2099, 1, 1, tzinfo=dt.UTC)))

    deleted = await log.prune_older_than(dt.datetime(2050, 1, 1, tzinfo=dt.UTC))

    assert deleted == 1
    remaining = await log.query()
    assert [e.component for e in remaining] == ["new"]


async def test_event_metadata_round_trips_through_json(database: Database) -> None:
    log = EventLog(database)
    bus = EventBus()
    log.attach(bus)
    await bus.publish(_event(metadata={"previous": "booting", "current": "healthy"}))

    events = await log.query()
    assert events[0].metadata == {"previous": "booting", "current": "healthy"}
