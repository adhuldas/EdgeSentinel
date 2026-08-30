from __future__ import annotations

import logging

import pytest

from edgeguard.core.events import Event, EventBus, Severity


async def test_publish_with_no_subscribers_does_nothing() -> None:
    bus = EventBus()
    await bus.publish(Event(type="noop", component="test"))


async def test_subscribed_handler_receives_published_event() -> None:
    bus = EventBus()
    received: list[Event] = []

    async def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe(handler)
    event = Event(type="network_lost", component="network", severity=Severity.WARNING)
    await bus.publish(event)

    assert received == [event]


async def test_multiple_handlers_all_receive_the_event() -> None:
    bus = EventBus()
    calls: list[str] = []

    async def a(event: Event) -> None:
        calls.append("a")

    async def b(event: Event) -> None:
        calls.append("b")

    bus.subscribe(a)
    bus.subscribe(b)
    await bus.publish(Event(type="x", component="test"))

    assert sorted(calls) == ["a", "b"]


async def test_subscribe_returns_handler_for_decorator_use() -> None:
    bus = EventBus()

    @bus.subscribe
    async def handler(event: Event) -> None:
        pass

    assert handler in bus._handlers


async def test_unsubscribe_stops_delivery() -> None:
    bus = EventBus()
    received: list[Event] = []

    async def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe(handler)
    bus.unsubscribe(handler)
    await bus.publish(Event(type="x", component="test"))

    assert received == []


def test_unsubscribe_unknown_handler_is_a_no_op() -> None:
    bus = EventBus()

    async def handler(event: Event) -> None:
        pass

    bus.unsubscribe(handler)  # must not raise


async def test_failing_handler_does_not_prevent_other_handlers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    bus = EventBus()
    received: list[Event] = []

    async def bad_handler(event: Event) -> None:
        raise RuntimeError("boom")

    async def good_handler(event: Event) -> None:
        received.append(event)

    bus.subscribe(bad_handler)
    bus.subscribe(good_handler)

    with caplog.at_level(logging.ERROR, logger="edgeguard.events"):
        await bus.publish(Event(type="x", component="test"))

    assert len(received) == 1
    assert any("raised while handling" in record.getMessage() for record in caplog.records)
