from __future__ import annotations

from collections.abc import Mapping

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.integrations.http import HttpEventPublisher


class _FakeTransport:
    """Records every call instead of making a real HTTP request."""

    def __init__(self, *, status: int = 200, raises: Exception | None = None) -> None:
        self.status = status
        self.raises = raises
        self.calls: list[tuple[str, bytes, Mapping[str, str], float]] = []

    async def __call__(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout_seconds: float
    ) -> int:
        self.calls.append((url, body, headers, timeout_seconds))
        if self.raises is not None:
            raise self.raises
        return self.status


def _event(
    *,
    type: str = "tick",
    component: str = "test",
    severity: Severity = Severity.INFO,
) -> Event:
    return Event(type=type, component=component, severity=severity)


async def test_attach_forwards_published_events() -> None:
    transport = _FakeTransport()
    publisher = HttpEventPublisher(url="https://example.com/hook", transport=transport)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event(type="state_change", component="runtime"))

    assert len(transport.calls) == 1
    url, body, headers, _timeout = transport.calls[0]
    assert url == "https://example.com/hook"
    assert b'"type": "state_change"' in body
    assert b'"component": "runtime"' in body
    assert headers["Content-Type"] == "application/json"


async def test_detach_stops_forwarding() -> None:
    transport = _FakeTransport()
    publisher = HttpEventPublisher(url="https://example.com/hook", transport=transport)
    bus = EventBus()
    publisher.attach(bus)
    publisher.detach(bus)

    await bus.publish(_event())

    assert transport.calls == []


async def test_attach_and_detach_are_idempotent() -> None:
    transport = _FakeTransport()
    publisher = HttpEventPublisher(url="https://example.com/hook", transport=transport)
    bus = EventBus()
    publisher.detach(bus)  # never attached
    publisher.attach(bus)
    publisher.attach(bus)  # already attached

    await bus.publish(_event())

    assert len(transport.calls) == 1
    publisher.detach(bus)
    publisher.detach(bus)  # already detached


async def test_extra_headers_are_merged_with_content_type() -> None:
    transport = _FakeTransport()
    publisher = HttpEventPublisher(
        url="https://example.com/hook",
        headers={"Authorization": "Bearer secret"},
        transport=transport,
    )
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event())

    _url, _body, headers, _timeout = transport.calls[0]
    assert headers["Authorization"] == "Bearer secret"
    assert headers["Content-Type"] == "application/json"


async def test_min_severity_filters_out_lower_severity_events() -> None:
    transport = _FakeTransport()
    publisher = HttpEventPublisher(
        url="https://example.com/hook", min_severity=Severity.WARNING, transport=transport
    )
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event(severity=Severity.INFO))
    await bus.publish(_event(severity=Severity.WARNING))
    await bus.publish(_event(severity=Severity.CRITICAL))

    assert len(transport.calls) == 2


async def test_transport_raising_is_logged_not_propagated() -> None:
    transport = _FakeTransport(raises=OSError("connection refused"))
    publisher = HttpEventPublisher(url="https://example.com/hook", transport=transport)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event())  # must not raise

    assert len(transport.calls) == 1


async def test_non_2xx_status_is_logged_not_raised() -> None:
    transport = _FakeTransport(status=500)
    publisher = HttpEventPublisher(url="https://example.com/hook", transport=transport)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event())  # must not raise

    assert len(transport.calls) == 1
