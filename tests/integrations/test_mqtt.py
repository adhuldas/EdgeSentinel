from __future__ import annotations

import pytest

from edgeguard.core.events import Event, EventBus, Severity
from edgeguard.integrations.mqtt import MqttPublisher


class _FakeMqttClient:
    """Records every call instead of talking to a real broker."""

    def __init__(self) -> None:
        self.connected: tuple[str, int] | None = None
        self.loop_started = False
        self.published: list[tuple[str, str, int]] = []
        self.disconnected = False

    def connect(self, host: str, port: int) -> object:
        self.connected = (host, port)
        return None

    def loop_start(self) -> object:
        self.loop_started = True
        return None

    def loop_stop(self) -> object:
        self.loop_started = False
        return None

    def disconnect(self) -> object:
        self.disconnected = True
        return None

    def publish(self, topic: str, payload: str, qos: int) -> object:
        self.published.append((topic, payload, qos))
        return None


class _RaisingMqttClient(_FakeMqttClient):
    def publish(self, topic: str, payload: str, qos: int) -> object:
        raise OSError("broker unreachable")


def _event(
    *,
    type: str = "tick",
    component: str = "test",
    severity: Severity = Severity.INFO,
) -> Event:
    return Event(type=type, component=component, severity=severity)


async def test_connect_calls_client_connect_and_starts_the_loop() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)

    await publisher.connect()

    assert client.connected == ("broker.local", 1883)
    assert client.loop_started


async def test_connect_is_idempotent() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)

    await publisher.connect()
    await publisher.connect()

    assert client.connected == ("broker.local", 1883)


async def test_disconnect_stops_the_loop_and_disconnects() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)
    await publisher.connect()

    await publisher.disconnect()

    assert not client.loop_started
    assert client.disconnected


async def test_disconnect_before_connect_is_a_no_op() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)

    await publisher.disconnect()  # must not raise

    assert not client.disconnected


async def test_attach_publishes_to_the_default_topic() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event(type="state_change", component="runtime"))

    assert len(client.published) == 1
    topic, payload, qos = client.published[0]
    assert topic == "edgeguard/runtime/state_change"
    assert '"type": "state_change"' in payload
    assert qos == 0


async def test_custom_fixed_topic() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", topic="devices/gateway-01", client=client)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event())

    assert client.published[0][0] == "devices/gateway-01"


async def test_custom_topic_builder() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(
        host="broker.local", topic=lambda event: f"custom/{event.type}", client=client
    )
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event(type="network_lost"))

    assert client.published[0][0] == "custom/network_lost"


async def test_detach_stops_forwarding() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)
    bus = EventBus()
    publisher.attach(bus)
    publisher.detach(bus)

    await bus.publish(_event())

    assert client.published == []


async def test_attach_and_detach_are_idempotent() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)
    bus = EventBus()
    publisher.detach(bus)  # never attached
    publisher.attach(bus)
    publisher.attach(bus)  # already attached

    await bus.publish(_event())

    assert len(client.published) == 1
    publisher.detach(bus)
    publisher.detach(bus)  # already detached


async def test_min_severity_filters_out_lower_severity_events() -> None:
    client = _FakeMqttClient()
    publisher = MqttPublisher(host="broker.local", min_severity=Severity.WARNING, client=client)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event(severity=Severity.INFO))
    await bus.publish(_event(severity=Severity.WARNING))
    await bus.publish(_event(severity=Severity.CRITICAL))

    assert len(client.published) == 2


async def test_publish_failure_is_logged_not_propagated() -> None:
    client = _RaisingMqttClient()
    publisher = MqttPublisher(host="broker.local", client=client)
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event())  # must not raise


async def test_events_published_before_connect_are_dropped_and_logged() -> None:
    publisher = MqttPublisher(host="broker.local")  # no client given, never connected
    bus = EventBus()
    publisher.attach(bus)

    await bus.publish(_event())  # must not raise, and must not try to build a real client


def test_constructing_without_a_client_does_not_require_paho_mqtt() -> None:
    # Must not raise ImportError just from construction -- only connect()
    # (with no client supplied) needs the real dependency.
    MqttPublisher(host="broker.local")


async def test_connect_without_a_client_or_paho_mqtt_raises_a_clear_import_error() -> None:
    publisher = MqttPublisher(host="broker.local")
    with pytest.raises(ImportError, match="edgeguard\\[mqtt\\]"):
        await publisher.connect()
