"""MQTT event forwarding.

Forwards events published on an :class:`~edgeguard.core.events.EventBus` to
an MQTT broker, so a central collector can observe device health without
polling each device's local SQLite timeline. Unlike
:mod:`edgeguard.integrations.http`, there is no MQTT client in the standard
library -- production use requires the ``paho-mqtt`` package (``pip install
edgeguard[mqtt]``). This module itself has no import-time dependency on it,
though: :class:`MqttPublisher` talks to :class:`MqttClient`, a small
structural :class:`~typing.Protocol` capturing only the handful of methods
it needs, so tests inject a fake instead of requiring the real dependency
to be installed. Only actually constructing a real client (when one isn't
supplied) needs ``paho-mqtt`` importable.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Protocol

from edgeguard.core.events import Event, EventBus, EventHandler, Severity

logger = logging.getLogger("edgeguard.integrations.mqtt")

#: Numeric rank for `min_severity` filtering -- see the same constant in
#: :mod:`edgeguard.diagnostics.timeline` for why severities aren't compared
#: by name.
_SEVERITY_RANK: dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.ERROR: 2,
    Severity.CRITICAL: 3,
}

TopicBuilder = Callable[[Event], str]


class MqttClient(Protocol):
    """The subset of an MQTT client's API :class:`MqttPublisher` needs.

    Satisfied structurally by ``paho.mqtt.client.Client`` -- no import of
    it is needed here -- and trivially by a fake in tests. Methods return
    ``object`` rather than ``None`` since real clients (``paho-mqtt``
    included) typically return a result object callers here don't care
    about; declaring ``object`` keeps any such return value compatible
    without depending on a specific client library's types.
    """

    def connect(self, host: str, port: int) -> object: ...
    def loop_start(self) -> object: ...
    def loop_stop(self) -> object: ...
    def disconnect(self) -> object: ...
    def publish(self, topic: str, payload: str, qos: int) -> object: ...


def _default_topic(prefix: str) -> TopicBuilder:
    def build(event: Event) -> str:
        return f"{prefix}/{event.component}/{event.type}"

    return build


#: Module-level singleton so it's safe to use as a default argument value
#: (ruff's B008 flags calling a function directly in a default expression).
_DEFAULT_TOPIC: TopicBuilder = _default_topic("edgeguard")


def _build_default_client() -> MqttClient:
    try:
        import paho.mqtt.client as paho
    except ImportError as exc:
        raise ImportError(
            "edgeguard.integrations.mqtt needs a real MQTT client when none "
            "is supplied via the `client` argument. Install the optional "
            "dependency with: pip install edgeguard[mqtt]"
        ) from exc
    client: MqttClient = paho.Client()
    return client


def _event_payload(event: Event) -> str:
    return json.dumps(
        {
            "type": event.type,
            "component": event.component,
            "severity": event.severity.value,
            "timestamp": event.timestamp.isoformat(),
            "metadata": event.metadata,
        }
    )


class MqttPublisher:
    """Forwards events published on an :class:`EventBus` to an MQTT broker.

    Example:
        >>> publisher = MqttPublisher(host="broker.local")
        >>> await publisher.connect()
        >>> publisher.attach(guard.events)
        >>> ...
        >>> publisher.detach(guard.events)
        >>> await publisher.disconnect()

    A real MQTT client (``paho-mqtt``'s) is synchronous and runs its own
    background network thread; every call into it here is wrapped in
    ``asyncio.to_thread``, the same pattern
    :class:`~edgeguard.persistence.database.Database` uses for ``sqlite3``,
    so it never blocks the event loop.

    Args:
        host, port: The MQTT broker to connect to.
        topic: Either a fixed topic string, or a callable computing one per
            event (see :func:`_default_topic` for the default:
            ``"edgeguard/{component}/{type}"``).
        qos: MQTT quality-of-service level for every publish.
        min_severity: Only forward events at this severity or above.
            ``None`` (default) forwards everything.
        client: An already-constructed :class:`MqttClient`. If omitted, one
            is lazily constructed from ``paho-mqtt`` the first time
            :meth:`connect` runs, raising :class:`ImportError` with
            install instructions if it isn't installed. Tests should
            always supply a fake here instead.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int = 1883,
        topic: str | TopicBuilder = _DEFAULT_TOPIC,
        qos: int = 0,
        min_severity: Severity | None = None,
        client: MqttClient | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._topic: TopicBuilder = topic if callable(topic) else (lambda _event: topic)
        self._qos = qos
        self._min_severity = min_severity
        self._client = client
        self._connected = False
        self._handler: EventHandler | None = None

    async def connect(self) -> None:
        """Connect to the broker and start its network loop.

        Safe to call more than once; a no-op while already connected.
        Lazily constructs a real ``paho-mqtt`` client here (not in
        ``__init__``) if none was supplied, so importing or constructing
        this class never requires the optional dependency -- only actually
        connecting for real does.
        """
        if self._connected:
            return
        if self._client is None:
            self._client = _build_default_client()
        client = self._client
        await asyncio.to_thread(client.connect, self._host, self._port)
        await asyncio.to_thread(client.loop_start)
        self._connected = True

    async def disconnect(self) -> None:
        """Stop the network loop and disconnect. Safe to call more than
        once, or if never connected."""
        if not self._connected or self._client is None:
            return
        client = self._client
        await asyncio.to_thread(client.loop_stop)
        await asyncio.to_thread(client.disconnect)
        self._connected = False

    def attach(self, events: EventBus) -> None:
        """Start forwarding events published on ``events``.

        Safe to call more than once; a no-op while already attached.
        """
        if self._handler is not None:
            return
        self._handler = events.subscribe(self._on_event)

    def detach(self, events: EventBus) -> None:
        """Stop forwarding. Safe to call more than once, or if never attached."""
        if self._handler is None:
            return
        events.unsubscribe(self._handler)
        self._handler = None

    async def _on_event(self, event: Event) -> None:
        if (
            self._min_severity is not None
            and _SEVERITY_RANK[event.severity] < _SEVERITY_RANK[self._min_severity]
        ):
            return
        if self._client is None:
            logger.warning("dropping event: MqttPublisher is not connected")
            return
        client = self._client
        topic = self._topic(event)
        try:
            await asyncio.to_thread(client.publish, topic, _event_payload(event), self._qos)
        except Exception:
            logger.exception("failed to publish event to MQTT topic %s", topic)
