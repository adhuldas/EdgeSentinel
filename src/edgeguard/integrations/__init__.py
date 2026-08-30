"""Optional external integrations: forwarding events off-device.

Everything in this package is opt-in -- an ``EdgeGuard`` runtime works
completely without it. Each integration subscribes to the runtime's
:class:`~edgeguard.core.events.EventBus` the same way
:class:`~edgeguard.diagnostics.timeline.EventLog` does, and forwards events
to an external system instead of (or in addition to) the local SQLite
timeline.

:mod:`edgeguard.integrations.http` is stdlib-only (``urllib`` wrapped in
``asyncio.to_thread``), so it needs no extra dependency.
:mod:`edgeguard.integrations.mqtt` needs a real MQTT client to talk to a
broker; production use requires the ``paho-mqtt`` package (``pip install
edgeguard[mqtt]``), but the module itself imports cleanly without it since
tests inject a fake client instead.
"""

from __future__ import annotations

from edgeguard.integrations.http import HttpEventPublisher, HttpTransport
from edgeguard.integrations.mqtt import MqttClient, MqttPublisher, TopicBuilder

__all__ = [
    "HttpEventPublisher",
    "HttpTransport",
    "MqttClient",
    "MqttPublisher",
    "TopicBuilder",
]
