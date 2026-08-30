"""Optional external integrations: forwarding events off-device.

Everything in this package is opt-in -- an ``EdgeSentinel`` runtime works
completely without it. Each integration subscribes to the runtime's
:class:`~edgesentinel.core.events.EventBus` the same way
:class:`~edgesentinel.diagnostics.timeline.EventLog` does, and forwards events
to an external system instead of (or in addition to) the local SQLite
timeline.

:mod:`edgesentinel.integrations.http` is stdlib-only (``urllib`` wrapped in
``asyncio.to_thread``), so it needs no extra dependency.
:mod:`edgesentinel.integrations.mqtt` needs a real MQTT client to talk to a
broker; production use requires the ``paho-mqtt`` package (``pip install
edgesentinel[mqtt]``), but the module itself imports cleanly without it since
tests inject a fake client instead.
"""

from __future__ import annotations

from edgesentinel.integrations.http import HttpEventPublisher, HttpTransport
from edgesentinel.integrations.mqtt import MqttClient, MqttPublisher, TopicBuilder

__all__ = [
    "HttpEventPublisher",
    "HttpTransport",
    "MqttClient",
    "MqttPublisher",
    "TopicBuilder",
]
