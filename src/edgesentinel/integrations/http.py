"""HTTP webhook event forwarding.

Forwards events published on an :class:`~edgesentinel.core.events.EventBus` as
JSON POST requests to a configured URL -- e.g. a central alerting or
aggregation service. Built on stdlib ``urllib.request`` wrapped in
``asyncio.to_thread``, the same pattern
:class:`~edgesentinel.persistence.database.Database` uses for ``sqlite3``, so
this integration needs no extra dependency.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, Mapping

from edgesentinel.core.events import Event, EventBus, EventHandler, Severity

logger = logging.getLogger("edgesentinel.integrations.http")

#: Numeric rank for `min_severity` filtering -- see the same constant in
#: :mod:`edgesentinel.diagnostics.timeline` for why severities aren't compared
#: by name.
_SEVERITY_RANK: dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.ERROR: 2,
    Severity.CRITICAL: 3,
}

#: Sends a JSON body to a URL and returns the HTTP status code. Injectable
#: so tests never make a real network call -- see ``_default_transport``
#: for the production implementation, driven entirely by fakes in tests
#: the same way :mod:`edgesentinel.network.monitor` checks are.
HttpTransport = Callable[[str, bytes, Mapping[str, str], float], Awaitable[int]]


async def _default_transport(
    url: str, body: bytes, headers: Mapping[str, str], timeout_seconds: float
) -> int:
    def send() -> int:
        request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return int(response.status)

    return await asyncio.to_thread(send)


def _event_payload(event: Event) -> bytes:
    return json.dumps(
        {
            "type": event.type,
            "component": event.component,
            "severity": event.severity.value,
            "timestamp": event.timestamp.isoformat(),
            "metadata": event.metadata,
        }
    ).encode("utf-8")


class HttpEventPublisher:
    """Forwards events published on an :class:`EventBus` as JSON POSTs to a
    webhook URL.

    Example:
        >>> publisher = HttpEventPublisher(url="https://example.com/hooks/edgesentinel")
        >>> publisher.attach(guard.events)
        >>> ...
        >>> publisher.detach(guard.events)

    A publish failure (network error, non-2xx status, or the transport
    raising) is logged, never propagated -- an observability sink can never
    be allowed to crash the reliability path it's observing, same as
    :meth:`EventBus.publish`.

    Args:
        url: Destination for each event, POSTed as a JSON body.
        headers: Extra HTTP headers (e.g. an ``Authorization`` token) sent
            with every request, alongside ``Content-Type``.
        timeout: Per-request timeout in seconds.
        min_severity: Only forward events at this severity or above,
            reducing load on both this device and the receiving service.
            ``None`` (default) forwards everything.
        transport: Override the HTTP call itself. Defaults to a real POST
            over the network; tests inject a fake that records calls
            instead of a real ``paho-mqtt``-style dependency.
    """

    def __init__(
        self,
        *,
        url: str,
        headers: Mapping[str, str] | None = None,
        timeout: float = 10.0,
        min_severity: Severity | None = None,
        transport: HttpTransport = _default_transport,
    ) -> None:
        self._url = url
        self._headers: dict[str, str] = {"Content-Type": "application/json", **(headers or {})}
        self._timeout = timeout
        self._min_severity = min_severity
        self._transport = transport
        self._handler: EventHandler | None = None

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
        try:
            status = await self._transport(
                self._url, _event_payload(event), self._headers, self._timeout
            )
            if status >= 400:
                logger.warning("webhook POST to %s returned status %d", self._url, status)
        except Exception:
            logger.exception("failed to POST event to webhook %s", self._url)
