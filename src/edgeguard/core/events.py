"""Event bus and event types for observing runtime behavior.

edgeguard emits a structured :class:`Event` for every occurrence worth
knowing about (state transitions now; network changes, crashes, incidents,
etc. in later phases). Consumers subscribe with plain async callables. The
bus makes no assumption about what a "component" is, so the same mechanism
serves the runtime today and the resilience/network/process subsystems
later without those subsystems depending on each other.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime as dt
import enum
import logging
from collections.abc import Awaitable, Callable

from edgeguard.core.state import RuntimeState

logger = logging.getLogger("edgeguard.events")


class Severity(enum.Enum):
    """Severity of a recorded :class:`Event`."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


@dataclasses.dataclass(frozen=True, slots=True)
class Event:
    """A single point-in-time occurrence recorded by edgeguard.

    Attributes:
        type: Machine-readable event type, e.g. ``"state_change"``,
            ``"network_lost"`` (later phases). Stable across releases.
        component: Subsystem that emitted the event, e.g. ``"runtime"``,
            ``"network"``, ``"supervisor"``.
        severity: How important the event is.
        timestamp: UTC time the event occurred.
        metadata: Arbitrary JSON-serializable event-specific detail.
    """

    type: str
    component: str
    severity: Severity = Severity.INFO
    timestamp: dt.datetime = dataclasses.field(default_factory=_utcnow)
    metadata: dict[str, object] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True, slots=True)
class StateChangeEvent:
    """Emitted whenever the runtime's lifecycle state changes."""

    previous: RuntimeState
    current: RuntimeState
    timestamp: dt.datetime = dataclasses.field(default_factory=_utcnow)


EventHandler = Callable[[Event], Awaitable[None]]
StateChangeHandler = Callable[[StateChangeEvent], Awaitable[None]]


class EventBus:
    """Minimal async publish/subscribe bus.

    Handlers run concurrently. A handler that raises is logged and does not
    prevent other handlers from running or the publisher from proceeding --
    an observability hook must never be able to crash the reliability path
    it is observing.
    """

    def __init__(self) -> None:
        self._handlers: list[EventHandler] = []

    def subscribe(self, handler: EventHandler) -> EventHandler:
        """Register ``handler`` to receive future events. Returns it unchanged
        so this can also be used as a decorator."""
        self._handlers.append(handler)
        return handler

    def unsubscribe(self, handler: EventHandler) -> None:
        """Remove a previously registered handler. No-op if not registered."""
        with contextlib.suppress(ValueError):
            self._handlers.remove(handler)

    async def publish(self, event: Event) -> None:
        """Deliver ``event`` to every subscribed handler."""
        if not self._handlers:
            return
        results = await asyncio.gather(
            *(handler(event) for handler in list(self._handlers)),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.error(
                    "event handler raised while handling %s event from %s",
                    event.type,
                    event.component,
                    exc_info=result,
                )
