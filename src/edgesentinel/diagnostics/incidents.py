"""Incident tracking.

edgesentinel's Phase 4 subsystems (network, process, storage) don't report
their health to applications directly -- they escalate through the
lifecycle state machine (see :mod:`edgesentinel.core.state`), so a
``state_change`` event is already a single, unified signal for "something
is wrong" regardless of which subsystem noticed it first. ``IncidentTracker``
groups spans of non-``HEALTHY`` time into :class:`Incident` records by
watching exactly that signal, rather than needing to know about every
subsystem's own event types.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from collections.abc import Iterable

from edgesentinel.core.events import Event, EventBus, EventHandler
from edgesentinel.core.state import RuntimeState

#: States considered "unhealthy" for incident-tracking purposes. BOOTING
#: and INITIALIZING (normal startup) and STOPPING/STOPPED (normal
#: shutdown) are deliberately excluded -- an incident is time spent NOT
#: healthy while otherwise running, not the startup/shutdown sequence
#: every runtime goes through regardless.
_INCIDENT_STATES = frozenset(
    {RuntimeState.DEGRADED, RuntimeState.OFFLINE, RuntimeState.RECOVERING, RuntimeState.FAILED}
)


@dataclasses.dataclass(frozen=True, slots=True)
class Incident:
    """A single span of time the runtime spent outside ``HEALTHY``.

    Attributes:
        started_at: When the runtime first left ``HEALTHY``.
        ended_at: When it returned to ``HEALTHY`` (or left the incident
            states for any other reason, e.g. shutting down while
            unhealthy), or ``None`` while still open.
        states: Every unhealthy state visited, in order (e.g.
            ``(DEGRADED, OFFLINE, RECOVERING)`` for a full outage that
            later recovers).
        events: Every event recorded on the bus while this incident was
            open, in publish order -- including whatever subsystem event
            triggered each state change, not just the ``state_change``
            events themselves.
    """

    started_at: dt.datetime
    ended_at: dt.datetime | None
    states: tuple[RuntimeState, ...]
    events: tuple[Event, ...]

    @property
    def is_open(self) -> bool:
        return self.ended_at is None

    @property
    def duration(self) -> dt.timedelta | None:
        """How long the incident lasted, or ``None`` while still open."""
        return None if self.ended_at is None else self.ended_at - self.started_at


@dataclasses.dataclass(slots=True)
class _OpenIncident:
    started_at: dt.datetime
    events: list[Event]
    states: list[RuntimeState] = dataclasses.field(default_factory=list)

    def freeze(self) -> Incident:
        return Incident(
            started_at=self.started_at,
            ended_at=None,
            states=tuple(self.states),
            events=tuple(self.events),
        )

    def close(self, ended_at: dt.datetime) -> Incident:
        return Incident(
            started_at=self.started_at,
            ended_at=ended_at,
            states=tuple(self.states),
            events=tuple(self.events),
        )


class _IncidentBuilder:
    """The grouping logic shared by :class:`IncidentTracker` (live, via an
    :class:`EventBus`) and :func:`build_incidents` (offline, from an
    already-recorded sequence of events)."""

    def __init__(self) -> None:
        self._closed: list[Incident] = []
        self._open: _OpenIncident | None = None

    def feed(self, event: Event) -> None:
        if self._open is not None:
            self._open.events.append(event)
        if event.type != "state_change":
            return
        current = RuntimeState(event.metadata["current"])
        if current in _INCIDENT_STATES:
            if self._open is None:
                self._open = _OpenIncident(started_at=event.timestamp, events=[event])
            self._open.states.append(current)
        elif self._open is not None:
            self._closed.append(self._open.close(event.timestamp))
            self._open = None

    @property
    def open_incident(self) -> Incident | None:
        return None if self._open is None else self._open.freeze()

    @property
    def closed(self) -> list[Incident]:
        return self._closed


class IncidentTracker:
    """Watches an :class:`EventBus` and groups ``state_change`` events into
    :class:`Incident` spans.

    Example:
        >>> tracker = IncidentTracker()
        >>> tracker.attach(guard.events)
        >>> ...
        >>> tracker.open_incident  # None while healthy
        >>> tracker.incidents      # closed incidents, oldest first
        >>> tracker.detach(guard.events)

    Args:
        max_history: Number of closed incidents to retain in memory, oldest
            dropped first. This is a live, in-process view for
            dashboards -- see :func:`build_incidents` for reconstructing
            the durable record from :class:`~edgesentinel.diagnostics.timeline.EventLog`.

    Raises:
        ValueError: if ``max_history`` is not positive.
    """

    def __init__(self, *, max_history: int = 100) -> None:
        if max_history < 1:
            raise ValueError(f"max_history must be >= 1, got {max_history}")
        self._max_history = max_history
        self._builder = _IncidentBuilder()
        self._handler: EventHandler | None = None

    def attach(self, events: EventBus) -> None:
        """Start tracking. Safe to call more than once; a no-op while
        already attached."""
        if self._handler is not None:
            return
        self._handler = events.subscribe(self._on_event)

    def detach(self, events: EventBus) -> None:
        """Stop tracking. Safe to call more than once, or if never attached."""
        if self._handler is None:
            return
        events.unsubscribe(self._handler)
        self._handler = None

    @property
    def open_incident(self) -> Incident | None:
        """The currently open incident, or ``None`` while healthy."""
        return self._builder.open_incident

    @property
    def incidents(self) -> tuple[Incident, ...]:
        """Closed incidents, oldest first, bounded by ``max_history``."""
        return tuple(self._builder.closed)

    async def _on_event(self, event: Event) -> None:
        self._builder.feed(event)
        overflow = len(self._builder.closed) - self._max_history
        if overflow > 0:
            del self._builder.closed[:overflow]


def build_incidents(events: Iterable[Event]) -> list[Incident]:
    """Reconstruct incidents from a sequence of already-recorded events.

    ``events`` must be in the order they were originally published
    (oldest first) -- see ``newest_first=False`` on
    :meth:`~edgesentinel.diagnostics.timeline.EventLog.query`. Used by the
    CLI, which has no live :class:`EventBus` to attach to and instead
    replays the durable timeline.

    The last incident may be open (``is_open`` True) if the event sequence
    ends before the runtime returned to ``HEALTHY``.
    """
    builder = _IncidentBuilder()
    for event in events:
        builder.feed(event)
    result = list(builder.closed)
    if builder.open_incident is not None:
        result.append(builder.open_incident)
    return result
