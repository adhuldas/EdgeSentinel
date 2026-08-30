"""Durable event timeline.

Every :class:`~edgeguard.core.events.Event` published on the runtime's
:class:`~edgeguard.core.events.EventBus` is transient -- it's delivered to
whatever handlers happen to be subscribed at the moment, then forgotten.
``EventLog`` subscribes once and persists a durable copy of everything to
local SQLite, so "what happened, and when" survives a restart and can be
inspected afterwards -- including by a separate process (e.g. the CLI),
which never runs inside the same asyncio event loop as the application
it's inspecting.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from collections.abc import Sequence
from typing import Protocol

from edgeguard.core.events import Event, EventBus, EventHandler, Severity

#: Numeric rank for filtering by minimum severity. Severities aren't
#: lexically ordered by name (``"critical"`` sorts before ``"error"``
#: despite being more severe), so filtering happens here, not in SQL.
_SEVERITY_RANK: dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.ERROR: 2,
    Severity.CRITICAL: 3,
}

#: Upper bound on rows scanned per query when a ``min_severity`` filter is
#: applied, since that filtering happens in Python after fetching. There
#: just aren't enough events on an edge device's local journal, kept
#: pruned, for this to matter in practice.
_SEVERITY_SCAN_LIMIT = 2000


class TimelineStore(Protocol):
    """Persistence hook an :class:`EventLog` is built on.

    Satisfied structurally by :class:`edgeguard.persistence.database.Database`
    -- no import of it is needed here.
    """

    async def insert_event(
        self, *, type: str, component: str, severity: str, timestamp: str, metadata: str
    ) -> None: ...

    async def load_events(
        self,
        *,
        component: str | None = None,
        type: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = 100,
        newest_first: bool = True,
    ) -> Sequence[sqlite3.Row]: ...

    async def prune_events_older_than(self, older_than: str) -> int: ...


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        type=row["type"],
        component=row["component"],
        severity=Severity(row["severity"]),
        timestamp=dt.datetime.fromisoformat(row["timestamp"]),
        metadata=json.loads(row["metadata"]),
    )


class EventLog:
    """Persists every event published on an :class:`EventBus` and lets it
    be queried afterwards.

    Example:
        >>> log = EventLog(guard.database)
        >>> log.attach(guard.events)
        >>> ...
        >>> recent = await log.query(min_severity=Severity.WARNING, limit=20)
        >>> log.detach(guard.events)

    Args:
        store: Persistence backend, structurally typed as
            :class:`TimelineStore` (a :class:`~edgeguard.persistence.database.Database`
            in practice).
    """

    def __init__(self, store: TimelineStore) -> None:
        self._store = store
        self._handler: EventHandler | None = None

    def attach(self, events: EventBus) -> None:
        """Start recording every event published on ``events``.

        Safe to call more than once; a no-op while already attached.
        """
        if self._handler is not None:
            return
        self._handler = events.subscribe(self._on_event)

    def detach(self, events: EventBus) -> None:
        """Stop recording. Safe to call more than once, or if never attached."""
        if self._handler is None:
            return
        events.unsubscribe(self._handler)
        self._handler = None

    async def _on_event(self, event: Event) -> None:
        await self._store.insert_event(
            type=event.type,
            component=event.component,
            severity=event.severity.value,
            timestamp=event.timestamp.isoformat(),
            metadata=json.dumps(event.metadata),
        )

    async def query(
        self,
        *,
        component: str | None = None,
        type: str | None = None,
        min_severity: Severity | None = None,
        since: dt.datetime | None = None,
        until: dt.datetime | None = None,
        limit: int = 100,
        newest_first: bool = True,
    ) -> list[Event]:
        """Return recorded events matching the given filters.

        Args:
            component / type: Exact-match filters.
            min_severity: Only events at this severity or above.
            since / until: Inclusive timestamp bounds.
            limit: Maximum number of events returned.
            newest_first: Most recent first (the default, for "what just
                happened") or oldest first (for reconstructing a timeline
                in the order it occurred, e.g. for
                :func:`~edgeguard.diagnostics.incidents.build_incidents`).
        """
        rows = await self._store.load_events(
            component=component,
            type=type,
            since=since.isoformat() if since is not None else None,
            until=until.isoformat() if until is not None else None,
            limit=limit if min_severity is None else _SEVERITY_SCAN_LIMIT,
            newest_first=newest_first,
        )
        events = [_row_to_event(row) for row in rows]
        if min_severity is not None:
            threshold = _SEVERITY_RANK[min_severity]
            events = [e for e in events if _SEVERITY_RANK[e.severity] >= threshold][:limit]
        return events

    async def prune_older_than(self, older_than: dt.datetime) -> int:
        """Delete events recorded before ``older_than``.

        Returns the number of rows deleted. The timeline grows by one row
        per published event; without pruning, a long-running device would
        accumulate history forever, which is exactly the kind of unbounded
        local storage growth this library exists to prevent.
        """
        return await self._store.prune_events_older_than(older_than.isoformat())
