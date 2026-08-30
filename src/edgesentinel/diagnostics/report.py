"""Human-readable rendering for events and incidents.

Kept separate from the domain objects (:mod:`edgesentinel.diagnostics.timeline`,
:mod:`edgesentinel.diagnostics.incidents`) so the CLI and any future dashboard
share the same formatting logic instead of each re-implementing it.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from edgesentinel.core.events import Event
from edgesentinel.diagnostics.incidents import Incident

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_event(event: Event) -> str:
    """Render a single event as one line of text."""
    ts = event.timestamp.strftime(_TIMESTAMP_FORMAT)
    return f"{ts}  {event.severity.value.upper():<8} {event.component:<12} {event.type}"


def format_timeline(events: Sequence[Event]) -> str:
    """Render a sequence of events, one per line."""
    if not events:
        return "(no events)"
    return "\n".join(format_event(event) for event in events)


def format_incident(incident: Incident) -> str:
    """Render a single incident as one line of text."""
    started = incident.started_at.strftime(_TIMESTAMP_FORMAT)
    if incident.is_open:
        span = f"{started} -> (ongoing)"
        status = "ONGOING"
    else:
        assert incident.ended_at is not None
        ended = incident.ended_at.strftime(_TIMESTAMP_FORMAT)
        span = f"{started} -> {ended} ({incident.duration})"
        status = "resolved"
    states = " -> ".join(state.value for state in incident.states)
    return f"[{status:>8}] {span}: {states}"


def format_incidents(incidents: Sequence[Incident]) -> str:
    """Render a sequence of incidents, one per line."""
    if not incidents:
        return "(no incidents)"
    return "\n".join(format_incident(incident) for incident in incidents)


def summarize_incidents(incidents: Sequence[Incident]) -> str:
    """A one-line summary: how many incidents, and total downtime."""
    closed = [incident for incident in incidents if not incident.is_open]
    open_count = len(incidents) - len(closed)
    durations = [incident.duration for incident in closed if incident.duration is not None]
    total = sum(durations, start=dt.timedelta())
    return f"{len(closed)} resolved incident(s), {open_count} ongoing, {total} total downtime"
