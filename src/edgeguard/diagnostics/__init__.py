"""Diagnostics: a durable event timeline and incident tracking built on top
of it.

edgeguard's runtime publishes events on an in-memory :class:`~edgeguard.core.events.EventBus`
that only lives as long as the process does. This package makes that history
durable (:class:`EventLog`), and derives higher-level "incidents" -- spans of
non-``HEALTHY`` time -- from it (:class:`IncidentTracker`, :func:`build_incidents`),
plus human-readable rendering for both (used by the CLI).
"""

from __future__ import annotations

from edgeguard.diagnostics.incidents import Incident, IncidentTracker, build_incidents
from edgeguard.diagnostics.report import (
    format_event,
    format_incident,
    format_incidents,
    format_timeline,
    summarize_incidents,
)
from edgeguard.diagnostics.timeline import EventLog, TimelineStore

__all__ = [
    "EventLog",
    "Incident",
    "IncidentTracker",
    "TimelineStore",
    "build_incidents",
    "format_event",
    "format_incident",
    "format_incidents",
    "format_timeline",
    "summarize_incidents",
]
