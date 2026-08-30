from __future__ import annotations

import datetime as dt

from edgeguard.core.events import Event, Severity
from edgeguard.core.state import RuntimeState
from edgeguard.diagnostics.incidents import Incident
from edgeguard.diagnostics.report import (
    format_event,
    format_incident,
    format_incidents,
    format_timeline,
    summarize_incidents,
)

_T0 = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)


def _event(**overrides: object) -> Event:
    defaults: dict[str, object] = {
        "type": "state_change",
        "component": "runtime",
        "severity": Severity.INFO,
        "timestamp": _T0,
        "metadata": {},
    }
    defaults.update(overrides)
    return Event(**defaults)  # type: ignore[arg-type]


def test_format_event_includes_timestamp_severity_component_and_type() -> None:
    line = format_event(_event(severity=Severity.WARNING, component="network", type="network_lost"))
    assert "WARNING" in line
    assert "network" in line
    assert "network_lost" in line
    assert "2024-01-01" in line


def test_format_timeline_joins_events_one_per_line() -> None:
    events = [_event(component="a"), _event(component="b")]
    rendered = format_timeline(events)
    lines = rendered.splitlines()
    assert len(lines) == 2
    assert "a" in lines[0]
    assert "b" in lines[1]


def test_format_timeline_handles_empty_sequence() -> None:
    assert format_timeline([]) == "(no events)"


def test_format_incident_open_shows_ongoing() -> None:
    incident = Incident(
        started_at=_T0,
        ended_at=None,
        states=(RuntimeState.DEGRADED,),
        events=(),
    )
    line = format_incident(incident)
    assert "ONGOING" in line
    assert "degraded" in line


def test_format_incident_closed_shows_duration() -> None:
    incident = Incident(
        started_at=_T0,
        ended_at=_T0 + dt.timedelta(minutes=5),
        states=(RuntimeState.OFFLINE, RuntimeState.RECOVERING),
        events=(),
    )
    line = format_incident(incident)
    assert "resolved" in line
    assert "0:05:00" in line
    assert "offline" in line
    assert "recovering" in line


def test_format_incidents_handles_empty_sequence() -> None:
    assert format_incidents([]) == "(no incidents)"


def test_summarize_incidents_counts_resolved_and_ongoing_and_totals_downtime() -> None:
    resolved = Incident(
        started_at=_T0,
        ended_at=_T0 + dt.timedelta(minutes=10),
        states=(RuntimeState.DEGRADED,),
        events=(),
    )
    ongoing = Incident(
        started_at=_T0,
        ended_at=None,
        states=(RuntimeState.OFFLINE,),
        events=(),
    )

    summary = summarize_incidents([resolved, ongoing])
    assert "1 resolved" in summary
    assert "1 ongoing" in summary
    assert "0:10:00" in summary


def test_summarize_incidents_on_empty_sequence() -> None:
    summary = summarize_incidents([])
    assert "0 resolved" in summary
    assert "0 ongoing" in summary
