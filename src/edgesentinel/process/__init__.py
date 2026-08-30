"""In-process task supervision and heartbeat-based staleness detection."""

from __future__ import annotations

from edgesentinel.process.supervisor import Supervisor
from edgesentinel.process.watchdog import UnknownWatchTargetError, Watchdog

__all__ = [
    "Supervisor",
    "UnknownWatchTargetError",
    "Watchdog",
]
