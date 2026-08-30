"""In-process task supervision and heartbeat-based staleness detection."""

from __future__ import annotations

from edgeguard.process.supervisor import Supervisor
from edgeguard.process.watchdog import UnknownWatchTargetError, Watchdog

__all__ = [
    "Supervisor",
    "UnknownWatchTargetError",
    "Watchdog",
]
