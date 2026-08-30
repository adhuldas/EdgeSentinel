"""edgeguard: a reliability runtime for Linux edge devices.

edgeguard helps applications survive real-world edge failures -- network
loss, DNS failure, process crashes, disk exhaustion, unexpected reboots --
without hand-rolling retry loops, journals, and recovery logic. Applications
describe *what* should happen; edgeguard handles *how* it survives failure.

This is Phase 6 of the project: the Phase 1 runtime (lifecycle state
machine, events, SQLite persistence), the Phase 2 resilience engine (retry,
backoff, timeout, circuit breaker, ``@guard.reliable()``), Phase 3 durable
operations (the intent journal and ``@guard.durable()``, with automatic
startup replay of unfinished operations), Phase 4 network/process/storage
monitoring (``NetworkMonitor``, ``Supervisor``, ``Watchdog``,
``StorageMonitor``, wired to the runtime via ``guard.watch_network()``,
``guard.supervise()``, ``guard.watchdog``, and ``guard.watch_storage()``),
Phase 5 diagnostics (a durable event timeline and incident tracking --
``guard.timeline``, ``guard.incidents``, see :mod:`edgeguard.diagnostics`
-- plus the ``edgeguard`` CLI for inspecting a runtime's on-disk state from
a separate process), and Phase 6 integrations: forwarding events to a
webhook or an MQTT broker (see :mod:`edgeguard.integrations`). See
CHANGELOG.md for current status.
"""

from __future__ import annotations

import logging as _logging

from edgeguard.core.events import Event, Severity, StateChangeEvent
from edgeguard.core.exceptions import (
    DurableOperationExhaustedError,
    EdgeGuardError,
    InvalidDurablePayloadError,
    InvalidStateTransitionError,
    RuntimeAlreadyStartedError,
    RuntimeNotStartedError,
)
from edgeguard.core.runtime import EdgeGuard
from edgeguard.core.state import RuntimeState
from edgeguard.durability.journal import Intent, IntentStatus
from edgeguard.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)
from edgeguard.resilience.retry import RetryAttempt, RetryPolicy
from edgeguard.resilience.timeout import OperationTimeoutError

# Library convention: emit no output unless the application configures a
# handler for the "edgeguard" logger hierarchy.
_logging.getLogger("edgeguard").addHandler(_logging.NullHandler())

__version__ = "0.1.0"

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitState",
    "DurableOperationExhaustedError",
    "EdgeGuard",
    "EdgeGuardError",
    "Event",
    "Intent",
    "IntentStatus",
    "InvalidDurablePayloadError",
    "InvalidStateTransitionError",
    "OperationTimeoutError",
    "RetryAttempt",
    "RetryPolicy",
    "RuntimeAlreadyStartedError",
    "RuntimeNotStartedError",
    "RuntimeState",
    "Severity",
    "StateChangeEvent",
    "__version__",
]
