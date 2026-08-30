"""edgesentinel: a reliability runtime for Linux edge devices.

edgesentinel helps applications survive real-world edge failures -- network
loss, DNS failure, process crashes, disk exhaustion, unexpected reboots --
without hand-rolling retry loops, journals, and recovery logic. Applications
describe *what* should happen; edgesentinel handles *how* it survives failure.

This is Phase 7 of the project: the Phase 1 runtime (lifecycle state
machine, events, SQLite persistence), the Phase 2 resilience engine (retry,
backoff, timeout, circuit breaker, ``@guard.reliable()``), Phase 3 durable
operations (the intent journal and ``@guard.durable()``, with automatic
startup replay of unfinished operations), Phase 4 network/process/storage
monitoring (``NetworkMonitor``, ``Supervisor``, ``Watchdog``,
``StorageMonitor``, wired to the runtime via ``guard.watch_network()``,
``guard.supervise()``, ``guard.watchdog``, and ``guard.watch_storage()``),
Phase 5 diagnostics (a durable event timeline and incident tracking --
``guard.timeline``, ``guard.incidents``, see :mod:`edgesentinel.diagnostics`
-- plus the ``edgesentinel`` CLI for inspecting a runtime's on-disk state from
a separate process), Phase 6 integrations (forwarding events to a webhook
or an MQTT broker, see :mod:`edgesentinel.integrations`), and Phase 7 hardware
metrics: CPU/memory/temperature monitoring with scoped mitigation, wired
via ``guard.watch_hardware()`` (see :mod:`edgesentinel.metrics`). See
CHANGELOG.md for current status.
"""

from __future__ import annotations

import logging as _logging

from edgesentinel.core.events import Event, Severity, StateChangeEvent
from edgesentinel.core.exceptions import (
    DurableOperationExhaustedError,
    EdgeSentinelError,
    InvalidDurablePayloadError,
    InvalidStateTransitionError,
    RuntimeAlreadyStartedError,
    RuntimeNotStartedError,
)
from edgesentinel.core.runtime import EdgeSentinel
from edgesentinel.core.state import RuntimeState
from edgesentinel.durability.journal import Intent, IntentStatus
from edgesentinel.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)
from edgesentinel.resilience.retry import RetryAttempt, RetryPolicy
from edgesentinel.resilience.timeout import OperationTimeoutError

# Library convention: emit no output unless the application configures a
# handler for the "edgesentinel" logger hierarchy.
_logging.getLogger("edgesentinel").addHandler(_logging.NullHandler())

__version__ = "0.1.0"

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitState",
    "DurableOperationExhaustedError",
    "EdgeSentinel",
    "EdgeSentinelError",
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
