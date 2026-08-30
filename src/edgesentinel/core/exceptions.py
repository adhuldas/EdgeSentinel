"""Exception hierarchy for edgesentinel.

Every exception raised directly by edgesentinel inherits from
:class:`EdgeSentinelError`, so applications can catch library failures with a
single `except` clause without needing to know internal exception types.
"""

from __future__ import annotations


class EdgeSentinelError(Exception):
    """Base class for all exceptions raised by edgesentinel."""


class InvalidStateTransitionError(EdgeSentinelError):
    """Raised when a lifecycle state transition is not permitted.

    edgesentinel's runtime states form a fixed graph (see
    :mod:`edgesentinel.core.state`). Attempting to move to a state that isn't
    reachable from the current one is a programming error, not a transient
    failure, so it raises rather than silently clamping to the nearest
    valid state.
    """

    def __init__(self, current: object, target: object) -> None:
        self.current = current
        self.target = target
        super().__init__(f"cannot transition from {current!r} to {target!r}")


class RuntimeAlreadyStartedError(EdgeSentinelError):
    """Raised when ``start()`` is called on a runtime that is already running."""


class RuntimeNotStartedError(EdgeSentinelError):
    """Raised when an operation requires a running runtime that was never started."""


class InvalidDurablePayloadError(EdgeSentinelError, TypeError):
    """Raised when a ``@guard.durable()`` call can't be safely journaled.

    Covers two distinct problems, both caught at the boundary between a
    durable function and the journal rather than deep inside SQLite:
    a decorated function's signature uses ``*args``/``**kwargs`` (so its
    bound arguments can't be reconstructed by name on replay), or the
    arguments a caller actually passed aren't JSON-serializable (so they
    can't be durably written at all). Inherits from :class:`TypeError`
    since both are, at heart, "you called/defined this wrong".
    """


class DurableOperationExhaustedError(EdgeSentinelError):
    """Raised when a durable operation fails on its final permitted attempt.

    Only raised when ``max_attempts`` is set on ``@guard.durable(...)`` and
    that limit has been reached; the underlying intent is left ``failed``
    in the journal rather than ``pending``, so it is not retried on the
    next replay. The original failure is chained via ``__cause__``.
    """

    def __init__(self, operation: str, intent_id: str, attempts: int) -> None:
        self.operation = operation
        self.intent_id = intent_id
        self.attempts = attempts
        super().__init__(
            f"durable operation {operation!r} (intent {intent_id}) "
            f"failed on its final attempt ({attempts})"
        )
