"""Lifecycle state machine for the EdgeSentinel runtime.

The runtime moves through a small, strongly-typed set of states. Only the
transitions listed in ``_TRANSITIONS`` are permitted; anything else raises
:class:`~edgesentinel.core.exceptions.InvalidStateTransitionError`. Making
illegal transitions raise (rather than just documenting which ones are
"supposed" to happen) is what keeps the runtime's behavior deterministic
under failure -- the same property this whole library exists to provide to
applications.
"""

from __future__ import annotations

import asyncio
import enum

from edgesentinel.core.exceptions import InvalidStateTransitionError


class RuntimeState(enum.Enum):
    """Lifecycle states of an :class:`~edgesentinel.core.runtime.EdgeSentinel` instance."""

    BOOTING = "booting"
    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    RECOVERING = "recovering"
    FAILED = "failed"
    STOPPING = "stopping"
    STOPPED = "stopped"


#: Directed graph of allowed transitions. STOPPED is terminal: once a
#: runtime is stopped it must be recreated, not restarted in place, so that
#: "stopped" always means every subsystem has actually released its
#: resources (file handles, sockets, DB connections).
_TRANSITIONS: dict[RuntimeState, frozenset[RuntimeState]] = {
    RuntimeState.BOOTING: frozenset({RuntimeState.INITIALIZING, RuntimeState.FAILED}),
    RuntimeState.INITIALIZING: frozenset(
        {RuntimeState.HEALTHY, RuntimeState.DEGRADED, RuntimeState.FAILED}
    ),
    RuntimeState.HEALTHY: frozenset(
        {
            RuntimeState.DEGRADED,
            RuntimeState.OFFLINE,
            RuntimeState.RECOVERING,
            RuntimeState.STOPPING,
            RuntimeState.FAILED,
        }
    ),
    RuntimeState.DEGRADED: frozenset(
        {
            RuntimeState.HEALTHY,
            RuntimeState.OFFLINE,
            RuntimeState.RECOVERING,
            RuntimeState.STOPPING,
            RuntimeState.FAILED,
        }
    ),
    RuntimeState.OFFLINE: frozenset(
        {
            RuntimeState.RECOVERING,
            RuntimeState.DEGRADED,
            RuntimeState.STOPPING,
            RuntimeState.FAILED,
        }
    ),
    RuntimeState.RECOVERING: frozenset(
        {
            RuntimeState.HEALTHY,
            RuntimeState.DEGRADED,
            RuntimeState.OFFLINE,
            RuntimeState.FAILED,
            RuntimeState.STOPPING,
        }
    ),
    RuntimeState.FAILED: frozenset({RuntimeState.RECOVERING, RuntimeState.STOPPING}),
    RuntimeState.STOPPING: frozenset({RuntimeState.STOPPED}),
    RuntimeState.STOPPED: frozenset(),
}


class StateMachine:
    """Concurrency-safe lifecycle state machine.

    Transitions are validated against :data:`_TRANSITIONS` and serialized
    with an internal lock so concurrent callers (e.g. the watchdog escalating
    to ``FAILED`` while the network monitor is escalating to ``OFFLINE``)
    can't race each other into an inconsistent state.
    """

    def __init__(self, initial: RuntimeState = RuntimeState.BOOTING) -> None:
        self._state = initial
        self._lock = asyncio.Lock()

    @property
    def current(self) -> RuntimeState:
        return self._state

    def can_transition(self, target: RuntimeState) -> bool:
        """Return whether ``target`` is reachable from the current state."""
        return target in _TRANSITIONS[self._state]

    async def transition(self, target: RuntimeState) -> RuntimeState:
        """Move to ``target`` and return the state transitioned *from*.

        Transitioning to the current state is a no-op (returns the current
        state without error) so callers don't need to special-case "already
        there". Any other unreachable target raises.

        Raises:
            InvalidStateTransitionError: if ``target`` is not reachable from
                the current state.
        """
        async with self._lock:
            if target == self._state:
                return self._state
            if not self.can_transition(target):
                raise InvalidStateTransitionError(self._state, target)
            previous = self._state
            self._state = target
            return previous
