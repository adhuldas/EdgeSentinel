"""Lifecycle orchestration: the ordered sequence of transitions that take a
runtime from boot to healthy, and from running to stopped.

This is deliberately separate from :mod:`edgeguard.core.runtime` so the
*sequence* of a startup or shutdown -- which states it passes through, what
happens if initialization fails -- can be tested without constructing a full
``EdgeGuard`` instance and its subsystems.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from edgeguard.core.events import Event, EventBus, Severity, StateChangeEvent
from edgeguard.core.state import RuntimeState, StateMachine

logger = logging.getLogger("edgeguard.lifecycle")

Hook = Callable[[], Awaitable[None]]
StateChangeHandler = Callable[[StateChangeEvent], Awaitable[None]]


class LifecycleManager:
    """Drives a :class:`StateMachine` through a startup or shutdown sequence.

    Every transition is reported both to the runtime's own
    ``on_state_change`` handlers and to the shared :class:`EventBus`, so
    subsystems that only care about "something changed" (diagnostics,
    metrics) don't need to know about the state-change-specific API.
    """

    def __init__(
        self,
        state_machine: StateMachine,
        events: EventBus,
        state_change_handlers: list[StateChangeHandler],
    ) -> None:
        self._states = state_machine
        self._events = events
        self._state_change_handlers = state_change_handlers

    async def _set_state(self, target: RuntimeState, *, component: str) -> None:
        previous = await self._states.transition(target)
        if previous == target:
            return
        logger.info("state transition: %s -> %s", previous.value, target.value)
        change = StateChangeEvent(previous=previous, current=target)
        for handler in list(self._state_change_handlers):
            await handler(change)
        await self._events.publish(
            Event(
                type="state_change",
                component=component,
                severity=Severity.INFO,
                metadata={"previous": previous.value, "current": target.value},
            )
        )

    async def set_state(self, target: RuntimeState, *, component: str = "runtime") -> None:
        """Move to ``target`` outside the boot/shutdown sequence.

        For subsystems that need to reflect their own health onto the
        runtime's lifecycle state after startup -- e.g. a network monitor
        moving to ``OFFLINE``, or a process supervisor escalating to
        ``FAILED``. Same validation and event-publishing as
        :meth:`boot`/:meth:`shutdown`; an illegal transition raises
        :class:`~edgeguard.core.exceptions.InvalidStateTransitionError`
        rather than being silently ignored, since a subsystem attempting an
        impossible transition (e.g. from ``STOPPED``) is a sign it reacted
        to a stale status.
        """
        await self._set_state(target, component=component)

    async def boot(self, *, on_init: Hook, component: str = "runtime") -> None:
        """Run the BOOTING -> INITIALIZING -> HEALTHY sequence.

        ``on_init`` is awaited while the state is INITIALIZING. If it
        raises, the runtime transitions to FAILED instead of HEALTHY and the
        exception propagates to the caller -- a runtime is never left
        looking healthy after a failed initialization.
        """
        await self._set_state(RuntimeState.BOOTING, component=component)
        await self._set_state(RuntimeState.INITIALIZING, component=component)
        try:
            await on_init()
        except Exception:
            logger.exception("runtime initialization failed")
            await self._set_state(RuntimeState.FAILED, component=component)
            raise
        await self._set_state(RuntimeState.HEALTHY, component=component)

    async def shutdown(self, *, on_stop: Hook, component: str = "runtime") -> None:
        """Run the STOPPING -> STOPPED sequence.

        ``on_stop`` is awaited while the state is STOPPING. It runs even if
        it raises -- shutdown always reaches STOPPED -- but the exception is
        re-raised to the caller afterwards so failures during teardown are
        never silently swallowed.
        """
        await self._set_state(RuntimeState.STOPPING, component=component)
        try:
            await on_stop()
        finally:
            await self._set_state(RuntimeState.STOPPED, component=component)
