"""Heartbeat-based staleness detection.

Some failures don't crash a task -- they just make it stop making
progress (blocked on a wedged serial port, a client stuck in a hung read).
A :class:`Watchdog` doesn't know *why* a registered component went quiet,
but it can tell *that* it did: components call :meth:`Watchdog.heartbeat`
from their own loop, and the watchdog escalates anything that hasn't
checked in within its configured timeout.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import time
from collections.abc import Awaitable, Callable

from edgeguard.core.events import Event, EventBus, Severity
from edgeguard.core.exceptions import EdgeGuardError, InvalidStateTransitionError
from edgeguard.core.state import RuntimeState

logger = logging.getLogger("edgeguard.process")

SetState = Callable[[RuntimeState], Awaitable[None]]
GetState = Callable[[], RuntimeState]
Sleep = Callable[[float], Awaitable[None]]
Now = Callable[[], float]

_UNSAFE_TO_ESCALATE = frozenset({RuntimeState.STOPPING, RuntimeState.STOPPED})


class UnknownWatchTargetError(EdgeGuardError):
    """Raised by :meth:`Watchdog.heartbeat` for a name never registered."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"no watchdog target registered as {name!r}")


@dataclasses.dataclass(slots=True)
class _Watch:
    timeout: float
    last_seen: float
    stale: bool = False


class Watchdog:
    """Tracks liveness of named components via periodic heartbeats.

    Example:
        >>> watchdog = Watchdog()
        >>> watchdog.register("sensor-reader", timeout=30.0)
        >>> ...
        >>> watchdog.heartbeat("sensor-reader")  # called from that task's own loop

    Args:
        poll_interval: Seconds between staleness checks once :meth:`start`
            is running.
        events / component: Where staleness/recovery events are published.
        set_state / get_state: Optional hooks letting the watchdog escalate
            the runtime to ``FAILED`` when something goes stale. Both or
            neither.
        sleep / now: Injectable clock, for deterministic tests.
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5.0,
        events: EventBus | None = None,
        component: str = "process",
        set_state: SetState | None = None,
        get_state: GetState | None = None,
        sleep: Sleep = asyncio.sleep,
        now: Now = time.monotonic,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError(f"poll_interval must be > 0, got {poll_interval}")
        if (set_state is None) != (get_state is None):
            raise ValueError("set_state and get_state must be given together, or neither")
        self._poll_interval = poll_interval
        self._events = events
        self._component = component
        self._set_state = set_state
        self._get_state = get_state
        self._sleep = sleep
        self._now = now
        self._watches: dict[str, _Watch] = {}
        self._task: asyncio.Task[None] | None = None

    def register(self, name: str, *, timeout: float) -> None:
        """Start tracking ``name``, considered stale if no heartbeat arrives
        within ``timeout`` seconds of the last one (or of registration)."""
        if timeout <= 0:
            raise ValueError(f"timeout must be > 0, got {timeout}")
        self._watches[name] = _Watch(timeout=timeout, last_seen=self._now())

    def unregister(self, name: str) -> None:
        """Stop tracking ``name``. No-op if it was never registered."""
        self._watches.pop(name, None)

    def heartbeat(self, name: str) -> None:
        """Record that ``name`` is alive right now.

        Synchronous and cheap -- meant to be called from a hot loop without
        awaiting anything.

        Raises:
            UnknownWatchTargetError: if ``name`` was never registered.
        """
        watch = self._watches.get(name)
        if watch is None:
            raise UnknownWatchTargetError(name)
        watch.last_seen = self._now()
        # Deliberately does not clear `watch.stale` here: that flag exists
        # so check_once() can tell a transition from a steady state and
        # publish "recovered" exactly once. Clearing it here would let a
        # heartbeat race past check_once and erase the transition, so a
        # stale target that recovers between polls would recover silently.

    @property
    def stale(self) -> tuple[str, ...]:
        """Names currently overdue for a heartbeat, as of right now."""
        now = self._now()
        return tuple(
            name for name, watch in self._watches.items() if now - watch.last_seen > watch.timeout
        )

    async def check_once(self) -> tuple[str, ...]:
        """Check every registered target, returning names newly gone stale.

        Publishes ``watchdog_target_stale`` the moment a target crosses its
        timeout, and ``watchdog_target_recovered`` if a heartbeat arrives
        for it afterwards -- each only once per transition, not on every
        poll while it remains stale or healthy.
        """
        now = self._now()
        newly_stale: list[str] = []
        for name, watch in self._watches.items():
            is_stale = now - watch.last_seen > watch.timeout
            if is_stale and not watch.stale:
                watch.stale = True
                newly_stale.append(name)
                if self._events is not None:
                    await self._events.publish(
                        Event(
                            type="watchdog_target_stale",
                            component=self._component,
                            severity=Severity.ERROR,
                            metadata={"name": name, "timeout": watch.timeout},
                        )
                    )
            elif not is_stale and watch.stale:
                watch.stale = False
                if self._events is not None:
                    await self._events.publish(
                        Event(
                            type="watchdog_target_recovered",
                            component=self._component,
                            metadata={"name": name},
                        )
                    )
        if newly_stale and self._set_state is not None:
            await self._escalate()
        return tuple(newly_stale)

    async def start(self) -> None:
        """Run an initial check and start polling every ``poll_interval``
        seconds. Safe to call more than once; a no-op while already
        running."""
        if self._task is not None:
            return
        await self.check_once()
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Stop polling. Safe to call more than once, or if never started."""
        if self._task is None:
            return
        task, self._task = self._task, None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _poll_loop(self) -> None:
        while True:
            await self._sleep(self._poll_interval)
            try:
                await self.check_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watchdog poll iteration failed")

    async def _escalate(self) -> None:
        assert self._set_state is not None
        assert self._get_state is not None
        if self._get_state() in _UNSAFE_TO_ESCALATE:
            return
        try:
            await self._set_state(RuntimeState.FAILED)
        except InvalidStateTransitionError:
            logger.debug(
                "watchdog could not move runtime to FAILED: no longer "
                "reachable from the current state"
            )
        except Exception:
            logger.exception("watchdog failed to update runtime state")
