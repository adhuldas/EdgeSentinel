"""Supervises a long-running asyncio task, restarting it with backoff.

edgesentinel doesn't supervise external OS processes -- that's what `systemd`
(or your container runtime) is for; see the README's "who should use this"
section. ``Supervisor`` restarts a coroutine *within* the same process: the
internal tasks an application relies on staying alive (a serial reader, an
MQTT client loop, a sensor poller). If it keeps crashing in a tight loop,
restarting forever just hides a task that will never recover behind an
endless stream of harmless-looking restart events, so the supervisor
counts crashes in a sliding window and gives up -- reporting itself
``crashed`` and, if wired to the runtime, escalating to ``FAILED`` --
instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.core.exceptions import InvalidStateTransitionError
from edgesentinel.core.state import RuntimeState
from edgesentinel.resilience.backoff import BackoffAlgorithm, compute_delay

logger = logging.getLogger("edgesentinel.process")

TaskFunc = Callable[[], Awaitable[None]]
SetState = Callable[[RuntimeState], Awaitable[None]]
GetState = Callable[[], RuntimeState]
Sleep = Callable[[float], Awaitable[None]]
Now = Callable[[], float]

#: States a crashed supervisor is willing to escalate out of. A supervisor
#: must never fight the runtime's own shutdown sequence.
_UNSAFE_TO_ESCALATE = frozenset({RuntimeState.STOPPING, RuntimeState.STOPPED})


class Supervisor:
    """Restarts ``func`` with backoff whenever it exits or raises.

    Example:
        >>> supervisor = Supervisor(read_serial_loop, name="serial-reader")
        >>> await supervisor.start()
        >>> ...
        >>> await supervisor.stop()

    Args:
        func: Zero-argument async callable representing the supervised
            task's main loop. It's expected to run until cancelled --
            returning normally is treated the same as raising, since an
            unexpected return means the task stopped doing its job just as
            much as a crash does.
        name: Identifier for this task in events and logs.
        max_crashes: Number of crashes allowed within ``window`` seconds
            before the supervisor gives up and reports itself ``crashed``
            instead of restarting again.
        window: Sliding time window, in seconds, crashes are counted over.
        backoff, initial_delay, max_delay, jitter: Passed to
            :func:`edgesentinel.resilience.backoff.compute_delay` for the
            delay before each restart.
        events / component: Where crash/restart/exhaustion events are
            published.
        set_state / get_state: Optional hooks letting a crashed supervisor
            escalate the runtime to ``FAILED``. Both or neither.
        sleep / now: Injectable clock, for deterministic tests.

    Raises:
        ValueError: if ``max_crashes`` or ``window`` is not positive, or if
            only one of ``set_state``/``get_state`` is given.
    """

    def __init__(
        self,
        func: TaskFunc,
        *,
        name: str,
        max_crashes: int = 3,
        window: float = 60.0,
        backoff: BackoffAlgorithm = "exponential",
        initial_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: bool = True,
        events: EventBus | None = None,
        component: str = "process",
        set_state: SetState | None = None,
        get_state: GetState | None = None,
        sleep: Sleep = asyncio.sleep,
        now: Now = time.monotonic,
    ) -> None:
        if max_crashes < 1:
            raise ValueError(f"max_crashes must be >= 1, got {max_crashes}")
        if window <= 0:
            raise ValueError(f"window must be > 0, got {window}")
        if (set_state is None) != (get_state is None):
            raise ValueError("set_state and get_state must be given together, or neither")
        self._func = func
        self._name = name
        self._max_crashes = max_crashes
        self._window = window
        self._backoff = backoff
        self._initial_delay = initial_delay
        self._max_delay = max_delay
        self._jitter = jitter
        self._events = events
        self._component = component
        self._set_state = set_state
        self._get_state = get_state
        self._sleep = sleep
        self._now = now
        self._failure_times: deque[float] = deque()
        self._crashed = False
        self._task: asyncio.Task[None] | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_running(self) -> bool:
        return self._task is not None

    @property
    def is_crashed(self) -> bool:
        """Whether the crash-loop threshold was exceeded and the supervisor
        gave up restarting. Cleared the next time :meth:`start` is called."""
        return self._crashed

    async def start(self) -> None:
        """Start supervising. Safe to call more than once; a no-op while
        already running."""
        if self._task is not None:
            return
        self._crashed = False
        self._failure_times.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop supervising. Safe to call more than once, or if never
        started, or after the supervisor already gave up on its own."""
        if self._task is None:
            return
        task, self._task = self._task, None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        attempt = 0
        while True:
            attempt += 1
            exc: Exception | None = None
            try:
                await self._func()
            except asyncio.CancelledError:
                raise
            except Exception as caught:
                exc = caught
            if self._events is not None:
                await self._events.publish(
                    Event(
                        type="task_crashed" if exc is not None else "task_exited",
                        component=self._component,
                        severity=Severity.ERROR if exc is not None else Severity.WARNING,
                        metadata={
                            "task": self._name,
                            "attempt": attempt,
                            "error": str(exc) if exc is not None else None,
                        },
                    )
                )
            if await self._register_failure():
                return
            delay = compute_delay(
                self._backoff,
                attempt,
                initial_delay=self._initial_delay,
                max_delay=self._max_delay,
                jitter=self._jitter,
            )
            if self._events is not None:
                await self._events.publish(
                    Event(
                        type="task_restarting",
                        component=self._component,
                        metadata={"task": self._name, "attempt": attempt, "delay": delay},
                    )
                )
            await self._sleep(delay)

    async def _register_failure(self) -> bool:
        """Record a failure, returning whether the crash-loop threshold was
        just exceeded (the caller should stop restarting)."""
        now = self._now()
        self._failure_times.append(now)
        while self._failure_times and now - self._failure_times[0] > self._window:
            self._failure_times.popleft()
        if len(self._failure_times) < self._max_crashes:
            return False
        logger.error(
            "supervised task %r crashed %d times within %gs; giving up",
            self._name,
            len(self._failure_times),
            self._window,
        )
        if self._events is not None:
            await self._events.publish(
                Event(
                    type="task_crash_loop_detected",
                    component=self._component,
                    severity=Severity.CRITICAL,
                    metadata={
                        "task": self._name,
                        "crashes": len(self._failure_times),
                        "window": self._window,
                    },
                )
            )
        if self._set_state is not None:
            await self._escalate()
        # Set together, right before returning, so a caller observing
        # is_crashed become True never sees a stale is_running still True --
        # escalation has already run by the time either is visible.
        self._crashed = True
        self._task = None
        return True

    async def _escalate(self) -> None:
        assert self._set_state is not None
        assert self._get_state is not None
        if self._get_state() in _UNSAFE_TO_ESCALATE:
            return
        try:
            await self._set_state(RuntimeState.FAILED)
        except InvalidStateTransitionError:
            logger.debug(
                "supervisor for %r could not move runtime to FAILED: no "
                "longer reachable from the current state",
                self._name,
            )
        except Exception:
            logger.exception("supervisor for %r failed to update runtime state", self._name)
