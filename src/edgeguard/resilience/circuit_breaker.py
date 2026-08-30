"""Circuit breaker: stop calling a dependency that's already failing.

::

    CLOSED  --  failure_threshold consecutive failures  -->  OPEN
    OPEN    --  recovery_timeout elapses                -->  HALF_OPEN
    HALF_OPEN -- trial call succeeds                     -->  CLOSED
    HALF_OPEN -- trial call fails                        -->  OPEN

While OPEN, calls are rejected immediately with
:class:`CircuitBreakerOpenError` instead of being attempted -- this is what
protects an already-struggling dependency (and the caller's own thread/task
budget) from being hammered by callers that would otherwise retry forever.
While HALF_OPEN, only one trial call is let through at a time; any other
caller that arrives while the trial is in flight is rejected the same way,
so a recovering dependency isn't immediately hit with a burst of concurrent
traffic the moment its timeout expires.
"""

from __future__ import annotations

import asyncio
import enum
import time
from collections.abc import Awaitable, Callable
from typing import Protocol, TypeVar

from edgeguard.core.exceptions import EdgeGuardError

T = TypeVar("T")


class CircuitState(enum.Enum):
    """States of a :class:`CircuitBreaker`."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(EdgeGuardError):
    """Raised when a call is rejected because the circuit is open."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"circuit breaker {name!r} is open")


class CircuitBreakerStore(Protocol):
    """Persistence hook a :class:`CircuitBreaker` can be given.

    Satisfied structurally by :class:`edgeguard.persistence.database.Database`
    -- no import of it is needed here.
    """

    async def save_circuit_breaker_state(
        self, name: str, *, state: str, failure_count: int, opened_at: float | None
    ) -> None: ...

    async def load_circuit_breaker_state(
        self, name: str
    ) -> tuple[str, int, float | None] | None: ...


class CircuitBreaker:
    """Concurrency-safe circuit breaker with optional cross-restart persistence.

    Args:
        failure_threshold: Consecutive failures (while CLOSED) that trip the
            breaker to OPEN.
        recovery_timeout: Seconds to wait after tripping before allowing a
            single trial call through (transitioning to HALF_OPEN).
        name: Identifier used for persistence and error messages. Only needs
            to be unique if you use more than one breaker with a shared
            ``store``.
        store: Optional persistence backend (e.g. ``guard.database``). If
            given, state survives process restarts -- see :meth:`restore`.
            The stored ``opened_at`` is wall-clock time, so the remaining
            recovery window is reconstructed approximately from the system
            clock on restore; this assumes the clock hasn't jumped
            significantly, which is a real edge-device failure mode this
            does not attempt to fully solve.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        *,
        name: str = "default",
        store: CircuitBreakerStore | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")
        if recovery_timeout < 0:
            raise ValueError(f"recovery_timeout must be >= 0, got {recovery_timeout}")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._store = store

        self._lock = asyncio.Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        # Only meaningful while _state is OPEN; a plain float (rather than
        # float | None) avoids an Optional-narrowing dance at every read,
        # since the invariant "OPEN implies this was just set" is maintained
        # entirely within this class.
        self._opened_at_monotonic: float = 0.0
        self._opened_at_wallclock: float | None = None
        self._half_open_in_flight = False

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    async def restore(self) -> None:
        """Load persisted state from ``store``, if configured.

        Call once after construction, before the breaker handles any calls.
        No-op if no ``store`` was given or nothing has been persisted yet.
        """
        if self._store is None:
            return
        record = await self._store.load_circuit_breaker_state(self.name)
        if record is None:
            return
        state_value, failure_count, opened_at_wallclock = record
        async with self._lock:
            self._failure_count = failure_count
            if state_value == CircuitState.OPEN.value and opened_at_wallclock is not None:
                elapsed = time.time() - opened_at_wallclock
                if elapsed >= self.recovery_timeout:
                    # The recovery window already passed while we were down;
                    # come back ready for a single trial call.
                    self._state = CircuitState.HALF_OPEN
                else:
                    self._state = CircuitState.OPEN
                    self._opened_at_monotonic = time.monotonic() - elapsed
                    self._opened_at_wallclock = opened_at_wallclock

    async def call(self, func: Callable[[], Awaitable[T]]) -> T:
        """Call ``func`` (a zero-argument async callable) through the breaker.

        Raises:
            CircuitBreakerOpenError: if the circuit is open (or half-open
                with a trial call already in flight) -- ``func`` is not
                invoked in that case.
        """
        await self._before_call()
        try:
            result = await func()
        except Exception:
            await self._on_failure()
            raise
        else:
            await self._on_success()
            return result

    async def _before_call(self) -> None:
        async with self._lock:
            if self._state is CircuitState.CLOSED:
                return
            if self._state is CircuitState.OPEN:
                if time.monotonic() - self._opened_at_monotonic < self.recovery_timeout:
                    raise CircuitBreakerOpenError(self.name)
                self._state = CircuitState.HALF_OPEN
                self._half_open_in_flight = True
                return
            # HALF_OPEN: allow exactly one trial call through at a time.
            if self._half_open_in_flight:
                raise CircuitBreakerOpenError(self.name)
            self._half_open_in_flight = True

    async def _on_success(self) -> None:
        async with self._lock:
            self._failure_count = 0
            self._half_open_in_flight = False
            self._state = CircuitState.CLOSED
            self._opened_at_monotonic = 0.0
            self._opened_at_wallclock = None
            await self._persist()

    async def _on_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            was_half_open = self._state is CircuitState.HALF_OPEN
            self._half_open_in_flight = False
            if was_half_open or self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at_monotonic = time.monotonic()
                self._opened_at_wallclock = time.time()
            await self._persist()

    async def _persist(self) -> None:
        if self._store is None:
            return
        await self._store.save_circuit_breaker_state(
            self.name,
            state=self._state.value,
            failure_count=self._failure_count,
            opened_at=self._opened_at_wallclock,
        )
