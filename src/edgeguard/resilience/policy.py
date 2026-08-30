"""Composes timeout, retry, and circuit breaker into a single decorator.

``EdgeGuard.reliable()`` is the primary developer-facing API for making an
async function resilient; it delegates to :func:`build_reliable_decorator`.
Each concern -- timeout, retry/backoff, circuit breaking -- is implemented
and tested independently (see ``timeout.py``, ``retry.py``,
``circuit_breaker.py``); this module only wires them together and reports
what happened onto the shared event bus, so it never needs to know how
those events get used (metrics, diagnostics timeline, ...) -- keeping the
composition decoupled from observability, per the project's design goals.

Composition order, outermost to innermost, for a single decorated call::

    circuit_breaker -> retry (with backoff) -> per-attempt timeout -> function

The circuit breaker wraps the *whole* retried operation, not each
individual attempt: a breaker trip means "this operation, even after
retrying, isn't succeeding" rather than "one attempt failed". This keeps a
single flaky call from tripping the breaker by itself, and means a call
rejected by an open breaker never enters the retry loop or waits out a
backoff delay it has no chance of needing.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from edgeguard.core.events import Event, EventBus, Severity
from edgeguard.resilience.backoff import BackoffAlgorithm
from edgeguard.resilience.circuit_breaker import CircuitBreaker
from edgeguard.resilience.retry import RetryAttempt, RetryPolicy
from edgeguard.resilience.timeout import with_timeout

logger = logging.getLogger("edgeguard.resilience")

P = ParamSpec("P")
T = TypeVar("T")


def build_reliable_decorator(
    *,
    retries: int = 3,
    backoff: BackoffAlgorithm = "exponential",
    initial_delay: float = 1.0,
    max_delay: float = 300.0,
    jitter: bool = True,
    retry_on: tuple[type[Exception], ...] = (Exception,),
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
    circuit_breaker: bool | CircuitBreaker = False,
    name: str | None = None,
    events: EventBus | None = None,
    component: str = "resilience",
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Build the decorator returned by ``EdgeGuard.reliable(...)``.

    See :meth:`edgeguard.core.runtime.EdgeGuard.reliable` for the parameter
    documentation -- this function has the same signature plus ``events``
    and ``component``, which the runtime supplies automatically.
    """
    retry_policy = (
        retry
        if retry is not None
        else RetryPolicy(
            max_attempts=retries,
            backoff=backoff,
            initial_delay=initial_delay,
            max_delay=max_delay,
            jitter=jitter,
            retry_on=retry_on,
        )
    )

    breaker: CircuitBreaker | None
    if circuit_breaker is True:
        breaker = CircuitBreaker(name=name or "reliable")
    elif isinstance(circuit_breaker, CircuitBreaker):
        breaker = circuit_breaker
    else:
        breaker = None

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        op_name = name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            async def attempt() -> T:
                return await with_timeout(lambda: func(*args, **kwargs), timeout)

            async def on_retry(info: RetryAttempt) -> None:
                logger.warning(
                    "retrying %s (attempt %d, next in %.2fs): %s",
                    op_name,
                    info.attempt,
                    info.delay,
                    info.exception,
                )
                if events is not None:
                    await events.publish(
                        Event(
                            type="retry_attempt",
                            component=component,
                            severity=Severity.WARNING,
                            metadata={
                                "operation": op_name,
                                "attempt": info.attempt,
                                "delay": info.delay,
                                "error": str(info.exception),
                            },
                        )
                    )

            async def run_with_retry() -> T:
                return await retry_policy.run(attempt, on_retry=on_retry)

            try:
                result = await (breaker.call(run_with_retry) if breaker else run_with_retry())
            except Exception as exc:
                if events is not None:
                    await events.publish(
                        Event(
                            type="operation_failed",
                            component=component,
                            severity=Severity.ERROR,
                            metadata={"operation": op_name, "error": str(exc)},
                        )
                    )
                raise
            else:
                if events is not None:
                    await events.publish(
                        Event(
                            type="operation_succeeded",
                            component=component,
                            metadata={"operation": op_name},
                        )
                    )
                return result

        return wrapper

    return decorator
