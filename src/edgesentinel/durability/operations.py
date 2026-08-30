"""Wires the intent journal to a decorator and to startup replay.

``EdgeSentinel.durable()`` is the primary developer-facing API; it delegates to
:func:`build_durable_decorator`. Every durable function's *bound arguments*
(not its return value -- there's no caller left to hand it to after a
crash) are journaled before the function runs, so
:func:`replay_pending` can call the exact same function again with the
exact same arguments the next time the runtime starts, without the
application registering anything beyond the ``@guard.durable(...)``
decorator itself.
"""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from typing import Any, TypeVar

from edgesentinel.core.events import Event, EventBus, Severity
from edgesentinel.core.exceptions import (
    DurableOperationExhaustedError,
    InvalidDurablePayloadError,
    RuntimeNotStartedError,
)
from edgesentinel.durability.journal import Intent, IntentJournal

logger = logging.getLogger("edgesentinel.durability")

T = TypeVar("T")

DurableFunc = Callable[..., Awaitable[T]]
ReplayHandler = Callable[[Intent], Awaitable[None]]


def _reject_variadic_parameters(func: DurableFunc[Any], operation: str) -> None:
    for param in inspect.signature(func).parameters.values():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise InvalidDurablePayloadError(
                f"durable operation {operation!r}: function {func.__name__!r} must not "
                f"declare *args or **kwargs -- every parameter needs a fixed name so its "
                f"bound arguments can be journaled and replayed by name (found "
                f"{param.kind.description} parameter {param.name!r})"
            )


def build_durable_decorator(
    *,
    operation: str,
    journal: IntentJournal,
    registry: MutableMapping[str, ReplayHandler],
    max_attempts: int | None = None,
    events: EventBus | None = None,
    component: str = "durability",
    is_started: Callable[[], bool] | None = None,
) -> Callable[[DurableFunc[T]], DurableFunc[T]]:
    """Build the decorator returned by ``EdgeSentinel.durable(operation, ...)``.

    See :meth:`edgesentinel.core.runtime.EdgeSentinel.durable` for parameter
    documentation. ``registry`` receives this operation's replay handler as
    a side effect of decoration -- :func:`replay_pending` uses it to find
    the right function for each unfinished intent at startup.

    ``is_started``, if given, is checked on every *call* (not decoration) of
    the wrapped function; unlike ``reliable()``, a durable call must write
    to the journal before it can run at all, so it has no meaningful
    behavior before the runtime has opened its database connection.
    """
    if max_attempts is not None and max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")

    def decorator(func: DurableFunc[T]) -> DurableFunc[T]:
        if operation in registry:
            raise ValueError(f"a durable operation named {operation!r} is already registered")
        _reject_variadic_parameters(func, operation)
        signature = inspect.signature(func)

        async def execute(intent: Intent) -> T:
            updated = await journal.mark_in_progress(intent.id)
            if events is not None:
                await events.publish(
                    Event(
                        type="durable_operation_started",
                        component=component,
                        metadata={
                            "operation": operation,
                            "intent_id": intent.id,
                            "attempt": updated.attempts,
                        },
                    )
                )
            try:
                result = await func(**intent.payload)
            except Exception as exc:
                exhausted = max_attempts is not None and updated.attempts >= max_attempts
                if exhausted:
                    await journal.mark_failed(intent.id, str(exc))
                else:
                    await journal.mark_pending_for_retry(intent.id, str(exc))
                if events is not None:
                    await events.publish(
                        Event(
                            type=(
                                "durable_operation_exhausted"
                                if exhausted
                                else "durable_operation_retry_pending"
                            ),
                            component=component,
                            severity=Severity.ERROR if exhausted else Severity.WARNING,
                            metadata={
                                "operation": operation,
                                "intent_id": intent.id,
                                "attempt": updated.attempts,
                                "error": str(exc),
                            },
                        )
                    )
                if exhausted:
                    raise DurableOperationExhaustedError(
                        operation, intent.id, updated.attempts
                    ) from exc
                raise
            else:
                await journal.mark_completed(intent.id)
                if events is not None:
                    await events.publish(
                        Event(
                            type="durable_operation_completed",
                            component=component,
                            metadata={"operation": operation, "intent_id": intent.id},
                        )
                    )
                return result

        async def replay(intent: Intent) -> None:
            await execute(intent)

        registry[operation] = replay

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            if is_started is not None and not is_started():
                raise RuntimeNotStartedError(
                    f"durable operation {operation!r} cannot be called before the "
                    f"runtime has started -- call guard.start() first"
                )
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            intent = await journal.record(operation, dict(bound.arguments))
            return await execute(intent)

        return wrapper

    return decorator


async def replay_pending(
    journal: IntentJournal,
    registry: Mapping[str, ReplayHandler],
    *,
    events: EventBus | None = None,
    component: str = "durability",
) -> None:
    """Replay every intent left ``PENDING``/``IN_PROGRESS`` by a previous run.

    Called once during ``EdgeSentinel.start()``, before the runtime is
    considered healthy. Each intent is replayed independently: one
    intent's failure (including exhausting ``max_attempts``) is logged and
    reported as an event but never stops the runtime from starting or the
    remaining intents from being replayed -- a stuck operation from a
    previous run must not be able to brick the device on its next boot.
    """
    for intent in await journal.pending():
        handler = registry.get(intent.operation)
        if handler is None:
            logger.warning(
                "no durable operation registered for %r; intent %s left pending "
                "(register the operation with @guard.durable(...) before calling "
                "guard.start() for it to be replayed)",
                intent.operation,
                intent.id,
            )
            if events is not None:
                await events.publish(
                    Event(
                        type="durable_operation_unhandled",
                        component=component,
                        severity=Severity.WARNING,
                        metadata={"operation": intent.operation, "intent_id": intent.id},
                    )
                )
            continue
        try:
            await handler(intent)
        except Exception:
            # execute() already journaled the failure and published an
            # event for it -- only keep replaying the *other* intents here.
            logger.info(
                "replay of intent %s (%r) did not complete on this attempt",
                intent.id,
                intent.operation,
            )
