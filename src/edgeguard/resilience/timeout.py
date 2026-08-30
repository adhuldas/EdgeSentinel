"""Per-call timeout enforcement.

A thin wrapper over :func:`asyncio.timeout` that raises an edgeguard-specific
exception instead of a bare :class:`TimeoutError`, so callers can catch
timeouts alongside other edgeguard failures via
:class:`~edgeguard.core.exceptions.EdgeGuardError` while still being able to
catch it as a plain :class:`TimeoutError` if that's more convenient.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from edgeguard.core.exceptions import EdgeGuardError

T = TypeVar("T")


class OperationTimeoutError(EdgeGuardError, TimeoutError):
    """Raised when an operation exceeds its configured timeout."""


async def with_timeout(func: Callable[[], Awaitable[T]], seconds: float | None) -> T:
    """Run ``func`` (a zero-argument async callable), bounding it to ``seconds``.

    ``seconds=None`` disables the timeout entirely and just awaits ``func()``.

    Raises:
        OperationTimeoutError: if ``func`` does not complete within ``seconds``.
    """
    if seconds is None:
        return await func()
    try:
        async with asyncio.timeout(seconds):
            return await func()
    except TimeoutError as exc:
        raise OperationTimeoutError(f"operation timed out after {seconds}s") from exc
