from __future__ import annotations

import asyncio

import pytest

from edgeguard.core.exceptions import EdgeGuardError
from edgeguard.resilience.timeout import OperationTimeoutError, with_timeout


async def test_fast_operation_completes_within_timeout() -> None:
    async def op() -> str:
        return "done"

    result = await with_timeout(op, 1.0)
    assert result == "done"


async def test_slow_operation_raises_operation_timeout_error() -> None:
    async def op() -> str:
        await asyncio.sleep(10)
        return "too slow"

    with pytest.raises(OperationTimeoutError):
        await with_timeout(op, 0.01)


async def test_operation_timeout_error_is_an_edgeguard_error() -> None:
    async def op() -> str:
        await asyncio.sleep(10)
        return "too slow"

    with pytest.raises(EdgeGuardError):
        await with_timeout(op, 0.01)


async def test_operation_timeout_error_is_also_a_timeout_error() -> None:
    async def op() -> str:
        await asyncio.sleep(10)
        return "too slow"

    with pytest.raises(TimeoutError):
        await with_timeout(op, 0.01)


async def test_none_seconds_disables_the_timeout() -> None:
    async def op() -> str:
        await asyncio.sleep(0.02)
        return "eventually done"

    result = await with_timeout(op, None)
    assert result == "eventually done"


async def test_function_exceptions_propagate_unchanged() -> None:
    async def op() -> str:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await with_timeout(op, 1.0)
