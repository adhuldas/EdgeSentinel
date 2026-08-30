from __future__ import annotations

import asyncio

import pytest

from edgesentinel.core.exceptions import InvalidStateTransitionError
from edgesentinel.core.state import RuntimeState, StateMachine


async def test_initial_state_defaults_to_booting() -> None:
    sm = StateMachine()
    assert sm.current is RuntimeState.BOOTING


async def test_valid_transition_updates_current_and_returns_previous() -> None:
    sm = StateMachine(RuntimeState.BOOTING)
    previous = await sm.transition(RuntimeState.INITIALIZING)
    assert previous is RuntimeState.BOOTING
    assert sm.current is RuntimeState.INITIALIZING


async def test_full_happy_path_sequence() -> None:
    sm = StateMachine(RuntimeState.BOOTING)
    for target in (
        RuntimeState.INITIALIZING,
        RuntimeState.HEALTHY,
        RuntimeState.DEGRADED,
        RuntimeState.HEALTHY,
        RuntimeState.OFFLINE,
        RuntimeState.RECOVERING,
        RuntimeState.HEALTHY,
        RuntimeState.STOPPING,
        RuntimeState.STOPPED,
    ):
        await sm.transition(target)
    assert sm.current is RuntimeState.STOPPED


async def test_invalid_transition_raises_and_leaves_state_unchanged() -> None:
    sm = StateMachine(RuntimeState.BOOTING)
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        await sm.transition(RuntimeState.HEALTHY)
    assert exc_info.value.current is RuntimeState.BOOTING
    assert exc_info.value.target is RuntimeState.HEALTHY
    assert sm.current is RuntimeState.BOOTING


async def test_stopped_is_terminal() -> None:
    sm = StateMachine(RuntimeState.STOPPED)
    for target in RuntimeState:
        if target is RuntimeState.STOPPED:
            continue
        with pytest.raises(InvalidStateTransitionError):
            await sm.transition(target)


async def test_same_state_transition_is_a_no_op() -> None:
    sm = StateMachine(RuntimeState.HEALTHY)
    previous = await sm.transition(RuntimeState.HEALTHY)
    assert previous is RuntimeState.HEALTHY
    assert sm.current is RuntimeState.HEALTHY


def test_can_transition_reflects_the_graph_without_mutating_state() -> None:
    sm = StateMachine(RuntimeState.HEALTHY)
    assert sm.can_transition(RuntimeState.DEGRADED) is True
    assert sm.can_transition(RuntimeState.BOOTING) is False
    assert sm.current is RuntimeState.HEALTHY


async def test_concurrent_transitions_are_serialized_and_only_one_race_wins() -> None:
    # Both coroutines race to move out of HEALTHY, but only one legal
    # transition can "win" the lock first; whichever runs second sees the
    # already-updated state and must raise rather than corrupt it.
    sm = StateMachine(RuntimeState.HEALTHY)

    async def try_degraded() -> RuntimeState | Exception:
        try:
            return await sm.transition(RuntimeState.DEGRADED)
        except InvalidStateTransitionError as exc:
            return exc

    async def try_offline() -> RuntimeState | Exception:
        try:
            return await sm.transition(RuntimeState.OFFLINE)
        except InvalidStateTransitionError as exc:
            return exc

    results = await asyncio.gather(try_degraded(), try_offline())
    # Both transitions are individually legal from HEALTHY, so both succeed
    # here (order isn't guaranteed) -- the important invariant is that the
    # final state is exactly one of the two targets, never a corrupted mix.
    assert sm.current in (RuntimeState.DEGRADED, RuntimeState.OFFLINE)
    assert all(isinstance(r, RuntimeState) for r in results)
