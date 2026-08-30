from __future__ import annotations

import pytest

from edgeguard.core.events import Event, EventBus
from edgeguard.core.exceptions import (
    DurableOperationExhaustedError,
    InvalidDurablePayloadError,
    RuntimeNotStartedError,
)
from edgeguard.durability.journal import Intent, IntentJournal, IntentStatus
from edgeguard.durability.operations import ReplayHandler, build_durable_decorator, replay_pending
from edgeguard.persistence.database import Database


class FlakyError(Exception):
    pass


@pytest.fixture
def journal(database: Database) -> IntentJournal:
    return IntentJournal(database)


@pytest.fixture
def registry() -> dict[str, ReplayHandler]:
    return {}


async def _all_intents(journal: IntentJournal) -> list[Intent]:
    # Test-only helper covering every status, not just pending()'s
    # PENDING/IN_PROGRESS filter -- production code never needs "all of
    # them regardless of status", so this reaches into the journal's
    # backing store directly rather than adding a method to it.
    from edgeguard.durability.journal import _row_to_intent

    rows = await journal._store.load_intents_by_status(
        tuple(status.value for status in IntentStatus)
    )
    return [_row_to_intent(row) for row in rows]


async def test_successful_call_completes_the_intent(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    @decorator
    async def op(value: int) -> int:
        return value * 2

    assert await op(21) == 42

    [intent] = await _all_intents(journal)
    assert intent.status is IntentStatus.COMPLETED
    assert intent.attempts == 1


async def test_positional_and_keyword_arguments_are_journaled_by_name(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)
    seen: dict[str, object] = {}

    @decorator
    async def op(sensor_id: str, value: float) -> None:
        seen["sensor_id"] = sensor_id
        seen["value"] = value

    await op("temp-1", value=21.5)

    [intent] = await _all_intents(journal)
    assert intent.payload == {"sensor_id": "temp-1", "value": 21.5}
    assert seen == {"sensor_id": "temp-1", "value": 21.5}


async def test_decorator_rejects_functions_with_var_positional_parameters(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    with pytest.raises(InvalidDurablePayloadError):

        @decorator
        async def op(*args: int) -> None:
            pass


async def test_decorator_rejects_functions_with_var_keyword_parameters(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    with pytest.raises(InvalidDurablePayloadError):

        @decorator
        async def op(**kwargs: int) -> None:
            pass


async def test_duplicate_operation_name_is_rejected(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    first_decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    @first_decorator
    async def first() -> None:
        pass

    second_decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    with pytest.raises(ValueError, match="already registered"):

        @second_decorator
        async def second() -> None:
            pass


async def test_is_started_false_rejects_calls_without_touching_the_journal(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(
        operation="op", journal=journal, registry=registry, is_started=lambda: False
    )

    @decorator
    async def op() -> None:
        pass

    with pytest.raises(RuntimeNotStartedError):
        await op()

    assert await _all_intents(journal) == []


async def test_is_started_true_allows_calls(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(
        operation="op", journal=journal, registry=registry, is_started=lambda: True
    )

    @decorator
    async def op() -> str:
        return "ok"

    assert await op() == "ok"


async def test_max_attempts_below_one_is_rejected(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    with pytest.raises(ValueError):
        build_durable_decorator(operation="op", journal=journal, registry=registry, max_attempts=0)


async def test_failed_call_with_attempts_remaining_stays_pending_and_reraises(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    @decorator
    async def op() -> None:
        raise FlakyError("boom")

    with pytest.raises(FlakyError):
        await op()

    [intent] = await _all_intents(journal)
    assert intent.status is IntentStatus.PENDING
    assert intent.attempts == 1
    assert intent.last_error == "boom"


async def test_max_attempts_exhausted_marks_failed_and_raises_exhausted_error(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    decorator = build_durable_decorator(
        operation="op", journal=journal, registry=registry, max_attempts=1
    )

    @decorator
    async def op() -> None:
        raise FlakyError("boom")

    with pytest.raises(DurableOperationExhaustedError):
        await op()

    [intent] = await _all_intents(journal)
    assert intent.status is IntentStatus.FAILED
    assert intent.attempts == 1


async def test_events_published_for_start_and_success(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    bus = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    bus.subscribe(collect)
    decorator = build_durable_decorator(
        operation="op", journal=journal, registry=registry, events=bus, component="test"
    )

    @decorator
    async def op() -> str:
        return "ok"

    assert await op() == "ok"

    assert [e.type for e in seen] == ["durable_operation_started", "durable_operation_completed"]
    assert all(e.component == "test" for e in seen)


async def test_events_published_when_exhausted(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    bus = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    bus.subscribe(collect)
    decorator = build_durable_decorator(
        operation="op",
        journal=journal,
        registry=registry,
        max_attempts=1,
        events=bus,
        component="test",
    )

    @decorator
    async def op() -> None:
        raise FlakyError("boom")

    with pytest.raises(DurableOperationExhaustedError):
        await op()

    assert [e.type for e in seen] == ["durable_operation_started", "durable_operation_exhausted"]


async def test_replay_calls_the_registered_handler_for_a_pending_intent(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    calls = 0
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    @decorator
    async def op(value: int) -> None:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise FlakyError("first attempt fails")

    with pytest.raises(FlakyError):
        await op(7)
    assert calls == 1

    await replay_pending(journal, registry)
    assert calls == 2

    [intent] = await _all_intents(journal)
    assert intent.status is IntentStatus.COMPLETED
    assert intent.attempts == 2


async def test_replay_skips_completed_intents(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    calls = 0
    decorator = build_durable_decorator(operation="op", journal=journal, registry=registry)

    @decorator
    async def op() -> None:
        nonlocal calls
        calls += 1

    await op()
    assert calls == 1

    await replay_pending(journal, registry)
    assert calls == 1  # already completed -- must not run again


async def test_replay_of_unregistered_operation_leaves_it_pending_and_publishes_event(
    journal: IntentJournal,
) -> None:
    bus = EventBus()
    seen: list[Event] = []

    async def collect(event: Event) -> None:
        seen.append(event)

    bus.subscribe(collect)

    intent = await journal.record("orphaned_operation", {})

    await replay_pending(journal, {}, events=bus, component="test")

    reloaded = await journal.get(intent.id)
    assert reloaded is not None
    assert reloaded.status is IntentStatus.PENDING
    assert [e.type for e in seen] == ["durable_operation_unhandled"]


async def test_replay_of_one_failing_intent_does_not_block_the_next(
    journal: IntentJournal, registry: dict[str, ReplayHandler]
) -> None:
    calls: list[str] = []

    failing_decorator = build_durable_decorator(
        operation="failing", journal=journal, registry=registry
    )
    ok_decorator = build_durable_decorator(operation="ok", journal=journal, registry=registry)

    @failing_decorator
    async def failing() -> None:
        calls.append("failing")
        raise FlakyError("always fails")

    @ok_decorator
    async def ok() -> None:
        calls.append("ok")

    with pytest.raises(FlakyError):
        await failing()
    await ok()  # completed already, recorded before replay runs

    another_ok_intent = await journal.record("ok", {})
    await journal.mark_in_progress(another_ok_intent.id)  # simulate crash mid-flight

    calls.clear()
    await replay_pending(journal, registry)

    # Both the still-pending "failing" intent and the crashed-mid-flight
    # "ok" intent get replayed -- one failing again must not stop the other.
    assert set(calls) == {"failing", "ok"}
