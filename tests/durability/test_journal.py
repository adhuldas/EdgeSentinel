from __future__ import annotations

import datetime as dt

import pytest

from edgesentinel.core.exceptions import InvalidDurablePayloadError
from edgesentinel.durability.journal import IntentJournal, IntentStatus
from edgesentinel.persistence.database import Database


@pytest.fixture
def journal(database: Database) -> IntentJournal:
    return IntentJournal(database)


async def test_record_creates_a_pending_intent_with_zero_attempts(journal: IntentJournal) -> None:
    intent = await journal.record("send_reading", {"sensor_id": "temp-1", "value": 21.5})

    assert intent.operation == "send_reading"
    assert intent.payload == {"sensor_id": "temp-1", "value": 21.5}
    assert intent.status is IntentStatus.PENDING
    assert intent.attempts == 0
    assert intent.last_error is None


async def test_record_rejects_a_non_json_serializable_payload(journal: IntentJournal) -> None:
    with pytest.raises(InvalidDurablePayloadError):
        await journal.record("send_reading", {"when": dt.datetime.now()})


async def test_get_returns_none_for_an_unknown_intent(journal: IntentJournal) -> None:
    assert await journal.get("does-not-exist") is None


async def test_mark_in_progress_increments_attempts_and_sets_status(
    journal: IntentJournal,
) -> None:
    intent = await journal.record("op", {})

    updated = await journal.mark_in_progress(intent.id)
    assert updated.status is IntentStatus.IN_PROGRESS
    assert updated.attempts == 1

    updated_again = await journal.mark_in_progress(intent.id)
    assert updated_again.attempts == 2


async def test_mark_completed_is_reflected_on_reload(journal: IntentJournal) -> None:
    intent = await journal.record("op", {})
    await journal.mark_in_progress(intent.id)

    await journal.mark_completed(intent.id)

    reloaded = await journal.get(intent.id)
    assert reloaded is not None
    assert reloaded.status is IntentStatus.COMPLETED
    assert reloaded.last_error is None


async def test_mark_pending_for_retry_keeps_attempts_and_records_the_error(
    journal: IntentJournal,
) -> None:
    intent = await journal.record("op", {})
    await journal.mark_in_progress(intent.id)

    await journal.mark_pending_for_retry(intent.id, "connection reset")

    reloaded = await journal.get(intent.id)
    assert reloaded is not None
    assert reloaded.status is IntentStatus.PENDING
    assert reloaded.attempts == 1
    assert reloaded.last_error == "connection reset"


async def test_mark_failed_is_reflected_on_reload(journal: IntentJournal) -> None:
    intent = await journal.record("op", {})
    await journal.mark_in_progress(intent.id)

    await journal.mark_failed(intent.id, "gave up")

    reloaded = await journal.get(intent.id)
    assert reloaded is not None
    assert reloaded.status is IntentStatus.FAILED
    assert reloaded.last_error == "gave up"


async def test_pending_returns_pending_and_in_progress_oldest_first(
    journal: IntentJournal,
) -> None:
    first = await journal.record("op", {"n": 1})
    second = await journal.record("op", {"n": 2})
    await journal.mark_in_progress(second.id)
    third = await journal.record("op", {"n": 3})
    await journal.mark_in_progress(third.id)
    await journal.mark_completed(third.id)

    pending = await journal.pending()

    assert [intent.id for intent in pending] == [first.id, second.id]


async def test_prune_completed_deletes_old_completed_intents(journal: IntentJournal) -> None:
    intent = await journal.record("op", {})
    await journal.mark_in_progress(intent.id)
    await journal.mark_completed(intent.id)

    deleted = await journal.prune_completed(dt.timedelta(seconds=-1))

    assert deleted == 1
    assert await journal.get(intent.id) is None


async def test_prune_completed_leaves_recent_completed_intents(journal: IntentJournal) -> None:
    intent = await journal.record("op", {})
    await journal.mark_in_progress(intent.id)
    await journal.mark_completed(intent.id)

    deleted = await journal.prune_completed(dt.timedelta(days=1))

    assert deleted == 0
    assert await journal.get(intent.id) is not None


async def test_marking_an_unknown_intent_raises_key_error(journal: IntentJournal) -> None:
    with pytest.raises(KeyError):
        await journal.mark_in_progress("does-not-exist")
