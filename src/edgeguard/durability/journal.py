"""The intent journal: a durable, append-then-update log of operations.

Every durable operation goes through exactly this lifecycle::

    PENDING --(execution starts)--> IN_PROGRESS --(succeeds)--> COMPLETED
                                          |
                                          +--(fails, attempts remain)--> PENDING
                                          |
                                          +--(fails, attempts exhausted)--> FAILED

An intent is written as ``PENDING`` *before* its side effects run, so a
crash at any point still leaves a durable record that the operation was
requested but not confirmed done. Both ``PENDING`` and ``IN_PROGRESS`` are
therefore "not yet finished" from a replay's point of view -- the
distinction exists for observability (did we crash before starting, or
mid-flight, when side effects may be partially applied?), not because they
need different replay handling.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import json
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from edgeguard.core.exceptions import InvalidDurablePayloadError


class IntentStatus(enum.Enum):
    """Lifecycle status of a single :class:`Intent`."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclasses.dataclass(frozen=True, slots=True)
class Intent:
    """A single durable operation's journal record.

    Attributes:
        id: Opaque, unique identifier (a UUID4 hex string) assigned when the
            intent is first recorded.
        operation: The name passed to ``guard.durable(operation=...)``.
        payload: The operation's original keyword arguments, already
            JSON-round-tripped -- values are exactly what a fresh
            ``json.loads(json.dumps(...))`` would produce, which matters if
            an application checks e.g. ``isinstance(x, tuple)`` on replay.
        status: Current lifecycle status.
        attempts: Number of times execution has been attempted so far.
        last_error: ``str()`` of the most recent failure, or ``None`` if
            the intent has never failed.
        created_at / updated_at: SQLite ``datetime('now')`` timestamps
            (UTC, ``"YYYY-MM-DD HH:MM:SS"``), kept as plain strings for
            consistency with the rest of the persistence layer.
    """

    id: str
    operation: str
    payload: dict[str, Any]
    status: IntentStatus
    attempts: int
    last_error: str | None
    created_at: str
    updated_at: str


class JournalStore(Protocol):
    """Persistence hook an :class:`IntentJournal` is built on.

    Satisfied structurally by :class:`edgeguard.persistence.database.Database`
    -- no import of it is needed here.
    """

    async def insert_intent(self, intent_id: str, operation: str, payload: str) -> None: ...

    async def update_intent(
        self, intent_id: str, *, status: str, attempts: int, last_error: str | None
    ) -> None: ...

    async def load_intent(self, intent_id: str) -> sqlite3.Row | None: ...

    async def load_intents_by_status(self, statuses: Sequence[str]) -> list[sqlite3.Row]: ...

    async def prune_completed_intents(self, older_than: str) -> int: ...


def _row_to_intent(row: sqlite3.Row) -> Intent:
    return Intent(
        id=row["id"],
        operation=row["operation"],
        payload=json.loads(row["payload"]),
        status=IntentStatus(row["status"]),
        attempts=row["attempts"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class IntentJournal:
    """Write-ahead journal of durable operations, backed by a ``JournalStore``."""

    def __init__(self, store: JournalStore) -> None:
        self._store = store

    async def record(self, operation: str, payload: Mapping[str, Any]) -> Intent:
        """Durably record a new, ``PENDING`` intent and return it.

        Raises:
            InvalidDurablePayloadError: if ``payload`` isn't JSON-serializable.
        """
        try:
            serialized = json.dumps(dict(payload))
        except TypeError as exc:
            raise InvalidDurablePayloadError(
                f"durable operation {operation!r} received arguments that "
                f"aren't JSON-serializable: {exc}"
            ) from exc
        intent_id = uuid.uuid4().hex
        await self._store.insert_intent(intent_id, operation, serialized)
        intent = await self.get(intent_id)
        if intent is None:
            # Only reachable if the store lied about the insert succeeding.
            raise RuntimeError(f"intent {intent_id!r} vanished immediately after being recorded")
        return intent

    async def get(self, intent_id: str) -> Intent | None:
        """Return the current record for ``intent_id``, or ``None`` if unknown."""
        row = await self._store.load_intent(intent_id)
        return None if row is None else _row_to_intent(row)

    async def mark_in_progress(self, intent_id: str) -> Intent:
        """Increment the attempt count and mark the intent ``IN_PROGRESS``.

        Called immediately before an operation's function body runs, on
        both the original call and every replay attempt.
        """
        current = await self._require(intent_id)
        attempts = current.attempts + 1
        await self._store.update_intent(
            intent_id,
            status=IntentStatus.IN_PROGRESS.value,
            attempts=attempts,
            last_error=current.last_error,
        )
        return dataclasses.replace(current, status=IntentStatus.IN_PROGRESS, attempts=attempts)

    async def mark_completed(self, intent_id: str) -> None:
        """Mark the intent ``COMPLETED``. It will never be replayed again."""
        current = await self._require(intent_id)
        await self._store.update_intent(
            intent_id,
            status=IntentStatus.COMPLETED.value,
            attempts=current.attempts,
            last_error=None,
        )

    async def mark_pending_for_retry(self, intent_id: str, error: str) -> None:
        """Record a failed attempt and return the intent to ``PENDING``.

        Used when a durable operation fails but has attempts remaining --
        it stays eligible for another live retry or the next startup replay.
        """
        current = await self._require(intent_id)
        await self._store.update_intent(
            intent_id,
            status=IntentStatus.PENDING.value,
            attempts=current.attempts,
            last_error=error,
        )

    async def mark_failed(self, intent_id: str, error: str) -> None:
        """Record a failed attempt and mark the intent ``FAILED``.

        Used once ``max_attempts`` is exhausted -- the intent is left in
        the journal for inspection but is no longer replayed.
        """
        current = await self._require(intent_id)
        await self._store.update_intent(
            intent_id,
            status=IntentStatus.FAILED.value,
            attempts=current.attempts,
            last_error=error,
        )

    async def pending(self) -> list[Intent]:
        """Return every ``PENDING`` or ``IN_PROGRESS`` intent, oldest first.

        This is the replay set: everything not yet confirmed ``COMPLETED``
        (or given up on as ``FAILED``).
        """
        rows = await self._store.load_intents_by_status(
            (IntentStatus.PENDING.value, IntentStatus.IN_PROGRESS.value)
        )
        return [_row_to_intent(row) for row in rows]

    async def prune_completed(self, older_than: dt.timedelta) -> int:
        """Delete ``COMPLETED`` intents last updated more than ``older_than`` ago.

        Returns the number of rows deleted. Without pruning, the journal
        grows by one row per durable call forever -- exactly the unbounded
        local storage growth edgeguard exists to prevent.
        """
        cutoff = (dt.datetime.now(dt.UTC) - older_than).strftime("%Y-%m-%d %H:%M:%S")
        return await self._store.prune_completed_intents(cutoff)

    async def _require(self, intent_id: str) -> Intent:
        intent = await self.get(intent_id)
        if intent is None:
            raise KeyError(f"no intent {intent_id!r} in the journal")
        return intent
