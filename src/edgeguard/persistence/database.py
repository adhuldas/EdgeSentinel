"""SQLite persistence layer.

edgeguard runs one process per device, so a single :mod:`sqlite3` connection
driven through :func:`asyncio.to_thread` is sufficient -- it keeps the event
loop non-blocking without pulling in an extra dependency (e.g. ``aiosqlite``)
for something the standard library already does well.

All access is serialized through an :class:`asyncio.Lock` rather than relied
upon SQLite's own locking, because a single ``sqlite3.Connection`` is not
safe to use concurrently from multiple threads even with
``check_same_thread=False``. WAL mode is enabled so that a separate reader
process (e.g. the CLI) never blocks a writer.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from edgeguard.persistence.migrations import MIGRATIONS


class Transaction:
    """Statement execution bound to an already-open SQLite transaction.

    Instances are only ever handed out by :meth:`Database.transaction`,
    which holds the database's lock for the transaction's whole duration --
    so methods here talk to the connection directly instead of re-acquiring
    that lock (which would deadlock).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        await asyncio.to_thread(self._conn.execute, sql, params)

    async def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> None:
        await asyncio.to_thread(self._conn.executemany, sql, list(params))

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        return await asyncio.to_thread(lambda: self._conn.execute(sql, params).fetchone())

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return await asyncio.to_thread(lambda: self._conn.execute(sql, params).fetchall())


class Database:
    """Async-friendly wrapper around a single SQLite connection."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    async def connect(self) -> None:
        """Open the database file, creating parent directories if needed.

        Safe to call more than once; subsequent calls are no-ops.
        """
        if self._conn is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await asyncio.to_thread(self._open)

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    async def close(self) -> None:
        """Close the connection. Safe to call more than once, or if never
        connected."""
        if self._conn is None:
            return
        conn, self._conn = self._conn, None
        await asyncio.to_thread(conn.close)

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError(f"database at {self._path} is not connected; call connect() first")
        return self._conn

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        conn = self._require_conn()
        async with self._lock:
            await asyncio.to_thread(conn.execute, sql, params)

    async def executemany(self, sql: str, params: Iterable[Sequence[Any]]) -> None:
        conn = self._require_conn()
        async with self._lock:
            await asyncio.to_thread(conn.executemany, sql, list(params))

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        conn = self._require_conn()
        async with self._lock:
            return await asyncio.to_thread(lambda: conn.execute(sql, params).fetchone())

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        conn = self._require_conn()
        async with self._lock:
            return await asyncio.to_thread(lambda: conn.execute(sql, params).fetchall())

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        """Run a block of statements atomically.

        Commits when the block exits cleanly; rolls back and re-raises if it
        raises. Held for the entire block, so callers must use the yielded
        :class:`Transaction` (not the outer :class:`Database`) for
        statements inside the block::

            async with db.transaction() as tx:
                await tx.execute("INSERT INTO ...", (...,))
                await tx.execute("UPDATE ...", (...,))
        """
        conn = self._require_conn()
        async with self._lock:
            await asyncio.to_thread(conn.execute, "BEGIN IMMEDIATE")
            try:
                yield Transaction(conn)
            except Exception:
                await asyncio.to_thread(conn.rollback)
                raise
            else:
                await asyncio.to_thread(conn.commit)

    async def migrate(self) -> None:
        """Apply any migrations from :data:`edgeguard.persistence.migrations.MIGRATIONS`
        that haven't already been recorded as applied."""
        conn = self._require_conn()
        async with self._lock:
            await asyncio.to_thread(self._migrate, conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "applied_at TEXT NOT NULL DEFAULT (datetime('now')))"
        )
        applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
        for migration in MIGRATIONS:
            if migration.version in applied:
                continue
            conn.executescript(migration.sql)
            conn.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                (migration.version, migration.name),
            )
        conn.commit()

    async def save_runtime_state(self, name: str, state: str) -> None:
        """Persist the runtime's identity and current lifecycle state.

        Single-row upsert: there is exactly one runtime per database file.
        """
        await self.execute(
            "INSERT INTO runtime_state (id, name, state, updated_at) "
            "VALUES (1, ?, ?, datetime('now')) "
            "ON CONFLICT(id) DO UPDATE SET "
            "name = excluded.name, state = excluded.state, updated_at = excluded.updated_at",
            (name, state),
        )

    async def load_runtime_state(self) -> sqlite3.Row | None:
        """Return the last persisted ``(name, state, updated_at)``, or ``None``
        if the runtime has never saved its state."""
        return await self.fetchone("SELECT name, state, updated_at FROM runtime_state WHERE id = 1")

    async def save_circuit_breaker_state(
        self, name: str, *, state: str, failure_count: int, opened_at: float | None
    ) -> None:
        """Persist a circuit breaker's state, keyed by its ``name``.

        ``opened_at`` is a wall-clock (``time.time()``) timestamp, not a
        monotonic one -- it needs to remain meaningful after a process
        restart, when any previously recorded monotonic clock reading is
        worthless.
        """
        await self.execute(
            "INSERT INTO circuit_breaker_state "
            "(name, state, failure_count, opened_at, updated_at) "
            "VALUES (?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(name) DO UPDATE SET "
            "state = excluded.state, failure_count = excluded.failure_count, "
            "opened_at = excluded.opened_at, updated_at = excluded.updated_at",
            (name, state, failure_count, opened_at),
        )

    async def load_circuit_breaker_state(self, name: str) -> tuple[str, int, float | None] | None:
        """Return ``(state, failure_count, opened_at)`` for a persisted circuit
        breaker, or ``None`` if it has never saved its state."""
        row = await self.fetchone(
            "SELECT state, failure_count, opened_at FROM circuit_breaker_state WHERE name = ?",
            (name,),
        )
        if row is None:
            return None
        return (row["state"], row["failure_count"], row["opened_at"])

    async def insert_intent(self, intent_id: str, operation: str, payload: str) -> None:
        """Durably record a new intent as ``pending`` with zero attempts.

        Called before an operation's side effects run, so the intent
        exists on disk even if the process dies before the operation
        starts -- the whole point of a write-ahead journal.
        """
        await self.execute(
            "INSERT INTO intents "
            "(id, operation, payload, status, attempts, last_error, created_at, updated_at) "
            "VALUES (?, ?, ?, 'pending', 0, NULL, datetime('now'), datetime('now'))",
            (intent_id, operation, payload),
        )

    async def update_intent(
        self, intent_id: str, *, status: str, attempts: int, last_error: str | None
    ) -> None:
        """Update an intent's status/attempts/last_error and touch ``updated_at``."""
        await self.execute(
            "UPDATE intents SET status = ?, attempts = ?, last_error = ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (status, attempts, last_error, intent_id),
        )

    async def load_intent(self, intent_id: str) -> sqlite3.Row | None:
        """Return the row for a single intent, or ``None`` if it doesn't exist."""
        return await self.fetchone("SELECT * FROM intents WHERE id = ?", (intent_id,))

    async def load_intents_by_status(self, statuses: Sequence[str]) -> list[sqlite3.Row]:
        """Return intents whose status is in ``statuses``, oldest first.

        Oldest-first ordering matters for replay: intents should be retried
        in the order their operations were originally requested. Ordered by
        the table's implicit ``rowid`` (monotonic insertion order) rather
        than ``created_at``, since ``datetime('now')`` only has one-second
        resolution and several intents can easily be recorded within the
        same second.
        """
        placeholders = ", ".join("?" for _ in statuses)
        return await self.fetchall(
            f"SELECT * FROM intents WHERE status IN ({placeholders}) ORDER BY rowid ASC",
            tuple(statuses),
        )

    async def prune_completed_intents(self, older_than: str) -> int:
        """Delete ``completed`` intents last updated before ``older_than``
        (an ISO-8601 / SQLite ``datetime()``-comparable timestamp string).

        Returns the number of rows deleted. The journal grows by one row per
        durable operation call; without pruning, a long-running device would
        accumulate history forever, which is exactly the kind of unbounded
        local storage growth this library exists to prevent.
        """
        conn = self._require_conn()
        async with self._lock:
            return await asyncio.to_thread(self._prune_completed_intents, conn, older_than)

    def _prune_completed_intents(self, conn: sqlite3.Connection, older_than: str) -> int:
        cursor = conn.execute(
            "DELETE FROM intents WHERE status = 'completed' AND updated_at < ?",
            (older_than,),
        )
        conn.commit()
        return cursor.rowcount

    async def insert_event(
        self, *, type: str, component: str, severity: str, timestamp: str, metadata: str
    ) -> None:
        """Durably record one event from the runtime's event bus.

        ``metadata`` is already-serialized JSON, not a mapping -- this
        layer doesn't know or care what an event's metadata means, only
        that it round-trips.
        """
        await self.execute(
            "INSERT INTO events (type, component, severity, timestamp, metadata) "
            "VALUES (?, ?, ?, ?, ?)",
            (type, component, severity, timestamp, metadata),
        )

    async def load_events(
        self,
        *,
        component: str | None = None,
        type: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = 100,
        newest_first: bool = True,
    ) -> list[sqlite3.Row]:
        """Return recorded events matching the given filters.

        Ordered by the table's implicit insertion order (``id``), not
        ``timestamp`` -- several events can share the same one-second
        SQLite timestamp resolution, and insertion order is always the
        true order they were published in.
        """
        clauses: list[str] = []
        params: list[str] = []
        if component is not None:
            clauses.append("component = ?")
            params.append(component)
        if type is not None:
            clauses.append("type = ?")
            params.append(type)
        if since is not None:
            clauses.append("timestamp >= ?")
            params.append(since)
        if until is not None:
            clauses.append("timestamp <= ?")
            params.append(until)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order = "DESC" if newest_first else "ASC"
        return await self.fetchall(
            f"SELECT * FROM events {where} ORDER BY id {order} LIMIT ?",
            (*params, limit),
        )

    async def prune_events_older_than(self, older_than: str) -> int:
        """Delete events recorded before ``older_than`` (an ISO-8601 /
        SQLite ``datetime()``-comparable timestamp string).

        Returns the number of rows deleted. Like the intent journal, the
        event timeline grows without bound unless pruned.
        """
        conn = self._require_conn()
        async with self._lock:
            return await asyncio.to_thread(self._prune_events_older_than, conn, older_than)

    def _prune_events_older_than(self, conn: sqlite3.Connection, older_than: str) -> int:
        cursor = conn.execute("DELETE FROM events WHERE timestamp < ?", (older_than,))
        conn.commit()
        return cursor.rowcount
