from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from edgeguard.persistence.database import Database
from edgeguard.persistence.migrations import MIGRATIONS


async def test_connect_is_idempotent(database: Database) -> None:
    await database.connect()  # already connected via fixture; must not raise
    assert database.is_connected


async def test_operations_before_connect_raise(db_path: Path) -> None:
    db = Database(db_path)
    with pytest.raises(RuntimeError, match="not connected"):
        await db.execute("SELECT 1")


async def test_migrate_records_all_migrations(database: Database) -> None:
    rows = await database.fetchall("SELECT version FROM schema_migrations ORDER BY version")
    assert [row["version"] for row in rows] == [m.version for m in MIGRATIONS]


async def test_migrate_is_idempotent(database: Database) -> None:
    await database.migrate()
    await database.migrate()
    rows = await database.fetchall("SELECT version FROM schema_migrations")
    assert len(rows) == len(MIGRATIONS)


async def test_save_and_load_runtime_state_round_trips(database: Database) -> None:
    assert await database.load_runtime_state() is None

    await database.save_runtime_state("gateway-01", "healthy")
    row = await database.load_runtime_state()
    assert row is not None
    assert row["name"] == "gateway-01"
    assert row["state"] == "healthy"


async def test_save_runtime_state_upserts_the_single_row(database: Database) -> None:
    await database.save_runtime_state("gateway-01", "healthy")
    await database.save_runtime_state("gateway-01", "degraded")

    row = await database.load_runtime_state()
    assert row is not None
    assert row["state"] == "degraded"

    count_row = await database.fetchone("SELECT COUNT(*) AS n FROM runtime_state")
    assert count_row is not None
    assert count_row["n"] == 1


async def test_execute_and_fetchall_round_trip(database: Database) -> None:
    await database.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    await database.execute("INSERT INTO widgets (name) VALUES (?)", ("bolt",))
    await database.execute("INSERT INTO widgets (name) VALUES (?)", ("nut",))

    rows = await database.fetchall("SELECT name FROM widgets ORDER BY name")
    assert [row["name"] for row in rows] == ["bolt", "nut"]


async def test_executemany(database: Database) -> None:
    await database.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    await database.executemany("INSERT INTO widgets (name) VALUES (?)", [("a",), ("b",), ("c",)])
    rows = await database.fetchall("SELECT COUNT(*) AS n FROM widgets")
    assert rows[0]["n"] == 3


async def test_transaction_commits_on_success(database: Database) -> None:
    await database.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    async with database.transaction() as tx:
        await tx.execute("INSERT INTO widgets (name) VALUES (?)", ("bolt",))
        await tx.execute("INSERT INTO widgets (name) VALUES (?)", ("nut",))

    rows = await database.fetchall("SELECT name FROM widgets ORDER BY name")
    assert [row["name"] for row in rows] == ["bolt", "nut"]


async def test_transaction_rolls_back_on_exception(database: Database) -> None:
    await database.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    await database.execute("INSERT INTO widgets (name) VALUES (?)", ("existing",))

    with pytest.raises(RuntimeError, match="simulated failure"):
        async with database.transaction() as tx:
            await tx.execute("INSERT INTO widgets (name) VALUES (?)", ("orphan",))
            raise RuntimeError("simulated failure")

    rows = await database.fetchall("SELECT name FROM widgets")
    assert [row["name"] for row in rows] == ["existing"]


async def test_close_is_idempotent_and_reconnect_works(db_path: Path) -> None:
    db = Database(db_path)
    await db.connect()
    await db.close()
    await db.close()  # must not raise

    db2 = Database(db_path)
    await db2.connect()
    await db2.migrate()
    assert await db2.load_runtime_state() is None
    await db2.close()


async def test_insert_and_load_intent_round_trips(database: Database) -> None:
    assert await database.load_intent("abc") is None

    await database.insert_intent("abc", "send_reading", '{"value": 1}')
    row = await database.load_intent("abc")
    assert row is not None
    assert row["operation"] == "send_reading"
    assert row["payload"] == '{"value": 1}'
    assert row["status"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] is None


async def test_update_intent_changes_status_attempts_and_error(database: Database) -> None:
    await database.insert_intent("abc", "send_reading", "{}")
    await database.update_intent("abc", status="failed", attempts=3, last_error="boom")

    row = await database.load_intent("abc")
    assert row is not None
    assert row["status"] == "failed"
    assert row["attempts"] == 3
    assert row["last_error"] == "boom"


async def test_load_intents_by_status_filters_and_orders_oldest_first(database: Database) -> None:
    await database.insert_intent("a", "op", "{}")
    await database.update_intent("a", status="completed", attempts=1, last_error=None)
    await database.insert_intent("b", "op", "{}")
    await database.insert_intent("c", "op", "{}")
    await database.update_intent("c", status="in_progress", attempts=1, last_error=None)

    rows = await database.load_intents_by_status(("pending", "in_progress"))
    assert [row["id"] for row in rows] == ["b", "c"]


async def test_prune_completed_intents_deletes_only_old_completed_rows(
    database: Database,
) -> None:
    await database.insert_intent("old-completed", "op", "{}")
    await database.update_intent("old-completed", status="completed", attempts=1, last_error=None)
    await database.execute(
        "UPDATE intents SET updated_at = '2000-01-01 00:00:00' WHERE id = ?", ("old-completed",)
    )

    await database.insert_intent("still-pending", "op", "{}")
    await database.execute(
        "UPDATE intents SET updated_at = '2000-01-01 00:00:00' WHERE id = ?", ("still-pending",)
    )

    deleted = await database.prune_completed_intents("2099-01-01 00:00:00")

    assert deleted == 1
    assert await database.load_intent("old-completed") is None
    assert await database.load_intent("still-pending") is not None


async def test_insert_and_load_events_round_trips(database: Database) -> None:
    await database.insert_event(
        type="state_change",
        component="runtime",
        severity="info",
        timestamp="2024-01-01T00:00:00+00:00",
        metadata='{"previous": "booting", "current": "healthy"}',
    )

    rows = await database.load_events()
    assert len(rows) == 1
    assert rows[0]["type"] == "state_change"
    assert rows[0]["component"] == "runtime"
    assert rows[0]["severity"] == "info"
    assert rows[0]["timestamp"] == "2024-01-01T00:00:00+00:00"
    assert rows[0]["metadata"] == '{"previous": "booting", "current": "healthy"}'


async def test_load_events_orders_by_insertion_id_not_timestamp(database: Database) -> None:
    # Same timestamp for both -- insertion order (id) must still disambiguate.
    for component in ("first", "second"):
        await database.insert_event(
            type="tick",
            component=component,
            severity="info",
            timestamp="2024-01-01T00:00:00+00:00",
            metadata="{}",
        )

    newest_first = await database.load_events()
    assert [row["component"] for row in newest_first] == ["second", "first"]

    oldest_first = await database.load_events(newest_first=False)
    assert [row["component"] for row in oldest_first] == ["first", "second"]


async def test_load_events_filters_by_component_type_and_time_range(database: Database) -> None:
    await database.insert_event(
        type="network_lost",
        component="network",
        severity="warning",
        timestamp="2024-01-01T00:00:00+00:00",
        metadata="{}",
    )
    await database.insert_event(
        type="state_change",
        component="runtime",
        severity="info",
        timestamp="2024-01-02T00:00:00+00:00",
        metadata="{}",
    )
    await database.insert_event(
        type="network_restored",
        component="network",
        severity="info",
        timestamp="2024-01-03T00:00:00+00:00",
        metadata="{}",
    )

    by_component = await database.load_events(component="network", newest_first=False)
    assert [row["type"] for row in by_component] == ["network_lost", "network_restored"]

    by_type = await database.load_events(type="state_change")
    assert len(by_type) == 1

    in_range = await database.load_events(
        since="2024-01-02T00:00:00+00:00",
        until="2024-01-02T23:59:59+00:00",
    )
    assert len(in_range) == 1
    assert in_range[0]["type"] == "state_change"


async def test_load_events_respects_limit(database: Database) -> None:
    for i in range(5):
        await database.insert_event(
            type="tick",
            component=str(i),
            severity="info",
            timestamp="2024-01-01T00:00:00+00:00",
            metadata="{}",
        )

    rows = await database.load_events(limit=2)
    assert len(rows) == 2


async def test_prune_events_older_than_deletes_only_old_rows(database: Database) -> None:
    await database.insert_event(
        type="tick",
        component="old",
        severity="info",
        timestamp="2000-01-01T00:00:00+00:00",
        metadata="{}",
    )
    await database.insert_event(
        type="tick",
        component="new",
        severity="info",
        timestamp="2099-01-01T00:00:00+00:00",
        metadata="{}",
    )

    deleted = await database.prune_events_older_than("2050-01-01T00:00:00+00:00")

    assert deleted == 1
    remaining = await database.load_events()
    assert [row["component"] for row in remaining] == ["new"]


async def test_100_concurrent_inserts_produce_exactly_100_rows(database: Database) -> None:
    await database.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")

    async def insert(value: int) -> None:
        await database.execute("INSERT INTO counters (value) VALUES (?)", (value,))

    await asyncio.gather(*(insert(i) for i in range(100)))

    row = await database.fetchone("SELECT COUNT(*) AS n FROM counters")
    assert row is not None
    assert row["n"] == 100

    values = {r["value"] for r in await database.fetchall("SELECT value FROM counters")}
    assert values == set(range(100))
