"""Schema migrations for edgesentinel's local SQLite database.

Migrations are plain, versioned SQL scripts applied in order and tracked in
the ``schema_migrations`` table. Once a migration has shipped, never edit
its SQL -- add a new migration instead. Devices in the field may already be
at that schema version, and rewriting history under them would make the
tracking table lie about what actually ran.
"""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class Migration:
    """A single, immutable, versioned schema change."""

    version: int
    name: str
    sql: str


MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        name="initial_schema",
        sql="""
        CREATE TABLE IF NOT EXISTS runtime_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            name TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    Migration(
        version=2,
        name="circuit_breaker_state",
        sql="""
        CREATE TABLE IF NOT EXISTS circuit_breaker_state (
            name TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            failure_count INTEGER NOT NULL,
            opened_at REAL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    Migration(
        version=3,
        name="intents",
        sql="""
        CREATE TABLE IF NOT EXISTS intents (
            id TEXT PRIMARY KEY,
            operation TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_intents_status ON intents (status);
        """,
    ),
    Migration(
        version=4,
        name="events",
        sql="""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            component TEXT NOT NULL,
            severity TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            metadata TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_component ON events (component);
        """,
    ),
]

CURRENT_SCHEMA_VERSION = MIGRATIONS[-1].version if MIGRATIONS else 0
