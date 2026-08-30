"""Shared pytest fixtures.

``asyncio_mode = "auto"`` (set in pyproject.toml) means async test functions
need no ``@pytest.mark.asyncio`` marker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from edgesentinel.persistence.database import Database


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "edgesentinel.sqlite3"


@pytest.fixture
async def database(db_path: Path) -> AsyncIterator[Database]:
    db = Database(db_path)
    await db.connect()
    await db.migrate()
    yield db
    await db.close()
