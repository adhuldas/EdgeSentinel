"""Durable operations: a write-ahead intent journal plus startup replay.

An operation decorated with ``guard.durable(...)`` is recorded to a local
SQLite journal *before* it runs. If the process crashes or the device loses
power mid-operation, the intent survives on disk; the next time the runtime
starts, it replays every unfinished intent by calling the same
function again with its original arguments. This gives applications
at-least-once execution across crashes and reboots without hand-rolling a
journal themselves.

See :mod:`edgesentinel.durability.journal` for the on-disk record and
:mod:`edgesentinel.durability.operations` for the decorator and replay logic.
"""

from __future__ import annotations

from edgesentinel.durability.journal import Intent, IntentJournal, IntentStatus
from edgesentinel.durability.operations import build_durable_decorator

__all__ = [
    "Intent",
    "IntentJournal",
    "IntentStatus",
    "build_durable_decorator",
]
