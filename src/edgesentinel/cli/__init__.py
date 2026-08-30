"""The ``edgesentinel`` command-line diagnostics tool.

Reads a runtime's on-disk SQLite database directly -- it never talks to a
running :class:`~edgesentinel.core.runtime.EdgeSentinel` process, so it works
just as well against a stopped device as a live one, from a separate
process, without needing any IPC of its own.
"""

from __future__ import annotations
