"""Resilience primitives: retry, backoff, timeout, circuit breaker.

Internal package -- import public names from :mod:`edgeguard` instead,
except for advanced use cases (sharing a :class:`CircuitBreaker` across
multiple decorated functions) that need direct access to these classes.
"""

from __future__ import annotations
