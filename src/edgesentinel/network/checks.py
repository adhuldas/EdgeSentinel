"""Stdlib-only connectivity primitives used to build layer checks.

These are plain building blocks, not the monitor itself -- see
:mod:`edgesentinel.network.monitor` for the piece that decides what a failed
check *means*. Kept dependency-free (no ``psutil``, no ``ping3``) since a
TCP connect attempt and a DNS lookup are all the standard library needs to
answer "can I reach this thing", and edgesentinel ships with zero runtime
dependencies by design.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

logger = logging.getLogger("edgesentinel.network")


async def tcp_reachable(host: str, port: int, *, timeout_seconds: float = 2.0) -> bool:
    """Return whether a TCP connection to ``host:port`` can be opened.

    Suitable for the GATEWAY and INTERNET layers (e.g. connect to the
    router's management port, or a well-known public host). Any failure --
    refused, unreachable, timed out, DNS failure -- is treated as "not
    reachable" rather than propagating, since a connectivity check that can
    itself raise defeats the point of checking.
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            _, writer = await asyncio.open_connection(host, port)
    except (OSError, TimeoutError):
        return False
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()
    return True


async def dns_resolves(hostname: str, *, timeout_seconds: float = 2.0) -> bool:
    """Return whether ``hostname`` resolves to an address.

    Suitable for the DNS layer. Uses the running loop's
    :meth:`~asyncio.AbstractEventLoop.getaddrinfo`, which offloads the
    (blocking) system resolver call to a thread pool internally -- no
    manual threading needed here.
    """
    loop = asyncio.get_running_loop()
    try:
        async with asyncio.timeout(timeout_seconds):
            await loop.getaddrinfo(hostname, None)
    except (OSError, TimeoutError):
        return False
    return True
