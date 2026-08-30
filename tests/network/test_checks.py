from __future__ import annotations

import asyncio
import socket

import pytest

from edgesentinel.network.checks import dns_resolves, tcp_reachable


async def _free_port() -> int:
    # Bind to port 0 to let the OS pick a free ephemeral port, then release
    # it -- deterministic and doesn't touch the network, just loopback.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


async def test_tcp_reachable_returns_true_for_a_listening_local_server() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        assert await tcp_reachable("127.0.0.1", port) is True
    finally:
        server.close()
        await server.wait_closed()


async def test_tcp_reachable_returns_false_for_a_closed_port() -> None:
    port = await _free_port()
    assert await tcp_reachable("127.0.0.1", port, timeout_seconds=0.5) is False


async def test_tcp_reachable_returns_false_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    async def hang(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(10)

    monkeypatch.setattr(asyncio, "open_connection", hang)

    assert await tcp_reachable("127.0.0.1", 1, timeout_seconds=0.01) is False


async def test_dns_resolves_returns_true_for_localhost() -> None:
    assert await dns_resolves("localhost") is True


async def test_dns_resolves_returns_false_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    class SlowLoop:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"unexpected loop attribute access: {name}")

        async def getaddrinfo(self, *args: object, **kwargs: object) -> object:
            await asyncio.sleep(10)
            return []

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: SlowLoop())

    assert await dns_resolves("example.com", timeout_seconds=0.01) is False


async def test_dns_resolves_returns_false_for_a_resolver_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingLoop:
        async def getaddrinfo(self, *args: object, **kwargs: object) -> object:
            raise socket.gaierror("name or service not known")

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: FailingLoop())

    assert await dns_resolves("this-host-does-not-exist.invalid") is False
