"""异步取消、关闭与心跳生命周期。"""

import asyncio
import struct
from unittest.mock import AsyncMock, MagicMock

import pytest

from xmtdx import AsyncTdxClient, Market
from xmtdx.commands.security_count import GetSecurityCountCmd
from xmtdx.commands.setup import SETUP_COMMANDS
from xmtdx.transport.async_ import AsyncTdxConnection


def frame(body=b""):
    return struct.pack("<IIIHH", 0, 0, 0, len(body), len(body)) + body


@pytest.mark.parametrize("phase", ["handshake", "response"])
def test_cancellation_closes_stream_and_next_query_uses_fresh_connection(phase):
    async def run():
        blocked = asyncio.Event()
        disconnected = asyncio.Event()
        handlers = set()
        connections = 0

        async def handle(reader, writer):
            nonlocal connections
            task = asyncio.current_task()
            handlers.add(task)
            connections += 1
            first = connections == 1
            try:
                for setup in SETUP_COMMANDS:
                    await reader.readexactly(len(setup))
                    if first and phase == "handshake":
                        blocked.set()
                        await reader.read()
                        disconnected.set()
                        return
                    writer.write(frame())
                    await writer.drain()
                await reader.readexactly(len(GetSecurityCountCmd(Market.SH).build_request()))
                if first:
                    blocked.set()
                    await reader.read()
                    disconnected.set()
                    return
                writer.write(frame(struct.pack("<H", 42)))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                handlers.discard(task)

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        conn = AsyncTdxConnection("127.0.0.1", server.sockets[0].getsockname()[1], timeout=0.2)
        try:
            if phase == "response":
                await conn.connect()
            operation = (
                conn.connect()
                if phase == "handshake"
                else conn.execute(GetSecurityCountCmd(Market.SH))
            )
            task = asyncio.create_task(operation)
            await asyncio.wait_for(blocked.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not conn.is_connected
            await asyncio.wait_for(disconnected.wait(), 0.5)
            await conn.connect()
            assert await conn.execute(GetSecurityCountCmd(Market.SH)) == 42
            assert connections == 2
        finally:
            await conn.close()
            server.close()
            await server.wait_closed()
            if handlers:
                await asyncio.gather(*handlers)

    asyncio.run(run())


def test_close_timeout_clears_references_and_aborts_transport():
    async def run():
        writer = MagicMock()
        never = asyncio.Event()
        writer.wait_closed = AsyncMock(side_effect=never.wait)
        conn = AsyncTdxConnection(timeout=0.02)
        conn._reader = MagicMock()
        conn._writer = writer
        await asyncio.wait_for(conn.close(), 0.2)
        assert conn._reader is None and conn._writer is None
        writer.transport.abort.assert_called_once()
        await conn.close()

    asyncio.run(run())


def test_repeated_connect_keeps_one_heartbeat():
    async def run():
        client = AsyncTdxClient(heartbeat_interval=10)
        client._conn = MagicMock()
        client._conn.connect = AsyncMock()
        client._conn.close = AsyncMock()
        await client.connect()
        task = client._heartbeat_task
        await client.connect()
        try:
            assert client._heartbeat_task is task
        finally:
            await client.close()
        assert task.done()

    asyncio.run(run())


def test_failed_heartbeat_does_not_prevent_connection_cleanup():
    async def run():
        client = AsyncTdxClient()
        client._conn = MagicMock()
        client._conn.close = AsyncMock()

        async def fail():
            raise RuntimeError("heartbeat bug")

        client._heartbeat_task = asyncio.create_task(fail())
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="heartbeat bug"):
            await client.close()
        client._conn.close.assert_awaited_once()
        assert client._heartbeat_task is None

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_context_connect_fails_over_against_local_tcp_server(asynchronous):
    from xmtdx import TdxClient

    async def run():
        done = asyncio.Event()

        async def handle(reader, writer):
            try:
                for setup in SETUP_COMMANDS:
                    await reader.readexactly(len(setup))
                    writer.write(frame())
                    await writer.drain()
                await reader.readexactly(len(GetSecurityCountCmd(Market.SH).build_request()))
                writer.write(frame(struct.pack("<H", 42)))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                done.set()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            if asynchronous:
                async with AsyncTdxClient(
                    "127.0.0.2",
                    port=port,
                    timeout=0.2,
                    fallback_hosts=["127.0.0.1"],
                    heartbeat_interval=0,
                ) as client:
                    assert await client.get_security_count(Market.SH) == 42
            else:

                def query():
                    with TdxClient(
                        "127.0.0.2", port=port, timeout=0.2, fallback_hosts=["127.0.0.1"]
                    ) as client:
                        return client.get_security_count(Market.SH)

                assert await asyncio.to_thread(query) == 42
            await asyncio.wait_for(done.wait(), 1)
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())
