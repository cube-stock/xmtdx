"""新版握手的端到端回归；服务端预期字节独立于被测常量。"""

import asyncio
import json
import socket
import struct
import threading
import zlib
from dataclasses import replace
from pathlib import Path

import pytest

from xmtdx import AsyncTdxClient, KlineCategory, Market, TdxClient
from xmtdx._client_logic import _validate_bars, _validate_minute_bars
from xmtdx.codec.frame import decompress_body, parse_header
from xmtdx.commands.minute_time import GetHistoryMinuteTimeDataCmd
from xmtdx.commands.security_bars import GetIndexBarsCmd, GetSecurityBarsCmd
from xmtdx.commands.security_quotes import GetSecurityQuotesCmd
from xmtdx.commands.transaction import GetHistoryTransactionDataCmd
from xmtdx.exceptions import TdxConnectionError, TdxDecodeError, TdxResponseError
from xmtdx.models import MinuteBar, SecurityBar

FIXTURES = Path(__file__).parents[1] / "fixtures" / "protocol_4"
INIT = bytes.fromhex("0c0218940001030003000d0001")
LEVEL = bytes.fromhex(
    "0c031899000120002000db0f7464786c6576656c"
    "000000295cf740110000000000000000000000000005"
)


def _recv(sock, length):
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise EOFError("客户端提前关闭")
        data.extend(chunk)
    return bytes(data)


def _frame(body, compressed=False):
    raw = zlib.compress(body) if compressed else body
    return struct.pack("<IIIHH", 7654321, 0, 0, len(raw), len(body)) + raw


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("failure", [None, "truncated", "compression", "timeout"])
def test_handshake_then_quotes_and_bars(asynchronous, failure):
    errors = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(3)
        port = listener.getsockname()[1]

        def serve():
            try:
                with listener.accept()[0] as sock:
                    sock.settimeout(3)
                    assert _recv(sock, 13) == INIT
                    response = _frame(bytes(189), compressed=True)
                    # 强制多次接收，并确保握手响应不会泄漏到下一条命令。
                    for offset in range(0, len(response), 3):
                        sock.sendall(response[offset:offset + 3])
                    assert _recv(sock, 42) == LEVEL
                    if failure == "timeout":
                        assert sock.recv(1) == b""
                        return
                    if failure == "truncated":
                        sock.sendall(_frame(bytes(179))[:20])
                        return
                    if failure == "compression":
                        sock.sendall(struct.pack("<IIIHH", 7654321, 0, 0, 3, 179) + b"bad")
                        assert sock.recv(1) == b""
                        return
                    sock.sendall(_frame(bytes(179)))
                    for name in ("quotes", "bars_sh"):
                        capture = json.loads((FIXTURES / f"{name}.json").read_text())
                        expected = bytes.fromhex(capture["request"])
                        assert _recv(sock, len(expected)) == expected
                        body = bytes.fromhex(capture["body"])
                        response = _frame(body, compressed=True)
                        sock.sendall(response[:7])
                        sock.sendall(response[7:])
            except BaseException as error:
                errors.append(error)

        server = threading.Thread(target=serve, daemon=True)
        server.start()

        async def run_async():
            client = AsyncTdxClient("127.0.0.1", port, timeout=0.5, heartbeat_interval=0,
                                    max_attempts=1)
            try:
                await client.connect()
                quotes = await client.get_security_quotes(
                    [(Market.SH, "600000"), (Market.SZ, "000001"), (Market.BJ, "920002")]
                )
                bars = await client.get_security_bars(
                    Market.SH, "600000", KlineCategory.DAY, 1, 3
                )
                assert len(quotes) == len(bars) == 3
                assert quotes[0]._raw and bars[0]._raw
            finally:
                await client.close()
                assert not client._conn.is_connected

        def run_sync():
            client = TdxClient("127.0.0.1", port, timeout=0.5, max_attempts=1)
            try:
                client.connect()
                quotes = client.get_security_quotes(
                    [(Market.SH, "600000"), (Market.SZ, "000001"), (Market.BJ, "920002")]
                )
                bars = client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 1, 3)
                assert len(quotes) == len(bars) == 3
                assert quotes[0]._raw and bars[0]._raw
            finally:
                client.close()
                assert not client._conn.is_connected

        try:
            if failure:
                error = TdxDecodeError if failure == "compression" else TdxConnectionError
                with pytest.raises(error):
                    asyncio.run(run_async()) if asynchronous else run_sync()
            else:
                asyncio.run(run_async()) if asynchronous else run_sync()
        finally:
            server.join(timeout=4)
        assert not server.is_alive()
        assert not errors


@pytest.mark.parametrize("name,market", [("bars_sh", Market.SH), ("bars_sz", Market.SZ)])
def test_captured_daily_bars_match_reviewed_values(name, market):
    capture = json.loads((FIXTURES / f"{name}.json").read_text())
    code = "600000" if market == Market.SH else "000001"
    bars = GetSecurityBarsCmd(market, code, KlineCategory.DAY, 1, 3).parse_response(
        bytes.fromhex(capture["body"])
    )
    assert len(bars) == len(capture["expected"])
    for bar, expected in zip(bars, capture["expected"], strict=True):
        assert bar.datetime_str == expected["datetime"]
        for field in ("open", "close", "high", "low", "vol", "amount"):
            assert getattr(bar, field) == expected[field]
    assert b"".join(bar._raw for bar in bars) == bytes.fromhex(capture["body"])[2:]


def test_captured_multi_market_quotes_keep_record_alignment():
    capture = json.loads((FIXTURES / "quotes.json").read_text())
    stocks = [(Market.SH, "600000"), (Market.SZ, "000001"), (Market.BJ, "920002")]
    quotes = GetSecurityQuotesCmd(stocks).parse_response(bytes.fromhex(capture["body"]))
    for quote, expected in zip(quotes, capture["expected"], strict=True):
        for field, value in expected.items():
            assert getattr(quote, field) == value
    assert b"".join(quote._raw for quote in quotes) == bytes.fromhex(capture["body"])[4:]


def test_captured_index_bars_preserve_breadth_and_record_boundaries():
    capture = json.loads((FIXTURES / "bars_index.json").read_text())
    bars = GetIndexBarsCmd(Market.SH, "000001", KlineCategory.DAY, 1, 3).parse_response(
        bytes.fromhex(capture["body"])
    )
    assert len(bars) == len(capture["expected"])
    for bar, expected in zip(bars, capture["expected"], strict=True):
        assert bar.datetime_str == expected["datetime"]
        for field in ("open", "close", "high", "low", "vol", "amount", "up_count", "down_count"):
            assert getattr(bar, field) == expected[field]
    assert b"".join(bar._raw for bar in bars) == bytes.fromhex(capture["body"])[2:]


@pytest.mark.parametrize("name", ["minute_history", "transaction_history"])
def test_captured_history_retains_every_field(name):
    capture = json.loads((FIXTURES / f"{name}.json").read_text())
    command = (GetHistoryMinuteTimeDataCmd(Market.SH, "600000", 20261028)
               if name == "minute_history" else
               GetHistoryTransactionDataCmd(Market.SH, "600000", 20261028, 0, 20))
    records = command.parse_response(bytes.fromhex(capture["body"]))
    assert len(records) == len(capture["expected"])
    for record, expected in zip(records, capture["expected"], strict=True):
        for field, value in expected.items():
            assert getattr(record, field) == value
    with pytest.raises(TdxDecodeError):
        command.parse_response(bytes.fromhex(capture["body"])[:-1])


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.json")), ids=lambda path: path.stem)
def test_captured_frame_lengths_and_compression(path):
    capture = json.loads(path.read_text())
    header = parse_header(bytes.fromhex(capture["header"]))
    assert decompress_body(header, bytes.fromhex(capture["raw_body"])) == bytes.fromhex(
        capture["body"]
    )


def test_old_handshake_failure_payloads_remain_errors():
    with pytest.raises(TdxDecodeError):
        GetSecurityBarsCmd(Market.SH, "600000", KlineCategory.DAY, 0, 3).parse_response(
            bytes.fromhex("2003")
        )
    with pytest.raises(TdxResponseError):
        GetSecurityQuotesCmd([(Market.SH, "600000")]).parse_response(bytes.fromhex("01070000"))


@pytest.mark.parametrize("changes", [
    {"hour": 24}, {"minute": 60}, {"open": -2, "close": -2, "high": -1, "low": -3}
])
def test_bars_reject_invalid_time_and_negative_price(changes):
    bar = SecurityBar(10, 10, 10, 10, 100, 1000, 2026, 10, 28, 15, 0)
    with pytest.raises(TdxResponseError):
        _validate_bars([replace(bar, **changes)], "bars")


@pytest.mark.parametrize("volume", [float("nan"), float("inf"), -1])
def test_minute_bars_reject_invalid_volume(volume):
    with pytest.raises(TdxResponseError, match="成交量非法"):
        _validate_minute_bars([MinuteBar(10, volume)], "minute")
