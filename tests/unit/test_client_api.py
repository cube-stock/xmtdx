"""高层 API 与协议边界测试。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from xmtdx import AsyncTdxClient, KlineCategory, Market, TdxClient, to_dataframe
from xmtdx._client_logic import _validate_bars
from xmtdx._pagination import _boundary_overlap
from xmtdx.exceptions import TdxResponseError
from xmtdx.models.bar import SecurityBar
from xmtdx.models.timeseries import TransactionRecord

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _bar(date: int, price: float = 10.0) -> SecurityBar:
    return SecurityBar(
        price,
        price,
        price,
        price,
        1.0,
        1.0,
        date // 10000,
        (date // 100) % 100,
        date % 100,
        15,
        0,
    )


@patch("xmtdx.client.TdxConnection")
def test_quotes_are_automatically_batched(_connection: object) -> None:
    client = TdxClient("127.0.0.1")
    sizes: list[int] = []

    def execute(command: object) -> list[object]:
        sizes.append(len(command.stocks))  # type: ignore[attr-defined]
        return []

    with patch.object(client, "_execute", side_effect=execute):
        result = client.get_security_quotes([(Market.SH, f"{index:06d}") for index in range(161)])
    assert result == []
    assert sizes == [80, 80, 1]


@patch("xmtdx.client.TdxConnection")
def test_get_bars_routes_and_range_pages(_connection: object) -> None:
    client = TdxClient("127.0.0.1")
    with (
        patch.object(client, "get_index_bars", return_value=[]) as index,
        patch.object(client, "get_security_bars", return_value=[]) as stock,
    ):
        assert client.get_bars(Market.SH, "000001", KlineCategory.DAY, 0) == []
        assert client.get_bars(Market.SH, "600000", KlineCategory.DAY, 0) == []
        index.assert_called_once()
        stock.assert_called_once()

    pages = [[_bar(20260201)] * 800, [_bar(20260115), _bar(20251231)]]
    with patch.object(client, "get_bars", side_effect=pages):
        ranged = client.get_bars_range(Market.SH, "600000", 20260101, 20260228)
    assert [(bar.year, bar.month, bar.day) for bar in ranged] == [
        (2026, 1, 15),
        (2026, 2, 1),
    ]


def test_transaction_overlap_only_removes_page_boundary() -> None:
    a = TransactionRecord(9, 30, 10.0, 1, 0, 0)
    b = TransactionRecord(9, 31, 10.0, 1, 0, 0)
    assert _boundary_overlap([b, b], [a, b]) == 1
    assert _boundary_overlap([a, b], [a, b]) == 2


@patch("xmtdx.client.TdxConnection")
def test_transaction_collector_follows_short_page_to_empty(_connection: object) -> None:
    client = TdxClient("127.0.0.1")
    a = TransactionRecord(9, 30, 10.0, 1, 0, 0)
    b = TransactionRecord(9, 31, 10.1, 2, 0, 0)
    c = TransactionRecord(9, 32, 10.2, 3, 1, 0)
    d = TransactionRecord(9, 33, 10.3, 4, 1, 0)
    starts: list[int] = []
    pages = [[c, d], [a, b, c], []]

    def fetch(start: int, count: int) -> list[TransactionRecord]:
        starts.append(start)
        assert count == 800
        return pages.pop(0)

    assert client._collect_transaction_records(fetch) == [a, b, c, d]
    assert starts == [0, 2, 5]


@patch("xmtdx.client.TdxConnection")
def test_transaction_collector_rejects_repeated_page(_connection: object) -> None:
    client = TdxClient("127.0.0.1")
    page = [TransactionRecord(9, 30, 10.0, 1, 0, 0)]
    with pytest.raises(TdxResponseError, match="重复页面"):
        client._collect_transaction_records(lambda _start, _count: page)


def test_async_transaction_collector_matches_sync_behavior() -> None:
    async def main() -> None:
        client = AsyncTdxClient("127.0.0.1", heartbeat_interval=0)
        older = TransactionRecord(9, 30, 10.0, 1, 0, 0)
        newer = TransactionRecord(9, 31, 10.1, 2, 1, 0)
        pages = [[newer], [older], []]

        async def fetch(_start: int, count: int) -> list[TransactionRecord]:
            assert count == 800
            return pages.pop(0)

        assert await client._collect_transaction_records(fetch) == [older, newer]

    asyncio.run(main())


def test_to_dataframe_excludes_raw_by_default() -> None:
    frame = to_dataframe([_bar(20260814)])
    assert "_raw" not in frame.columns
    assert frame.iloc[0]["year"] == 2026
    with pytest.raises(TypeError):
        to_dataframe([object()])


def test_async_quote_batching() -> None:
    from xmtdx import AsyncTdxClient

    async def main() -> None:
        client = AsyncTdxClient("127.0.0.1", heartbeat_interval=0)
        sizes: list[int] = []

        async def execute(command: object) -> list[object]:
            sizes.append(len(command.stocks))  # type: ignore[attr-defined]
            return []

        with patch.object(client, "_execute", side_effect=execute):
            result = await client.get_security_quotes(
                [(Market.SH, f"{index:06d}") for index in range(81)]
            )
        assert result == []
        assert sizes == [80, 1]

    asyncio.run(main())


def test_sync_response_error_rotates_to_fallback_host() -> None:
    first = MagicMock()
    first.is_connected = False
    first.execute.side_effect = TdxResponseError("mismatch")
    second = MagicMock()
    second.is_connected = False
    second.execute.return_value = 123

    with patch("xmtdx.client.TdxConnection", side_effect=[first, second]):
        client = TdxClient("primary", fallback_hosts=["fallback"], max_attempts=2)
        assert client.get_security_count(Market.SH) == 123

    first.close.assert_called()
    second.connect.assert_called_once()
    assert client._host == "fallback"


def test_async_response_error_rotates_to_fallback_host() -> None:
    from xmtdx import AsyncTdxClient

    async def main() -> None:
        first = MagicMock()
        first.is_connected = False
        first.connect = AsyncMock()
        first.close = AsyncMock()
        first.execute = AsyncMock(side_effect=TdxResponseError("mismatch"))
        second = MagicMock()
        second.is_connected = False
        second.connect = AsyncMock()
        second.close = AsyncMock()
        second.execute = AsyncMock(return_value=456)

        with patch("xmtdx.client.AsyncTdxConnection", side_effect=[first, second]):
            client = AsyncTdxClient(
                "primary",
                fallback_hosts=["fallback"],
                max_attempts=2,
                heartbeat_interval=0,
            )
            assert await client.get_security_count(Market.SH) == 456
            await client.close()

        assert client._host == "fallback"
        second.connect.assert_awaited_once()

    asyncio.run(main())


def test_bar_semantic_validation_detects_corruption() -> None:
    bad_date = _bar(19890101)
    with pytest.raises(TdxResponseError, match="日期非法"):
        _validate_bars([bad_date], "test")

    bad_ohlc = _bar(20260814)
    bad_ohlc.high = 9.0
    with pytest.raises(TdxResponseError, match="最高价"):
        _validate_bars([bad_ohlc], "test")
