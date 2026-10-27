"""数据完整性、故障切换与保守降级回归。"""

import asyncio
import inspect
import struct
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from xmtdx import AsyncTdxClient, KlineCategory, Market, TdxClient
from xmtdx._client_logic import market_stat_from_quotes
from xmtdx.codec.price_rules import compute_price_limits
from xmtdx.commands.block_info import GetBlockInfoCmd
from xmtdx.commands.fund_flow import GetHistoryFundFlowCmd
from xmtdx.exceptions import TdxConnectionError, TdxDecodeError, TdxResponseError
from xmtdx.models import MinuteBar, SecurityBar, SecurityInfo, TransactionRecord


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def client(request):
    return AsyncTdxClient(heartbeat_interval=0) if request.param else TdxClient()


async def invoke(client, method, *args):
    result = getattr(client, method)(*args)
    return await result if inspect.isawaitable(result) else result


def stock(code, market=Market.SH):
    return SecurityInfo(market, code, "测试", 100, 2, 10)


def bar(volume=10000, day=8):
    return SecurityBar(10, 10, 10, 10, volume, volume * 10, 2025, 1, day, 15, 0)


def test_security_list_short_pages_advance_by_received_count(client):
    records = [stock("600000"), stock("600001"), stock("600002")]
    with (
        patch.object(client, "get_report_file", return_value=b""),
        patch.object(client, "get_security_count", side_effect=[3, 0]),
        patch.object(client, "get_security_list", side_effect=[records[:1], records[1:]]) as fetch,
    ):
        assert asyncio.run(invoke(client, "get_security_list_all")) == records
    assert [call.args[1] for call in fetch.call_args_list] == [0, 1]


@pytest.mark.parametrize(
    "pages",
    [
        [[stock("600000")], []],
        [[stock("600000")], [stock("600000")]],
        [[stock("000001", Market.SZ)]],
        [[stock("600000"), stock("600001"), stock("600002")]],
    ],
)
def test_security_list_rejects_incomplete_or_inconsistent_pages(client, pages):
    with (
        patch.object(client, "get_report_file", return_value=b""),
        patch.object(client, "get_security_count", return_value=2),
        patch.object(client, "get_security_list", side_effect=pages),
        pytest.raises(TdxResponseError),
    ):
        asyncio.run(invoke(client, "get_security_list_all"))


def test_history_fund_flow_requires_daily_volume_coverage(client):
    records = [TransactionRecord(10, 0, 10, 1, 0)]
    with (
        patch.object(client, "_execute", return_value=[]),
        patch.object(client, "get_security_bars", return_value=[bar()]),
        patch.object(client, "get_history_transaction_data", side_effect=[records, []]),
        pytest.raises(TdxResponseError, match="覆盖不完整"),
    ):
        asyncio.run(invoke(client, "get_history_fund_flow", Market.SH, "600000", 0, 1))


@pytest.mark.parametrize("volume", [10000, 10100])
def test_history_fund_flow_converts_hands_to_shares(client, volume):
    records = [TransactionRecord(10, 0, 10, 100, 0)]
    with (
        patch.object(client, "_execute", return_value=[]),
        patch.object(client, "get_security_bars", return_value=[bar(volume)]),
        patch.object(client, "get_history_transaction_data", side_effect=[records, []]),
    ):
        flow = asyncio.run(invoke(client, "get_history_fund_flow", Market.SH, "600000", 0, 1))
    assert flow[0].medium_in == 100000


@pytest.mark.parametrize("error", [None, TdxConnectionError("offline")])
def test_limits_are_unknown_when_listing_window_cannot_be_verified(client, error):
    with patch.object(client, "get_security_bars", return_value=[], side_effect=error):
        assert asyncio.run(invoke(client, "get_price_limits", Market.SH, "600000", "测试", 10)) == (
            None,
            None,
        )


@pytest.mark.parametrize("now", [datetime(2026, 10, 25, 10), datetime(2026, 10, 26, 9)])
def test_current_minute_pre_session_does_not_request_stale_data(client, now):
    with (
        patch(
            "xmtdx.client._now_in_shanghai",
            return_value=now.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
        ),
        patch.object(
            client, "get_history_minute_time_data", return_value=[MinuteBar(10, 1)]
        ) as history,
    ):
        assert asyncio.run(invoke(client, "get_minute_time_data", Market.SH, "600000")) == []
        history.assert_not_called()


@pytest.mark.parametrize("source", ["history", "current"])
def test_current_minute_requires_same_day_bar_and_reference_price(client, source):
    now = datetime(2025, 1, 8, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    minutes = [MinuteBar(10, 1)]
    with (
        patch("xmtdx.client._now_in_shanghai", return_value=now),
        patch("xmtdx.client._today_in_shanghai", return_value=20250108),
        patch.object(
            client,
            "get_history_minute_time_data",
            return_value=minutes if source == "history" else [],
        ),
        patch.object(client, "_execute", return_value=minutes),
        patch.object(client, "get_security_bars", return_value=[bar()]),
        patch.object(
            client, "get_security_quotes", return_value=[SimpleNamespace(pre_close=10, price=10)]
        ),
    ):
        assert asyncio.run(invoke(client, "get_minute_time_data", Market.SH, "600000")) == minutes


def test_current_minute_rejects_previous_trading_day(client):
    now = datetime(2025, 1, 9, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    with (
        patch("xmtdx.client._now_in_shanghai", return_value=now),
        patch("xmtdx.client._today_in_shanghai", return_value=20250109),
        patch.object(client, "get_history_minute_time_data", return_value=[MinuteBar(10, 1)]),
        patch.object(client, "get_security_bars", return_value=[bar()]),
    ):
        assert asyncio.run(invoke(client, "get_minute_time_data", Market.SH, "600000")) == []


@pytest.mark.parametrize("payload", [b"x", b"bad", b"\x00" * 10])
def test_nonempty_truncated_fund_flow_is_not_empty_success(payload):
    with pytest.raises(TdxDecodeError):
        GetHistoryFundFlowCmd(Market.SH, "600000", 0, 1).parse_response(payload)


def test_fund_flow_rejects_impossible_calendar_date():
    body = bytes(9) + struct.pack("<H9I", 1, 20260231, *([0] * 8))
    with pytest.raises(TdxDecodeError, match="日期"):
        GetHistoryFundFlowCmd(Market.SH, "600000", 0, 1).parse_response(body)


@pytest.mark.parametrize("payload", [b"x", b"\x00" * 3])
def test_nonempty_truncated_file_response_is_not_eof(payload):
    with pytest.raises(TdxDecodeError):
        GetBlockInfoCmd("file", 0, 10).parse_response(payload)


@pytest.mark.parametrize(
    "values", [(10, 10, 10, 1), (-1, 0, 0, 1), (1.5, 0, 0, 2), (float("nan"), 0, 0, 1)]
)
def test_market_statistics_rejects_invalid_counts(values):
    price, pre_close, low, high = values
    quote = SimpleNamespace(price=price, pre_close=pre_close, low=low, high=high, amount=0, vol=0)
    with pytest.raises(TdxResponseError):
        market_stat_from_quotes([quote])


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_price_limits_rejects_nonfinite_preclose(value):
    with pytest.raises(ValueError):
        compute_price_limits(Market.SH, "600000", "测试", value)


@pytest.mark.parametrize("days", [0, -1, 1.5, float("nan"), True])
def test_price_limits_rejects_invalid_listing_days(days):
    with pytest.raises(ValueError):
        compute_price_limits(Market.SH, "600000", "测试", 10, listed_days=days)


@pytest.mark.parametrize(
    "options",
    [
        {"timeout": 0},
        {"timeout": float("inf")},
        {"timeout": float("nan")},
        {"port": 0},
        {"port": 65536},
        {"port": 1.5},
        {"max_attempts": 1.5},
        {"max_attempts": True},
    ],
)
@pytest.mark.parametrize("kind", [TdxClient, AsyncTdxClient])
def test_connection_options_are_validated(kind, options):
    with pytest.raises(ValueError):
        kind(**options)


def test_context_manager_retries_backup_connection():
    first, second = MagicMock(), MagicMock()
    first.is_connected = second.is_connected = False
    first.connect.side_effect = TdxConnectionError("offline")
    with patch("xmtdx.client.TdxConnection", side_effect=[first, second]):
        with TdxClient("primary", fallback_hosts=["backup"]) as client:
            assert client._host == "backup"
    second.connect.assert_called_once()
    second.close.assert_called()


def test_async_context_manager_retries_backup_connection():
    async def run():
        first, second = MagicMock(), MagicMock()
        first.is_connected = second.is_connected = False
        first.connect = AsyncMock(side_effect=TdxConnectionError("offline"))
        first.close = AsyncMock()
        second.connect = AsyncMock()
        second.close = AsyncMock()
        with patch("xmtdx.client.AsyncTdxConnection", side_effect=[first, second]):
            async with AsyncTdxClient(
                "primary", fallback_hosts=["backup"], heartbeat_interval=0
            ) as client:
                assert client._host == "backup"
        second.connect.assert_awaited_once()
        second.close.assert_awaited()

    asyncio.run(run())


def test_invalid_bar_semantics_retries_backup():
    bad = bar()
    bad.high = 9
    first, second = MagicMock(), MagicMock()
    first.execute.return_value = [bad]
    second.execute.return_value = [bar()]
    with patch("xmtdx.client.TdxConnection", side_effect=[first, second]):
        client = TdxClient("primary", fallback_hosts=["backup"])
        assert client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0) == [bar()]
    first.close.assert_called()


def test_async_invalid_bar_semantics_retries_backup():
    async def run():
        bad = bar()
        bad.high = 9
        first, second = MagicMock(), MagicMock()
        first.execute = AsyncMock(return_value=[bad])
        first.close = AsyncMock()
        second.execute = AsyncMock(return_value=[bar()])
        second.close = AsyncMock()
        with patch("xmtdx.client.AsyncTdxConnection", side_effect=[first, second]):
            client = AsyncTdxClient("primary", fallback_hosts=["backup"], heartbeat_interval=0)
            assert await client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0) == [
                bar()
            ]
        first.close.assert_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("kind", [TdxClient, AsyncTdxClient])
def test_auto_reconnect_false_does_not_try_backup_on_connect(kind):
    first = MagicMock()
    first.is_connected = False
    if kind is AsyncTdxClient:
        first.connect = AsyncMock(side_effect=TdxConnectionError("offline"))
        first.close = AsyncMock()
    else:
        first.connect.side_effect = TdxConnectionError("offline")
    connection_name = "AsyncTdxConnection" if kind is AsyncTdxClient else "TdxConnection"
    with patch(f"xmtdx.client.{connection_name}", return_value=first) as factory:
        client = kind("primary", fallback_hosts=["backup"], auto_reconnect=False)
        with pytest.raises(TdxConnectionError):
            asyncio.run(invoke(client, "connect"))
        factory.assert_called_once()
        first.connect.assert_called_once()


def test_minute_validation_failure_is_not_swallowed(client):
    now = datetime(2025, 1, 8, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    with (
        patch("xmtdx.client._now_in_shanghai", return_value=now),
        patch("xmtdx.client._today_in_shanghai", return_value=20250108),
        patch.object(client, "get_security_bars", return_value=[bar()]),
        patch.object(client, "get_history_minute_time_data", return_value=[MinuteBar(10000, 1)]),
        patch.object(
            client, "get_security_quotes", return_value=[SimpleNamespace(pre_close=10, price=10)]
        ),
        pytest.raises(TdxResponseError, match="严重偏离"),
    ):
        asyncio.run(invoke(client, "get_minute_time_data", Market.SH, "600000"))


def test_missing_minute_reference_is_not_unverified_success(client):
    now = datetime(2025, 1, 8, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    with (
        patch("xmtdx.client._now_in_shanghai", return_value=now),
        patch("xmtdx.client._today_in_shanghai", return_value=20250108),
        patch.object(client, "get_security_bars", return_value=[bar()]),
        patch.object(client, "get_history_minute_time_data", return_value=[MinuteBar(10, 1)]),
        patch.object(client, "get_security_quotes", return_value=[]),
        pytest.raises(TdxResponseError, match="参考行情"),
    ):
        asyncio.run(invoke(client, "get_minute_time_data", Market.SH, "600000"))
