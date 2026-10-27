"""同步与异步客户端的错误传播、降级和分页完整性。"""

import asyncio
import inspect
from pathlib import Path
from unittest.mock import patch

import pytest

from xmtdx import AsyncTdxClient, KlineCategory, Market, TdxClient, to_dataframe
from xmtdx.codec.price_rules import compute_price_limits
from xmtdx.commands.security_quotes import GetSecurityQuotesCmd
from xmtdx.exceptions import TdxConnectionError, TdxDecodeError, TdxResponseError
from xmtdx.models import SecurityBar, SecurityInfo, TransactionRecord


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def client(request):
    if request.param:
        return AsyncTdxClient("127.0.0.1", heartbeat_interval=0)
    return TdxClient("127.0.0.1")


async def invoke(client, method, *args):
    result = getattr(client, method)(*args)
    return await result if inspect.isawaitable(result) else result


def bar(day=20):
    return SecurityBar(10, 10, 10, 10, 1, 1000, 2026, 1, day, 15, 0)


@pytest.mark.parametrize("error", [ValueError, TypeError, RuntimeError])
@pytest.mark.parametrize(
    "method,args,dependency",
    [
        ("get_history_fund_flow", (Market.SH, "600000", 0, 1), "_execute"),
        ("get_security_list_all", (), "get_report_file"),
        ("get_price_limits", (Market.SH, "600000", "浦发银行", 10), "get_security_bars"),
        ("get_minute_time_data", (Market.SH, "600000"), "get_history_minute_time_data"),
    ],
)
def test_program_errors_are_not_swallowed(client, error, method, args, dependency):
    with (
        patch.object(client, dependency, side_effect=error("bug")),
        pytest.raises(error, match="bug"),
    ):
        asyncio.run(invoke(client, method, *args))


@pytest.mark.parametrize("error", [TdxConnectionError, TdxDecodeError, TdxResponseError])
def test_history_fund_flow_protocol_errors_fall_back(client, error):
    records = [TransactionRecord(10, 0, 10, 1, 0, 0)]
    with (
        patch.object(client, "_execute", side_effect=error("unavailable")),
        patch.object(client, "get_security_bars", return_value=[bar()]),
        patch.object(client, "get_history_transaction_data", side_effect=[records, []]),
    ):
        flows = asyncio.run(invoke(client, "get_history_fund_flow", Market.SH, "600000", 0, 1))
    assert len(flows) == 1
    assert flows[0].small_in == 1000
    assert (flows[0].year, flows[0].month, flows[0].day) == (2026, 1, 20)


def test_history_fund_flow_rejects_missing_transactions(client):
    with (
        patch.object(client, "_execute", return_value=[]),
        patch.object(client, "get_security_bars", return_value=[bar()]),
        patch.object(client, "get_history_transaction_data", return_value=[]),
        pytest.raises(TdxResponseError, match="历史逐笔成交为空"),
    ):
        asyncio.run(invoke(client, "get_history_fund_flow", Market.SH, "600000", 0, 1))


def test_missing_industry_is_optional(client):
    stock = SecurityInfo(Market.SH, "600000", "浦发银行", 100, 2, 10)
    with (
        patch.object(client, "get_report_file", side_effect=TdxConnectionError("offline")),
        patch.object(client, "get_security_count", side_effect=[1, 0]),
        patch.object(client, "get_security_list", return_value=[stock]),
    ):
        assert asyncio.run(invoke(client, "get_security_list_all")) == [stock]


def test_missing_market_statistics_uses_response_error(client):
    with (
        patch.object(client, "get_security_quotes", return_value=[]),
        pytest.raises(TdxResponseError, match="市场统计"),
    ):
        asyncio.run(invoke(client, "get_market_stat"))


def test_bar_range_rejects_repeated_page(client):
    with (
        patch.object(client, "get_bars", return_value=[bar()] * 800),
        pytest.raises(TdxResponseError, match="分页停滞"),
    ):
        asyncio.run(invoke(client, "get_bars_range", Market.SH, "600000", 20260101, 20260131))


def test_bar_range_rejects_exhausted_page_budget(client):
    with (
        patch("xmtdx.client.MAX_PAGES", 1),
        patch.object(client, "get_bars", return_value=[bar()] * 800),
        pytest.raises(TdxResponseError, match="最大分页数"),
    ):
        asyncio.run(invoke(client, "get_bars_range", Market.SH, "600000", 20260101, 20260131))


def test_bar_range_sorts_deduplicates_and_filters(client):
    with patch.object(
        client, "get_bars", side_effect=[[bar(20)] * 800, [bar(20), bar(10), bar(1)]]
    ) as fetch:
        result = asyncio.run(
            invoke(client, "get_bars_range", Market.SH, "600000", 20260105, 20260131)
        )
    assert [record.day for record in result] == [10, 20]
    assert fetch.call_args_list[1].args == (Market.SH, "600000", KlineCategory.DAY, 800, 800)


def test_bar_range_preserves_index_models(client):
    from xmtdx.models import IndexBar

    index = IndexBar(10, 10, 10, 10, 1, 1000, 2026, 1, 20, 15, 0, up_count=5, down_count=3)
    with patch.object(client, "get_bars", return_value=[index]):
        result = asyncio.run(
            invoke(client, "get_bars_range", Market.SH, "000001", 20260101, 20260131)
        )
    assert result == [index]
    assert isinstance(result[0], IndexBar)
    assert result[0].up_count == 5


def test_index_routing_agrees_with_price_rules(client):
    with patch.object(client, "get_index_bars", return_value=[]) as fetch:
        assert (
            asyncio.run(invoke(client, "get_bars", Market.SH, "885001", KlineCategory.DAY, 0)) == []
        )
    fetch.assert_called_once()
    assert compute_price_limits(Market.SH, "885001", "", 10) == (None, None)


def test_quote_dataframe_uses_only_new_time_field():
    fixture = Path(__file__).parents[1] / "fixtures" / "security_quotes.hex"
    quotes = GetSecurityQuotesCmd([(Market.SH, "600000")]).parse_response(
        bytes.fromhex(fixture.read_text().strip())
    )
    frame = to_dataframe(quotes)
    assert "server_time" not in frame.columns
    assert frame.iloc[0]["quote_time"] == quotes[0].quote_time
    assert "_raw" not in frame.columns
    assert "_raw" in to_dataframe(quotes, include_raw=True).columns


def test_price_limits_rejects_removed_and_positional_arguments():
    with pytest.raises(TypeError):
        compute_price_limits(Market.SH, "600000", "浦发银行", 10, finance_info=None)
    with pytest.raises(TypeError):
        compute_price_limits(Market.SH, "600000", "浦发银行", 10, 6)
    assert compute_price_limits(Market.SH, "600000", "浦发银行", 10, listed_days=6) == (11, 9)


def test_fund_flow_rejects_incomplete_volume(client):
    from types import SimpleNamespace

    with (
        patch.object(client, "get_security_quotes", return_value=[SimpleNamespace(vol=2)]),
        patch.object(
            client,
            "get_transaction_data",
            side_effect=[[TransactionRecord(10, 0, 10, 1, 0, 0)], []],
        ),
        pytest.raises(TdxResponseError, match="覆盖不完整"),
    ):
        asyncio.run(invoke(client, "get_fund_flow", Market.SH, "600000"))


def test_transaction_collection_keeps_identical_trades_in_one_page(client):
    trade = TransactionRecord(10, 0, 10, 1, 0, 0)
    pages = [[trade, trade], []]

    def fetch(start, count):
        assert count == 800
        return pages.pop(0)

    async def fetch_async(start, count):
        return fetch(start, count)

    fetch_page = fetch_async if isinstance(client, AsyncTdxClient) else fetch
    assert asyncio.run(invoke(client, "_collect_transaction_records", fetch_page)) == [trade, trade]
