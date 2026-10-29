"""真实通达信服务器 smoke test。

默认跳过；设置 XMTDX_LIVE=1 后执行。
"""

from __future__ import annotations

import asyncio
import os

import pytest

from scripts.validate_live import _network_environment
from xmtdx import AsyncTdxClient, KlineCategory, Market, TdxClient

_LIVE_ENABLED = os.getenv("XMTDX_LIVE") == "1"
_LIVE_HOST = os.getenv("XMTDX_HOST", "180.153.18.170")

pytestmark = pytest.mark.skipif(
    not _LIVE_ENABLED,
    reason="set XMTDX_LIVE=1 to run live integration tests",
)


@pytest.fixture(autouse=True)
def direct_network():
    network = _network_environment([_LIVE_HOST])
    if any(route["tunnel"] for route in network["routes"]):
        pytest.fail("目标路由经过 VPN/TUN，需要先使目标直连")
    if not all(route["interface"] for route in network["routes"]):
        pytest.skip("无法确认目标路由，未执行直连验证")


def test_sync_live_smoke() -> None:
    with TdxClient(_LIVE_HOST, timeout=5.0) as client:
        assert client.get_security_count(Market.SH) > 0
        assert client.get_security_count(Market.SZ) > 0
        quotes = client.get_security_quotes([(Market.SH, "600000"), (Market.SZ, "000001")])
        assert [quote.code for quote in quotes] == ["600000", "000001"]
        assert all(quote.price > 0 for quote in quotes)
        assert client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 3)
        assert client.get_index_bars(Market.SH, "000001", KlineCategory.DAY, 0, 3)


def test_async_live_smoke() -> None:
    async def main() -> None:
        async with AsyncTdxClient(_LIVE_HOST, timeout=5.0) as client:
            assert await client.get_security_count(Market.SH) > 0
            assert await client.get_security_count(Market.SZ) > 0
            quotes = await client.get_security_quotes(
                [(Market.SH, "600000"), (Market.SZ, "000001")]
            )
            assert [quote.code for quote in quotes] == ["600000", "000001"]
            assert all(quote.price > 0 for quote in quotes)
            assert await client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 3)
            assert await client.get_index_bars(Market.SH, "000001", KlineCategory.DAY, 0, 3)

    asyncio.run(main())


def test_sync_async_historical_results_match() -> None:
    with TdxClient(_LIVE_HOST, timeout=5) as client:
        bars = client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 1, 3)
        indices = client.get_index_bars(Market.SH, "000001", KlineCategory.DAY, 1, 3)
        date = bars[-1].year * 10000 + bars[-1].month * 100 + bars[-1].day
        minutes = client.get_history_minute_time_data(Market.SH, "600000", date)
        trades = client.get_history_transaction_data(Market.SH, "600000", date, 0, 20)
        ranged = client.get_bars_range(Market.SH, "600000", date, date)

    async def main():
        async with AsyncTdxClient(_LIVE_HOST, timeout=5, heartbeat_interval=0) as client:
            assert await client.get_security_bars(
                Market.SH, "600000", KlineCategory.DAY, 1, 3
            ) == bars
            assert await client.get_index_bars(
                Market.SH, "000001", KlineCategory.DAY, 1, 3
            ) == indices
            assert await client.get_history_minute_time_data(Market.SH, "600000", date) == minutes
            assert await client.get_history_transaction_data(
                Market.SH, "600000", date, 0, 20
            ) == trades
            assert await client.get_bars_range(Market.SH, "600000", date, date) == ranged

    asyncio.run(main())
