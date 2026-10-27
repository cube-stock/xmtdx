"""客户端共享的纯计算逻辑；不执行网络 IO。"""

import math
from collections.abc import Iterator, Sequence
from datetime import datetime
from zoneinfo import ZoneInfo

from ._config import QUOTE_PAGE_SIZE
from .exceptions import TdxResponseError
from .models import (
    FundFlow,
    HistoricalFundFlow,
    Market,
    MarketStat,
    MinuteBar,
    SecurityBar,
    SecurityInfo,
    SecurityQuote,
    TransactionRecord,
)

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _today_in_shanghai() -> int:
    return int(datetime.now(_SHANGHAI_TZ).strftime("%Y%m%d"))


def _now_in_shanghai() -> datetime:
    return datetime.now(_SHANGHAI_TZ)


def _validate_fund_flow_coverage(records: list[TransactionRecord], quote: SecurityQuote) -> None:
    """保证逐笔至少覆盖取得行情快照时已经公布的成交量。"""
    transaction_volume = sum(record.vol for record in records)
    if transaction_volume + 1e-6 < quote.vol:
        raise TdxResponseError(
            "当日逐笔成交覆盖不完整: "
            f"transaction_volume={transaction_volume}, quote_volume={quote.vol}"
        )


def _classify_fund_flow(records: list[TransactionRecord]) -> FundFlow:
    stats = {
        "super_in": 0.0,
        "large_in": 0.0,
        "medium_in": 0.0,
        "small_in": 0.0,
        "super_out": 0.0,
        "large_out": 0.0,
        "medium_out": 0.0,
        "small_out": 0.0,
    }

    for record in records:
        amount = record.price * record.vol * 100.0
        direction = "in" if record.buyorsell == 0 else "out" if record.buyorsell == 1 else None
        if not direction:
            continue

        if amount > 1_000_000:
            stats[f"super_{direction}"] += amount
        elif amount > 200_000:
            stats[f"large_{direction}"] += amount
        elif amount > 40_000:
            stats[f"medium_{direction}"] += amount
        else:
            stats[f"small_{direction}"] += amount

    return FundFlow(**stats)


def _date_from_bar(bar: SecurityBar) -> int:
    return bar.year * 10000 + bar.month * 100 + bar.day


def _unique_hosts(primary: str, fallbacks: Sequence[str]) -> list[str]:
    hosts: list[str] = []
    for host in (primary, *fallbacks):
        if host and host not in hosts:
            hosts.append(host)
    if not hosts:
        raise ValueError("至少需要一个行情服务器")
    return hosts


def _validate_bars(bars: Sequence[SecurityBar], context: str) -> None:
    for index, bar in enumerate(bars):
        values = (bar.open, bar.close, bar.high, bar.low, bar.vol, bar.amount)
        if not all(math.isfinite(value) for value in values):
            raise TdxResponseError(f"{context}[{index}] 包含非有限数值")
        try:
            datetime(bar.year, bar.month, bar.day)
        except ValueError:
            raise TdxResponseError(
                f"{context}[{index}] 日期非法: {bar.year}-{bar.month}-{bar.day}"
            ) from None
        if not 1990 <= bar.year <= 2100:
            raise TdxResponseError(f"{context}[{index}] 日期非法: {bar.year}-{bar.month}-{bar.day}")
        if bar.vol < 0 or bar.amount < 0:
            raise TdxResponseError(f"{context}[{index}] 成交量或成交额为负")
        if bar.high + 0.001 < max(bar.open, bar.close, bar.low):
            raise TdxResponseError(f"{context}[{index}] 最高价关系非法")
        if bar.low - 0.001 > min(bar.open, bar.close, bar.high):
            raise TdxResponseError(f"{context}[{index}] 最低价关系非法")


def _validate_minute_bars(
    bars: Sequence[MinuteBar], context: str, reference_price: float | None = None
) -> None:
    for index, bar in enumerate(bars):
        if not math.isfinite(bar.price) or bar.price <= 0:
            raise TdxResponseError(f"{context}[{index}] 价格非法: {bar.price}")
        if bar.vol < 0:
            raise TdxResponseError(f"{context}[{index}] 成交量为负: {bar.vol}")
        if reference_price and not reference_price / 100 <= bar.price <= reference_price * 100:
            raise TdxResponseError(
                f"{context}[{index}] 价格 {bar.price} 与参考价 {reference_price} 严重偏离"
            )


def _before_a_share_session(now: datetime) -> bool:
    return now.weekday() >= 5 or (now.hour, now.minute) < (9, 15)


def _historical_fund_flow_from_records(
    date: int, records: list[TransactionRecord]
) -> HistoricalFundFlow:
    return _historical_fund_flow_from_flow(date, _classify_fund_flow(records))


def _historical_fund_flow_from_flow(date: int, flow: FundFlow) -> HistoricalFundFlow:
    year = date // 10000
    month = (date // 100) % 100
    day = date % 100
    return HistoricalFundFlow(
        year=year,
        month=month,
        day=day,
        super_in=flow.super_in,
        super_out=flow.super_out,
        large_in=flow.large_in,
        large_out=flow.large_out,
        medium_in=flow.medium_in,
        medium_out=flow.medium_out,
        small_in=flow.small_in,
        small_out=flow.small_out,
    )


def filter_a_shares(
    stocks: list[SecurityInfo], market: Market, industries: dict[str, tuple[str, str]]
) -> list[SecurityInfo]:
    prefixes = {Market.SH: ("60", "68"), Market.SZ: ("00", "30")}.get(market, ())
    result = []
    for stock in stocks:
        if stock.code.startswith(prefixes):
            if stock.code in industries:
                stock.industry_tdx, stock.industry_sw = industries[stock.code]
            result.append(stock)
    return result


def quote_batches(stocks: list[tuple[Market, str]]) -> Iterator[list[tuple[Market, str]]]:
    if not stocks:
        raise ValueError("stocks 不能为空")
    if len(set(stocks)) != len(stocks):
        raise ValueError("stocks 不能包含重复证券")
    for start in range(0, len(stocks), QUOTE_PAGE_SIZE):
        yield stocks[start : start + QUOTE_PAGE_SIZE]


def market_stat_from_quotes(quotes: list[SecurityQuote]) -> MarketStat:
    if not quotes:
        raise TdxResponseError("无法获取市场统计数据")
    quote = quotes[0]
    up, down, neutral, total = map(int, (quote.price, quote.pre_close, quote.low, quote.high))
    return MarketStat(
        up_count=up,
        down_count=down,
        neutral_count=neutral,
        suspended_count=max(0, total - up - down - neutral),
        total_count=total,
        total_amount=quote.amount,
        total_volume=quote.vol,
    )
