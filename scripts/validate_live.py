"""对真实 7709 行情服务器执行可重复的能力矩阵验证。"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from xmtdx import KNOWN_HOSTS, AsyncTdxClient, KlineCategory, Market, TdxClient
from xmtdx._client_logic import validate_history_volume
from xmtdx._config import MAX_PAGES, TRANSACTION_PAGE_SIZE
from xmtdx._pagination import TransactionPages
from xmtdx.validation import validate_timeout


def _date(bar: Any) -> int:
    return bar.year * 10000 + bar.month * 100 + bar.day


def _require(value: Any, message: str) -> Any:
    if not value:
        raise RuntimeError(message)
    return value


def _run_check(
    report: dict[str, Any], name: str, operation: Callable[[], Any], *, warning: bool = False
) -> Any:
    started = time.monotonic()
    try:
        value = operation()
        size = len(value) if hasattr(value, "__len__") else None
        report[name] = {
            "ok": True,
            "status": "pass",
            "elapsed_ms": round((time.monotonic() - started) * 1000),
            "size": size,
        }
        return value
    except Exception as error:
        report[name] = {
            "ok": False,
            "status": "warn" if warning else "fail",
            "elapsed_ms": round((time.monotonic() - started) * 1000),
            "error": f"{type(error).__name__}: {error}",
        }
        return None


def _skip(checks: dict[str, Any], name: str, reason: str) -> None:
    checks[name] = {"ok": False, "status": "skipped", "reason": reason}


def _route_info(host: str) -> dict[str, Any]:
    """只读检查路由；不修改系统代理、VPN 或路由。"""
    command = (
        ["route", "-n", "get", host] if sys.platform == "darwin" else ["ip", "route", "get", host]
    )
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=2, check=False)
        match = re.search(r"interface:\s*(\S+)|\bdev\s+(\S+)", result.stdout)
        interface = next((part for part in match.groups() if part), None) if match else None
        tunnel = interface.startswith(("utun", "tun", "tap", "ppp", "wg")) if interface else None
        return {"host": host, "interface": interface, "tunnel": tunnel}
    except (OSError, subprocess.TimeoutExpired):
        return {"host": host, "interface": None, "tunnel": None}


def _network_environment(hosts: list[str]) -> dict[str, Any]:
    removed = []
    for key in list(os.environ):
        if key.lower().endswith("_proxy") and key.lower() != "no_proxy":
            del os.environ[key]
            removed.append(key)
    return {
        "removed_proxy_variables": sorted(removed),
        "socket_module": socket.socket.__module__,
        "routes": [_route_info(host) for host in dict.fromkeys(hosts)],
        "note": "原生 TCP 不使用系统 HTTP/SOCKS 代理；路由检查不能排除透明网络拦截。",
    }


def _report_success(report: dict[str, Any]) -> bool:
    checks = report["checks"]
    return (
        checks.get("host_probe", {}).get("status") == "pass"
        and checks.get("async_core", {}).get("status") == "pass"
        and not any(check["status"] == "fail" for check in checks.values())
    )


async def _validate_async(hosts: list[str], timeout: float) -> dict[str, Any]:
    report: dict[str, Any] = {}
    started = time.monotonic()
    try:
        client = await AsyncTdxClient.from_best_host(
            hosts, timeout=timeout, ping_timeout=timeout, heartbeat_interval=0
        )
        try:
            count = await client.get_security_count(Market.SH)
            bars = await client.get_bars(Market.SH, "600000", KlineCategory.DAY, 0, 2)
            quotes = await client.get_security_quotes([(Market.SH, "600000")])
            _require(count > 0 and bars and quotes, "异步基础查询返回空数据")
            report["async_core"] = {
                "ok": True,
                "status": "pass",
                "elapsed_ms": round((time.monotonic() - started) * 1000),
                "count": count,
                "bars": len(bars),
                "quotes": len(quotes),
            }
        finally:
            await client.close()
    except Exception as error:
        report["async_core"] = {
            "ok": False,
            "status": "fail",
            "elapsed_ms": round((time.monotonic() - started) * 1000),
            "error": f"{type(error).__name__}: {error}",
        }
    return report


def _check_market_stat(client: TdxClient) -> object:
    stat = client.get_market_stat()
    if (
        stat.up_count + stat.down_count + stat.neutral_count + stat.suspended_count
        != stat.total_count
    ):
        raise RuntimeError("市场计数不守恒")
    return stat


def _check_price_limits(client: TdxClient) -> list[tuple[float | None, float | None]]:
    samples = [
        (Market.SH, "600000", "浦发银行"),
        (Market.SZ, "300750", "宁德时代"),
        (Market.SH, "688981", "中芯国际"),
        (Market.SH, "999999", "上证指数"),
    ]
    quotes = client.get_security_quotes([(market, code) for market, code, _ in samples])
    if len(quotes) != len(samples):
        raise RuntimeError("涨跌停验证行情数量不符")
    results = []
    for quote, (market, code, name) in zip(quotes, samples, strict=True):
        limits = client.get_price_limits(market, code, name, quote.pre_close)
        if code == "999999":
            if limits != (None, None):
                raise RuntimeError("指数不应有涨跌停价")
        elif None in limits:
            raise RuntimeError(f"{code} 涨跌停价缺失")
        results.append(limits)
    return results


def _check_flow_amount(client: TdxClient, flow: Any) -> float:
    classified = sum(
        (
            flow.super_in,
            flow.large_in,
            flow.medium_in,
            flow.small_in,
            flow.super_out,
            flow.large_out,
            flow.medium_out,
            flow.small_out,
        )
    )
    quote = _require(client.get_security_quotes([(Market.SH, "600000")]), "缺少金额覆盖率参考行情")[
        0
    ]
    if not math.isfinite(classified) or not math.isfinite(quote.amount) or quote.amount <= 0:
        raise RuntimeError("金额参考值缺失或非法，无法计算覆盖率")
    if classified / quote.amount < 0.90:
        raise RuntimeError("分档金额低于行情快照成交额的 90%，需检查中性成交及盘中更新影响")
    return classified / quote.amount


def _check_quote_batching(client: TdxClient, securities: list[Any] | None = None) -> dict[str, int]:
    if securities is None:
        securities = client.get_security_list_all()
    stocks = [(record.market, record.code) for record in securities[:81]]
    _require(len(stocks) == 81, "证券列表不足以验证跨 80 只分片")
    quotes = client.get_security_quotes(stocks)
    _require([(q.market, q.code) for q in quotes] == stocks, "批量行情顺序或数量不符")
    return {"requested": len(stocks), "received": len(quotes)}


def _check_bar_range(client: TdxClient, history_date: int) -> dict[str, int]:
    recent = client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 10)
    expected = [bar for bar in recent if _date(bar) == history_date]
    actual = client.get_bars_range(Market.SH, "600000", history_date, history_date)
    _require(expected and actual == expected, "日期闭区间 K 线与直接查询不一致")
    return {"date": history_date, "bars": len(actual)}


def _check_history_consistency(client: TdxClient, history_date: int) -> dict[str, Any]:
    daily = client.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 10)
    bar = next((bar for bar in daily if _date(bar) == history_date), None)
    _require(bar, "缺少历史日线以核对数据")
    minutes = _require(
        client.get_history_minute_time_data(Market.SH, "600000", history_date), "历史分时为空"
    )
    minute_shares = sum(record.vol for record in minutes) * 100
    # 分时为整数手，日线为股；允许一手的取整差异。
    _require(abs(minute_shares - bar.vol) <= 100, "历史分时与日线成交量不一致")
    _require(abs(minutes[-1].price - bar.close) <= 0.005, "历史分时末价与日线收盘不一致")
    pages = TransactionPages()
    for _ in range(MAX_PAGES):
        records = client.get_history_transaction_data(
            Market.SH, "600000", history_date, pages.start, TRANSACTION_PAGE_SIZE
        )
        if pages.add(records):
            break
    else:
        raise RuntimeError("历史逐笔超过最大分页数")
    _require(pages.records, "历史逐笔为空")
    validate_history_volume(pages.records, bar)
    for record in pages.records:
        _require(bar.low - 0.005 <= record.price <= bar.high + 0.005, "历史逐笔超出日线高低价")
    return {
        "date": history_date,
        "daily_shares": bar.vol,
        "minute_shares": minute_shares,
        "minute_close": minutes[-1].price,
        "daily_close": bar.close,
        "transaction_records": len(pages.records),
        "transaction_shares": sum(record.vol for record in pages.records) * 100,
        "transaction_amount": sum(record.price * record.vol * 100 for record in pages.records),
        "daily_amount": bar.amount,
    }


def _check_history_totals(details: dict[str, Any]) -> None:
    # 覆盖完整不等于口径完全相同；差异单独报告，不能伪装为完全一致。
    _require(
        abs(details["transaction_shares"] - details["daily_shares"]) <= 100,
        "逐笔与日线成交量总计不一致；需核对盘后记录及服务端口径，不能宣称精确一致",
    )
    _require(
        math.isclose(details["transaction_amount"], details["daily_amount"], rel_tol=1e-6),
        "逐笔与日线成交额总计不一致；需核对手数取整及服务端口径",
    )


def validate(hosts: list[str], timeout: float, full_list: bool, quick: bool) -> dict[str, Any]:
    validate_timeout(timeout)
    network = _network_environment(hosts)
    report: dict[str, Any] = {
        "hosts": hosts,
        "started_at": datetime.now().astimezone().isoformat(),
        "network": network,
        "checks": {},
    }
    checks: dict[str, Any] = report["checks"]
    dependent = [
        "connect",
        "security_count_sh",
        "security_count_sz",
        "security_count_bj",
        "security_list_all",
        "quotes_sh",
        "quotes_sz",
        "quotes_bj",
        "index_bars",
        "minute_current",
        "minute_history",
        "transaction_current",
        "transaction_history",
        "finance",
        "xdxr",
        "company_categories",
        "company_content",
        "market_stat",
        "price_limits",
        "block_info",
        "report_file",
        "fund_flow_current",
        "fund_flow_history",
        "fund_flow_amount_diagnostic",
        "async_core",
        "quote_batching",
        "bar_range",
        "history_consistency",
        "history_trade_totals",
    ] + [f"stock_bars_{category.name.lower()}" for category in KlineCategory]
    for name in dependent:
        _skip(checks, name, "前置检查尚未通过")
    routes = network["routes"]
    if any(route.get("tunnel") for route in routes):
        checks["network_route"] = {
            "ok": False,
            "status": "fail",
            "error": "目标路由经过 VPN/TUN，未执行直连验证",
        }
        _skip(checks, "host_probe", "需要先使目标路由绕过 VPN/TUN")
        return report
    route_known = bool(routes) and all(route.get("interface") for route in routes)
    checks["network_route"] = {
        "ok": route_known,
        "status": "pass" if route_known else "warn",
        "reason": "目标路由未发现隧道接口" if route_known else "无法确认目标路由，直连状态尚未验证",
    }
    ranked = _run_check(
        checks,
        "host_probe",
        lambda: _require(
            TdxClient.ping_all(hosts, 7709, timeout), "没有通过握手和证券数量查询的主机"
        ),
    )
    if not ranked:
        return report
    ordered = [host for host, _latency in ranked]
    client = TdxClient(
        ordered[0], timeout=timeout, fallback_hosts=ordered[1:], max_attempts=min(2, len(ordered))
    )
    try:
        _run_check(checks, "connect", client.connect)
        if checks["connect"]["status"] != "pass":
            return report
        sock = client._conn._sock
        network["connection"] = {
            "host": client._host,
            "tcp_peer": sock.getpeername() if sock is not None else None,
        }
        for market in Market:
            _run_check(
                checks,
                f"security_count_{market.name.lower()}",
                lambda market=market: _require(client.get_security_count(market), "证券数量为 0"),
            )
        securities = None
        if full_list:
            securities = _run_check(
                checks,
                "security_list_all",
                lambda: _require(client.get_security_list_all(), "沪深 A 股列表为空"),
            )
        else:
            _skip(checks, "security_list_all", "未指定 --full-list")
        for market, code in [(Market.SH, "600000"), (Market.SZ, "000001"), (Market.BJ, "920002")]:
            _run_check(
                checks,
                f"quotes_{market.name.lower()}",
                lambda market=market, code=code: _require(
                    client.get_security_quotes([(market, code)]), "行情为空"
                ),
            )
        stock_bars = {}
        for category in KlineCategory:
            stock_bars[category.name] = _run_check(
                checks,
                f"stock_bars_{category.name.lower()}",
                lambda category=category: _require(
                    client.get_security_bars(Market.SH, "600000", category, 0, 2),
                    f"{category.name} K 线为空",
                ),
            )
        _run_check(
            checks,
            "index_bars",
            lambda: _require(
                client.get_index_bars(Market.SH, "000001", KlineCategory.DAY, 0, 5), "指数 K 线为空"
            ),
        )
        daily = stock_bars.get("DAY")
        today = int(datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d"))
        historical_dates = [_date(bar) for bar in daily if _date(bar) < today] if daily else []
        _run_check(
            checks, "minute_current", lambda: client.get_minute_time_data(Market.SH, "600000")
        )
        if historical_dates:
            history_date = max(historical_dates)
            _run_check(
                checks,
                "minute_history",
                lambda: _require(
                    client.get_history_minute_time_data(Market.SH, "600000", history_date),
                    "历史分时为空",
                ),
            )
            _run_check(
                checks,
                "transaction_history",
                lambda: _require(
                    client.get_history_transaction_data(Market.SH, "600000", history_date, 0, 20),
                    "历史逐笔为空",
                ),
            )
        else:
            for name in ("minute_history", "transaction_history"):
                _skip(checks, name, "未取得过去交易日的日 K 线")
        _run_check(
            checks,
            "transaction_current",
            lambda: _require(
                client.get_transaction_data(Market.SH, "600000", 0, 20), "当日逐笔为空"
            ),
        )
        _run_check(checks, "finance", lambda: client.get_finance_info(Market.SH, "600000"))
        _run_check(checks, "xdxr", lambda: client.get_xdxr_info(Market.SH, "600000"))
        categories = _run_check(
            checks,
            "company_categories",
            lambda: _require(
                client.get_company_info_category(Market.SH, "600000"), "公司资料目录为空"
            ),
        )
        if categories and categories[0].length > 0:
            first = categories[0]
            _run_check(
                checks,
                "company_content",
                lambda: _require(
                    client.get_company_info_content(
                        Market.SH, "600000", first.filename, first.start, min(first.length, 4096)
                    ),
                    "公司资料内容为空",
                ),
            )
        else:
            _skip(checks, "company_content", "目录不可用或首个条目为空")
        _run_check(checks, "market_stat", lambda: _check_market_stat(client))
        _run_check(checks, "price_limits", lambda: _check_price_limits(client))
        if not quick:
            details = _run_check(
                checks, "quote_batching", lambda: _check_quote_batching(client, securities)
            )
            if details is not None:
                checks["quote_batching"]["details"] = details
            if historical_dates:
                history_date = max(historical_dates)
                details = _run_check(
                    checks, "bar_range", lambda: _check_bar_range(client, history_date)
                )
                if details is not None:
                    checks["bar_range"]["details"] = details
                details = _run_check(
                    checks, "history_consistency",
                    lambda: _check_history_consistency(client, history_date),
                )
                if details is not None:
                    checks["history_consistency"]["details"] = details
                    _run_check(
                        checks, "history_trade_totals", lambda: _check_history_totals(details),
                        warning=True,
                    )
            else:
                for name in ("bar_range", "history_consistency", "history_trade_totals"):
                    _skip(checks, name, "未取得过去交易日的日 K 线")
            _run_check(
                checks,
                "block_info",
                lambda: _require(client.get_block_info("block_gn.dat"), "板块文件为空"),
            )
            _run_check(
                checks,
                "report_file",
                lambda: _require(client.get_report_file("tdxhy.cfg"), "报表文件为空"),
            )
            flow = _run_check(
                checks, "fund_flow_current", lambda: client.get_fund_flow(Market.SH, "600000")
            )
            if flow is not None:
                _run_check(
                    checks,
                    "fund_flow_amount_diagnostic",
                    lambda: _check_flow_amount(client, flow),
                    warning=True,
                )
            _run_check(
                checks,
                "fund_flow_history",
                lambda: _require(
                    client.get_history_fund_flow(Market.SH, "600000", 1, 1), "历史资金流为空"
                ),
            )
        else:
            for name in (
                "block_info",
                "report_file",
                "fund_flow_current",
                "fund_flow_history",
                "fund_flow_amount_diagnostic",
                "quote_batching",
                "bar_range",
                "history_consistency",
                "history_trade_totals",
            ):
                _skip(checks, name, "--quick 跳过耗时检查")
    finally:
        client.close()
    checks.update(asyncio.run(_validate_async(ordered[:2], timeout)))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", action="append", dest="hosts", help="候选服务器，可重复")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--full-list", action="store_true", help="额外扫描沪深 A 股完整列表")
    parser.add_argument("--quick", action="store_true", help="跳过文件下载和资金流等耗时检查")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    args = parser.parse_args()

    try:
        validate_timeout(args.timeout)
    except ValueError as error:
        parser.error(str(error))
    report = validate(args.hosts or KNOWN_HOSTS, args.timeout, args.full_list, args.quick)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for name, result in report["checks"].items():
            marker = result["status"].upper()
            detail = result.get("error", result.get("reason", f"{result.get('elapsed_ms')} ms"))
            print(f"{marker:4} {name:30} {detail}")
    return 0 if _report_success(report) else 1


if __name__ == "__main__":
    raise SystemExit(main())
