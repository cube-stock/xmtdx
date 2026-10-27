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
            _require(count > 0 and bars, "异步基础查询返回空数据")
            report["async_core"] = {
                "ok": True,
                "status": "pass",
                "elapsed_ms": round((time.monotonic() - started) * 1000),
                "count": count,
                "bars": len(bars),
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
        if full_list:
            _run_check(
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
                    client.get_history_fund_flow(Market.SH, "600000", 0, 1), "历史资金流为空"
                ),
            )
        else:
            for name in (
                "block_info",
                "report_file",
                "fund_flow_current",
                "fund_flow_history",
                "fund_flow_amount_diagnostic",
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
