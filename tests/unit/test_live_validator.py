"""在线验证器的报告语义；所有网络行为均模拟。"""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts import validate_live as live
from xmtdx import KlineCategory
from xmtdx.exceptions import TdxDecodeError


def test_no_hosts_is_failure_and_dependents_are_skipped():
    with (
        patch.object(live.TdxClient, "ping_all", return_value=[]),
        patch.object(live, "_network_environment", return_value={"routes": []}),
    ):
        report = live.validate(["offline"], 1, False, True)
    assert report["checks"]["host_probe"]["status"] == "fail"
    assert report["checks"]["quotes_sz"]["status"] == "skipped"
    assert not live._report_success(report)


def test_proxy_variables_removed_without_exposing_values(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://secret:password@127.0.0.1:7890")
    monkeypatch.setenv("all_proxy", "socks5://127.0.0.1:7890")
    monkeypatch.setenv("NO_PROXY", "localhost")
    with patch.object(live, "_route_info", return_value={"interface": "en0", "tunnel": False}):
        result = live._network_environment(["host"])
    assert "HTTP_PROXY" not in live.os.environ
    assert "all_proxy" not in live.os.environ
    assert live.os.environ["NO_PROXY"] == "localhost"
    assert "secret" not in json.dumps(result)
    assert result["routes"][0]["interface"] == "en0"


def test_tunnel_route_prevents_misleading_direct_test():
    with (
        patch.object(
            live,
            "_network_environment",
            return_value={"routes": [{"interface": "utun4", "tunnel": True}]},
        ),
        patch.object(live.TdxClient, "ping_all") as probe,
    ):
        report = live.validate(["host"], 1, False, True)
    assert report["checks"]["network_route"]["status"] == "fail"
    probe.assert_not_called()


def test_one_category_failure_does_not_discard_daily_results():
    client = SimpleNamespace()
    client.connect = lambda: None
    client.close = lambda: None
    client._host = "host"
    client._conn = SimpleNamespace(_sock=None)
    client.get_security_count = lambda market: 1
    client.get_security_quotes = lambda stocks: [SimpleNamespace(pre_close=10)]
    client.get_security_bars = lambda market, code, category, start, count: (
        (_ for _ in ()).throw(TdxDecodeError("minute unavailable"))
        if category == KlineCategory.MIN_5
        else [SimpleNamespace(year=2025, month=1, day=8)]
    )
    client.get_index_bars = lambda *args: [1]
    client.get_minute_time_data = lambda *args: []
    client.get_history_minute_time_data = lambda *args: [1]
    client.get_history_transaction_data = lambda *args: [1]
    client.get_transaction_data = lambda *args: [1]
    client.get_finance_info = lambda *args: object()
    client.get_xdxr_info = lambda *args: []
    client.get_company_info_category = lambda *args: []
    with (
        patch.object(live, "_network_environment", return_value={"routes": []}),
        patch.object(live.TdxClient, "ping_all", return_value=[("host", 0.1)]),
        patch.object(live, "TdxClient", return_value=client) as kind,
        patch.object(live, "_check_market_stat", return_value=object()),
        patch.object(live, "_check_price_limits", return_value=[]),
        patch.object(
            live, "_validate_async", return_value={"async_core": {"ok": True, "status": "pass"}}
        ),
    ):
        kind.ping_all.return_value = [("host", 0.1)]
        report = live.validate(["host"], 1, False, True)
    checks = report["checks"]
    assert checks["stock_bars_min_5"]["status"] == "fail"
    assert checks["stock_bars_day"]["status"] == "pass"
    assert checks["minute_history"]["status"] == "pass"
    assert checks["company_content"]["status"] == "skipped"


def test_coverage_warning_does_not_override_volume_validation():
    checks = {}
    live._run_check(
        checks,
        "amount_diagnostic",
        lambda: (_ for _ in ()).throw(RuntimeError("neutral trades")),
        warning=True,
    )
    assert checks["amount_diagnostic"]["status"] == "warn"
    assert not checks["amount_diagnostic"]["ok"]


def test_amount_diagnostic_without_valid_denominator_is_warning():
    client = SimpleNamespace(get_security_quotes=lambda stocks: [SimpleNamespace(amount=0)])
    flow = SimpleNamespace(
        super_in=0,
        large_in=0,
        medium_in=0,
        small_in=0,
        super_out=0,
        large_out=0,
        medium_out=0,
        small_out=0,
    )
    checks = {}
    live._run_check(checks, "amount", lambda: live._check_flow_amount(client, flow), warning=True)
    assert checks["amount"]["status"] == "warn"


def test_quote_batching_uses_full_list_instead_of_assuming_first_page_contains_stocks():
    stocks = [SimpleNamespace(market=1, code=f"{600000 + index}") for index in range(81)]
    client = SimpleNamespace(get_security_quotes=lambda requested: stocks)
    assert live._check_quote_batching(client, stocks) == {"requested": 81, "received": 81}


def test_quote_batching_rejects_reordered_results():
    stocks = [SimpleNamespace(market=1, code=f"{600000 + index}") for index in range(81)]
    client = SimpleNamespace(get_security_quotes=lambda requested: stocks[::-1])
    with pytest.raises(RuntimeError, match="顺序或数量"):
        live._check_quote_batching(client, stocks)


def test_history_total_differences_do_not_claim_exact_match():
    details = {"daily_shares": 1000, "transaction_shares": 1200,
               "daily_amount": 10000, "transaction_amount": 12000}
    with pytest.raises(RuntimeError, match="不能宣称精确一致"):
        live._check_history_totals(details)
