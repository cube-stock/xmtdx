"""使用本地参考项目离线核对同一批公开响应，不访问网络或安装依赖。"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "protocol_4"
BAR_FIELDS = ("open", "close", "high", "low", "vol", "amount")
QUOTE_FIELDS = ("price", "pre_close", "open", "high", "low", "vol", "amount")


def _capture(name: str) -> dict[str, Any]:
    capture: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text())
    return capture


def _equal(actual: object, expected: object, context: str) -> None:
    if actual != expected:
        raise ValueError(f"{context}: actual={actual!r}, expected={expected!r}")


def compare_pytdxdata(root: Path) -> dict[str, int]:
    sys.path.insert(0, str(root / "src"))
    bars_module = importlib.import_module("pytdxdata.protocol.commands.kline")
    quotes_module = importlib.import_module("pytdxdata.protocol.commands.quotes")
    bars_count = 0
    for name, market, code in (
        ("bars_sh", 1, "600000"), ("bars_sz", 0, "000001"), ("bars_index", 1, "000001")
    ):
        capture = _capture(name)
        factory = (bars_module.GetIndexBarsCmd if name == "bars_index"
                   else bars_module.GetSecurityBarsCmd)
        records = factory(market, code, 4, 1, 3).parse_response(bytes.fromhex(capture["body"]))
        _equal(len(records), len(capture["expected"]), name)
        for record, expected in zip(records, capture["expected"], strict=True):
            for field in BAR_FIELDS:
                _equal(getattr(record, field), expected[field], f"{name}.{field}")
            date = f"{record.year:04}-{record.month:02}-{record.day:02}"
            _equal(date, expected["datetime"][:10], f"{name}.date")
        bars_count += len(records)
    capture = _capture("quotes")
    stocks = [(1, "600000"), (0, "000001"), (2, "920002")]
    quotes = quotes_module.GetSecurityQuotesCmd(stocks).parse_response(
        bytes.fromhex(capture["body"])
    )
    _equal(len(quotes), len(capture["expected"]), "quotes.count")
    for quote, expected in zip(quotes, capture["expected"], strict=True):
        for field in ("market", "code", *QUOTE_FIELDS):
            _equal(getattr(quote, field), expected[field], f"quotes.{field}")
        for side in ("bid", "ask"):
            for level in range(1, 6):
                price, volume = getattr(quote, side)[level - 1]
                _equal(price, expected[f"{side}{level}"], f"quotes.{side}{level}")
                _equal(volume, expected[f"{side}_vol{level}"], f"quotes.{side}_vol{level}")
    return {"bars": bars_count, "quotes": len(quotes)}


def compare_tdxdata(root: Path) -> dict[str, int]:
    sys.path.insert(0, str(root / "src"))
    bars_module = importlib.import_module("tdxdata.commands.bars")
    quotes_module = importlib.import_module("tdxdata.commands.quotes")
    bars_count = 0
    for name in ("bars_sh", "bars_sz"):
        capture = _capture(name)
        records = bars_module.parse_bars_response(bytes.fromhex(capture["body"]), 4)
        _equal(len(records), len(capture["expected"]), name)
        for record, expected in zip(records, capture["expected"], strict=True):
            for field in BAR_FIELDS:
                _equal(record[field], expected[field], f"{name}.{field}")
            _equal(record["datetime"][:10], expected["datetime"][:10], f"{name}.date")
        bars_count += len(records)
    # 该参考版本仅接受 1/2/5 条报价；不能拿三市场包证明其支持任意批量。
    capture = _capture("quote_sh")
    decompressed = bytes.fromhex(capture["header"] + capture["body"])
    # 参考解析器直接消费帧头加解压负载；不依赖其启发式 zlib 搜索。
    quotes = quotes_module.parse_quotes(decompressed)
    _equal(len(quotes), 1, "quote_sh.count")
    quote, expected = quotes[0], capture["expected"][0]
    for field in ("market", "code", *QUOTE_FIELDS):
        reference_field = "last_close" if field == "pre_close" else field
        _equal(quote[reference_field], expected[field], f"quote_sh.{field}")
    for side in ("bid", "ask"):
        for level in range(1, 6):
            _equal(quote[side][level - 1], expected[f"{side}{level}"], f"quote_sh.{side}{level}")
            _equal(quote[f"{side}_vol"][level - 1], expected[f"{side}_vol{level}"], side)
    return {"bars": bars_count, "quotes": len(quotes)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pytdxdata", type=Path, help="已检查的 pytdxdata 源码根目录")
    parser.add_argument("--tdxdata", type=Path, help="已检查的 tdxdata 源码根目录")
    args = parser.parse_args()
    if args.pytdxdata is None and args.tdxdata is None:
        parser.error("至少提供一个本地参考项目目录")
    report: dict[str, Any] = {}
    for name, root, operation in (
        ("pytdxdata", args.pytdxdata, compare_pytdxdata),
        ("tdxdata", args.tdxdata, compare_tdxdata),
    ):
        if root is None:
            continue
        try:
            if not (root / "src" / name).is_dir():
                raise ValueError(f"参考源码目录不存在: {root / 'src' / name}")
            report[name] = {"status": "pass", "matched": operation(root.resolve())}
        except Exception as error:
            report[name] = {"status": "fail", "error": f"{type(error).__name__}: {error}"}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return int(any(value["status"] != "pass" for value in report.values()))


if __name__ == "__main__":
    raise SystemExit(main())
