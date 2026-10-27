"""命令布局、证券身份与参数校验测试。"""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from xmtdx import KlineCategory, Market
from xmtdx.codec.price import put_price
from xmtdx.commands.finance_info import GetFinanceInfoCmd
from xmtdx.commands.security_bars import GetIndexBarsCmd
from xmtdx.commands.security_quotes import GetSecurityQuotesCmd
from xmtdx.commands.transaction import GetTransactionDataCmd
from xmtdx.exceptions import TdxDecodeError, TdxResponseError
from xmtdx.models.bar import IndexBar
from xmtdx.validation import validate_code, validate_date, validate_filename

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _index_record(
    date: int,
    open_diff: int,
    close_diff: int,
    high_diff: int,
    low_diff: int,
    up_count: int,
    down_count: int,
) -> bytes:
    return b"".join(
        (
            struct.pack("<I", date),
            put_price(open_diff),
            put_price(close_diff),
            put_price(high_diff),
            put_price(low_diff),
            b"\x00" * 8,
            struct.pack("<HH", up_count, down_count),
        )
    )


def test_index_bars_parse_breadth_without_losing_record_alignment() -> None:
    body = struct.pack("<H", 2)
    body += _index_record(20260814, 100_000, 1_000, 2_000, -1_000, 3210, 1900)
    body += _index_record(20260815, 500, -500, 1_000, -1_500, 3000, 2100)

    bars = GetIndexBarsCmd(Market.SH, "000001", KlineCategory.DAY, 0, 2).parse_response(body)

    assert [(bar.year, bar.month, bar.day) for bar in bars] == [
        (2026, 8, 14),
        (2026, 8, 15),
    ]
    assert [(bar.open, bar.close) for bar in bars] == [(100.0, 101.0), (101.5, 101.0)]
    assert [(bar.up_count, bar.down_count) for bar in bars] == [(3210, 1900), (3000, 2100)]
    assert all(isinstance(bar, IndexBar) for bar in bars)


def test_current_transaction_retains_num_orders() -> None:
    body = bytes.fromhex((FIXTURES / "transaction.hex").read_text().strip())
    records = GetTransactionDataCmd(Market.SH, "600000", 0, 800).parse_response(body)
    assert records
    assert all(record.num_orders is not None for record in records)


def test_transaction_page_size_is_capped_at_800() -> None:
    with pytest.raises(ValueError, match="count.*800"):
        GetTransactionDataCmd(Market.SH, "600000", 0, 801)


def test_quote_and_finance_reject_mismatched_security() -> None:
    quote_body = bytes.fromhex((FIXTURES / "security_quotes.hex").read_text().strip())
    with pytest.raises(TdxResponseError, match="不匹配"):
        GetSecurityQuotesCmd([(Market.SZ, "000001")]).parse_response(quote_body)

    finance_body = bytes.fromhex((FIXTURES / "finance_info.hex").read_text().strip())
    with pytest.raises(TdxResponseError, match="不匹配"):
        GetFinanceInfoCmd(Market.SZ, "000001").parse_response(finance_body)


def test_corrected_kline_category_values() -> None:
    assert KlineCategory.MIN_1_ALT == 8
    assert KlineCategory.DAY_ALT == 9
    assert KlineCategory.SEASON == 10
    assert KlineCategory.YEAR == 11
    assert not hasattr(KlineCategory, "MIN_3")
    assert not hasattr(KlineCategory, "YEAR_ALT")


def test_public_argument_validation() -> None:
    for invalid in ("60000", "ABCDEF", "６０００００"):
        with pytest.raises(ValueError):
            validate_code(invalid)
    with pytest.raises(ValueError):
        validate_date(20260230)
    with pytest.raises(ValueError):
        validate_date(202681)
    with pytest.raises(ValueError):
        validate_filename("bad\x00name", 100, "ascii")


@pytest.mark.parametrize("command_name", ["block", "report"])
def test_file_chunk_protocol_and_boundaries(command_name):
    from xmtdx.commands.block_info import GetBlockInfoCmd
    from xmtdx.commands.report_file import GetReportFileCmd

    command_class = GetBlockInfoCmd if command_name == "block" else GetReportFileCmd
    command = command_class("tdxhy.cfg", 30000, 128)
    request = command.build_request()
    assert request[:12] == bytes.fromhex("0c37186a00016e006e00b906")
    assert struct.unpack_from("<II", request, 12) == (30000, 128)
    assert request[20:].rstrip(b"\x00") == b"tdxhy.cfg"
    assert command.parse_response(b"\x00" * 4 + b"payload") == b"payload"
    assert command.parse_response(b"") == b""
    with pytest.raises(TdxDecodeError):
        command.parse_response(b"\x00" * 3)
    with pytest.raises(ValueError, match="length"):
        command_class("tdxhy.cfg", 0, 30001)
    with pytest.raises(ValueError, match="start"):
        command_class("tdxhy.cfg", -1, 128)
