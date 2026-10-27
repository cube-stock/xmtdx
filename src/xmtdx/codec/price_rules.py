"""A 股价格限制规则引擎。"""

import math

from ..models.enums import Market
from .security import is_index


def get_no_limit_window_days(market: Market, code: str, name: str) -> int:
    """返回上市初期不设涨跌幅限制的交易日窗口。

    返回值:
        0: 默认按常规涨跌幅限制处理
        1: 北交所上市首日不设涨跌幅限制
        5: 沪深主板/创业板/科创板上市前 5 个交易日不设涨跌幅限制
    """
    if is_index(market, code, name):
        return 0

    if code.startswith(("43", "83", "87", "92")):
        return 1

    if market == Market.SH and code.startswith(("60", "68")):
        return 5
    if market == Market.SZ and code.startswith(("00", "30")):
        return 5

    return 0


def compute_price_limits(
    market: Market,
    code: str,
    name: str,
    pre_close: float,
    *,
    listed_days: int | None = None,
) -> tuple[float | None, float | None]:
    """根据板块规则计算涨跌停价。

    Returns:
        (limit_up, limit_down)

    无涨跌幅限制或当前规则无法可靠判断时返回 ``(None, None)``。

    Args:
        listed_days:
            已上市交易天数（按交易日计，首日=1）。
            若提供该值，函数会按上市初期无涨跌幅限制规则优先返回 ``(None, None)``。
            未提供时只计算常规板块规则，不确认上市状态。
    """
    if not math.isfinite(pre_close):
        raise ValueError("pre_close 必须为有限数值")
    if listed_days is not None and (
        isinstance(listed_days, bool) or not isinstance(listed_days, int) or listed_days < 1
    ):
        raise ValueError("listed_days 必须为正整数")
    if pre_close <= 0:
        return None, None

    # 指数/板块类代码通常无涨跌停。
    if is_index(market, code, name):
        return None, None

    no_limit_window_days = get_no_limit_window_days(market, code, name)
    if listed_days is not None and 0 < listed_days <= no_limit_window_days:
        return None, None

    # 2026-07-06 起沪深主板风险警示股票也为 10%；风险警示状态不再
    # 覆盖科创板、创业板和北交所各自的板块涨跌幅。
    limit_pct = 0.10
    if code.startswith("688") or code.startswith("300") or code.startswith("301"):
        limit_pct = 0.20
    elif code.startswith(("43", "83", "87", "92")):
        limit_pct = 0.30

    def _round_price(p: float) -> float:
        return round(p + 0.00001, 2)

    limit_up = _round_price(pre_close * (1 + limit_pct))
    limit_down = _round_price(pre_close * (1 - limit_pct))

    return limit_up, limit_down
