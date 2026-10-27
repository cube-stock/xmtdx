"""证券类别判定，共用于 K 线路由与价格规则。"""

from ..models.enums import Market


def is_index(market: Market, code: str, name: str = "") -> bool:
    if market == Market.SH and code.startswith(
        ("000", "880", "881", "882", "883", "884", "885", "999")
    ):
        return True
    if market == Market.SZ and code.startswith(("395", "399")):
        return True
    return "指数" in name or "板块" in name
