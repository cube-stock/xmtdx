"""7709 行情初始化与 tdxlevel 协商报文。

连接后依次发送两条报文，每条均须完整读取响应。旧券商握手会导致部分
查询返回空数据或短包。
未知字段按原始字节保留，不推断其语义。
"""

from typing import Final

SETUP_INIT: Final[bytes] = bytes.fromhex("0c0218940001030003000d0001")
SETUP_LEVEL: Final[bytes] = bytes.fromhex(
    "0c031899000120002000db0f7464786c6576656c"
    "000000295cf740110000000000000000000000000005"
)

SETUP_COMMANDS: Final[tuple[bytes, ...]] = (SETUP_INIT, SETUP_LEVEL)
