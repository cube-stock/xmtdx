"""公开协议参数的集中校验。"""

import math
from datetime import datetime


def validate_code(code: str) -> str:
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError("code 必须是 6 位 ASCII 数字")
    return code


def validate_uint16(value: int, name: str, *, minimum: int = 0) -> int:
    if not minimum <= value <= 0xFFFF:
        raise ValueError(f"{name} 必须在 {minimum}..65535 范围内")
    return value


def validate_count(value: int, maximum: int) -> int:
    if not 1 <= value <= maximum:
        raise ValueError(f"count 必须在 1..{maximum} 范围内")
    return value


def validate_date(value: int) -> int:
    text = str(value)
    if len(text) != 8 or not text.isascii() or not text.isdigit():
        raise ValueError("date 必须是有效的 YYYYMMDD 日期")
    try:
        datetime.strptime(text, "%Y%m%d")
    except ValueError as error:
        raise ValueError("date 必须是有效的 YYYYMMDD 日期") from error
    return value


def validate_filename(filename: str, maximum_bytes: int, encoding: str) -> bytes:
    if not filename or "\x00" in filename:
        raise ValueError("filename 不能为空或包含 NUL")
    encoded = filename.encode(encoding)
    if len(encoded) > maximum_bytes:
        raise ValueError(f"filename 编码后不能超过 {maximum_bytes} 字节")
    return encoded


def validate_timeout(timeout: float) -> float:
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout 必须为有限正数")
    return timeout


def validate_port(port: int) -> int:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("port 必须为 1..65535 的整数")
    return port


def validate_attempts(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("max_attempts 必须为正整数")
    return value
