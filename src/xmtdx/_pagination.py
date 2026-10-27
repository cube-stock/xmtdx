"""分页累积与完整性校验；状态对象不执行 IO。"""

import hashlib
from collections.abc import Sequence

from ._client_logic import _date_from_bar
from ._config import BAR_PAGE_SIZE, FILE_CHUNK_SIZE
from .exceptions import TdxResponseError
from .models import SecurityBar, TransactionRecord
from .validation import validate_date


def _record_signature(
    record: TransactionRecord,
) -> tuple[int, int, float, int, int, int, int | None]:
    return (
        record.hour,
        record.minute,
        record.price,
        record.vol,
        record.buyorsell,
        record.unknown_last,
        record.num_orders,
    )


def _page_signature(
    records: list[TransactionRecord],
) -> tuple[tuple[int, int, float, int, int, int, int | None], ...]:
    return tuple(_record_signature(record) for record in records)


def _boundary_overlap(existing: list[TransactionRecord], new_page: list[TransactionRecord]) -> int:
    """返回较早页尾部与现有较新记录头部的最大边界重叠数。"""
    limit = min(len(existing), len(new_page))
    existing_signatures = [_record_signature(record) for record in existing[:limit]]
    new_signatures = [_record_signature(record) for record in new_page[-limit:]]
    for size in range(limit, 0, -1):
        if new_signatures[-size:] == existing_signatures[:size]:
            return size
    return 0


class TransactionPages:
    """按从新到旧的页次累积逐笔，保留页内重复，仅消除边界重叠。"""

    def __init__(self) -> None:
        self.start = 0
        self.records: list[TransactionRecord] = []
        self._seen: set[tuple[tuple[int, int, float, int, int, int, int | None], ...]] = set()

    def add(self, records: list[TransactionRecord]) -> bool:
        if not records:
            return True
        signature = _page_signature(records)
        if signature in self._seen:
            raise TdxResponseError(f"逐笔成交在偏移 {self.start} 返回重复页面")
        self._seen.add(signature)
        overlap = _boundary_overlap(self.records, records)
        new = records[:-overlap] if overlap else records
        if not new:
            raise TdxResponseError(f"逐笔成交在偏移 {self.start} 没有产生新记录")
        self.records = new + self.records
        self.start += len(records)
        if self.start > 0xFFFF:
            raise TdxResponseError("逐笔成交超过协议 uint16 分页范围")
        return False


class BarRange:
    """区间 K 线按时间去重并排序，检测分页停滞。"""

    def __init__(self, start_date: int, end_date: int) -> None:
        validate_date(start_date)
        validate_date(end_date)
        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        self.start_date = start_date
        self.end_date = end_date
        self.start = 0
        self._bars: dict[tuple[int, int, int, int, int], SecurityBar] = {}
        self._seen: set[tuple[int, int, int, int, int]] = set()

    def add(self, bars: Sequence[SecurityBar]) -> bool:
        if not bars:
            return True
        keys = [(bar.year, bar.month, bar.day, bar.hour, bar.minute) for bar in bars]
        if all(key in self._seen for key in keys):
            raise TdxResponseError(f"K 线在偏移 {self.start} 分页停滞")
        self._seen.update(keys)
        for key, bar in zip(keys, bars, strict=True):
            if self.start_date <= _date_from_bar(bar) <= self.end_date:
                self._bars[key] = bar
        self.start += len(bars)
        return (
            min(_date_from_bar(bar) for bar in bars) <= self.start_date or len(bars) < BAR_PAGE_SIZE
        )

    def result(self) -> list[SecurityBar]:
        return [self._bars[key] for key in sorted(self._bars)]


class FileChunks:
    """累积已知长度板块文件或以短页结束的报表文件。"""

    def __init__(self, filename: str, *, size: int | None = None, digest: str = "") -> None:
        self.filename = filename
        self.size = size
        self.digest = digest
        self._data = bytearray()
        self._previous: bytes | None = None

    @property
    def start(self) -> int:
        return len(self._data)

    @property
    def complete(self) -> bool:
        return self.size is not None and self.start >= self.size

    def add(self, chunk: bytes) -> bool:
        if self.size is not None:
            if not chunk and not self.complete:
                raise TdxResponseError(f"{self.filename} 在偏移 {self.start} 提前结束")
        elif chunk and chunk == self._previous:
            raise TdxResponseError(f"{self.filename} 在偏移 {self.start} 返回重复分片")
        self._data.extend(chunk)
        self._previous = chunk
        return self.complete if self.size is not None else len(chunk) < FILE_CHUNK_SIZE

    def result(self) -> bytes:
        if self.size is not None and self.start != self.size:
            raise TdxResponseError(
                f"{self.filename} 长度不符: expected={self.size}, actual={self.start}"
            )
        if len(self.digest) == 32 and hashlib.md5(self._data).hexdigest() != self.digest.lower():
            raise TdxResponseError(f"{self.filename} MD5 校验失败")
        return bytes(self._data)
