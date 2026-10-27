"""共享分页状态的完整性边界。"""

import hashlib

import pytest

from xmtdx._config import FILE_CHUNK_SIZE
from xmtdx._pagination import FileChunks, TransactionPages
from xmtdx.exceptions import TdxResponseError
from xmtdx.models import TransactionRecord


def test_transaction_uint16_limit_is_not_partial_success():
    pages = TransactionPages()
    # 以大量同值成交构造真实达到协议分页范围的页面，不能全局去重。
    records = [TransactionRecord(10, 0, 10, 1, 0, 0)] * 65536
    with pytest.raises(TdxResponseError, match="uint16"):
        pages.add(records)


def test_transaction_all_overlap_is_rejected():
    pages = TransactionPages()
    a = TransactionRecord(10, 0, 10, 1, 0, 0)
    b = TransactionRecord(10, 1, 10, 1, 0, 0)
    pages.add([a, b])
    with pytest.raises(TdxResponseError, match="没有产生新记录"):
        pages.add([a])


def test_report_file_requires_distinct_full_chunks():
    chunks = FileChunks("report")
    assert not chunks.add(b"A" * FILE_CHUNK_SIZE)
    with pytest.raises(TdxResponseError, match="重复分片"):
        chunks.add(b"A" * FILE_CHUNK_SIZE)


def test_report_file_stops_at_short_chunk():
    chunks = FileChunks("report")
    assert not chunks.add(b"A" * FILE_CHUNK_SIZE)
    assert chunks.add(b"end")
    assert chunks.result() == b"A" * FILE_CHUNK_SIZE + b"end"


def test_block_file_allows_identical_chunks_and_checks_hash():
    payload = b"A" * (FILE_CHUNK_SIZE * 2)
    chunks = FileChunks("block", size=len(payload), digest=hashlib.md5(payload).hexdigest().upper())
    assert not chunks.add(payload[:FILE_CHUNK_SIZE])
    assert chunks.add(payload[FILE_CHUNK_SIZE:])
    assert chunks.result() == payload


@pytest.mark.parametrize(
    "payload,pattern", [(b"", "提前结束"), (b"long", "长度不符"), (b"ok", "MD5")]
)
def test_block_file_rejects_truncation_overrun_and_bad_hash(payload, pattern):
    chunks = FileChunks("block", size=2, digest="0" * 32)
    with pytest.raises(TdxResponseError, match=pattern):
        chunks.add(payload)
        chunks.result()
