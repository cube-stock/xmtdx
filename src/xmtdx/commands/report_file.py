"""报表与基础信息文件下载，使用与板块分片相同的 0x06B9 协议。"""

from .._config import FILE_CHUNK_SIZE
from .block_info import GetBlockInfoCmd


class GetReportFileCmd(GetBlockInfoCmd):
    """分段获取报表或基础信息文件，默认每次请求 30000 字节。"""

    def __init__(self, filename: str, start: int, length: int = FILE_CHUNK_SIZE) -> None:
        super().__init__(filename, start, length)
