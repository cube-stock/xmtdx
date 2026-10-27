"""高层行情 API：TdxClient（同步）和 AsyncTdxClient（asyncio）。"""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from types import TracebackType
from typing import TypeVar

from ._client_logic import (
    _before_a_share_session,
    _classify_fund_flow,
    _date_from_bar,
    _historical_fund_flow_from_flow,
    _historical_fund_flow_from_records,
    _now_in_shanghai,
    _today_in_shanghai,
    _unique_hosts,
    _validate_bars,
    _validate_fund_flow_coverage,
    _validate_minute_bars,
    filter_a_shares,
    market_stat_from_quotes,
    quote_batches,
)
from ._config import (
    BAR_PAGE_SIZE,
    CLIENT_TIMEOUT,
    DEFAULT_PORT,
    FILE_CHUNK_SIZE,
    MAX_PAGES,
    SECURITY_PAGE_SIZE,
    TRANSACTION_PAGE_SIZE,
)
from ._pagination import BarRange, FileChunks, TransactionPages
from .codec.block import parse_block_dat
from .codec.industry import parse_tdxhy_cfg
from .codec.price_rules import compute_price_limits, get_no_limit_window_days
from .codec.security import is_index
from .commands.base import BaseCommand
from .commands.block_info import GetBlockInfoCmd, GetBlockInfoMetaCmd
from .commands.company_info import GetCompanyInfoCategoryCmd, GetCompanyInfoContentCmd
from .commands.finance_info import GetFinanceInfoCmd
from .commands.fund_flow import GetHistoryFundFlowCmd
from .commands.minute_time import GetHistoryMinuteTimeDataCmd, GetMinuteTimeDataCmd
from .commands.report_file import GetReportFileCmd
from .commands.security_bars import GetIndexBarsCmd, GetSecurityBarsCmd
from .commands.security_count import GetSecurityCountCmd
from .commands.security_list import GetSecurityListCmd
from .commands.security_quotes import GetSecurityQuotesCmd
from .commands.transaction import GetHistoryTransactionDataCmd, GetTransactionDataCmd
from .commands.xdxr_info import GetXdxrInfoCmd
from .exceptions import TdxConnectionError, TdxDecodeError, TdxResponseError
from .models.bar import IndexBar, SecurityBar
from .models.enums import KlineCategory, Market
from .models.finance import CompanyInfoCategory, FinanceInfo, TdxBlock, XdxrRecord
from .models.quote import SecurityQuote
from .models.security import SecurityInfo
from .models.stats import FundFlow, HistoricalFundFlow, MarketStat
from .models.timeseries import MinuteBar, TransactionRecord
from .transport.async_ import AsyncTdxConnection, ping_all_async
from .transport.sync import KNOWN_HOSTS, TdxConnection, ping_all

_T = TypeVar("_T")

# ============================================================
# 同步客户端
# ============================================================


class TdxClient:
    """同步通达信行情客户端，支持 IP 优选与断线自动重连。

    使用示例::

        # 单台服务器
        with TdxClient("180.153.18.170") as c:
            bars = c.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 100)

        # 自动从候选列表中选延迟最低的服务器
        with TdxClient.from_best_host() as c:
            count = c.get_security_count(Market.SH)
    """

    def __init__(
        self,
        host: str = KNOWN_HOSTS[0],
        port: int = DEFAULT_PORT,
        timeout: float = CLIENT_TIMEOUT,
        auto_reconnect: bool = True,
        fallback_hosts: Sequence[str] = (),
        max_attempts: int = 2,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts 必须至少为 1")
        self._hosts = _unique_hosts(host, fallback_hosts)
        self._host_index = 0
        self._host = self._hosts[0]
        self._port = port
        self._timeout = timeout
        self._auto_reconnect = auto_reconnect
        self._max_attempts = max_attempts
        self._conn = TdxConnection(self._host, port, timeout)

    # ------------------------------------------------------------------ #
    # 工厂方法：自动优选最低延迟服务器
    # ------------------------------------------------------------------ #

    @classmethod
    def from_best_host(
        cls,
        hosts: list[str] = KNOWN_HOSTS,
        port: int = DEFAULT_PORT,
        timeout: float = CLIENT_TIMEOUT,
        ping_timeout: float = 5.0,
        auto_reconnect: bool = True,
        max_attempts: int = 2,
    ) -> "TdxClient":
        """测量 hosts 中所有服务器延迟，按最低延迟优先配置候选服务器；首次查询或 connect() 时连接。

        若所有服务器均不可达，回退到 hosts[0]。
        """
        ranked = ping_all(hosts, port, ping_timeout)
        ordered = [item[0] for item in ranked] or list(hosts)
        if not ordered:
            raise ValueError("hosts 不能为空")
        return cls(
            ordered[0],
            port,
            timeout,
            auto_reconnect,
            fallback_hosts=ordered[1:],
            max_attempts=max_attempts,
        )

    @staticmethod
    def ping_all(
        hosts: list[str] = KNOWN_HOSTS,
        port: int = DEFAULT_PORT,
        timeout: float = 5.0,
    ) -> list[tuple[str, float]]:
        """测量多台服务器延迟，返回按延迟排序的 (host, seconds) 列表。"""
        return ping_all(hosts, port, timeout)

    # ------------------------------------------------------------------ #
    # 连接管理
    # ------------------------------------------------------------------ #

    def connect(self) -> None:
        self._conn.connect()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "TdxClient":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # 内部执行：含自动重连
    # ------------------------------------------------------------------ #

    def _execute(self, cmd: "BaseCommand[_T]") -> _T:
        """执行命令；传输或响应错误时使用全新连接并按主机顺序重试。"""
        attempts = self._max_attempts if self._auto_reconnect else 1
        last_error: TdxConnectionError | TdxDecodeError | TdxResponseError | None = None
        for attempt in range(attempts):
            if attempt > 0:
                self._conn.close()
                self._host_index = (self._host_index + 1) % len(self._hosts)
                self._host = self._hosts[self._host_index]
                self._conn = TdxConnection(self._host, self._port, self._timeout)
            try:
                if not self._conn.is_connected:
                    self._conn.connect()
                return self._conn.execute(cmd)
            except (TdxConnectionError, TdxDecodeError, TdxResponseError) as error:
                last_error = error
                self._conn.close()
        assert last_error is not None
        raise last_error

    # ------------------------------------------------------------------ #
    # 市场信息
    # ------------------------------------------------------------------ #

    def get_security_count(self, market: Market) -> int:
        """获取市场证券总数。"""
        return self._execute(GetSecurityCountCmd(market))

    def get_security_list(self, market: Market, start: int) -> list[SecurityInfo]:
        """获取证券列表（每页约1000条，按 start 分页）。"""
        return self._execute(GetSecurityListCmd(market, start))

    def get_security_list_all(self) -> list[SecurityInfo]:
        """获取沪深 A 股完整证券列表，并尽力挂载行业信息。

        注意：
            `Market.BJ` 的证券列表请求长期存在服务器超时问题，当前版本暂不纳入此方法。
        """
        # 1. 尝试获取行业配置
        industry_map = {}
        try:
            cfg_data = self.get_report_file("tdxhy.cfg")
            if cfg_data:
                industry_map = parse_tdxhy_cfg(cfg_data)
        except (TdxConnectionError, TdxDecodeError, TdxResponseError):
            pass

        all_stocks: list[SecurityInfo] = []
        # BJ 列表不稳定，当前仅扫描沪深。
        for market in [Market.SH, Market.SZ]:
            count = self.get_security_count(market)
            for start in range(0, count, SECURITY_PAGE_SIZE):
                stocks = self.get_security_list(market, start)
                all_stocks.extend(filter_a_shares(stocks, market, industry_map))
        return all_stocks

    def get_security_quotes(self, stocks: list[tuple[Market, str]]) -> list[SecurityQuote]:
        """批量获取实时五档行情；超过 80 只时自动按协议上限分片。"""
        quotes: list[SecurityQuote] = []
        for batch in quote_batches(stocks):
            result: list[SecurityQuote] = self._execute(GetSecurityQuotesCmd(batch))
            quotes.extend(result)
        return quotes

    def get_price_limits(
        self, market: Market, code: str, name: str, pre_close: float
    ) -> tuple[float | None, float | None]:
        """按当前交易状态计算涨跌停价。

        对上市初期不设涨跌幅限制的标的，会先用日 K 线条数估算已上市交易天数。
        """
        listed_days: int | None = None
        no_limit_window_days = get_no_limit_window_days(market, code, name)
        if no_limit_window_days > 0:
            try:
                bars = self.get_security_bars(
                    market, code, KlineCategory.DAY, 0, no_limit_window_days + 1
                )
                listed_days = len(bars)
            except (TdxConnectionError, TdxDecodeError, TdxResponseError):
                listed_days = None

        return compute_price_limits(
            market,
            code,
            name,
            pre_close,
            listed_days=listed_days,
        )

    # ------------------------------------------------------------------ #
    # K 线
    # ------------------------------------------------------------------ #

    def get_security_bars(
        self,
        market: Market,
        code: str,
        category: KlineCategory,
        start: int,
        count: int = BAR_PAGE_SIZE,
    ) -> list[SecurityBar]:
        """获取 K 线数据（最多800条/次，按 start 分页）。"""
        bars = self._execute(GetSecurityBarsCmd(market, code, category, start, count))
        _validate_bars(bars, "security_bars")
        return bars

    def get_index_bars(
        self,
        market: Market,
        code: str,
        category: KlineCategory,
        start: int,
        count: int = BAR_PAGE_SIZE,
    ) -> list[IndexBar]:
        """获取指数 K 线数据。"""
        bars = self._execute(GetIndexBarsCmd(market, code, category, start, count))
        _validate_bars(bars, "index_bars")
        return bars

    def get_bars(
        self,
        market: Market,
        code: str,
        category: KlineCategory,
        start: int,
        count: int = BAR_PAGE_SIZE,
    ) -> list[SecurityBar] | list[IndexBar]:
        """按市场与代码自动路由股票或指数 K 线。"""
        if is_index(market, code):
            return self.get_index_bars(market, code, category, start, count)
        return self.get_security_bars(market, code, category, start, count)

    def get_bars_range(
        self,
        market: Market,
        code: str,
        start_date: int,
        end_date: int,
        category: KlineCategory = KlineCategory.DAY,
    ) -> list[SecurityBar] | list[IndexBar]:
        """分页获取日期闭区间内的 K 线，并按时间升序去重返回。"""
        pages = BarRange(start_date, end_date)
        for _ in range(MAX_PAGES):
            page = self.get_bars(market, code, category, pages.start, BAR_PAGE_SIZE)
            if pages.add(page):
                return pages.result()
        raise TdxResponseError("K 线区间超过最大分页数")

    # ------------------------------------------------------------------ #
    # 分时
    # ------------------------------------------------------------------ #

    def get_minute_time_data(self, market: Market, code: str) -> list[MinuteBar]:
        """获取今日分时数据（盘中返回截至当前，全天最多约 240 条）。"""
        today = _today_in_shanghai()
        try:
            bars = self.get_history_minute_time_data(market, code, today)
            if bars:
                return bars
        except (TdxConnectionError, TdxDecodeError, TdxResponseError):
            pass
        if _before_a_share_session(_now_in_shanghai()):
            return []

        bars = self._execute(GetMinuteTimeDataCmd(market, code))
        if not bars:
            return []
        reference_price: float | None = None
        try:
            quotes = self.get_security_quotes([(market, code)])
            if quotes:
                reference_price = quotes[0].pre_close or quotes[0].price
            _validate_minute_bars(bars, "minute_time", reference_price)
        except TdxResponseError:
            latest = self.get_security_bars(market, code, KlineCategory.DAY, 0, 1)
            if not latest or _date_from_bar(latest[-1]) != today:
                return []
            raise
        return bars

    def get_history_minute_time_data(self, market: Market, code: str, date: int) -> list[MinuteBar]:
        """获取历史某日分时数据（date: YYYYMMDD）。"""
        bars = self._execute(GetHistoryMinuteTimeDataCmd(market, code, date))
        _validate_minute_bars(bars, "history_minute_time")
        return bars

    # ------------------------------------------------------------------ #
    # 逐笔成交
    # ------------------------------------------------------------------ #

    def get_transaction_data(
        self, market: Market, code: str, start: int, count: int = TRANSACTION_PAGE_SIZE
    ) -> list[TransactionRecord]:
        """获取当日逐笔成交（分页）。"""
        return self._execute(GetTransactionDataCmd(market, code, start, count))

    def get_history_transaction_data(
        self, market: Market, code: str, date: int, start: int, count: int = TRANSACTION_PAGE_SIZE
    ) -> list[TransactionRecord]:
        """获取历史逐笔成交（date: YYYYMMDD，分页）。"""
        return self._execute(GetHistoryTransactionDataCmd(market, code, date, start, count))

    # ------------------------------------------------------------------ #
    # 财务 / 公司
    # ------------------------------------------------------------------ #

    def get_xdxr_info(self, market: Market, code: str) -> list[XdxrRecord]:
        """获取除权除息历史记录。"""
        return self._execute(GetXdxrInfoCmd(market, code))

    def get_finance_info(self, market: Market, code: str) -> FinanceInfo:
        """获取最新财务数据。"""
        return self._execute(GetFinanceInfoCmd(market, code))

    def get_company_info_category(self, market: Market, code: str) -> list[CompanyInfoCategory]:
        """获取公司信息文件目录。"""
        return self._execute(GetCompanyInfoCategoryCmd(market, code))

    def get_company_info_content(
        self, market: Market, code: str, filename: str, offset: int, length: int
    ) -> str:
        """读取公司信息文本。"""
        return self._execute(GetCompanyInfoContentCmd(market, code, filename, offset, length))

    def get_block_info(self, filename: str) -> list[TdxBlock]:
        """获取并解析板块文件（行业、概念、风格等）。

        常用文件名：
          'block_zs.dat'  - 行业/指数板块
          'block_gn.dat'  - 概念板块
          'block_fg.dat'  - 风格板块
        """
        size, digest = self._execute(GetBlockInfoMetaCmd(filename))
        chunks = FileChunks(filename, size=size, digest=digest)
        for _ in range(MAX_PAGES):
            if chunks.complete:
                return parse_block_dat(chunks.result(), filename)
            chunk = self._execute(GetBlockInfoCmd(filename, chunks.start, FILE_CHUNK_SIZE))
            chunks.add(chunk)
        return parse_block_dat(chunks.result(), filename)

    def get_report_file(self, filename: str) -> bytes:
        """从服务器拉取大文件（如 'base_info.zip'）。"""
        chunks = FileChunks(filename)
        for _ in range(MAX_PAGES):
            chunk = self._execute(GetReportFileCmd(filename, chunks.start, FILE_CHUNK_SIZE))
            if chunks.add(chunk):
                return chunks.result()
        raise TdxResponseError(f"{filename} 超过最大分片数 {MAX_PAGES}")

    def get_market_stat(self) -> MarketStat:
        """获取 A 股全市场涨跌统计概况（基于 880005 行情统计）。

        注意：
            `suspended_count` 是 `total - up - down - neutral` 的残差估算值，
            用于保证计数守恒，不应视为协议已明确验证的停牌字段。
        """
        quotes = self.get_security_quotes([(Market.SH, "880005")])
        return market_stat_from_quotes(quotes)

    def _collect_transaction_records(
        self,
        fetch_page: Callable[[int, int], list[TransactionRecord]],
        page_size: int = TRANSACTION_PAGE_SIZE,
    ) -> list[TransactionRecord]:
        pages = TransactionPages()
        while True:
            records = fetch_page(pages.start, page_size)
            if pages.add(records):
                return pages.records

    def get_fund_flow(self, market: Market, code: str) -> FundFlow:
        """获取个股当日资金流向分布（基于 L1 逐笔数据统计）。"""
        quotes = self.get_security_quotes([(market, code)])
        if not quotes:
            raise TdxResponseError(f"无法获取 {market.name}:{code} 行情以校验资金流完整性")
        records = self._collect_transaction_records(
            lambda start, page_size: self.get_transaction_data(market, code, start, page_size),
        )
        _validate_fund_flow_coverage(records, quotes[0])
        return _classify_fund_flow(records)

    def get_history_fund_flow(
        self, market: Market, code: str, start: int, count: int
    ) -> list[HistoricalFundFlow]:
        """获取个股历史日线资金流向序列。

        优先走 Category 22；若返回空列表或出现连接、解码、响应异常，则回退为
        “日 K 线取日期 + 历史逐笔成交重算资金流”的兼容实现。
        """
        try:
            direct = self._execute(GetHistoryFundFlowCmd(market, code, start, count))
        except (TdxConnectionError, TdxDecodeError, TdxResponseError):
            direct = []
        if direct:
            return direct

        bars = self.get_security_bars(market, code, KlineCategory.DAY, start, count)
        results: list[HistoricalFundFlow] = []
        for bar in bars:
            date = _date_from_bar(bar)
            if date == _today_in_shanghai():
                results.append(
                    _historical_fund_flow_from_flow(date, self.get_fund_flow(market, code))
                )
                continue
            records = self._collect_transaction_records(
                lambda page_start, page_size: self.get_history_transaction_data(
                    market, code, date, page_start, page_size
                ),
            )
            if not records:
                raise TdxResponseError(f"{market.name}:{code} 在 {date} 的历史逐笔成交为空")
            results.append(_historical_fund_flow_from_records(date, records))
        return results


# ============================================================
# 异步客户端
# ============================================================


class AsyncTdxClient:
    """异步通达信行情客户端（asyncio）。

    使用示例::

        async with AsyncTdxClient("180.153.18.170") as c:
            bars = await c.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 100)

    注意：
        单个 AsyncTdxClient 仅维护一条 TCP 连接；并发调用会在连接内串行执行。
    """

    def __init__(
        self,
        host: str = KNOWN_HOSTS[0],
        port: int = DEFAULT_PORT,
        timeout: float = CLIENT_TIMEOUT,
        auto_reconnect: bool = True,
        heartbeat_interval: float = 60.0,
        fallback_hosts: Sequence[str] = (),
        max_attempts: int = 2,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts 必须至少为 1")
        self._hosts = _unique_hosts(host, fallback_hosts)
        self._host_index = 0
        self._host = self._hosts[0]
        self._port = port
        self._timeout = timeout
        self._auto_reconnect = auto_reconnect
        self._heartbeat_interval = heartbeat_interval
        self._max_attempts = max_attempts
        self._conn = AsyncTdxConnection(self._host, port, timeout)
        self._execute_lock = asyncio.Lock()
        self._heartbeat_task: asyncio.Task[None] | None = None

    @classmethod
    async def from_best_host(
        cls,
        hosts: list[str] = KNOWN_HOSTS,
        port: int = DEFAULT_PORT,
        timeout: float = CLIENT_TIMEOUT,
        ping_timeout: float = 5.0,
        auto_reconnect: bool = True,
        heartbeat_interval: float = 60.0,
        max_attempts: int = 2,
    ) -> "AsyncTdxClient":
        """按延迟配置候选服务器；首次查询或 connect() 时连接。"""
        ranked = await ping_all_async(hosts, port, ping_timeout)
        ordered = [item[0] for item in ranked] or list(hosts)
        if not ordered:
            raise ValueError("hosts 不能为空")
        return cls(
            ordered[0],
            port,
            timeout,
            auto_reconnect,
            heartbeat_interval,
            fallback_hosts=ordered[1:],
            max_attempts=max_attempts,
        )

    @staticmethod
    async def ping_all(
        hosts: list[str] = KNOWN_HOSTS,
        port: int = DEFAULT_PORT,
        timeout: float = 5.0,
    ) -> list[tuple[str, float]]:
        """测量多台服务器延迟，返回按延迟排序的 (host, seconds) 列表。"""
        return await ping_all_async(hosts, port, timeout)

    async def connect(self) -> None:
        await self._conn.connect()
        self._start_heartbeat()

    async def close(self) -> None:
        await self._stop_heartbeat()
        await self._conn.close()

    async def __aenter__(self) -> "AsyncTdxClient":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    def _start_heartbeat(self) -> None:
        """启动后台心跳任务。"""
        if self._heartbeat_interval <= 0:
            return
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _stop_heartbeat(self) -> None:
        """停止并清理心跳任务。"""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        """心跳循环：定期发送轻量级请求保活。"""
        while True:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                # 使用 get_security_count 作为心跳包
                await self.get_security_count(Market.SH)
            except asyncio.CancelledError:
                break
            except (TdxConnectionError, TdxDecodeError, TdxResponseError):
                # 心跳失败通常意味着连接已断开
                # 下一次正常的业务请求或下一次心跳会通过 _execute 触发重连
                pass

    async def _execute(self, cmd: "BaseCommand[_T]") -> _T:
        """执行命令；传输或响应错误时使用全新连接并按主机顺序重试。"""
        async with self._execute_lock:
            attempts = self._max_attempts if self._auto_reconnect else 1
            last_error: TdxConnectionError | TdxDecodeError | TdxResponseError | None = None
            for attempt in range(attempts):
                if attempt > 0:
                    await self._conn.close()
                    self._host_index = (self._host_index + 1) % len(self._hosts)
                    self._host = self._hosts[self._host_index]
                    self._conn = AsyncTdxConnection(self._host, self._port, self._timeout)
                try:
                    if not self._conn.is_connected:
                        await self._conn.connect()
                    return await self._conn.execute(cmd)
                except (TdxConnectionError, TdxDecodeError, TdxResponseError) as error:
                    last_error = error
                    await self._conn.close()
            assert last_error is not None
            raise last_error

    async def get_security_count(self, market: Market) -> int:
        return await self._execute(GetSecurityCountCmd(market))

    async def get_security_list(self, market: Market, start: int) -> list[SecurityInfo]:
        return await self._execute(GetSecurityListCmd(market, start))

    async def get_security_list_all(self) -> list[SecurityInfo]:
        """获取沪深 A 股完整证券列表，并尽力挂载行业信息。

        注意：
            `Market.BJ` 的证券列表请求长期存在服务器超时问题，当前版本暂不纳入此方法。
        """
        industry_map = {}
        try:
            cfg_data = await self.get_report_file("tdxhy.cfg")
            if cfg_data:
                industry_map = parse_tdxhy_cfg(cfg_data)
        except (TdxConnectionError, TdxDecodeError, TdxResponseError):
            pass

        all_stocks: list[SecurityInfo] = []
        # BJ 列表不稳定，当前仅扫描沪深。
        for market in [Market.SH, Market.SZ]:
            count = await self.get_security_count(market)
            for start in range(0, count, SECURITY_PAGE_SIZE):
                stocks = await self.get_security_list(market, start)
                all_stocks.extend(filter_a_shares(stocks, market, industry_map))
        return all_stocks

    async def get_security_quotes(self, stocks: list[tuple[Market, str]]) -> list[SecurityQuote]:
        quotes: list[SecurityQuote] = []
        for batch in quote_batches(stocks):
            result: list[SecurityQuote] = await self._execute(GetSecurityQuotesCmd(batch))
            quotes.extend(result)
        return quotes

    async def get_price_limits(
        self, market: Market, code: str, name: str, pre_close: float
    ) -> tuple[float | None, float | None]:
        """按当前交易状态计算涨跌停价。"""
        listed_days: int | None = None
        no_limit_window_days = get_no_limit_window_days(market, code, name)
        if no_limit_window_days > 0:
            try:
                bars = await self.get_security_bars(
                    market, code, KlineCategory.DAY, 0, no_limit_window_days + 1
                )
                listed_days = len(bars)
            except (TdxConnectionError, TdxDecodeError, TdxResponseError):
                listed_days = None

        return compute_price_limits(
            market,
            code,
            name,
            pre_close,
            listed_days=listed_days,
        )

    async def get_security_bars(
        self,
        market: Market,
        code: str,
        category: KlineCategory,
        start: int,
        count: int = BAR_PAGE_SIZE,
    ) -> list[SecurityBar]:
        bars = await self._execute(GetSecurityBarsCmd(market, code, category, start, count))
        _validate_bars(bars, "security_bars")
        return bars

    async def get_index_bars(
        self,
        market: Market,
        code: str,
        category: KlineCategory,
        start: int,
        count: int = BAR_PAGE_SIZE,
    ) -> list[IndexBar]:
        bars = await self._execute(GetIndexBarsCmd(market, code, category, start, count))
        _validate_bars(bars, "index_bars")
        return bars

    async def get_bars(
        self,
        market: Market,
        code: str,
        category: KlineCategory,
        start: int,
        count: int = BAR_PAGE_SIZE,
    ) -> list[SecurityBar] | list[IndexBar]:
        if is_index(market, code):
            return await self.get_index_bars(market, code, category, start, count)
        return await self.get_security_bars(market, code, category, start, count)

    async def get_bars_range(
        self,
        market: Market,
        code: str,
        start_date: int,
        end_date: int,
        category: KlineCategory = KlineCategory.DAY,
    ) -> list[SecurityBar] | list[IndexBar]:
        pages = BarRange(start_date, end_date)
        for _ in range(MAX_PAGES):
            page = await self.get_bars(market, code, category, pages.start, BAR_PAGE_SIZE)
            if pages.add(page):
                return pages.result()
        raise TdxResponseError("K 线区间超过最大分页数")

    async def get_minute_time_data(self, market: Market, code: str) -> list[MinuteBar]:
        today = _today_in_shanghai()
        try:
            bars = await self.get_history_minute_time_data(market, code, today)
            if bars:
                return bars
        except (TdxConnectionError, TdxDecodeError, TdxResponseError):
            pass
        if _before_a_share_session(_now_in_shanghai()):
            return []

        bars = await self._execute(GetMinuteTimeDataCmd(market, code))
        if not bars:
            return []
        reference_price: float | None = None
        try:
            quotes = await self.get_security_quotes([(market, code)])
            if quotes:
                reference_price = quotes[0].pre_close or quotes[0].price
            _validate_minute_bars(bars, "minute_time", reference_price)
        except TdxResponseError:
            latest = await self.get_security_bars(market, code, KlineCategory.DAY, 0, 1)
            if not latest or _date_from_bar(latest[-1]) != today:
                return []
            raise
        return bars

    async def get_history_minute_time_data(
        self, market: Market, code: str, date: int
    ) -> list[MinuteBar]:
        bars = await self._execute(GetHistoryMinuteTimeDataCmd(market, code, date))
        _validate_minute_bars(bars, "history_minute_time")
        return bars

    async def get_transaction_data(
        self, market: Market, code: str, start: int, count: int = TRANSACTION_PAGE_SIZE
    ) -> list[TransactionRecord]:
        return await self._execute(GetTransactionDataCmd(market, code, start, count))

    async def get_history_transaction_data(
        self, market: Market, code: str, date: int, start: int, count: int = TRANSACTION_PAGE_SIZE
    ) -> list[TransactionRecord]:
        return await self._execute(GetHistoryTransactionDataCmd(market, code, date, start, count))

    async def get_xdxr_info(self, market: Market, code: str) -> list[XdxrRecord]:
        return await self._execute(GetXdxrInfoCmd(market, code))

    async def get_finance_info(self, market: Market, code: str) -> FinanceInfo:
        return await self._execute(GetFinanceInfoCmd(market, code))

    async def get_company_info_category(
        self, market: Market, code: str
    ) -> list[CompanyInfoCategory]:
        return await self._execute(GetCompanyInfoCategoryCmd(market, code))

    async def get_company_info_content(
        self, market: Market, code: str, filename: str, offset: int, length: int
    ) -> str:
        return await self._execute(GetCompanyInfoContentCmd(market, code, filename, offset, length))

    async def get_block_info(self, filename: str) -> list[TdxBlock]:
        """获取并解析板块文件（行业、概念、风格等）。"""
        size, digest = await self._execute(GetBlockInfoMetaCmd(filename))
        chunks = FileChunks(filename, size=size, digest=digest)
        for _ in range(MAX_PAGES):
            if chunks.complete:
                return parse_block_dat(chunks.result(), filename)
            chunk = await self._execute(GetBlockInfoCmd(filename, chunks.start, FILE_CHUNK_SIZE))
            chunks.add(chunk)
        return parse_block_dat(chunks.result(), filename)

    async def get_report_file(self, filename: str) -> bytes:
        """从服务器拉取大文件。"""
        chunks = FileChunks(filename)
        for _ in range(MAX_PAGES):
            chunk = await self._execute(GetReportFileCmd(filename, chunks.start, FILE_CHUNK_SIZE))
            if chunks.add(chunk):
                return chunks.result()
        raise TdxResponseError(f"{filename} 超过最大分片数 {MAX_PAGES}")

    async def get_market_stat(self) -> MarketStat:
        """获取 A 股全市场涨跌统计概况（基于 880005 行情统计）。

        注意：
            `suspended_count` 是 `total - up - down - neutral` 的残差估算值，
            用于保证计数守恒，不应视为协议已明确验证的停牌字段。
        """
        quotes = await self.get_security_quotes([(Market.SH, "880005")])
        return market_stat_from_quotes(quotes)

    async def _collect_transaction_records(
        self,
        fetch_page: Callable[[int, int], Awaitable[list[TransactionRecord]]],
        page_size: int = TRANSACTION_PAGE_SIZE,
    ) -> list[TransactionRecord]:
        pages = TransactionPages()
        while True:
            records = await fetch_page(pages.start, page_size)
            if pages.add(records):
                return pages.records

    async def get_fund_flow(self, market: Market, code: str) -> FundFlow:
        """获取个股当日资金流向分布（基于 L1 逐笔数据统计）。"""
        quotes = await self.get_security_quotes([(market, code)])
        if not quotes:
            raise TdxResponseError(f"无法获取 {market.name}:{code} 行情以校验资金流完整性")
        records = await self._collect_transaction_records(
            lambda start, page_size: self.get_transaction_data(market, code, start, page_size),
        )
        _validate_fund_flow_coverage(records, quotes[0])
        return _classify_fund_flow(records)

    async def get_history_fund_flow(
        self, market: Market, code: str, start: int, count: int
    ) -> list[HistoricalFundFlow]:
        """获取个股历史日线资金流向序列。

        优先走 Category 22；若返回空列表或出现连接、解码、响应异常，则回退为
        “日 K 线取日期 + 历史逐笔成交重算资金流”的兼容实现。
        """
        try:
            direct = await self._execute(GetHistoryFundFlowCmd(market, code, start, count))
        except (TdxConnectionError, TdxDecodeError, TdxResponseError):
            direct = []
        if direct:
            return direct

        bars = await self.get_security_bars(market, code, KlineCategory.DAY, start, count)
        results: list[HistoricalFundFlow] = []
        for bar in bars:
            date = _date_from_bar(bar)
            if date == _today_in_shanghai():
                results.append(
                    _historical_fund_flow_from_flow(date, await self.get_fund_flow(market, code))
                )
                continue
            records = await self._collect_transaction_records(
                lambda page_start, page_size: self.get_history_transaction_data(
                    market, code, date, page_start, page_size
                ),
            )
            if not records:
                raise TdxResponseError(f"{market.name}:{code} 在 {date} 的历史逐笔成交为空")
            results.append(_historical_fund_flow_from_records(date, records))
        return results
