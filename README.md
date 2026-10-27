# xmtdx

通达信私有 7709/TCP 行情协议的 A 股客户端实现，零运行时依赖。协议并无官方公开规范，字段与命令来自兼容实现、抓包和真实服务器交叉验证，因此属于兼容性逆向开发，不包含通达信客户端代码。

当前源码版本：**0.3.1**。

## 特性

- **零依赖**：纯标准库，Python ≥ 3.10
- **同步 + asyncio 双接口**：`TdxClient` / `AsyncTdxClient`，commands 层不含任何 IO
- **完整类型注解**：strict `mypy` + `ruff` 通过
- **高可用传输**：同步/异步均支持 `ping_all()`、`from_best_host()` 和候选服务器重试
- **响应语义校验**：校验证券身份、日期、OHLC、成交量、资金流覆盖率和文件完整性
- **批量与区间 API**：行情超过 80 只自动分片，K 线支持股票/指数自动路由和日期区间分页
- **保留协议信息**：解析模型按需保留 `_raw: bytes` 和 `unknown_N` 字段
- **保活心跳机制**：`AsyncTdxClient` 自动发送心跳包，帮助维持长连接
- **沪深 A 股完整列表**：`get_security_list_all()` 自动过滤非 A 股品种并尽力挂载行业信息
- **北交所列表限制**：`get_security_list(Market.BJ, start)` 当前不能稳定获取，BJ 暂未纳入 `get_security_list_all()`
- **全市场涨跌统计**：一键获取全 A 股涨/跌/平家数及总成交额
- **离线 + 本地传输回归测试**：覆盖解析、异步并发串行化、超时、自动重连与坏包处理

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .                  # 开发模式
pip install -e ".[dev,pandas]" build  # 完整开发与构建环境
pip install -e ".[pandas]"        # 含 pandas（可选）
```

## 快速开始

### 同步

```python
from xmtdx import TdxClient, Market, KlineCategory

# 指定服务器
with TdxClient("180.153.18.170") as c:
    count = c.get_security_count(Market.SH)
    bars  = c.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 5)
    for b in bars:
        print(b.year, b.month, b.day, b.open, b.close, b.high, b.low, b.vol)

# 自动测速选最低延迟服务器
with TdxClient.from_best_host() as c:
    quotes = c.get_security_quotes([(Market.SH, "600000"), (Market.SZ, "000001")])
    print(quotes[0].price, quotes[0].bid1, quotes[0].ask1)
```

### asyncio

```python
import asyncio
from xmtdx import AsyncTdxClient, Market, KlineCategory

async def main():
    async with AsyncTdxClient("180.153.18.170") as c:
        bars = await c.get_security_bars(Market.SH, "600000", KlineCategory.DAY, 0, 5)
        print(bars[0])

asyncio.run(main())
```

### 高可用工具

```python
from xmtdx import ping_all, KNOWN_HOSTS

# 并发测速，返回按延迟排序的 [(host, seconds), ...]
results = ping_all(KNOWN_HOSTS, timeout=5.0)
for host, ms in results:
    print(f"{host}  {ms*1000:.0f} ms")

# 自动选最优服务器
with TdxClient.from_best_host(ping_timeout=5.0) as c:
    ...

# asyncio 版本同样支持（工厂本身也要 await）
client = await AsyncTdxClient.from_best_host(ping_timeout=5.0)
```

候选服务器见导出的 `KNOWN_HOSTS`，可自行传入服务器列表。

`from_best_host()` 会把测速可达的主机配置为候选列表，但单次调用最多尝试
`max_attempts` 台（默认 2 台）。直接使用 `TdxClient(host)` 且不传
`fallback_hosts` 时，重连仍是同一台主机。显式 `connect()`、上下文管理器与查询
遵循相同的重试配置。异步请求取消会关闭受影响连接并传播取消异常。

测速通过仅表示握手及证券数量查询成功，不保证行情、K 线等全部能力可用。
本次直连抽查中行情为空、K 线返回短包，详见 [在线验证记录](docs/protocol-coverage.md#本次在线验证2026-10-27)。


## API

### TdxClient

| 方法 | 说明 |
|------|------|
| `get_security_count(market)` | 市场证券总数 |
| `get_security_list(market, start)` | 证券列表（每页 ~1000 条；BJ 当前不能稳定获取） |
| `get_security_list_all()` | 沪深 A 股列表（尽力挂载行业信息；BJ 暂未纳入） |
| `get_market_stat()` | 全市场 A 股涨跌统计（家数、成交额） |
| `get_security_quotes([(market, code), ...])` | 批量实时五档行情（任意数量，内部每 80 只分片） |
| `get_price_limits(market, code, name, pre_close)` | 计算当前涨跌停价（日线不足以确认上市状态时返回 None） |
| `get_security_bars(market, code, category, start, count=800)` | K 线（股票） |
| `get_index_bars(market, code, category, start, count=800)` | K 线（指数） |
| `get_bars(market, code, category, start, count=800)` | 按代码自动路由股票/指数 K 线 |
| `get_bars_range(market, code, start_date, end_date, category=DAY)` | 获取日期闭区间 K 线，自动分页、排序和去重 |
| `get_minute_time_data(market, code)` | 今日分时（盘中返回截至当前，全天最多约 240 条） |
| `get_history_minute_time_data(market, code, date)` | 历史某日分时，`date=YYYYMMDD` |
| `get_transaction_data(market, code, start, count=800)` | 当日逐笔成交（分页） |
| `get_history_transaction_data(market, code, date, start, count=800)` | 历史逐笔成交 |
| `get_fund_flow(market, code)` | 当日资金流向统计（800 条分页至结束并校验成交量覆盖） |
| `get_history_fund_flow(market, code, start, count)` | 历史日线资金流向序列（优先 Category 22，空回包或协议异常时回退重算） |
| `get_xdxr_info(market, code)` | 除权除息历史 |
| `get_finance_info(market, code)` | 最新财务数据 |
| `get_company_info_category(market, code)` | 公司信息文件目录 |
| `get_company_info_content(market, code, filename, offset, length)` | 公司信息文本 |
| `get_block_info(filename)` | 板块信息（行业、概念、风格等） |
| `get_report_file(filename)` | 批量拉取大文件（如 'base_info.zip', 'gpcw.txt'） |

`AsyncTdxClient` 提供与同步版对应的查询方法与高可用入口，均为 `async def`。
单个 `AsyncTdxClient` 仅维护一条 TCP 连接；并发调用会在连接内串行执行。

所有 dataclass 结果可用 `to_dataframe(records)` 转为 DataFrame；需安装
`xmtdx[pandas]`，默认不暴露 `_raw`，调试时可传 `include_raw=True`。

### KlineCategory

```
MIN_1  MIN_1_ALT  MIN_5  MIN_15  MIN_30  MIN_60
DAY  DAY_ALT  WEEK  MONTH  SEASON  YEAR
```

## 已知限制

- `xmtdx` 当前不能稳定获取 BJ 证券列表；`get_security_count(Market.BJ)` 可用，但 `get_security_list(Market.BJ, start)` 经常超时，因此 `get_security_list_all()` 暂不纳入 BJ。
- 公共服务器的逐笔单页按 800 条使用；若返回重复页、分页范围耗尽或逐笔量低于查询前的行情快照成交量，资金流接口会抛出 `TdxResponseError`，不会返回已知不完整的统计。
- `get_history_fund_flow()` 的 Category 22 在空回包或连接、解码、响应异常时回退，需要历史逐笔数据；服务器对过去交易日返回空数据时会报错，不会把“缺数据”伪装成全零资金流。
- 今日分时在周末、开盘前返回空；其余时间需要最新日线确认日期，并以行情参考价交叉校验。缺少必要日线或行情会报错。
- 历史资金流回退会将逐笔的手换算为股，校验对应日线成交量；证券全集会检查未过滤列表的数量和重复情况，拒绝返回已知不完整结果。
- 心跳、重连和候选服务器只能提高可用性，不构成生产环境 SLA；公共服务器地址及行为可能随时变化。

## 数据与单位

查询结果采用带类型注解的 dataclass；模型定义见 `src/xmtdx/models/`。
协议解析记录按需保留 `_raw` 和未知字段，汇总结果、证券列表等并非都有 `_raw`。
`to_dataframe()` 默认排除内部字段，传入 `include_raw=True` 可保留。

| 数据 | 约定 |
|---|---|
| `SecurityBar` / `IndexBar` | OHLC、量额和时间；指数额外含 `up_count/down_count` |
| `SecurityQuote` | 五档行情；成交量 `vol` 为手、成交额 `amount` 为元 |
| `quote_time` | 单证券快照时间 `HH:MM:SS.mmm`，不含日期，不代表当前墙上时间 |
| `limit_up/limit_down` | 行情解析默认 `None`；通过 `get_price_limits()` 计算 |
| `TransactionRecord` | `buyorsell` 为 0 买、1 卖、2 中性、8 集合竞价；历史 `num_orders` 为 `None` |
| `FinanceInfo` | 股本为股、金额为元 |
| `XdxrRecord` | 股本类字段为万股；`category == 1` 的分红、送转、配股归一化为每股 |
| `FundFlow` / `HistoricalFundFlow` | 金额为元，提供主力净流入等派生属性 |

资金流按 L1 单笔成交金额分档，不等同于官方资金流口径。当日接口以查询开始时的
行情成交量校验最低覆盖；逐笔更新可能使总量高于该快照。分类与协议验证说明见
[协议覆盖矩阵](docs/protocol-coverage.md)。

涨跌停采用现有当前规则，不能还原历史日期规则；上市交易天数可通过
`compute_price_limits(..., listed_days=...)` 明确传入。规则实现见
`src/xmtdx/codec/price_rules.py`，规则变化应单独验证。

从 0.2.x 升级请阅读 [0.3.0 迁移说明](docs/migration-0.3.md)，本次更严格的校验行为见
[0.3.1 变更说明](docs/changes-0.3.1.md)。

## 使用边界

xmtdx 是对未公开行情协议的兼容性逆向实现，仅供研究和技术验证。使用者应自行确认
数据来源、服务条款、授权和再分发要求。公共服务器不保证永久开放、数据完整、实时性
或适合交易用途；任何交易决策都应使用具备相应授权和服务保障的数据源复核。

## 架构

```
src/xmtdx/
├── client.py          # 公共客户端：查询编排与连接生命周期
├── _client_logic.py   # 共享校验、分类和业务计算，无 IO
├── _pagination.py     # 共享分页状态与完整性检查，无 IO
├── _config.py         # 连接默认值与协议分页上限
├── transport/
│   ├── sync.py        # TdxConnection（socket）+ ping_host / ping_all
│   ├── async_.py      # AsyncTdxConnection（asyncio）
│   └── capture.py     # 请求/帧头/压缩体/解压体的诊断捕获
├── commands/          # 每条命令：build_request() + parse_response()，无 IO
├── codec/             # price / volume / datetime / frame 编解码
├── models/            # dataclass 与派生属性
├── validation.py      # 协议参数校验
└── dataframe.py       # 可选 pandas 适配
```

commands 层不依赖 transport，可独立单测。transport 层负责 TCP、握手、帧解压、分发。
高层客户端默认超时 5 秒，底层连接默认 15 秒，均可显式配置。

## 开发

```bash
# 单元测试（无需网络）
python -m pytest tests/unit --cov=xmtdx --cov-report=term-missing --cov-fail-under=70

# 集成测试（需要网络，默认跳过）
XMTDX_LIVE=1 python -m pytest tests/integration/

# 在线能力矩阵（自动移除进程代理变量，JSON 输出）
python scripts/validate_live.py --json

# 未知字段探测脚本
python scripts/probe_unknowns.py

# 类型检查
python -m mypy src

# lint
python -m ruff check .

# 构建
python -m build
```

## 协议说明

通达信使用私有二进制 TCP 协议：

- **帧格式**：16 字节响应头（含 zipsize / unzipsize），body 按需 zlib 解压
- **价格编码**：变长有符号整数（类 LEB128，bit8=继续，bit7=符号，首字节低 6 位 + 后续低 7 位）
- **成交量编码**：4 字节自定义浮点（字节 3 = 指数，字节 0-2 = 精度），仅用于明确采用该格式的字段，包括证券列表昨收；不用于差分价格解码
- **握手**：连接后必须顺序发送 3 条 setup 命令，响应丢弃
- **价格缩放**：行情、分时、逐笔通常除以 100；K 线除以 1000，按各命令处理差分基准

各命令的支持和验证状态见 [协议覆盖矩阵](docs/protocol-coverage.md)。在线行情会受交易时段、服务器版本、标的停牌和服务器临时故障影响；“解析已覆盖”不等于任何时刻每台公共服务器都可用。

在线工具使用原生 TCP，记录初始连接对端与目标路由，不修改系统代理或 VPN 配置。
发现 VPN/TUN 路由时停止直连验证；无法读取路由时明确标记尚未验证直连。
JSON 中的 `status` 区分 `pass`、`fail`、`warn`、`skipped`，只有 `pass` 的 `ok` 为真。
必需查询失败会返回非零退出码；金额覆盖率警告不代替成交量完整性校验。
