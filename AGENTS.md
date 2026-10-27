# 工程协作指南

本文件适用于整个仓库，供参与开发的 AI 编码助手与维护者使用。沟通、变更说明和新增文档默认使用中文；代码标识符沿用现有英文命名。

## 项目定位

`xmtdx` 是通达信私有 7709/TCP 行情协议的 A 股客户端，支持 Python 3.10 及以上版本，提供同步与 asyncio 接口。核心运行时仅使用标准库，pandas 为可选依赖。

协议没有官方公开规范。实现与字段解释应以源码、离线夹具和实际验证证据为依据，不能将推测写成已确认的协议语义。范围与限制参见 `README.md` 和 `docs/protocol-coverage.md`。

## 目录与职责

- `src/xmtdx/client.py`：同步与异步客户端、查询编排及连接生命周期。
- `src/xmtdx/_client_logic.py`、`_pagination.py`：共享纯计算与分页状态，无网络 IO。
- `src/xmtdx/_config.py`：连接默认值与协议分页上限。
- `src/xmtdx/commands/`：请求构造与响应解析；不得包含网络 IO。
- `src/xmtdx/transport/`：同步/异步连接、收发、超时、重连、心跳及响应捕获。
- `src/xmtdx/codec/`：帧、价格、成交量、日期等编解码与相关规则。
- `src/xmtdx/models/`：强类型数据模型与枚举。
- `src/xmtdx/validation.py`、`_binary.py`：协议参数校验和二进制读取辅助逻辑。
- `src/xmtdx/dataframe.py`：可选 pandas 转换。
- `tests/unit/`：离线解析与本地模拟传输回归测试。
- `tests/fixtures/`：原始十六进制响应与预期 JSON 数据。
- `tests/integration/`：真实服务器测试，默认跳过。
- `scripts/`：在线验证、协议探测与专项回归脚本。
- `.github/workflows/`：常规 CI 与手动在线验证工作流。

## 环境准备

在仓库根目录执行，优先使用项目虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,pandas]" build
```

项目采用 `src` 布局，通过可编辑安装导入源码。构建后端为 Hatchling；依赖和工具配置以 `pyproject.toml` 为准。

## 实现约定

- 保持 Python 3.10 兼容性，不引入仅新版本支持的语法或标准库 API。
- 遵循现有类型注解与 dataclass 风格；源码须通过 strict mypy。避免使用宽泛的 `Any` 或忽略检查来绕过类型问题。
- Ruff 目标版本为 `py310`，行宽配置为 100，启用 `E`、`F`、`I`、`UP` 规则。
- 维持核心零运行时依赖约定；pandas 相关行为保持可选，不影响基础包导入。
- 公共查询接口变更应检查同步与异步版本的一致性，并按需更新 `src/xmtdx/__init__.py` 的导出。
- 保留结果模型中的 `_raw` 原始字节和已有 `unknown_N` 字段。未经验证，不丢弃未知字段或擅自命名。
- 修改解析逻辑时明确字节序、字段长度、缩放单位和偏移，检查截断、非法长度及解压异常等边界。
- 沿用 `exceptions.py` 中的异常体系，不将坏包、缺失数据或不完整分页静默转换为成功结果或全零统计。
- 修改连接行为时，同时考虑同步/异步的超时、资源释放、重连及并发串行化语义。
- 公共 API 变更需明确版本和迁移说明，不为已移除的旧接口重新添加兼容层；改变字段含义、单位或已知限制时同步更新文档。

## 验证方式

先运行与改动相关的测试；涉及代码的变更在交付前执行常规 CI 检查：

```bash
python -m pytest tests/unit --cov=xmtdx --cov-report=term-missing --cov-fail-under=70
python -m mypy src
python -m ruff check .
python -m build
```

常规 CI 覆盖 Python 3.10、3.12、3.14；另有允许失败的 90% 覆盖率目标任务，不能将其与 70% 的必过门槛混淆。纯文档改动检查内容、路径和命令准确性即可。

新增或修复协议行为应补充能复现问题的离线测试，优先复用 `tests/fixtures/` 的夹具方式。传输问题优先使用本地模拟服务器测试，避免单元测试依赖公网和实时行情。

需要验证真实服务器行为时，可执行以下命令；这些操作会访问外部行情服务器：

```bash
XMTDX_LIVE=1 python -m pytest tests/integration/
python scripts/validate_live.py --json
```

在线行情测试必须移除进程中的 HTTP/HTTPS/ALL/SOCKS 等代理变量，并检查目标路由。
`validate_live.py` 自动处理该步骤；其他测试入口需显式清除代理环境。若路由经过 VPN/TUN，
先使目标直连；无法确认时不得把结果描述为已验证直连。不要自动修改系统代理或 VPN 设置。

集成测试可通过 `XMTDX_HOST` 指定服务器。在线验证结果受服务器和交易时段影响，应区分网络故障、服务端限制与实现错误；不得仅凭一次在线成功替代离线回归。

## 变更与交付

- 开始修改前检查工作区状态，保留用户已有改动，避免无关重构或批量格式化。
- 新增接口或改变行为时，更新 README 中的用法、限制以及必要的协议覆盖说明。
- 抓包固化为夹具前检查并移除敏感信息；不提交账号、授权信息、私有抓包或本地生成的构建与缓存产物。
- 交付说明应简述变更、验证结果和仍存在的限制；未执行或失败的检查如实说明。
