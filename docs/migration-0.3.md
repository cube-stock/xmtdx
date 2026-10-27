# 0.3.0 迁移说明

本次版本整理共享业务与分页逻辑，保留 Python ≥3.10、核心零运行时依赖及同步/异步查询。

## 需要修改的调用

- `SecurityQuote.server_time` 已删除，读取和构造记录均改用 `quote_time`。该值是单证券行情快照更新时间，不含日期。
- `to_dataframe()` 的行情结果不再包含 `server_time` 列，使用 `quote_time`。
- `compute_price_limits()` 删除未使用的 `finance_info` 参数，`listed_days` 改为仅限关键字：
  `compute_price_limits(market, code, name, pre_close, listed_days=6)`。
- 内部 `_format_server_time` 已删除，内部辅助函数不承诺稳定导入路径。

## 错误与分页

- 缺失市场统计统一抛出 `TdxResponseError`，不再抛出普通 `RuntimeError`。
- 行业、分时、价格限制及历史资金流的降级只捕获明确的连接、解码、响应异常；
  `ValueError`、`TypeError` 等参数或程序错误不再被静默吞掉。
- K 线区间遇到重复停滞或达到分页上限仍未结束时明确报错，不返回已知不完整结果。

## 开发工具

在线能力验证统一使用 `python scripts/validate_live.py --json`；日期命名的一次性验证脚本已移除，
其中独有的价格限制、市场计数与资金流金额覆盖检查已合并。未知字段探测仍单独保留，其假设未被证实。
