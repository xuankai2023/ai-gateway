# AI Gateway 基准基线（Benchmark Baseline）

> 本文件记录在**干净环境**下运行 `tests/k6/gateway_benchmark.js` 的基准数据，作为后续回归基准。
> 每次迭代阶段完成后必须在此追加新的基线行，便于对比。

## 运行条件

| 项 | 值 |
|---|---|
| 机器 | MacBook Pro 14" M4 Max 64GB（开发机） |
| Go 版本 | 1.24+ |
| 网关配置 | `config.Default()`（RequestTimeout=30s, AttemptTimeout=5s, 默认限流开启） |
| Redis | miniredis v2（内存版，实际生产建议用真实 Redis） |
| DB | fake SQL driver（实际生产用 MySQL 8.0+） |
| 上游 | httptest mock（固定延迟 50ms，返回静态 JSON） |

## 运行命令

```bash
# 1. 启动网关（本地开发模式）
go run ./apps/gateway/cmd/main.go --config config.dev.yaml

# 2. 另一终端跑 k6 基准
k6 run tests/k6/gateway_benchmark.js \
    --env BASE_URL=http://localhost:8080 \
    --env API_KEY=sk-benchmark-key \
    --env MODEL=gpt-4o-mini

# 3. 收集 JSON summary 输出到文件
k6 run tests/k6/gateway_benchmark.js 2>&1 | tee run-$(date +%Y%m%d-%H%M%S).log
```

## 基线数据

| 阶段 | 日期 | RPS | P50 | P95 | P99 | TTFT P99 | 错误率 | 备注 |
|---|---|---|---|---|---|---|---|---|
| 17（初始集成+熔断+账本） | 2026-11-11 | — | — | — | — | — | — | 待跑 k6 实机数据 |
| — | — | — | — | — | — | — | — | — |

## 场景覆盖说明

基准脚本覆盖了核心完整链路：

```
Auth → RateLimit → Idempotency → SemanticCache → Gray → Ledger → Relay(Retry+Breaker) → Provider(HTTP) → LedgerCapture → usagelog
```

每条链路包含：
- **Redis 交互**：Auth 缓存命中后 skip（首次 mock DB）、RateLimit Lua 真执行、Ledger trust_quota 跳过预占
- **Relay 重试/熔断**：默认 breaker threshold=5，mock 上游全部 200 所以不触发
- **账本结算**：trust_quota=10 默认跳过预占 → 直接扣 finalCost（约 $0.000025）
- **异步 usagelog**：batch tick 每 1s 合并写入，不阻塞主链路

## 回归阈值（CI）

k6 脚本内置了 thresholds，CI 里可以用更严格的值覆盖：

```yaml
# GitHub Actions CI example
- run: k6 run tests/k6/gateway_benchmark.js \
    --threshold "http_req_duration{p(99)}<3000" \
    --threshold "error_rate<0.01"
```

## 故障演练结果记录

| 演练项 | 日期 | 降级行为 | 告警触发 | 恢复时间 | 备注 |
|---|---|---|---|---|---|
| kill Redis | — | — | — | — | — |
| kill MySQL | — | — | — | — | — |
| 上游 5xx 风暴 | — | — | — | — | — |
| 单渠道全灭 | — | — | — | — | — |
