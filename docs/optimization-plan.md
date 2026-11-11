# 网关优化执行计划（不含账本）

## 阶段 0：错误契约与重试基础

- 在 `pkg/provider` 新增结构化上游错误 `UpstreamError`（含状态码、错误类型、是否可重试）。
- 改造 OpenAI、Anthropic、火山方舟三家 Provider 的所有上游错误出口，统一返回 `UpstreamError`。
- 重写 `routing.DecideRelayRetry`，改为通过 `errors.As` 识别 `UpstreamError`，并处理熔断打开错误、客户端取消、超时。
- 删除不再使用的 `routing.RetryError`。

## 阶段 1：重试按渠道展开

- 新增 `routing.PlanAttempts`，按优先级降序、层内加权洗牌，把渠道展开成有序的尝试列表。
- 重试次数改为按渠道数计算，每个渠道单次请求最多尝试一次。
- `gateway.max_attempts` 加入配置，默认 4，替换硬编码的 `maxRetryPerRequest`。
- Handler 的非流式、流式、fallback 路径统一改为遍历尝试列表。

## 阶段 2：熔断与重试咬合

- 熔断打开错误统一识别为“该渠道不可用，换下一个”，不直接结束请求。
- 流式路径把“建连 + 等待首帧”包进熔断器，首帧成功即视为成功，失败时取消派生 context。
- fallback 路径补上熔断包裹。
- abilities 被全部熔断过滤后，fallback 候选也做熔断过滤，避免绕过熔断。
- 熔断 key 从 provider type 调整为 provider + base_url。

## 阶段 3：渠道禁用策略

- 区分永久禁用与瞬态失败：401/403 及上游模型不存在才禁用，429/5xx/超时/网络错误不禁用。
- Provider 不支持流式时只跳过当前渠道，不再写 `enabled=0`。
- 新增 `003_abilities_health` 迁移，增加连续失败次数、禁用截止时间、禁用原因字段。
- 路由查询过滤冷却中的渠道；连续失败达到阈值后进入冷却，成功后清零。

## 阶段 4：调用链收敛

- 新增 `internal/relay`，承载候选计划、逐渠道尝试、熔断、非流式/流式调用。
- Handler 只保留请求解析、响应编解码和结果输出。
- 流式与非流式共用同一套 attempt 结果结构，删除重复的 4 份调用逻辑。

## 阶段 5：可观测性

- OTel 增加 exporter 配置（enabled、endpoint、insecure、sample_ratio），未配置时保持 Noop。
- Handler 与 relay 统一使用 `otel_ctx`，让上游调用继承 span。
- 每次 attempt 创建 child span，记录 provider、渠道 ID、重试序号、状态码。
- 熔断器状态变更回调 `RecordCircuitBreaker`。
- 删除缓存命中率指标，待缓存实现后再恢复。
- 新增重试次数指标。

## 阶段 6：测试补齐

- Provider 层用 `httptest.Server` 覆盖 200、401、429、500、超时、非法响应。
- `DecideRelayRetry` 用表驱动覆盖各类错误与重试余量。
- `PlanAttempts` 用固定随机种子验证同层多渠道展开顺序。
- 熔断阈值与超时改为可注入，覆盖 CLOSED → OPEN → HALF_OPEN → CLOSED。
- Handler 注入 fake 路由与 fake Provider，覆盖换渠道成功、全部失败、fallback 三类场景。

## 阶段 7：文档对齐

- `startup-skeleton.md` 中 PostgreSQL 改为 MySQL。
- README 中 5 个机制的落地状态改为“已落地 / 部分落地 / 仅设计”。
- 同步 `p0-minimal-ai.md` 中与当前实现不一致的说明。
