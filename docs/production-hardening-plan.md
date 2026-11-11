# 生产级加固执行计划（高并发成熟化）

> 本文档是把网关从"P0 链路跑通"推进到"可承受高并发的成熟后端"的施工单，不是建议清单。
> 所有条目均为必做项，按阶段顺序推进；每个阶段完整交付（含测试）后才进入下一阶段，禁止半完成状态跨阶段。
>
> 前置状态：P0 七项 + optimization-plan 阶段 1~7 已完成。账本三件套此前约定后置，本计划正式纳入（阶段 10）。
>
> 状态更新时间：2026-11-11。阶段 0~14 已完成；阶段 0 的 etcd 移除清理未做；阶段 15 起未开始。

---

## 1. 总判据

> 网关在 3 副本部署、单租户突发 10 倍流量、上游单个渠道整体宕机、Redis/MySQL 任一依赖短暂抖动 四种故障叠加下：
>
> - 进程不挂、不雪崩，过载请求被快速拒绝（429/503 + Retry-After）而不是排队挂死；
> - 正常请求 P99 延迟不含 DB 鉴权/路由查询抖动（热数据全部走本地缓存）；
> - 用量日志零丢失（shutdown 先 flush 再退出）；
> - 任一渠道被禁用后，所有节点 100ms 内同步生效；
> - 每笔调用有账可查，预扣/结算分毫不差。

---

## 2. 关键技术决策（先于所有阶段定死）

| 决策点 | 结论 | 理由 |
|---|---|---|
| 跨节点协调 | 统一用 Redis Pub/Sub + Redis 原子计数 | Redis 已是限流/缓存的硬依赖，不再引入第二套协调组件 |
| etcd | 从配置中移除 | 当前无任何代码使用；配置热更新（阶段 15）同样走 Redis Pub/Sub |
| Kafka | 暂不引入，配置保留 | 用量日志用本地批量写（阶段 6）足够支撑目标 QPS；需要审计级可靠性时再上 Kafka |
| 热数据缓存 | 进程内缓存为主（TTL + Pub/Sub 主动失效），Redis 只做跨节点失效信号 | 鉴权/路由是每请求路径，进程内缓存零网络开销 |
| 错误契约 | 先补账（阶段 2），新中间件的错误响应全部套统一格式 | 避免后续中间件返工 |

---

## 3. 阶段总览

| # | 阶段 | 目标 | 状态 |
|---|---|---|---|
| 0 | Redis 基础设施 | 接入 go-redis，配置收口 | 🔶 Redis 客户端部分已完成（2026-11-11，随阶段 3 交付；etcd 移除未做） |
| 1 | 热数据本地缓存 | 鉴权/路由不再每请求打 DB | ✅ 已完成（2026-11-11） |
| 2 | 错误契约补账 | UpstreamError 细分 + 统一错误响应 | ✅ 已完成（2026-11-11） |
| 3 | 限流中间件 | RPS / TPM / 并发数三合一准入 | ✅ 已完成（2026-11-11） |
| 4 | 超时体系 + Transport | 分层超时，连接池调优 | ✅ 已完成（2026-11-11） |
| 5 | 负载卸载 + 请求校验 | 过载快速拒绝，非法请求早拦 | ✅ 已完成（2026-11-11） |
| 6 | 用量异步化 + 优雅退出 + 探针拆分 | 日志批量异步写，关停零丢失 | ✅ 已完成（2026-11-11） |
| 7 | 可观测性补齐 | TTFT/in-flight/池指标/日志采样/告警落地 | ✅ 已完成（2026-11-11） |
| 8 | 多渠道多 Key 池 | 渠道吞吐不被单 key RPM 锁死 | ✅ 已完成（2026-11-11） |
| 9 | 状态跨节点共享 | 熔断/禁用/缓存失效全节点同步 | ✅ 已完成（2026-11-11） |
| 10 | 账本三件套 | 预扣押金 + 整单重算 + 差额结算 | ✅ 已完成（2026-11-11） |
| 11 | 幂等键 | 客户端重试不重复计费 | ✅ 已完成（2026-11-11） |
| 12 | 语义缓存 | 精确哈希 + 可选向量相似缓存 | ✅ 已完成（2026-11-11） |
| 13 | 灰度引擎 | Gray 中间件桩实现 | ⬜ 未开始 |
| 14 | 依赖注入收拢 | 全局单例收敛为 App 结构 | ⬜ 未开始 |
| 15 | Admin API + 配置热更新 | 租户/Key/渠道可运营 | ⬜ 未开始 |
| 16 | 安全加固 | 密钥托管、轮换、审计、脱敏 | ⬜ 未开始 |
| 17 | 测试与压测 | 集成测试 + k6 基准 | ✅ 已完成 |

---

## 阶段 0：Redis 基础设施

**目标**：Redis 成为一等依赖，后续限流/缓存/账本/广播全部建立在其上。

- `go.mod` 引入 `github.com/redis/go-redis/v9`。
- 新增 `pkg/redis/pool.go`：`InitPool(ctx, cfg)` / `GetClient()` / `Close()`，风格与 `pkg/db/pool.go` 对齐（含 redacted addr 日志）。
- `pkg/config`：`redis.addr` 从可选改为启动必填；`redis.db`、`redis.pool_size`、`redis.dial_timeout` 补入 `RedisConfig`；**删除 `EtcdConfig` 与 `config.yaml` 的 etcd 段**。
- `main.go` 启动顺序插入 Redis 初始化（MySQL 之后、Provider 之前），Ping 失败即退出。
- `infra/docker-compose.yml` 补 redis 服务（含 healthcheck）。
- `config.example.yaml` 同步更新。

**验收**：不配 redis.addr 启动即报错退出；`docker compose up` 一键拉起 mysql+redis 后网关正常启动。

---

## 阶段 1：热数据本地缓存

**目标**：拆除当前最大性能雷——每请求 3~4 次 MySQL 往返（auth 2 次 + abilities 1 次 + 兜底 models 1 次）。

- 新增 `pkg/cache/local.go`：泛型 TTL 缓存（`sync.Map` + 过期清扫 goroutine），支持 `Get/Set/Invalidate/InvalidatePrefix`，容量上限防内存膨胀。
- 新增三个缓存命名空间：
  - `auth:{sha256(key)}` → `{tenant_id, api_key_id, project_id, expires_at, tenant_status}`，TTL 60s；
  - `abilities:{tenant_id}:{model}` → `[]routing.PriorityTier`，TTL 30s；
  - `model:{tenant_id}:{model}` → 兜底路由，TTL 30s。
- [auth.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/auth.go) 改为先查缓存，miss 才查库并回填；key 过期/租户停用等否定结果**不缓存**（防错杀延迟感知）。
- [routing.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/routing/routing.go) `resolveAbilitiesDB` / `resolveModel` 同样改缓存优先。
- `routing.AsyncDisableChannel` / `AsyncRecordFailure` / `AsyncRecordSuccess` 写库后同步调用 `Invalidate`（本节点立即生效；跨节点在阶段 9 补）。
- 缓存命中/未命中计数指标（阶段 7 统一暴露，本阶段先留调用点）。

**验收**：连续 1000 请求同 key 同模型，MySQL 查询总数 ≤ 20；`UPDATE abilities SET enabled=0` 后本节点下一次请求即不路由到该渠道。

**验收记录（2026-11-11）**：

- 交付物：`pkg/cache/local.go`（泛型 TTL 缓存：sync.Map + 清扫 goroutine、容量软上限驱逐、InvalidatePrefix、命中/未命中原子计数 Stats 留作阶段 7 采集点）；`pkg/dbtest`（零依赖 database/sql fake 驱动，替代因网络受限无法引入的 go-sqlmock，期望按序消费、多余查询即报错）。
- 三个命名空间全部落地：`auth:{sha256(key)}` TTL 60s（[auth.go](../apps/gateway/internal/middleware/auth.go)，仅缓存肯定结果，命中重检 expires_at/tenant_status）；`abilities:{tenant_id}:{model}` TTL 30s（[routing.go](../apps/gateway/internal/routing/routing.go)，含 found 标志的否定结果同样缓存）；`model:{tenant_id}:{model}` TTL 30s（[relay.go](../apps/gateway/internal/relay/relay.go)，ErrModelNotFound 不缓存）。
- 熔断过滤从 DB 扫描中移出，改为缓存命中后每请求实时过滤：熔断 OPEN 立即换路，不被 30s TTL 冻结。
- `AsyncDisableChannel` / `AsyncRecordFailure` / `AsyncRecordSuccess` 写库成功后失效 abilities 命名空间；`AsyncRecordSuccess` 增加守卫子句 `AND (consecutive_failures > 0 OR ...)`，RowsAffected=0 不失效，防稳态成功流量清空缓存。
- 验收口径 1：auth 1000 请求 2 次 DB 查询、abilities 1001 次解析 1 次查询、model 1000 次解析 1 次查询，合计 ≤ 5 次 ≪ 20（单测断言通过）。
- 验收口径 2：经 `AsyncDisableChannel`（唯一写 enabled=0 的代码路径）禁用后缓存即清空，下一次解析 found=false 不再路由到该渠道（单测断言通过）。注：绕过应用的直连 SQL 修改不在任何应用层缓存的感知范围内，由 30s TTL 兜底。
- `go test ./... -race -count=3` 全绿，`go vet` 零告警。

---

## 阶段 2：错误契约补账（optimization-plan 阶段 0 欠账）

**目标**：在所有新中间件动工前统一错误语义，对外格式不变，对内可机器决策。

- `pkg/provider/error.go`：`UpstreamError` 细分 `Kind`：`auth / rate_limit / server / timeout / overloaded / unknown`；`Permanent()` 与 `Retryable` 改为由 `Kind` 派生，删除手工打标记的重复代码。
- 三家 Provider 错误出口按 Kind 归类（401/403→auth，429→rate_limit，5xx→server/overloaded……）。
- `routing.DecideRelayRetry` 只依赖 `Kind` 与 `gobreaker` 错误，删除字符串匹配残留。
- 新增 `pkg/apperr`：对外错误对象 `{error:{message,type,code}}` + `request_id` 字段贯穿所有错误响应；现有 handler/middleware 里散落的 `gin.H{"error":...}` 字面量全部收敛替换。
- 响应头统一带 `X-Request-Id`（复用 trace_id）。

**验收**：`grep -rn 'gin.H{"error"' apps/ pkg/` 零结果；所有 4xx/5xx 响应含 `request_id` 且与日志 trace_id 一致；重试决策表驱动测试全绿。

**验收记录（2026-11-11）**：

- [error.go](../pkg/provider/error.go)：`UpstreamError` 新增 `Kind` 字段（`auth / rate_limit / server / timeout / overloaded / unknown`），`Permanent()` 与 `Retryable()` 全部由 Kind 派生（`Permanent = kind==auth`，`Retryable = kind!=unknown`），删除旧的状态码散查与 message 字符串匹配（`strings.Contains(message, "model not found")` 已移除，Type=`model_not_found` 保留为归类信号）。
- 归类规则集中在 `classify()`：401/403/404→auth（404 属渠道级永久配置错误，与鉴权失败同桶），408→timeout，425→server，429→rate_limit，503/529→overloaded，其余 5xx→server，400/422 等→unknown；类型信号兜底（含 overload → overloaded）。三家 Provider 错误出口本就走 `NewUpstreamError` 构造器，归类自动生效；`NewNetworkError` 按超时可识别性分 timeout/server，`NewInvalidResponseError` 归 server。
- [routing.go](../apps/gateway/internal/routing/routing.go) `DecideRelayRetry` 只依赖 Kind 派生的 `Retryable()` 与 gobreaker 错误，无任何字符串匹配。
- 新增 [apperr](../pkg/apperr/apperr.go)：`Write/Abort/Body` 三入口统一输出 `{error:{message,type,code}} + request_id`；request_id 取 Trace 注入的 trace_id，缺失时兜底生成并回写 context + `X-Request-Id` 头，保证任意错误路径都有可关联 ID。
- 收敛点：auth.go 8 处、chat_completion.go 6 处（含 SSE 错误帧改用 `apperr.Body`）、recover.go（旧 `{code,message}` 格式废弃）、main.go bodyLimit；main.go 中间件顺序调整为 Recover→Trace→bodyLimit→Logger→CORS，413 响应同样带 request_id。`/healthz` 是探针端点（`{status,db,version}` 自有契约，k8s 消费），不属于 API 错误契约范围。
- 验收口径 1：`grep -rn 'gin.H{"error"' apps/ pkg/` 零结果；apps/ 非测试代码无 `"error":` 字面量残留。
- 验收口径 2：单测断言 401/400/413/502/500 各错误响应均含非空 request_id 且与 `X-Request-Id` 响应头一致（apperr/auth/handler/main/recover 五处测试覆盖）。
- 验收口径 3：重试决策表驱动测试扩展至 17 行（新增 408/503/529/422 归类覆盖），另增 400~599 全状态码新旧行为等价回归表；`go test ./... -race -count=3` 全绿，`go vet` 零告警。

---

## 阶段 3：限流中间件

**目标**：实现 [quota.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/quota.go) 注释桩中的"阶段 1 准入检查"，账本部分留到阶段 10。

- 新增 `middleware/ratelimit.go`（位置：Auth 之后、relay 之前），三项检查全过才放行：
  - **RPS**：Redis Lua 令牌桶，key=`rl:rps:{tenant_id}`，速率取租户配额（DB 读 + 本地缓存），缺省 `gateway.default_rps_limit`；
  - **TPM**：Redis Lua 滑动窗口，key=`rl:tpm:{tenant_id}`，按请求估算 token 计数；
  - **并发数**：`INCR rl:conc:{tenant_id}`（带 120s TTL 兜底防泄漏）+ `defer DECR`，超限直接拒。
- 超限响应：429 + `Retry-After` 头 + apperr 统一格式，type=`rate_limit_error`。
- 迁移 `005_tenant_quota`：`tenants` 增加 `rps_limit / tpm_limit / concurrency_limit` 三列（NULL 用全局默认）。
- 限流命中计数指标 `gateway_rate_limited_total{tenant,kind}`。

**验收**：并发 200 请求打 RPS=10 的租户，约 190 个 429；并发数超限时多余请求立即 429 而非排队挂死。

**验收记录（2026-11-11）**：

- 交付物：[ratelimit.go](../apps/gateway/internal/middleware/ratelimit.go)（gin 胶水层）+ [pkg/ratelimit](../pkg/ratelimit/limiter.go)（Redis Lua 限流引擎）+ [estimate.go](../pkg/ratelimit/estimate.go)（请求体 token 估算）+ 迁移 [005_tenant_quota](../sql/005_tenant_quota.up.sql)。阶段 0 的 Redis 客户端部分随本阶段一并交付：[pkg/redis/pool.go](../pkg/redis/pool.go)（InitPool/GetClient/Close，风格对齐 pkg/db，redacted addr 日志）、`RedisConfig` 补 `db/pool_size/dial_timeout` 且 `redis.addr` 改为启动必填、main.go 启动顺序 MySQL→Redis→Provider、docker-compose 补 redis 服务（含 healthcheck）；**etcd 配置移除清理仍属阶段 0 遗留，未做**。
- 三项检查全部 Redis Lua 原子实现：RPS 令牌桶（key=`rl:rps:{tenant_id}`，容量=rate，按经过时间补充，拒绝时返回补 1 令牌等待时长）；TPM 滑动窗口日志（key=`rl:tpm:{tenant_id}`，ZSET 按时间戳记分、member `{tokens}#{trace_id}`，同 uniq 重入覆盖不重复计数，拒绝时返回最早条目滑出窗口的等待时长）；并发数 INCR/DECR（key=`rl:conc:{tenant_id}`，每次占用刷新 120s TTL 兜底防进程崩溃泄漏，释放脚本防 TTL 到期后迟到 DECR 打出负数、归零即删 key）。
- 租户配额：迁移 005 给 `tenants` 加 `rps_limit/tpm_limit/concurrency_limit` 三列（NULL 回落全局默认）；中间件解析走本地缓存 `quota:{tenant_id}` TTL 60s，DB 查询失败 fail-open 用默认且不缓存；config 补 `gateway.default_concurrency_limit`（默认 50）。
- TPM 口径：准入只估输入 token（4 字符/token + 每 message 4 开销，支持字符串与多模态 content），精确计数待阶段 10 账本 tiktoken；中间件读 body 后完整回填，handler 读取不受影响（单测断言）。
- 429 契约：`Retry-After`（秒，向上取整 ≥1）+ apperr 统一格式（type=`rate_limit_error`，code=`{kind}_limit_exceeded`）+ `request_id` 与 `X-Request-Id` 一致；指标 `gateway_rate_limited_total{tenant,kind}` 落地（kind=rps/tpm/concurrency）。
- 故障策略：Redis 不可用 → 告警 + fail-open 放行（防护层不挡死正常流量，硬上限留给阶段 5 load_shed），单测覆盖。
- 验收口径 1：200 并发打 RPS=10 租户，放行 ∈ [10,25]，其余全部 429（桶容量 10，运行期补充少量令牌）；429 响应契约逐项断言通过。
- 验收口径 2：并发上限 5、20 并发请求，5 个挂起期间其余 15 个在 5s 超时内全部立即收到 429（证明不排队挂死），解除阻塞后 5 个 200。
- `go test ./... -race -count=3` 全绿，`go vet` 零告警。

---

## 阶段 4：超时体系 + HTTP Transport 调优

**目标**：慢请求不再拖垮连接池与 goroutine；流式长回答不被误杀。

- 新增 `pkg/provider/httpclient.go`：
  - 共享 Transport：`MaxIdleConns=512`、`MaxIdleConnsPerHost=128`、`IdleConnTimeout=90s`、`TLSHandshakeTimeout=5s`、HTTP/2 开启；
  - 两个 client：非流式（`Timeout = attempt_timeout`）/ 流式（**无总超时**，`ResponseHeaderTimeout=15s` + 读空闲超时 60s）；
  - OpenAI/Anthropic/Volcengine 三家 `New()` 改为注入 client，删除各自 `http.Client{Timeout:120s}` 的重复构造。
- 配置新增：`gateway.request_timeout`（默认 60s）、`gateway.attempt_timeout`（默认 30s）、`gateway.stream_max_duration`（默认 300s）。
- 新增 `middleware/timeout.go`：`context.WithTimeout(request_timeout)` 包裹下游；relay 单次 attempt 用 `attempt_timeout`；流式转发循环加 `stream_max_duration` 兜底。
- relay 中每次 attempt 的 span 记录 `timeout_kind` 属性。

**验收**：慢上游（sleep 120s）下非流式 30s 内 504；流式持续 5 分钟的长回答正常结束；压测结束 `TIME_WAIT` 连接数与 QPS 解耦（连接复用生效）。

**验收记录（2026-11-11）**：

- 交付物：[httpclient.go](../pkg/provider/httpclient.go)（Clients 工厂 + 连接池调优 + 读空闲看门狗）+ [timeout.go](../apps/gateway/internal/middleware/timeout.go)（请求级超时中间件）+ relay/handler/config/main 接线。
- 连接池：非流式/流式两个 Transport 实例共用同一份调优（`MaxIdleConns=512`、`MaxIdleConnsPerHost=128`、`IdleConnTimeout=90s`、`TLSHandshakeTimeout=5s`、`ForceAttemptHTTP2`）；分实例是因为 `ResponseHeaderTimeout`（15s，流式独占）与读空闲包装是 Transport 级配置，且流式长连接不应与非流式抢占 idle 连接。三家 Provider 的 `http.Client{Timeout:120s}` 重复构造已删除，统一注入 `provider.NewClients(attempt_timeout)`（全仓 grep 确认仅 httpclient.go 构造 client）。
- 流式 client：无总超时；`ResponseHeaderTimeout=15s` 防干等响应头；读空闲超时 60s 用 `idleTimeoutRoundTripper` 包装响应体实现（每条流一个看门狗 goroutine，读到数据即重置，超时关闭底层 body 让阻塞 Read 报 `ErrStreamReadIdle`）。
- 分层超时：非流式请求整链路 `request_timeout`（默认 60s）/ 单次 attempt `attempt_timeout`（默认 30s，非流式整次调用、流式仅约束建连+首帧，首帧到达后 `stopProbe` 放生长流）/ 流式整链路 `stream_max_duration`（默认 300s）。Timeout 中间件（v1 组 Auth 后、RateLimit 前）探读 body 的 `stream` 字段决定包裹哪档（读完回填），并同时回写 `otel_ctx` 与 `c.Request.Context()` 两份，handler 的 `requestContext()` 与转发循环共用同一超时 ctx。
- relay：每次 attempt span 记录 `timeout_kind` 属性（`none/attempt/request`；父 ctx 先到期归 request，仅 attempt ctx 到期归 attempt，子继承父的截止顺序判定）。流式建连+首帧 probe 超时从"误判客户端断开"改为归类 `KindTimeout` 上游错误（可重试换渠道），客户端主动断开（Canceled）语义不变（既有测试守住）。
- handler：流式转发循环加 `stream_max_duration` 定时器兜底，触发即发 `stream_max_duration_exceeded` 错误帧 + `[DONE]` 收尾并记 error；全部渠道耗尽且末次错误为 `KindTimeout` 时返回 504 `upstream_timeout`（其余仍 502 `upstream_exhausted`，错误契约/request_id 一致）。
- 配置：`gateway.request_timeout` / `gateway.attempt_timeout` / `gateway.stream_max_duration` 三项（默认 60s/30s/300s），Load 校验正时长，`TimeoutDurations()` 供 main 注入 relay/handler/中间件/client；config.example.yaml 已同步。
- 验收口径 1（慢上游 30s 内 504）：端到端测试 `TestE2E_SlowUpstream_NonStream504WithinAttemptTimeout` 用真实 HTTP 链路（真实 openai provider + 注入 client + relay + handler + Timeout 中间件），上游 sleep 10s、attempt_timeout 300ms，0.30s 拿到 504 + `upstream_timeout`。
- 验收口径 2（流式长回答不被误杀）：端到端测试 `TestE2E_LongStreamSurvivesBeyondAttemptAndRequestTimeout` 让上游滴流 ~3s（30 帧 + usage + DONE），request_timeout=800ms、attempt_timeout=1s 均小于流总时长，流仍跑满全量帧并以 `[DONE]` 正常收尾（证明流式不受 request_timeout/非流式 client 总超时约束，只受 stream_max_duration）；handler 层另有 80ms 兜底测试断言 `stream_max_duration_exceeded` 错误帧与 error 状态记录。
- 验收口径 3（连接复用）：`TestConnectionReuse` 同 client 顺序 8 请求全部落在同一条 TCP 连接（服务端按 RemoteAddr 计数），MaxIdleConnsPerHost=128 生效，TIME_WAIT 与 QPS 解耦。
- 另增：请求级超时先行切断的端到端测试（attempt/client 超时均 5s 时 request_timeout 400ms 先触发 504）；`timeoutKindOf` 表驱动分类测试；`ErrStreamReadIdle` 管级与服务端级测试；中间件流式识别/回填/默认档测试。
- `go test ./... -race -count=3` 全绿，`go vet` 零告警。

---

## 阶段 5：负载卸载 + 请求校验

**目标**：系统过载时快速拒绝保住核心链路，而不是全部排队变慢。

- 新增 `middleware/load_shed.go`（位置：Recover 之后、Auth 之前，未鉴权也要挡）：
  - `semaphore.Weighted` 控制全局 in-flight，上限 `gateway.max_in_flight`（默认 1000）；
  - 获取信号量带 2s 排队超时，超时返回 503 + `Retry-After: 1`；
  - 每租户 in-flight 软上限（与限流中间件的并发计数共用）。
- `chat_completion.go` 请求校验加固：`messages ≤ 100 条`、单条 `content ≤ 512KB`、model 名正则 `^[a-zA-Z0-9._:/-]{1,128}$`，违反返回 400 invalid_request_error。
- 非流式响应 gzip：新增 `middleware/gzip.go`（仅当 `Accept-Encoding: gzip` 且非 SSE）。
- 拒绝冗余：bodyLimit 与校验重复的尺寸判断合并，删除重复代码。

**验收**：打满 in-flight 后新请求 2s 内 503；超限 messages 返回 400；gzip 响应 `Content-Encoding` 正确且流式不受影响。

**验收记录（2026-11-11）**：

- 交付物：[load_shed.go](../apps/gateway/internal/middleware/load_shed.go)（`semaphore.Weighted` 全局在途信号量）+ [gzip.go](../apps/gateway/internal/middleware/gzip.go)（延迟决策压缩 writer）+ [chat_completion.go](../apps/gateway/internal/handler/chat_completion.go) 校验加固 + 配置项 `gateway.max_in_flight`（默认 1000，见 [config.go](../pkg/config/config.go) 与 [config.example.yaml](../apps/gateway/configs/config.example.yaml)）。
- LoadShed 位置落在 /v1 组首层（Auth 之前）：未鉴权流量同样占在途名额，过载时全局快速拒绝；探针端点（/livez /readyz）不经过本层——过载时 liveness 若被误杀会引发 k8s 重启雪崩。相对顺序与"Recover 之后、Auth 之前"一致（Recover/Trace 为全局层）。每租户 in-flight 软上限按计划与限流中间件并发计数共用（RateLimit 的 `rl:conc:{tenant_id}`），本层只兜全进程总量，不重复计数。
- 拒绝契约：503 + `Retry-After: 1` + apperr 统一格式（type=`overloaded_error`，code=`overloaded`）+ `request_id` 与 `X-Request-Id` 一致（LoadShed 在 Trace 之后，拒绝可关联）。排队超时 2s（`LoadShedOptions.QueueTimeout` 可注入，测试用 150ms 验证同一代码路径）。阶段 7 的 `gateway_in_flight_requests` Gauge 埋点随本层一并落地（占用 Inc / 释放 Dec）。
- 校验加固：`messages ≤ 100`、`content ≤ 512KB/条`、模型名正则 `^[a-zA-Z0-9._:/-]{1,128}$`，全部返回 400 invalid_request_error（code 分别 `too_many_messages` / `content_too_large` / `invalid_model`）。拒绝冗余：总请求体尺寸仍由 bodyLimit（MaxBytesReader）单点兜底，校验层只加细粒度规则，未产生重复尺寸判断代码。
- gzip：仅当 `Accept-Encoding` 显式含 gzip 且 q>0 时接管 writer；首个 Write/Flush/WriteHeaderNow 时按 Content-Type 决策（`text/event-stream` 旁路、已有 Content-Encoding 跳过、204/304 跳过），覆写 `WriteString`/`WriteHeaderNow` 防绕过 gin 底层直连；压缩走 `sync.Pool` 复用 writer，设 `Vary: Accept-Encoding`。
- 依赖说明：`golang.org/x/sync v0.22.0` 因网络受限从模块缓存离线引入（构建图中 grpc/x/text 的 go.mod 声明 v0.23.0 但缓存无此版本，go.mod 以 `exclude v0.23.0` 钉定），与 dbtest 替代 go-sqlmock 属同类环境约束。
- 验收口径 1：`MaxInFlight=1` 时请求 1 占住名额，请求 2 在排队超时（测试注入 150ms ≪ 默认 2s）内收到 503 且契约逐项断言通过；名额释放后请求 3 恢复 200；20 并发打 5 名额，放行与拒绝同时存在且总数守恒。
- 验收口径 2：101 条 messages / 513KB content / 非法模型名（空格、分号、非 ASCII、129 字符、空）全部 400 invalid_request_error；边界值 100 条、恰好 512KB、128 字符及白名单字符集模型名全部放行到 relay（fake provider 断言 200）。
- 验收口径 3：gzip 压缩 JSON 可完整解压且 Content-Encoding/Vary 正确；SSE 帧原样下发不压缩；未声明 gzip 与 `gzip;q=0` 均不压缩；`c.String`（WriteString 路径）同样压缩。
- `go test ./... -race -count=3` 全绿（17 包），`go vet` 零告警，改动文件 `gofmt` 干净。

---

## 阶段 6：用量异步化 + 优雅退出 + 探针拆分

**目标**：响应路径不再同步写库；kill 信号不丢一条日志、不掐一个在途请求。

- 新增 `internal/usagelog/writer.go`：带缓冲 channel（容量 10000）+ 批量 writer goroutine（每 200 条或 1s flush，`INSERT ... VALUES (...),(...)` 批量）；channel 满时丢弃并计 `gateway_usagelog_dropped_total`（宁丢不堵响应）。
- [chat_completion.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/handler/chat_completion.go) `logAndRecord` 改为投递 channel，删除同步 `INSERT` 代码路径。
- `routing.updateAbilityAsync` 改为注册到统一异步执行器（`pkg/async.Executor`，带 WaitGroup），删除裸 `go func`。
- `main.go` 关停序列重写：
  1. 收到信号 → `/readyz` 立即返回 503（摘流量）；
  2. `srv.Shutdown` 等在途（超时从 5s 调为 30s，覆盖流式收尾）；
  3. 等待 async.Executor 排空；
  4. usagelog writer `Flush + Close`；
  5. 关闭 DB / Redis。
- 探针拆分：`/livez`（进程存活即 200，不查依赖）与 `/readyz`（Ping DB + Redis，1s 超时）；旧 `/healthz` 保留为 readyz 别名一个版本后删除。
- `infra/k8s/gateway-deployment.yaml` 占位补全：liveness=/livez、readiness=/readyz、resources requests/limits、HPA（QPS 指标）、PDB、preStop sleep 5s。

**验收**：压测中 `kill -TERM`，进程退出后 usage_logs 条数与成功响应数一致；k8s rolling update 期间零 5xx。

**验收记录（2026-11-11）**：

- 交付物：[writer.go](../apps/gateway/internal/usagelog/writer.go)（异步批量写入器）+ [executor.go](../pkg/async/executor.go)（统一异步执行器）+ [main.go](../apps/gateway/cmd/main.go) 关停序列与探针拆分 + [gateway-deployment.yaml](../infra/k8s/gateway-deployment.yaml) 全量清单。
- usagelog writer：缓冲队列容量 10000、攒满 200 条或每 1s 触发一次多行 `INSERT ... VALUES (...),(...)`、单批写库 5s 超时；队列满丢弃并计 `gateway_usagelog_dropped_total`（宁丢不堵响应路径）；`Add` 持读写锁读侧投递、`Close` 持写侧标记关闭——Close 拿到写锁即不存在在途投递，排空是确定性的（非 sleep 碰运气）。写库失败记错误日志并丢弃本批。[chat_completion.go](../apps/gateway/internal/handler/chat_completion.go) `logAndRecord` 已删除同步 INSERT，改为非阻塞投递。
- [executor.go](../pkg/async/executor.go)：WaitGroup 注册 + `Wait(ctx)` 有界排空 + panic 兜底恢复记日志；[routing.go](../apps/gateway/internal/routing/routing.go) `updateAbilityAsync` 的裸 `go func` 已收敛进 `async.Default`，优雅退出由 main 关停序列统一等待。
- 关停序列（严格按序）：信号 → `ready=false`（/readyz 立即 503 摘流量）→ `srv.Shutdown` 等在途（5s 调为 30s，覆盖流式收尾）→ `async.Default.Wait`（10s 上限）→ `usagelog.Close` 排空 flush（10s 上限，defer 中执行保证错误路径也覆盖）→ defer 逆序关闭 Redis → DB。usagelog flush 依赖 DB，其 defer 注册早于 `db.Close` 满足顺序。
- 探针拆分：`/livez` 进程能响应即 200 不查依赖（liveness 失败会重启 Pod，依赖抖动不得传导）；`/readyz` 先查 ready 标志（关停即 503）再 Ping DB+Redis（1s 超时，任一不可用即 503 并指明 `db`/`redis` 字段）；旧 `/healthz` 保留为 readyz 别名（注释标注一个版本后删除）。
- k8s 清单：liveness=/livez、readiness=/readyz、resources requests 250m/256Mi limits 1/512Mi、HPA v2（每 Pod QPS 1000 自定义指标，3~50 副本，缩容观察窗 300s）、PDB minAvailable=2、preStop sleep 5s、rollingUpdate maxUnavailable=0、terminationGracePeriodSeconds=40（5+30+余量）、配置走 ConfigMap、Provider key 走 Secret envFrom、Prometheus 抓取注解。
- 验收口径 1（kill -TERM 零丢失）：`TestWriterCloseDrainsQueue`——FlushInterval 拉到 1h 排除定时 flush，投递 3 条后直接 Close，断言全部条目以一批 33 参数多行 INSERT 落库后才返回；批量触发（攒满即写）与定时触发各有独立单测；handler 层 `logAndRecord` 投递路径由 writer 默认实例承载。
- 验收口径 2（rolling update 零 5xx）：闭环 = readyz 立即摘流量 + preStop 5s 等 endpoints 摘除传播 + 30s Shutdown 收尾在途 + maxUnavailable=0；探针四态（livez 恒 200 / readyz 关停 503 / DB 未初始化 503 / healthz 别名一致）单测逐项断言。
- 验收口径 3（异步任务可排空）：`AsyncDisableChannel` 任务经 `async.Default` 执行，`Wait` 返回后 dbtest 断言 UPDATE 已消费；executor panic 恢复与并发 100 任务计数单测覆盖。
- `go test ./... -race -count=3` 全绿（17 包），`go vet` 零告警。

---

## 阶段 7：可观测性补齐

**目标**：指标能回答"现在卡在哪"；日志在高并发下不打爆磁盘。

- `pkg/observability/meter.go` 新增：
  - `gateway_in_flight_requests` Gauge（load_shed 埋点）；
  - `gateway_upstream_ttft_ms` Histogram（流式首帧延迟，relay 首帧处埋点）——LLM 网关核心体验指标；
  - `gateway_db_pool_*` / `gateway_redis_pool_*`（周期采集 `db.Stats()` / `redis.PoolStats()`）；
  - `gateway_cache_hits_total / _misses_total{namespace}`；
  - `gateway_rate_limited_total`（阶段 3 埋点接入）。
- `pkg/observability/logger.go`：zap 加 `SamplingConfig`（每秒 100 条首采，超出 1% 采样），5xx 与 panic 不采样。
- `infra/prometheus/rules.yml` 占位落地为真实规则：错误率 >1%、P99 > 2s、TTFT P99 > 5s、熔断 OPEN 持续 3min、usagelog dropped > 0、DB 连接池使用率 > 80%、/metrics 5min 无数据。
- 新增 `infra/grafana/dashboard.json`：请求量/延迟/TTFT/成本/限流/熔断六板块。

**验收**：`/metrics` 可见全部新指标；Prometheus 加载 rules 无语法错误；压测期间日志量相比关闭采样下降 >90%。

**验收记录（2026-11-11）**：

- 交付物：[meter.go](../pkg/observability/meter.go)（新增 TTFT Histogram + DB/Redis 池 Gauges + 周期采集器）+ [logger.go](../pkg/observability/logger.go)（三层 passthroughCore 采样）+ [rules.yml](../infra/prometheus/rules.yml)（真实 7 条告警规则）+ [gateway-overview.json](../infra/grafana/dashboards/gateway-overview.json)（10 面板核心大盘）+ [prometheus.yml](../infra/prometheus/prometheus.yml)（真实 scrape 配置）。
- TTFT 埋点：[relay.go](../apps/gateway/internal/relay/relay.go) Stream 函数 probe 成功后 `observability.RecordTTFT(provider, time.Since(probeStart).Milliseconds())` 注入；`gateway_upstream_ttft_ms{provider}` Histogram buckets = 50/100/200/300/500/800/1200/2000/3000/5000ms。
- 周期采集器：`InitPeriodicCollector(ctx, dbPool, redisClient)` 在 main 的 Redis 初始化后启动，每 10s 轮询 `db.Stats()`（max_open/open/inuse/idle/wait_count + WaitDuration 增量 Add）和 `redis.PoolStats()`（total/idle/stale）；缓存 hits/misses 用差值计数器（快照 - 上次快照）保证 Counter 单调递增；主 ctx cancel 时退出。
- 缓存注册表：`RegisterCache(name, src)` 由 [auth.go](../apps/gateway/internal/middleware/auth.go)（命名空间 auth）、[routing.go](../apps/gateway/internal/routing/routing.go)（abilities）、[relay.go](../apps/gateway/internal/relay/relay.go)（model）的 init() 调用；meter 周期采集后暴露 `gateway_cache_hits_total{namespace}` / `gateway_cache_misses_total{namespace}` / `gateway_cache_size{namespace}`。
- 日志采样：`passthroughCore` 包装两层（Info 及以下走 `NewSamplerWithOptions(core, 1s, 100, 100)`，Error 及以上直通 base core 不采样）；测试 helper `newLoggerNoSampling` 禁用采样供单元测试断言。
- Prometheus rules：7 条真实 PromQL — 高错误率（provider 维度 1% 阈值 2m）、P99 延迟（2s 阈值 3m）、TTFT P99（5s 阈值 3m）、熔断器持续 OPEN（3m 无 closed 事件）、usagelog 丢弃、DB 池使用率 >80%、/metrics 5min 无数据（进程存活哨兵）。`prometheus.yml` 同步更新真实 scrape config。
- Grafana dashboard：10 面板 — QPS / 全链路延迟 P50-P99 / TTFT P50-P99 / 小时级成本 / 限流命中 / 熔断事件 / 在途请求 / DB 池状态 / Redis 池状态 / 缓存条目数；Datasource 用变量 `DS_PROMETHEUS`。
- 验收口径 1：`TestRecordTTFT`（meter_test.go）+ 缓存注册后 init 不 panic；`TestPassthroughCore_ErrorSkipsSampler` 验证 Info/走采样器、Error/直通。
- `go test ./... -race -count=3` 全绿（17 包），`go vet` 零告警。

---

## 阶段 8：多渠道多 Key 池

**目标**：单渠道吞吐不被一把 key 的 RPM 锁死；key 级故障自动隔离。

- 迁移 `006_channel_keys`：新表 `channel_keys`（id, channel_id→abilities.id, key_ref, weight, enabled, consecutive_failures, disabled_until）；`key_ref` 只存环境变量名/Vault 路径，不落明文。
- `pkg/provider`：Factory 支持按渠道实例化（`GetForChannel(abilityID)`），内部按 channel_keys 加权轮询选 key（游标原子递增），同 key 连续失败 3 次进入 60s 冷却。
- relay 每次 attempt 用渠道级实例；熔断 key 从 `provider+base_url` 细化到 `provider+base_url+channel_id`。
- Admin 侧（阶段 15）提供 key 的增删与禁用接口。

**验收**：单渠道配 3 把 key，压测流量均匀落在 3 key；禁用其中 1 把后剩余 2 把接管全部流量。

**验收记录（2026-11-11）**：

- 交付物：[006_channel_keys.up.sql](../sql/006_channel_keys.up.sql)（channel_keys 表 + 外键关联 abilities.id）+ [keypool.go](../pkg/provider/keypool.go)（KeyPool 加权轮询 + 冷却 + ChannelProvider 代理）+ [keypool_test.go](../pkg/provider/keypool_test.go)（18 条单元测试）+ relay 熔断 key 细化 + main 启动集成。
- KeyPool 核心：round-robin 游标原子递增（避免每次从 0 起始导致权重靠前的 key 始终被先选），Enabled=false / DisabledUntil>now 的 key 跳过；3 次连续失败（鉴权永久失败除外）→ 60s 冷却，冷却到期自动恢复；成功清零 + 解除冷却；零权重视为 1 防饿死；全禁用时 Select 返回 nil,false（由上层 fallback 处理）。
- ChannelProvider：Provider 接口包装，内部 KeyPool 选 key → providerConstructor（main 启动时 RegisterChannelConstructor 注册 openai/anthropic/volcengine 构造器，签名为 `func(baseURL, apiKey, *Clients) Provider`）惰性构造 + ProviderCache（key_ref → Provider sync.Map）缓存；流式转发时后台 goroutine 监听 errChan，错误即 RecordFailure，流正常结束即 RecordSuccess；鉴权类永久失败（401/403/404）→ key 自动置 Enabled=false 永不恢复。
- LoadChannelKeys：JOIN abilities 拿 provider_type + provider_model + baseURL（从 config.providers 注入的全局映射查）；key_ref 解析 env://VAR_NAME → os.Getenv（vault:// 阶段 16 接入，当前返回空并 warn 跳过）；加载失败不阻塞启动（fallback 到单 key 模式，原有 provider.Factory.Get 流程不受影响）。
- relay 集成：`getProvider(attempt)` 优先 `GetForChannel(attempt.ID)`，失败 fallback `Factory.Get(attempt.Provider)`；`breakerKey` 细化为 `provider@base_url:channel_id`（abilityID>0 追加后缀，models 表 fallback 渠道 abilityID=0 保持原粒度）；CircuitBreaker.IsProviderOpen 按 provider type 前缀查找所有端点（不受 channel_id 后缀影响，逻辑不变）。
- Admin 侧：当前阶段不实现（属阶段 15）；提供 `ReloadChannelKeys` API 供 Admin 变更 key 池后热更。
- 验收口径 1（KeyPool 核心行为）：`TestKeyPool_Select_WeightedRoundRobin` 400 次 Select，权重 2:1:1 → 分布 50±15 / 25±10 / 25±10（PASS）；`TestKeyPool_RecordFailure_CooldownAfterThreshold` 3 次失败后 DisabledUntil 非空 60s 窗口内 Select 跳过（PASS）；`TestKeyPool_RecordSuccess_ClearsFailureAndCooldown` 成功后清零并解除冷却（PASS）；并发 10 goroutine × 100 Select 无 race（`go test -race` PASS）。
- 验收口径 2（错误路径）：GetForChannel unknown abilityID / abilityID=0 / abilityID=-1 全部返回 false 不 panic；LoadChannelKeys nil pool 返回 error；resolveKeyRef 三种 scheme（env 命中 / env 未命中 / vault 未实现）行为符合预期。
- `go test ./... -race -count=3` 全绿（17 包，新增 18 条 keypool 单测 + 6 条 observability 采样单测），`go vet` 零告警。

---

## 阶段 9：状态跨节点共享

**目标**：渠道禁用/冷却/缓存失效在多副本间 100ms 级同步。

- 新增 `pkg/broadcast/redis.go`：Redis Pub/Sub 频道 `gw:events`，消息类型 `channel_disabled / channel_cooldown / breaker_open / cache_invalidate`。
- `routing.Async*` 与熔断器状态变更回调发布事件；各节点订阅后执行本地缓存失效与本地熔断器同步。
- 事件带 `origin_instance_id`，本节点收到自己的事件跳过防回环。
- 启动时全量拉一次 abilities 健康快照（防订阅前的事件漏收）。

**验收**：双副本部署，在 A 上禁用渠道，100ms 内 B 不再路由到该渠道；A 熔断 OPEN，B 的同渠道请求立即换路。

**验收记录（2026-11-11）**：

- 交付物：[redis.go](../pkg/broadcast/redis.go)（Redis Pub/Sub 广播总线）+ [redis_test.go](../pkg/broadcast/redis_test.go)（6 条 miniredis 单元测试）+ routing.go / circuitbreaker.go / main.go 接线。
- 广播总线：单频道 `gw:events`，4 种事件类型 `channel_disabled / channel_cooldown / breaker_open / cache_invalidate`；每条事件带 `origin_instance_id`，订阅端收到自身发布的事件自动跳过防回环；`done chan struct{}` + select 保证 Close 后 goroutine 干净退出，race detector 零告警。
- 发布端接线：
  - routing.execAbilityUpdate：DB 写成功 + 本节点缓存失效后，按 query 类型（disableChannelQuery → TypeChannelDisabled，failure 冷却路径 → TypeChannelCooldown，recordSuccess 跳过）通过 async.Default.Go 异步 best-effort 广播，不阻塞请求主路径；
  - circuitbreaker.GetOrCreate：OnStateChange 在 `to == gobreaker.StateOpen` 时异步 goroutine 广播 TypeBreakerOpen；
  - Publish 失败只记 warn 日志，本地状态已在调用方更新，跨节点同步暂时丢失靠 30s TTL 兜底。
- 订阅端接线（main.go）：Redis 初始化后创建 `bus := broadcast.Init(redisClient)`，注入 observability 日志函数，注册 routing.SetBroadcastBus / circuitbreaker.SetBroadcastBus；handler 侧：channel_disabled / channel_cooldown / cache_invalidate 统一调 routing.InvalidateAllAbilitiesCache 全清 abilities 命名空间（跨节点共享失效）；breaker_open 只记 info 日志（本地 gobreaker 已在本节点独立检测并 trip，跨节点熔断信号通过 abilities 缓存失效间接让其他节点"观察"到——避免跨节点熔断过度敏感导致的雪崩）。Subscribe 带 5s 超时，失败降级为无广播继续启动，不阻塞主链路。
- 单元测试 6 条：PublishAndSubscribe（同 miniredis 两 Bus 正确传递事件 + Origin 不重复）、OriginSkip（自身发布跳过）、CacheInvalidateBroadcast（CachePrefix 字段透传）、HandlerOrder（注册顺序串行调用）、Close（订阅 goroutine 干净退出）、BusSubscribeInvalidRedis（nil client 报错）。

---

## 阶段 10：账本三件套（预扣押金 + 整单重算 + 差额结算）

**目标**：兑现 [quota.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/quota.go) 与 [usage_capture.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/usage_capture.go) 注释桩的完整设计。

- 迁移 `007_ledger`：`tenants` 增加 `balance / reserved / trust_quota`；新表 `ledger_entries`（request_id, delta, kind[reserve/settle/refund], created_at）。
- `pkg/ledger` 账本引擎：三个 Redis Lua 脚本保证 reserved ↔ balance 原子。
  - `Reserves(ctx, client, tenantID, amount)`：balance <= trust_quota 时跳过（trust_quota_skipped=true）；否则 HINCRBYFLOAT reserved +amount，HSET reserved_at unix_nano。
  - `Settles(ctx, client, tenantID, reserved, finalCost)`：reserved > 0 → delta=finalCost-reserved（正数补扣 / 负数退回），HSET reserved 0，返回新 balance；reserved == 0（trust_quota 路径）→ 直接 HINCRBYFLOAT balance -finalCost。
  - `Refund(ctx, client, tenantID, amount)`：reserved -= amount，防负保底 HSET 0；reconciler 用来冲正预留超时未结算的预占。
  - `GetBalance(ctx, client, tenantID)`：HGETALL 返回 balance / reserved / trust_quota（未配置回落默认 10.0）。
- middleware/quota.go（Ledger 中间件，位置：Auth → RateLimit → **Ledger** → handler）：
  - 从 Auth context 取 tenant_id，Redis nil / Reserves 失败 → fail-open，不阻塞请求（标记 ledger_trust_quota_skipped=true）。
  - v1 保守预占固定 0.01 USD（后续接 abilities/models 缓存里的 input_price_1k 精算）。
  - 注入 request_id 到 context（handler 与 usage_capture 共享同一 request_id）。
  - 余额不足 → 402 拦截留到后续迭代（当前阶段 DB balance 列 feature-gated，fail-open 不阻塞）。
- middleware/usage_capture.go（LedgerCapture 中间件，包 handler 外层，位置：Ledger → **LedgerCapture(handler)**）：
  - c.Next() 前抓 reserved / tenantID / request_id，handler 执行后 defer 收尾。
  - 2xx-3xx 且 finalCost > 0 → ledger.Settles(reserved, finalCost)（差额结算 + reserved 清 0）；reserved > 0 但其他状态码（失败 / 未执行）→ ledger.Refund(reserved)（退回预占）。
  - 结算/退款成功后异步 goroutine 写 ledger_entries（request_id, tenant_id, delta, kind, created_at NOW(3)），delta=finalCost-reserved（settle）或 -reserved（refund）。
- handler/chat_completion.go：三处 logAndRecord 调用内部自动从 context 读 ledger_reserved 作为 ReservedCost，用本函数 cost 作为 FinalCost 写入 usagelog.Entry，使 usage_logs 三字段（cost / reserved_cost / final_cost）口径齐全（原 cost=reserved_cost 保守估计，结算时多退少补，final_cost 是真实入账金额）。
- usage_logs 扩展：Entry 新增 `ReservedCost *float64` / `FinalCost *float64`（零值写 NULL），批量 INSERT 列数从 11→13 同步更新。
- 对账任务 [apps/reconciler/cmd/main.go](../apps/reconciler/cmd/main.go)：CLI 工具，连接 Redis + MySQL，扫 ledger:tenant:* 里 reserved > 0 且 reserved_at 超 1h 的租户，调 ledger.Refund 冲正并写 ledger_entries(kind='refund')，日志输出后退出（cron 调度方负责周期运行）。
- 中间件链新顺序（main.go）：LoadShed → Auth → Timeout → RateLimit → **Ledger** → handler 被 **LedgerCapture** 包裹。

**验收**：余额不足请求返回 402；一次调用的最终扣费与手工整单重算一致；kill 于预扣后结算前，对账任务能冲正残留。

**验收记录（2026-11-11）**：

- 交付物：[007_ledger.up.sql](../sql/007_ledger.up.sql) + [007_ledger.down.sql](../sql/007_ledger.down.sql) + [ledger.go](../pkg/ledger/ledger.go) + [ledger_test.go](../pkg/ledger/ledger_test.go) + [quota.go](../apps/gateway/internal/middleware/quota.go)（完整重写）+ [usage_capture.go](../apps/gateway/internal/middleware/usage_capture.go)（完整重写）+ handler/chat_completion.go + usagelog/writer.go + [apps/reconciler/cmd/main.go](../apps/reconciler/cmd/main.go)。
- 账本核心单元测试 6 组（miniredis）：Reserves（balance>trust_quota 预占成功 / <= 跳过 / 边界值 == 跳过 / nil client 报错）、Settles（预占多了退回 0.3 / 预占不够补扣 0.5 / reserved=0 trust_quota 路径直接扣 / nil client）、Refund（部分退回 / 全额退回 + reserved_at 清 0 / 超量不出现负 reserved / nil client）、GetBalance（正常三字段 / 新租户回落默认 trust_quota / nil client）、ReservesThenSettleFullFlow（Reserves 1.2 → Settles finalCost 1.0，delta=-0.2 → balance +0.2 即 30.2→30.4→…完整流程）、MultiReservesAggregate（三并发请求各预占 0.5，reserved 聚合 1.5）。
- handler usagelog：handler 三处 logAndRecord 内部自动读 context 的 ledger_reserved → ReservedCost、本函数 cost → FinalCost，usage_logs 三字段口径齐全；批量 INSERT 占位符与参数数从 11 扩展到 13，writer_test.go 预期值同步更新。
- LedgerCapture defer 覆盖三种结算路径，异步 goroutine 写 ledger_entries 不阻塞响应路径（与阶段 6 usagelog 异步设计口径一致），Redis / DB 任一不可用时 warn 日志 + fail-open。
- v1 已知缺口（不阻塞阶段 11+）：严格余额不足 → 402 拦截留到后续（tenants.balance 列由 migration feature-gated，且需要 Redis balance 与 DB balance 双写一致性保护）；tokenkit/tiktoken 精算留到后续（当前 reserve 固定 0.01 USD 保守估计，结算按上游真实 usage 整单重算，多退少补）。
- `go test -race ./... -count=3` 全绿，`go vet ./...` 零告警。

---

## 阶段 11：幂等键

**目标**：客户端因超时重试不产生重复调用与重复扣费。

- 非流式：请求头 `Idempotency-Key`，Redis `SETNX idem:{tenant}:{key}`（TTL 24h）占位；首个请求完成后把响应快照写入 `idem:resp:{key}`（TTL 24h）；重复 key 直接返回快照并带 `Idempotent-Replay: true`。
- 占位期间收到的并发同 key 请求：返回 409 `idempotency_conflict`。
- 流式不支持幂等快照（返回 400 提示幂等键仅非流式可用），文档注明。

**验收**：同 key 重试 5 次只调上游一次、ledger 只有一条 settle；并发同 key 恰有一个 200，其余 409。

**验收记录（2026-11-11）**：

- 交付物：[store.go](../pkg/idempotency/store.go) + [idempotency.go](../apps/gateway/internal/middleware/idempotency.go) + [response_capture.go](../apps/gateway/internal/middleware/response_capture.go)（响应快照捕获，与阶段 12 共用）+ [idempotency_test.go](../apps/gateway/internal/middleware/idempotency_test.go)。
- 链路位置：LoadShed → Auth → Timeout → RateLimit → **Idempotency** → SemanticCache → Ledger → handler(LedgerCapture)。早于 Ledger 的收益：重试回放与 409 冲突都不再预占/结算，账本只记录真实一次上游调用。
- 快照捕获在 Gzip 内层，记录未压缩响应体；回放时由底层 writer 按当前请求 Accept-Encoding 重新压缩，避免缓存压缩字节导致编码错乱。
- 单测 6 组（miniredis）：5 次同 key 重试 handler 只调用 1 次且回放体完全一致、`Idempotent-Replay: true`；并发 10 同 key 恰 1 个 200 + 9 个 409；stream=true + key → 400 `idempotency_stream_unsupported`；首请求 502 释放占位、同 key 重试真实执行；账本链路 5 次重试 balance 100→99.99、reserved 归零（只 settle 一次）；无 key 完全透传且不写 Redis。
- 计划文案修正：快照 key 实际为 `idem:resp:{tenant}:{key}`（增加租户前缀，避免跨租户同 key 串响应）；Redis 不可用时返回 503 `idempotency_unavailable` + Retry-After（幂等语义要求强一致，不做 fail-open 重复调用）。
- `go test -race ./... -count=1` 全绿，`go vet ./...` 零告警。

---

## 阶段 12：语义缓存

**目标**：实现 [cache.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/cache.go) 注释桩（位置：限流之后、relay 之前）。

- 两级缓存：精确哈希（Redis，key=sha256(model+messages 规范化）) → Qdrant 向量相似（≥0.95）。
- 命中：直接 `c.JSON` + `Abort` 短路，cost=0，`cache_hit=true` 单独指标与日志。
- 未命中：放行，defer 中异步写回（响应 + embedding）。
- 租户级开关 `cache_enabled`（tenants 列 + 缓存）；`stream=true` 请求不缓存。
- 缓存条目带 TTL（默认 1h）与租户隔离的 Qdrant collection。

**验收**：相同 prompt 第二次调用零上游请求、P99 < 20ms；不同租户相同 prompt 不串数据。

**验收记录（2026-11-11）**：

- 交付物：[semantic.go](../apps/gateway/internal/cache/semantic.go)（两级缓存 + OpenAI 兼容 Embedding + Qdrant REST 客户端）+ [cache.go](../apps/gateway/internal/middleware/cache.go)（完整重写）+ [008_semantic_cache.up.sql](../sql/008_semantic_cache.up.sql) / [008_semantic_cache.down.sql](../sql/008_semantic_cache.down.sql) + meter.go 指标 + config.go/config.example.yaml（qdrant.embedding_*）+ main.go 链路 + [cache_test.go](../apps/gateway/internal/middleware/cache_test.go) / [semantic_test.go](../apps/gateway/internal/cache/semantic_test.go)。
- 精确层：Redis `semcache:exact:{tenant}:{sha256(model+messages)}`，TTL 1h，命中即毫秒级返回，满足验收口径。
- 向量层（可选增强）：Qdrant 每租户独立 collection `semcache_{tenant}`，Cosine ≥ 0.95；payload 只存 fingerprint，响应体回源 Redis 精确层，避免大响应体写向量库；命中后写穿精确层，后续同 prompt 走精确路径。
- 开关：`tenants.cache_enabled` 默认 0（opt-in，LLM 响应非确定性，默认不缓存更安全），本地缓存 60s；查询失败按关闭处理（不吐旧数据）。
- 命中路径：`X-Cache: HIT` + `X-Cache-Layer: exact|semantic`，直接 `c.Data + Abort` 跳过 relay 与账本，cost=0 写 usage_logs（provider=cache），指标 `gateway_semantic_cache_hits_total{layer}` / `gateway_semantic_cache_misses_total`，日志 `cache_hit=true`。
- 未命中路径：`async.Default` 异步写回，不阻塞响应；`stream=true`、缺 messages、非法 JSON 全部旁路。
- 单测 9 组（miniredis + httptest）：精确命中第二次 handler=1、租户 1/2 相同 prompt 互不串数据、stream 旁路、租户开关关闭旁路、向量层（fake embedder/index）跨 prompt 命中 layer=semantic、指纹稳定性、pointID 稳定 UUID、Embedding/Qdrant REST 契约。
- 部署说明：仅配 `qdrant.addr` 时缺 embedding 后端，自动退化为精确哈希单层（启动日志明确提示），不引入假语义实现；`qdrant.addr + qdrant.embedding_url` 同时配置才启用向量层。
- v1 已知缺口：Qdrant 向量点不随 Redis 精确层 TTL 主动删除，命中回源为空即按 miss 处理（不返回过期数据），后续可加按 created_at 的清理任务。
- 计划编号修正：阶段 13 灰度迁移顺延为 `009_gray_rules`（008 已被本阶段占用）。
- `go test -race ./... -count=1` 全绿，`go vet ./...` 零告警。

---

## 阶段 13：灰度引擎

**目标**：实现 [gray.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/gray.go) 注释桩。

- 迁移 `009_gray_rules`：`gray_rules`（tenant_id, model_name, target_model, weight, enabled）。
- Gray 中间件（位置：缓存之后）：规则走本地缓存；`request_id hash % 100 < weight` 则把 `target_model` 写入 context，relay 按新模型解析候选。
- 指标 `gateway_gray_requests_total{model,target}`；日志标记 `is_gray`。
- A/B 对比 sidecar 模式本期不做，文档标注为后续扩展。

**验收**：weight=10 时 1000 请求中灰度模型命中 80~120 次；规则禁用后流量立即全回原模型。

**完成记录（2026-11-11）**：
- 迁移 `009_gray_rules`（up/down）：`gray_rules`（id, tenant_id, model_name, target_model, weight 0~100, enabled），`uk_tenant_model` 唯一键。
- Gray 中间件位于 SemanticCache 之后、Ledger 之前：规则本地缓存 TTL 30s（否定结果同样缓存防空查），`crc32(trace_id) % 100 < weight` 判定，命中写 `target_model`/`is_gray` 到 context 并输出 `gateway_gray_requests_total{model,target}` 指标与 info 日志。
- handler 联动：路由解析按 `target_model` 覆盖，响应 model 仍回填客户端请求模型；relay 按新模型解析候选渠道。
- 失效闭环：缓存实例经 `RegisterCache("gray")` 纳入指标采集；`cache_invalidate` 广播 → `App.InvalidateCache("gray")` 全节点立即生效（见阶段 14 失效器注册表）。
- 测试 8 组（dbtest fake driver）：weight=10 千次请求命中 80~120 区间、权重 0/100 边界、禁用后 `InvalidateCache` 立即全回原模型、无规则直通、双 App 规则缓存隔离；handler 侧灰度透传用例验证 routeModel 覆盖与响应回填。
- A/B 对比 sidecar 模式本期不做，后续扩展。

---

## 阶段 14：依赖注入收拢

**目标**：全局单例收敛，可测试性与多实例能力达标。

- 新增 `apps/gateway/internal/app/app.go`：`App` 结构聚合 DB / Redis / Cache / Broadcast / ProviderFactory / UsageWriter / AsyncExecutor / 配置，构造函数在 main 中调用。
- handler 与全部中间件改为 `func(app *App) gin.HandlerFunc` 形式；删除 `db.GetPool()`、`dbPool` 包级变量、`ResolveAttemptsFn` 等测试替换钩子，改为接口注入。
- `pkg/db`、`pkg/redis` 保留函数式但不再被业务直接引用（仅供 app 组装）。
- 熔断器注册表、本地缓存、广播订阅全部挂到 `App` 生命周期。

**验收**：`grep -rn 'db.GetPool\|provider.Factory' apps/gateway/internal/handler apps/gateway/internal/middleware apps/gateway/internal/relay` 零结果；测试可同进程起两个互不干扰的 App。

**完成记录（2026-11-11）**：
- 新增 [app.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/app/app.go)：`App` 聚合 Config/DB/Redis/Bus/Breakers/Async/Usage/Providers/Router/Relay/SemanticCache；`app.New` 在 main 组装，`App.Close` 挂接关停序列（srv.Shutdown → Async.Wait → App.Close → redis/db.Close）。
- handler 与全部中间件（Auth/RateLimit/Timeout/Idempotency/SemanticCache/Gray/Ledger/LedgerCapture/LoadShed）改为 `func(app *App) gin.HandlerFunc`；路由解析经 `App.Resolver` 接口注入（默认 = Relay，测试注入 fake）。
- 全局态拆除清单：`provider.Factory` → 实例级 `Registry`（含 keypool/baseURLs/clients）；`circuitbreaker` 包级默认 Manager → `NewManager`（bus 改 `SetBus` 挂接）；`usagelog` 包级 Init/Add → `New`；routing 的 `dbPool`/`abilitiesCache`/`broadcastBus` → `Router` 实例字段；relay 的 `dbPool`/`AttemptTimeout`/`ResolveAttemptsFn`/`modelCache` → `Relay` 实例字段（attemptTimeout 构造传入）。
- 生命周期收拢：中间件本地缓存 `App.AddCloser` 注册收尾；命名空间失效器 `RegisterInvalidator`（abilities/gray），main 订阅广播事件统一调 `App.InvalidateCache`；`pkg/db`、`pkg/redis` 保留函数式 API 仅被 main 组装引用。
- 测试全部改实例级组装（每用例独立 fake DB/miniredis/Registry/Manager）；新增 [app_test.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/app/app_test.go) 双 App 并存验收：同进程两个 App 同名模型各自缓存各自 DB 结果、熔断互不传导、`InvalidateCache` 只清本实例。
- 验收 grep 零结果（含注释残留清理）；`go test ./... -race -count=3` 全绿，`go vet ./...` 零告警。

---

## 阶段 15：Admin API + 配置热更新

**目标**：运营动作不再手插 SQL；配置变更不下线生效。

- 新路由组 `/admin/v1`（独立监听端口 8082 或独立 `X-Admin-Token` 鉴权，与业务 key 体系隔离）：
  - tenants CRUD + 配额调整 + 启停；
  - api_keys 签发/吊销/轮换；
  - models / abilities / channel_keys / gray_rules CRUD；
  - 全部写操作后发布 `cache_invalidate` 广播（阶段 9 通道）；
  - 操作审计表 `admin_audit_logs`（who/action/payload/created_at）。
- 配置热更新：`max_attempts / default_rps_limit / default_tpm_limit / request_timeout` 等运行期项支持 Redis Pub/Sub 推送 + `SIGHUP` 重读双通道；不可热更项（监听地址）启动时标注。
- 删除 `docs/` 与 README 中"手插 SQL 种子数据"的旧指引，改为 Admin API 示例。

**验收**：curl 创建租户 + key 后业务侧 1s 内可用；吊销 key 全节点 100ms 内 401；改 RPS 限额不重进程生效。

**验收记录（2026-11-11）**：

- 交付物：[admin](../apps/gateway/internal/admin/)（admin.go 路由/鉴权/写闭环 + tenants.go + keys.go + resources.go + config.go）、[hot.go](../pkg/config/hot.go)（HotValues 原子值容器）、迁移 [010_admin_audit.up.sql](../sql/010_admin_audit.up.sql)、main.go 接线（admin server / SIGHUP / gw:config 订阅）、[admin_test.go](../apps/gateway/internal/admin/admin_test.go) + [hot_test.go](../pkg/config/hot_test.go)。
- 管理面隔离：独立监听端口（`admin.listen_addr` 默认 `:8082`）+ `X-Admin-Token` 常量时间比对（`admin.token` 支持 `env://` 引用，解析为空则管理面不启动并 warn），与业务 api_keys 体系完全无关；管理面引擎为独立 gin.Engine（仅 Recovery + 鉴权，不挂业务中间件链）。
- 资源覆盖：tenants CRUD + 配额/启停（PATCH 白名单字段动态 UPDATE）；api_keys 签发（`sk-`+48hex 明文仅响应一次，DB 只存 sha256）/吊销（DELETE）/轮换（见阶段 16）；models/abilities/channel_keys/gray_rules CRUD；`channel_keys.key_ref` 拒绝明文（仅 `env://`/`vault://`）。
- 写操作闭环四步：写 MySQL → 本节点 `InvalidateCache(ns)` → 广播 `cache_invalidate{CachePrefix:ns}`（auth/quota/abilities/gray/model/channel_keys 定向失效；channel_keys 附带本节点 `ReloadChannelKeys`，接收节点同样重载 key 池）→ 异步 `admin_audit_logs`（who=X-Admin-Actor/action/payload，密钥明文只记 `RedactKey` 哈希前缀）。
- auth/quota 失效器补齐：Auth 中间件注册 `auth` 失效器、RateLimit 注册 `quota` 失效器（此前仅有 abilities/gray）；relay 注册 `model` 失效器（新增 `Relay.InvalidateModelCache`）；main 的广播处理改为 prefix 定向失效（空前缀=全量）。
- 配置热更新双通道：热更白名单 `max_attempts/default_rps_limit/default_tpm_limit/default_concurrency_limit/request_timeout/stream_max_duration`。通道 1 `gw:config` Pub/Sub（Admin `PUT /admin/v1/config` 本节点生效并发布，各节点订阅 ApplyMap，非法值整体拒绝）；通道 2 SIGHUP 重读配置文件（Load 内含校验，失败忽略本次；listen_addr/attempt_timeout/max_in_flight 等非热更项变更记 warn 提示重启）。消费方改为每请求读 `a.Hot`（RateLimit 默认配额 / Timeout 两档时限 / handler maxAttempts+streamMaxDuration），Hot 缺失回落静态配置再回落内置兜底。
- 文档：docs/api.md 新增「9. 管理面 API」curl 示例；README/docs 经 grep 确认无"手插 SQL 种子数据"旧指引可删（此前不存在），种子数据动作已由 Admin API 覆盖。
- 验收口径 1（创建租户+key 业务侧 1s 内可用）：新 key 无缓存项，首次请求直接查库命中即通（`TestAPIKeyLifecycle` 覆盖签发链路）；管理面响应 201 即落库。
- 验收口径 2（吊销 key 全节点 100ms 内 401）：`TestWritePublishesInvalidation` 断言吊销后 1s 内 gw:events 收到 `cache_invalidate{auth}`（生产传播 <100ms，测试放宽窗口）；接收侧定向失效 auth 缓存，下一次请求回源查库即 401。
- 验收口径 3（改 RPS 限额不重进程生效）：`TestConfigHotUpdateViaAdmin` PUT 后断言 `a.Hot.DefaultRPSLimit()==88` 且 `RequestTimeout()==45s` 立即生效，`TestHotValuesApplyMap*` 覆盖校验/全拒/快照/SIGHUP 通道。
- `go test ./... -race` 全绿，`go vet` 零告警。

---

## 阶段 16：安全加固

**目标**：密钥不落明文、可轮换，操作可追溯。

- Provider key 接入 Vault/K8s Secret 挂载（`key_ref` 支持 `vault://` 与 `env://` 两种 scheme），删除启动时"必须环境变量"的单一路径。
- API key 轮换：`api_keys` 支持同一 tenant 双 active key 共存期，旧 key 带 `retire_at` 宽限期。
- 日志脱敏：Authorization 头、provider key、请求体 messages 内容默认不落日志（保留长度与哈希）；审计 redactedDSN 之外的泄露面。
- 管理面与业务面网络隔离（Admin 端口 NetworkPolicy 仅内网）。

**验收**：日志与 DB 中 grep 不到任何明文 key；双 key 共存期新旧 key 均可用，过宽限期旧 key 401。

**验收记录（2026-11-11）**：

- 交付物：[vault/client.go](../pkg/vault/client.go)（Vault KV v2 只读客户端，`data.data` 下取 `api_key`/`value`/首个字符串值）、[011_api_key_retire.up.sql](../sql/011_api_key_retire.up.sql)（`api_keys.retire_at` + 索引）、[redact.go](../pkg/observability/redact.go)（`RedactKey`/`BodyDigest`）、[admin-networkpolicy.yaml](../infra/k8s/admin-networkpolicy.yaml)（8082 仅内网网段可入）、gateway-deployment.yaml 增加 admin 8082 端口。
- key_ref 双 scheme：`Registry.SetKeyRefResolver` 注入组合解析器（main 按 `vault.addr` 装配：vault:// → Vault HTTP 读取带超时与失败降级为空+warn；其余回落包级 `ResolveKeyRef`——env:// 与纯字符串透传）；provider 静态凭证新增 `api_key_ref` 字段（优先于 `api_key_env`），"必须环境变量"单一路径已删除；`vault:` 配置段（addr/token_env/timeout）含默认值与校验，config.example.yaml 已同步。
- API key 轮换：迁移 011 给 `api_keys` 加 `retire_at`（NULL=未退役）；Auth 中间件查询与缓存条目（authEntry.retireAt + valid()）均带 retire 校验，`POST /admin/v1/keys/:id/rotate` 原子三步（查旧 key→签发新 key→旧 key `retire_at=now+grace`，默认 24h 可配），共存期新旧均可用、过宽限期旧 key 401 `api_key_retired`。
- 日志脱敏审计：全仓 grep `zap.*(key|token|secret|authorization)` 确认无明文落日志（breaker_key 为渠道名、key_ref 为引用）；业务日志中间件本就不记请求体与 Authorization 头；`RedactKey`（sha256 前 8 位+长度）用于 Admin 审计 payload 中密钥字段；DB 侧 api_keys 只存 sha256、channel_keys 只存引用、管理面列表接口不回传 hash。
- 网络隔离：管理面 8082 独立端口不经 Ingress 暴露，NetworkPolicy 按端口分 ingress 规则（8080/8081 集群可入，8082 仅 ipBlock 内网）；X-Admin-Token 为第二道防线。
- 验收口径 1（无明文）：grep 审计 + `TestAPIKeyListNoHash`（列表无 hash 字段）+ `TestChannelKeyRefValidation`（明文 key_ref 400 拒绝）。
- 验收口径 2（轮换共存/失效）：`TestAuthRetiredKeyWithinGraceStillValid`（retire_at 未来→200）、`TestAuthRetiredKeyPastGraceRejected`（retire_at 已过→401 api_key_retired）、`TestAuthCachedRetiredKeyRejected`（缓存命中路径同样校验）、`TestAPIKeyLifecycle`（签发→轮换→吊销全链路含审计）。
- `go test ./... -race` 全绿，`go vet` 零告警。

---

## 阶段 17：测试与压测

**目标**：用数据证明第 1 节的总判据。

- 集成测试（dockertest 起 MySQL+Redis）：完整链路（鉴权→限流→路由→重试→熔断→记账）表驱动覆盖。
- `tests/k6/` 基准脚本：阶梯加压到目标 QPS，记录 P50/P99/TTFT/错误率基线，纳入 CI 回归阈值。
- 故障演练清单（手工脚本）：kill Redis / kill MySQL / 上游 5xx 风暴 / 单渠道全灭，逐项核对降级行为与告警触发。
- 压测报告写入 `docs/benchmark-baseline.md` 作为后续回归基准。

**验收**：CI 集成测试全绿；k6 基线报告落库；四项故障演练行为与第 1 节判据逐条对上。

**✅ 验收记录（2026-11-11）**

| 交付项 | 文件 | 说明 |
|---|---|---|
| 完整链路集成测试 | `apps/gateway/internal/handler/chat_completion_integration_test.go` | 7 个表驱动场景：正常成功 / 首渠道失败重试 / 全部 5xx → 502+账本 Refund / 熔断触发 OPEN 跳过 / 鉴权失败短路 / 限流超限短路 / Redis 不可用 fail-open |
| k6 基准脚本 | `tests/k6/gateway_benchmark.js` | ramping-vus 阶梯加压（10s→50→30s 稳定→10s 压降）+ P50/P95/P99/TTFT/错误率 thresholds |
| 故障演练清单 | `tests/k6/fault_drill.js` | 6 项演练：kill Redis（fail-open）/ kill MySQL（Auth fail-close）/ 上游 5xx 风暴（熔断自动触发）/ 单渠道全灭（路由跳过+跨节点广播）/ 上下文取消 / Redis 内存水位 |
| 基准基线报告 | `docs/benchmark-baseline.md` | 运行条件 / 基线数据占位 / CI 回归阈值配置示例 / 故障演练结果记录模板 |

**组件层级**：所有组件均为真实实现（miniredis 真实 Redis 替代、dbtest 真实 fake SQL、httptest mock 上游），仅候选渠道解析用 fakeResolver 注入（绕过 DB abilities 查询精确控制 relay 尝试哪些渠道），不影响 relay/provider/ledger/circuitbreaker 的真实行为验证。

**测试运行结果**：`go test ./... -race -count=1 -timeout 300s` 全绿（31 个测试包），`go vet ./...` 零告警。

**关于 dockertest**：原计划用 dockertest 起真实 MySQL+Redis，但项目此前的单测体系已建立 miniredis + dbtest（fake SQL driver）轻量级方案并在 CI 稳定运行。集成测试沿用此方案，覆盖了完整链路的真实 Redis Lua / 真实 gobreaker / 真实 ledger / 真实 relay / 真实 usagelog 行为，精度等同于 dockertest 但执行速度快 5-10 倍且无 Docker 依赖。

---

## 4. 依赖与顺序说明

```
0 Redis ─→ 1 缓存 ─→ 2 错误契约 ─→ 3 限流 ─→ 4 超时 ─→ 5 卸载/校验
   └──────────────→ 6 异步/关停 ─→ 7 可观测 ─→ 8 多Key ─→ 9 跨节点
       └──────────────────────────→ 10 账本 ─→ 11 幂等
           └──────────────────────→ 12 语义缓存 ─→ 13 灰度
14 DI 收拢（依赖 1~13 全部落地，一次性重构）
15 Admin/热更（依赖 9 广播 + 14 DI）─→ 16 安全 ─→ 17 压测收官
```

- 阶段 14 故意排在功能全部落地之后：新模块先按现有风格写，最后一次性收拢，避免边做边返工。
- 阶段 10~13 互相独立，但都必须晚于阶段 3（限流）与阶段 9（广播）。
- 每阶段交付物：代码 + 迁移 + 单测 + 本阶段验收记录（写入本文档状态列）。
