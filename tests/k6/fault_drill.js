// 故障演练清单：人工/半自动化故障注入脚本
//
// 核心思想：生产加固完成后，主动注入故障验证降级行为与告警链路。
// 每条演练项 = [故障注入方式] + [期望网关行为] + [期望告警触发]。
//
// 运行前：网关需已启动并注册好至少 2 个可用渠道。
//   go run ./apps/gateway/cmd/main.go
//   # 另开一个终端跑演练脚本
//   k6 run tests/k6/gateway_benchmark.js --vus 10 --duration 30s  # 保持基线流量

// ═══════════════════════════════════════════════════════════════════
// 演练 1：kill Redis（限流 fail-open + 账本 trust_quota 路径）
// ═══════════════════════════════════════════════════════════════════
//
// [注入]
//   docker stop redis-gateway   # 或 redis-cli SHUTDOWN
//
// [期望网关行为]
//   ✅ Auth：若密钥已缓存 → 401 不触发（Redis 缓存 miss 会查 DB 兜底）
//   ✅ RateLimit：fail-open（Redis Lua 执行失败）→ 放行请求
//   ✅ Ledger：trust_quota 路径跳过预占（ledger_reserved=0）→ 直接结算
//   ✅ Relay：不受影响（纯 HTTP 调用）
//   ❌ 不应出现：500 连锁崩溃；Redis 恢复后限流自动生效
//
// [期望告警]
//   ⚠️  observability 发出 redis_down 告警 + Redis 重连后恢复告警
//
// [恢复]
//   docker start redis-gateway
//   等待 5-10s 确认网关日志里 Redis 重连成功
//
// [手动验证命令]
//   curl -X POST http://localhost:8080/v1/chat/completions \
//     -H "Authorization: Bearer sk-key" \
//     -d '{"model":"m","messages":[{"role":"user","content":"hi"}]}'
//   # 应返回 200（非 500）

// ═══════════════════════════════════════════════════════════════════
// 演练 2：kill MySQL（Auth fail-close + usagelog 丢弃）
// ═══════════════════════════════════════════════════════════════════
//
// [注入]
//   docker stop mysql-gateway
//
// [期望网关行为]
//   ✅ Auth：fail-close → 所有请求 401
//   ✅ usagelog writer：异步写入失败 → 不阻塞响应，记录 error log
//   ✅ ledger_entries：同上，异步写入失败 → 不阻塞
//   ❌ 不应出现：Panic 级错误；goroutine 泄漏
//
// [期望告警]
//   ⚠️  observability 发出 db_down 告警
//
// [恢复]
//   docker start mysql-gateway
//   等待 DB 重连 + Auth 缓存过期（key_ttl）后正常
//
// [手动验证命令]
//   # MySQL 挂了应该 401，而不是 500/连接耗尽
//   curl -s -o /dev/null -w "%{http_code}" ...  # → 401

// ═══════════════════════════════════════════════════════════════════
// 演练 3：上游 5xx 风暴（熔断自动触发 + 跨节点同步）
// ═══════════════════════════════════════════════════════════════════
//
// [注入方式 A：直接 mock 上游]
//   # 把某个渠道的 provider endpoint 改成永远返回 500 的 server
//
// [注入方式 B：k6 脚本模拟 5xx 风暴]
//   # 先保证正常压测跑通，再把上游 mock 换成总是 500
//   # 发 10 次请求 → 熔断器 threshold=5 → 连续 5 次失败 trip → OPEN
//
// [期望网关行为]
//   ✅ 熔断器 CLOSED → OPEN（约 5 次连续失败）
//   ✅ 熔断 OPEN 期间，所有请求跳过熔断渠道 → 直接 fallback 到下一个渠道
//   ✅ fallback 成功时，失败渠道的 consecutive_failures 重置
//   ✅ 熔断器 OPEN 时间窗到期（默认 30s）后进入 HALF-OPEN → 放行探测流量
//   ❌ 不应出现：全部渠道同时熔断（除非全部故障）；goroutine 泄漏
//
// [期望告警]
//   ⚠️ broadcast 广播到其他节点：breaker:{key}:open
//   ⚠️ observability 发出 circuit_breaker_open 告警
//
// [恢复]
//   上游恢复 → HALF-OPEN 探测流量成功 → 回到 CLOSED
//
// [手动验证命令]
//   # 看熔断器 key 和状态
//   redis-cli HGETALL breaker:<key>   # 或者用 admin API（如果有）

// ═══════════════════════════════════════════════════════════════════
// 演练 4：单渠道全灭（路由跳过 + 跨节点同步）
// ═══════════════════════════════════════════════════════════════════
//
// [注入]
//   将 abilities 表中某个渠道设为 disabled / consecutive_failures 超高
//   # 通过 migrate tool：
//   go run ./apps/migrate/cmd/main.go abilities-disable --provider=openai
//
// [期望网关行为]
//   ✅ routing.Router.ResolveAttempts 跳过 disabled 渠道
//   ✅ 熔断渠道在 attempts 列表里被 go breaker 快速跳过（不发 HTTP）
//   ✅ 广播到所有节点同步路由状态
//   ❌ 不应出现：尝试已 disabled 渠道；请求 502
//
// [期望告警]
//   ⚠️ observability 发出 route_health_degraded 告警

// ═══════════════════════════════════════════════════════════════════
// 演练 5：上下文取消 / 客户端断开（Relay 不泄漏 goroutine）
// ═══════════════════════════════════════════════════════════════════
//
// [注入]
//   # 客户端发请求后立即断开
//   timeout 0.1 curl -X POST http://localhost:8080/v1/chat/completions \
//     -H "Authorization: Bearer sk-key" -d '...' &
//   # 或用 k6：设 vus=50 + 极低 iterations=1 让 VU 快速退出
//
// [期望网关行为]
//   ✅ Relay 的 context 取消 → attempt goroutine 退出
//   ✅ Ledger 收到 ctx.Done() → CancelReserve（信任额度跳过预占但已 settle → 不退款）
//   ✅ usagelog writer：Append 不阻塞（channel 有 buffer）
//   ❌ 不应出现：goroutine 泄漏（pprof goroutines 数上升后不降）

// ═══════════════════════════════════════════════════════════════════
// 演练 6：Redis 内存水位告警 → 限流降级策略验证
// ═══════════════════════════════════════════════════════════════════
//
// [注入]
//   redis-cli CONFIG SET maxmemory 1mb     # 触发内存告警
//   redis-cli CONFIG SET maxmemory-policy allkeys-lru
//   # 或生产环境的 Redis 告警直接触发
//
// [期望网关行为]
//   ✅ Redis OOM → Lua 脚本执行 error → RateLimit fail-open
//   ✅ 日志 + 指标记录限流 fail-open 次数
//   ✅ 非阻塞降级：不影响正常请求
//
// [恢复]
//   redis-cli CONFIG SET maxmemory 0
