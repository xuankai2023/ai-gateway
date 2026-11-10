// QuotaMiddleware 🔴 New API 借鉴：预扣押金模型
//
// 位置：第 5 层
// 依赖：pkg/quota（令牌桶 + Redis Lua）+ etcd + gin.Context（需已有 tenant / quota）
//
// 职责（两阶段）：
//
//	阶段 1：准入检查（通过才往下走）
//	─────────────────────────────
//	1. 并发数检查 → etcd 原子 Incr（进来 +1），defer Decr（出去 -1）
//	   超过 concurrency_limit → 排队（5s 超时 或 429）
//	2. RPS / TPM 检查 → Redis Lua 脚本原子扣减令牌
//	3. 月度预算检查 → MySQL 聚合查询本月已用 cost
//	   超过 budget 时策略：402 Payment Required 或 自动降档到 fallback_model
//
//	阶段 2：预扣押金（🔴 New API 借鉴，通过准入后执行）
//	─────────────────────────────
//	4. tokenkit 本地估算输入 token（OpenAI 家族用 tiktoken 精确计数，其余按文本特征加权估算）
//	5. 组装定价：输入 token × input_price_1k / 1000 × price_ratio × completion_ratio...
//	6. 预扣金额 = 估算输入 token × input_price_1k / 1000（只算输入，输出未知，保守估计）
//	7. 余额 > trust_quota（默认 $10）的用户 → 跳过预扣（高并发优化）
//	8. Redis Lua 原子扣减：if Quota < amount then 0 end; HINCRBY Quota -amount; HINCRBY Reserved +amount
//	9. 扣减成功 → ctx.Set("request_id", uuid.New())
//
// 🔴 为什么只算输入？押金不是最终账单，只是一份临时授权。
//
//	结算时用上游真实 usage 整单重算，预扣多退少补（见 usage_capture.go）。
//
// 🔴 为什么信任额度以上跳过预扣？预扣是 Redis 写操作，高并发下账本压力大。
//
//	信任额度内的"小额用户"先放行，结算时一次性校准。
//
// Abort 条件：限流 → 429 / 预算耗尽 → 402 / 并发超 → 排队或 429
// 为什么在 Cache 之前？缓存命中也要先过限流，防止绕过。
package middleware
