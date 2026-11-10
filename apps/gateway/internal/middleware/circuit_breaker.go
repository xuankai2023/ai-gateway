// CircuitBreakerMiddleware
//
// 🔴 New API 借鉴：取消独立熔断中间件，合并进重试循环
//
// New API 的做法：重试循环里每轮都重新选渠道，失败自动异步禁用该渠道，
// 下次（所有并发请求）选渠道时直接跳过。"熔断"是调度器的副作用，不是独立组件。
//
// 我们的架构选择：**保留 CircuitBreakerMiddleware 作为健康保护的第一道防线，
// 但把"自动降级换渠道"的逻辑和重试循环合并**：
//
//	① CircuitBreakerMiddleware：只负责 OPEN / HALF_OPEN / CLOSED 状态管理
//	   - 用 gobreaker 实现本地熔断
//	   - etcd Watch 同步熔断状态（多 Pod 一致）
//	   - 熔断器 OPEN → 直接切 fallback_model（从 quotas 表读），不经过重试循环
//
//	② 重试循环（合并到 ProviderSelectMiddleware 里）：
//	   - retry 数字复用为优先级层级下标（🔴 New API 借鉴）
//	     priorities = [100, 80, 50, 0]  // 优先级层
//	     target = priorities[retry]     // 第 0 轮 100 → 第 1 轮 80 → 第 2 轮 50
//	   - 每轮重试 = 重新调 abilities 表选渠道 + 降级一层优先级
//	   - 失败异步副作用：Redis Pub/Sub 广播禁用该渠道（所有节点立刻生效）
//
// 🔴 为什么不直接抄 New API 把 CircuitBreaker 去掉？
//
//	New API 没把"熔断保护"作为独立概念——渠道不可用就是不可用，下一轮换。
//	但我们有更多渠道类型（本地开源模型 + 外部云厂商），
//	有些渠道应该"全局熔断 30s 再试"，有些应该"立刻换下一个"。
//	所以 CircuitBreaker + 重试循环是两层保护，各有职责。
//
// 位置：第 8 层（现在实际上被合并进第 9 层的重试循环里）
package middleware
