// ProviderSelectMiddleware + 重试循环（🔴 New API 借鉴：retry 复用 = 换渠道 + 降级）
//
// 位置：第 9 层（核心调度层）
// 依赖：abilities 倒排索引（🔴）+ pkg/provider.Factory + Redis + gin.Context
//
// ============================================================================
// New API 借鉴的核心设计：retry 数字同时驱动时间维度和空间维度
// ============================================================================
// 重试循环伪代码：
//
//	for retry := 0; retry <= maxRetry; retry++ {
//	    // ① retry 同时是优先级层级下标
//	    priorities := getPriorityLevels(tenant.tier, model_name)  // [100, 80, 50, 0]
//	    targetPriority := priorities[retry]                       // 第 0 轮 100 → 第 1 轮 80
//
//	    // ② 调 abilities 倒排索引选渠道（一次索引查询）
//	    channels := db.Query(`
//	        SELECT * FROM abilities
//	        WHERE group_name = ? AND model = ? AND enabled = true AND priority = ?
//	        ORDER BY weight
//	        LIMIT 1
//	    `, tenant.tier, model_name, targetPriority)
//
//	    // ③ 创建 Provider 实例
//	    provider := provider.Factory.Get(channels[0].provider)
//	    providerModel := channels[0].provider_model
//
//	    // ④ 调 Handler（调真实 API）
//	    err := callProvider(provider, providerModel)
//
//	    if err == nil { return }  // 成功退出
//
//	    // ⑤ 决策：这个错误值得重试吗？（只看错误元标记，不写业务判断）
//	    decision := DecideRelayRetry(err)  // retry / stop / fallback
//
//	    // ⑥ 副作用：异步禁用失败渠道（Redis Pub/Sub 广播，所有节点立刻生效）
//	    asyncDisableChannel(channels[0].channel_id)
//
//	    if decision == stop { break }
//	}
//
// 🔴 关键：每轮重试 = 换一个渠道 + 自动降一层优先级
//
//	一个 retry 数字同时驱动"时间维度（第几次重试）"和"空间维度（用第几层优先级）"
//	零额外状态对象，代码简洁度拉满
//
// ============================================================================
// abilities 倒排索引查询（🔴 New API 借鉴）
// ============================================================================
// 为什么不用正排：
//
//	正排：models 表全表扫描 → O(渠道数 × 模型数)
//	倒排：abilities 复合主键直接命中 → O(1)
//
// 命中率保障：
//  1. 同层内多个渠道 → 平滑加权随机（权重全零等概率，小权重放大 100 倍防饿死）
//  2. 同会话优先复用上次成功的渠道（保证上下文不漂移）
//  3. 一个渠道多密钥 → 轮询取用，游标持久化（缓存重建也不从头来）
//
// ============================================================================
// DecideRelayRetry 决策函数（🔴 New API 借鉴，只有 99 行，9 个 return）
// ============================================================================
// 按优先级排列：
//  1. 严格会话亲和失败 → stop（钉死渠道换了没用）
//  2. 钉死渠道失败 → stop
//  3. 渠道级错误（401/403）→ 异步禁用 + retry
//  4. IsSkipRetryError 标记 → stop（不可重试）
//  5. retry 次数耗尽 → stop
//  6. 2xx 响应 → stop（怎么会 2xx 还 err？说明是我们的错）
//  7. 未知状态码 → stop
//  8. 命中可重试码（429 / 500-503 / 505 / 525-599）→ retry
//  9. 兜底 → stop
//
// 策略事件全量写入请求日志 → 出问题能回答"当时为什么没重试"（可复盘）
// ============================================================================
package middleware
