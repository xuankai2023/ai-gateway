// UsageCaptureMiddleware 🔴 New API 借鉴：预扣押金 → 整单重算 → 差额退款
//
// 位置：第 10 层（最内层，包着最终 Handler + 重试循环）
//
// ============================================================================
// 完整三阶段账本（New API 核心设计，约 200 行，值得整读）
// ============================================================================
//
//	阶段 1：预扣押金（QuotaMiddleware 第 5 层已执行）
//	─────────────────────────────
//	已扣：Redis Lua 原子扣减 amount（估算输入 token × input_price_1k / 1000）
//	冻结：Redis HINCRBY key 'Reserved' +amount
//	语义：这是押金，不是最终账单
//
//	阶段 2：整单重算（本中间件 defer 里执行）
//	─────────────────────────────
//	New API 的 completeCost 计算（所有字段单独乘倍率，不用一个大乘数）：
//
//	  实际费用 =
//	      输入 token × input_price_1k / 1000
//	    + 缓存读 token × cache_read_price_1k / 1000      🔴
//	    + 缓存写 token × cache_write_price_1k / 1000      🔴
//	    + 图片 token × image_price_1k / 1000               🔴
//	    + 输出 token × output_price_1k / 1000              🔴
//	  × price_ratio（租户分组倍率）
//
//	🔴 为什么不简化为 (input+output) × 单价？
//	   输入和输出价格经常不一样（GPT-4 输出是输入的 2 倍）；
//	   缓存读写价格不一样（Claude 长效缓存写更贵）；
//	   图片 token 有独立费率。整单拆开乘才精准。
//
//	🔴 必须处理的语义陷阱：
//	   - OpenAI 的 prompt_tokens 包含缓存部分 → 要剔除再按缓存价单算
//	   - Claude 的 input_tokens 本不含缓存 → 不能减
//	   - OpenAI 的 cache-write 报未调整的前缀数 → 可能导致基础量为负 → clamp 到 0
//
//	阶段 3：差额结算（本中间件 defer 里执行）
//	─────────────────────────────
//	  delta = 实际费用 - 预扣押金
//	  if delta > 0  → Redis Lua 补扣 HINCRBY Quota -delta
//	  if delta < 0  → Redis Lua 退款 HINCRBY Quota +|delta|
//	  if delta == 0 → 不动
//	  HSET Reserved 0  // 清零冻结
//
//	🔴 结算两步提交防双退：
//	   1. 先扣资金 → 置 fundingSettled = true
//	   2. 再写 MySQL UsageLog（记录 reserved_cost + cost 两字段）
//	   第 1 步失败 → 不做第 2 步；第 2 步失败（DB 挂了）→ 有资金已扣但没账本
//	   这种极端情况靠对账任务修（每小时跑一次）
//
// ============================================================================
// SSE 流式场景（🔴 New API SSE 引擎配合）
// ============================================================================
// 流式请求：上游是 SSE，逐帧转发给客户端
// 关键边界：上游返回 200 → SSE 响应头已发给客户端 → 中途断流再也不可能重试
//
//	只能记录 EndReason（9 种：clean / timeout / upstream_err / client_disconnect ...）
//	按**已收到的数据**结算，不是按请求算
//
// 断开即止损（🔴 New API 最重要的防御设计）：
//
//	主循环监听客户端 ctx.Done() → 立即 close(upstreamResp.Body)
//	不为一个已经放弃的请求继续烧钱烧 token
//
// ============================================================================
// 本中间件 defer 里做的事（不管 Handler 成功失败）
// ============================================================================
// 1. 从 gin.Context 取 retry_count、channel_used、provider 等上下文
// 2. 拿到上游真实 usage（流式逐帧累计 / 非流式一次性）
// 3. completeCost 整单重算
// 4. Redis Lua 差额结算（多退少补）
// 5. Kafka 投递 usage event（异步落库 UsageLog，不阻塞响应）
// 6. 更新 Prometheus 指标：requests_total / cost_total / duration_ms
// 7. cache_hit = true → cost = 0，指标单独记
package middleware
