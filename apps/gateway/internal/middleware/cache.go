// CacheMiddleware 🟢 语义缓存（Phase 1 补全）
//
// 位置：第 6 层
// 依赖：Qdrant 向量库 + pkg/provider.Embedding + Redis + gin.Context（需已有 tenant）
//
// 职责：
//  1. 如果租户 cache_enabled = false → 跳过（Next）
//  2. 从请求提取 Prompt text
//  3. 精确哈希查 Redis → 命中直接返回（极快）
//  4. 没命中 → 调 Embedding 算向量 → Qdrant 搜 top1 + 相似度 ≥ 0.95
//  5. 命中 → ctx.JSON(200, cachedResponse) + ctx.Abort() ← 短路后面所有层
//  6. 没命中 → Next()，defer 里把结果写回 Qdrant
//
// 关键：必须自己返回响应 + Abort，不能只塞数据等后面中间件。
//
//	否则 7-10 层还会走一遍，缓存命中失去零延迟意义。
package middleware
