// GrayMiddleware 🟢 模型灰度引擎（Phase 2 补全）
//
// 位置：第 7 层
// 依赖：etcd Watch /gray/ 前缀 + gin.Context（需已有 tenant / tenant_models）
//
// 职责：
//  1. 查 etcd 灰度规则：/gray/{tenant_id}/{model_name} → target_model + weight
//  2. 流量切分策略：request_id hash % 100 < weight → 切灰度模型
//  3. A/B 对比策略：主链路走原模型，异步 Sidecar 同时调灰度模型做对比
//  4. ctx.Set("target_model", resolvedModel)
//  5. ctx.Set("is_gray", true/false)
//
// 为什么在 Cache 之后？缓存命中直接 Abort 返回，不用处理灰度。
package middleware
