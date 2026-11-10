// Package middleware 网关中间件链（洋葱模型，顺序不能乱）
//
// Chain 里的 10 层 + 1 最终 Handler：
//
//	① RecoverMiddleware          最外层，兜底 panic，统一错误格式
//	② TraceMiddleware            注入 OTel span + trace_id / request_id
//	③ LoggerMiddleware           结构化日志（带 trace_id）
//	④ AuthMiddleware             API Key 鉴权 → 查出 tenant / quota / 可用模型
//	⑤ QuotaMiddleware            🔴 New API 借鉴：预扣押金 + etcd 分布式限流
//	⑥ CacheMiddleware 🟢         语义缓存检查（Qdrant + Embedding）→ 命中则 Abort 短路
//	⑦ GrayMiddleware 🟢         模型灰度打标（流量切分 / A-B 对比）
//	⑧ 重试循环（和 ⑨⑩ 合并）     🔴 New API 借鉴：retry 数字复用 = 换渠道 + 优先级降级
//	⑨ ProviderSelectMiddleware   abilities 倒排索引查询 → 选渠道 + 创建 Provider
//	⑩ UsageCaptureMiddleware     🔴 New API 借鉴：预扣押金 → 整单重算 → 差额退款
//
// 顺序约束：详见各文件内注释。
// 🔴 标记 = New API 借鉴的设计
package middleware

import "github.com/gin-gonic/gin"

// Chain 返回当前已实现的中间件链，顺序不可调换：
// Recover 最外层兜 panic → Trace 注入 trace_id → Logger 记录请求 → Auth 鉴权。
func Chain() []gin.HandlerFunc {
	return []gin.HandlerFunc{
		Recover(),
		Trace(),
		Logger(),
		Auth(),
	}
}
