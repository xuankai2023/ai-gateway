// Package middleware 网关中间件链（洋葱模型，顺序不能乱）
//
// 阶段 14：全部有状态中间件改为 func(app *app.App) gin.HandlerFunc 形式，
// 依赖（DB / Redis / 缓存 / 熔断器 / 广播 / 配置）统一经 app.App 注入；
// 链路组装在 apps/gateway/cmd/main.go 的 newRouter 中。
//
// 分层顺序：
//
//	① Recover          最外层，兜底 panic，统一错误格式
//	② Trace            注入 OTel span + trace_id / request_id
//	③ Logger           结构化日志（带 trace_id）
//	④ LoadShed         全局在途信号量（过载快速拒绝）
//	⑤ Auth             API Key 鉴权 → tenant / api_key / project 写 context
//	⑥ Timeout          请求级超时（流式用 stream_max_duration）
//	⑦ RateLimit        RPS / TPM / 并发三剑客（租户配额缓存 60s）
//	⑧ Idempotency      幂等键占位 / 快照回放
//	⑨ SemanticCache    语义缓存（命中短路，cost=0）
//	⑩ Gray             模型灰度打标（trace_id 哈希流量切分）
//	⑪ Ledger           账本预扣押金
//	⑫ LedgerCapture    结算 / 退款（包 handler 外层，defer 收尾）
//	→ Handler          解析 / 校验 / 路由 / relay / 响应
//
// 顺序约束：详见各文件内注释。
package middleware
