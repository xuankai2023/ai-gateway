// LedgerCaptureMiddleware 账本结算 / 退款（包在 handler 外层，defer 收尾）
//
// 职责：
//  1. 从 context 取 Ledger 中间件写入的 ledger_reserved / ledger_trust_quota_skipped
//  2. 调用 c.Next() 放行 handler
//  3. defer 收尾：
//     - 从 context 取 handler 写入的 ledger_final_cost
//     - 2xx~3xx 且 finalCost > 0 → ledger.Settles(reserved, finalCost)
//     - 其他情况且 reserved > 0 → ledger.Refund(reserved)
//  4. DB ledger_entries 异步写入 + credit_lots 批次扣减（方案 B）
//
// Redis / client 不可用 → fail-open：不阻塞响应。
package middleware

import (
	"context"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/creditlots"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/observability"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// LedgerCapture 账本结算 / 退款中间件（阶段 14：func(app) 形式）；包装 handler，defer 在 handler 返回后收尾
func LedgerCapture(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		// --- c.Next() 之前：抓 handler 可能覆盖的 context 值 ---
		var reserved ledger.MicroUSD
		if v, ok := c.Get("ledger_reserved"); ok {
			if r, ok := v.(int64); ok {
				reserved = ledger.MicroUSD(r)
			}
		}
		var tenantID int64
		if v, ok := c.Get("tenant_id"); ok {
			tenantID, _ = v.(int64)
		}

		defer func() {
			// --- handler 返回后：结算 / 退款 ---
			if a.Redis == nil {
				// Redis 未初始化，什么都做不了（fail-open 已在 Ledger 阶段标记）
				return
			}
			if tenantID <= 0 {
				return
			}

			var finalCost ledger.MicroUSD
			if v, ok := c.Get("ledger_final_cost"); ok {
				if f, ok := v.(int64); ok {
					finalCost = ledger.MicroUSD(f)
				}
			}
			status := c.Writer.Status()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// 读取中间件预生成的 request_id（quota.go 注入）
			requestID := c.GetString("request_id")
			// trace_id 在同步阶段捕获：异步 goroutine 里再读 gin.Context 会与 context 复用产生数据竞争
			traceID := c.GetString("trace_id")

			var entryKind string
			var entryDelta ledger.MicroUSD
			settled := false

			if status >= 200 && status < 400 && finalCost > 0 {
				// 成功路径：结算
				_, err := ledger.Settles(ctx, a.Redis, tenantID, reserved, finalCost)
				if err != nil {
					observability.L().Warn("ledger: settle failed",
						zap.String("trace_id", c.GetString("trace_id")),
						zap.Int64("tenant_id", tenantID),
						zap.Int64("reserved", int64(reserved)),
						zap.Int64("final_cost", int64(finalCost)),
						zap.Error(err),
					)
					// 结算失败：释放预占，避免预占悬挂；不写 settle 流水
					if reserved > 0 {
						_ = ledger.Refund(ctx, a.Redis, tenantID, reserved)
						entryKind = "refund"
						entryDelta = -reserved
					}
				} else {
					settled = true
					// ledger_entries 记账：delta = finalCost - reserved（结算差额）
					entryKind = "settle"
					entryDelta = finalCost - reserved
				}
			} else if reserved > 0 {
				// 失败 / 未执行完整路径：退回预占
				err := ledger.Refund(ctx, a.Redis, tenantID, reserved)
				if err != nil {
					observability.L().Warn("ledger: refund failed",
						zap.String("trace_id", c.GetString("trace_id")),
						zap.Int64("tenant_id", tenantID),
						zap.Int64("reserved", int64(reserved)),
						zap.Error(err),
					)
				}
				// ledger_entries 记账：退款记负 delta（冲正 reserved）
				entryKind = "refund"
				entryDelta = -reserved
			} else {
				// 无预占、无结算——trust_quota 跳过或完全失败，不写 ledger_entries
				return
			}

			// 异步非阻塞写 ledger_entries + 扣减 credit_lots（主流程已完成 Redis 结算/退款）
			// （挂 App 异步执行器：优雅退出时统一排空，不裸起 goroutine）
			if a.DB != nil && a.Async != nil && (entryDelta != 0 || (settled && finalCost > 0)) {
				a.Async.Go(func() {
					ctxDB, cancelDB := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancelDB()
					if entryDelta != 0 {
						_, _ = a.DB.ExecContext(ctxDB,
							`INSERT INTO ledger_entries (request_id, tenant_id, delta, kind, created_at) VALUES (?, ?, ?, ?, NOW(3))`,
							requestID, tenantID, int64(entryDelta), entryKind,
						)
					}
					// 方案 B：Redis 结算成功后，按 SQL 从最早到期批次扣 remaining（A10）
					if settled && finalCost > 0 {
						if _, err := creditlots.Deduct(ctxDB, a.DB, tenantID, finalCost); err != nil {
							observability.L().Warn("ledger: credit lots deduct failed",
								zap.String("trace_id", traceID),
								zap.Int64("tenant_id", tenantID),
								zap.Error(err),
							)
						}
					}
				})
			}
		}()

		c.Next()
	}
}
