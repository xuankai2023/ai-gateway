// LedgerMiddleware 账本预扣押金（v2：整数 μUSD + 余额不足 402）
//
// 位置：Auth / RateLimit 之后，handler 之前。
//
// 流程：
//  1. 从 Auth context 取 tenant_id，按请求体估算输入 token
//  2. 通过 models 定价快照（pricing 引擎）计算预占金额；models 无记录时回退 abilities 单价
//  3. ledger.ReservesV2 预占；available < estimated → 402 insufficient_balance
//  4. trust_quota 命中 / Redis 不可用 / 定价估算失败 → fail-open，不阻断请求
package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/pricing"
	"ai-gateway/pkg/ratelimit"
	"ai-gateway/pkg/requestid"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// reservedAmountV1 兼容旧版本的保底预占金额（$0.01 = 10,000 μUSD）
const reservedAmountV1 ledger.MicroUSD = 10_000

// Ledger 账本预扣押金中间件（阶段 14：func(app) 形式）
func Ledger(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 0. 生成并注入 request_id（handler 和 LedgerCapture 都读取它）
		if c.GetString("request_id") == "" {
			c.Set("request_id", requestid.New())
		}

		// 1. Auth 必写 tenant_id；缺失说明中间件链被错误裁剪
		raw, ok := c.Get("tenant_id")
		if !ok {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "tenant_missing", "internal error: tenant context missing")
			return
		}
		tenantID, _ := raw.(int64)
		if tenantID <= 0 {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "tenant_missing", "internal error: tenant context missing")
			return
		}

		// 2. 估算本次请求成本（定价失败 → fail-open，estimated=0）
		//    同一份 ModelRow 快照会写入 context，handler 结算必须复用它（A8）。
		estimated, pricingRow, estimatedOK := estimateRequestCost(c, a, tenantID)
		if pricingRow != nil {
			c.Set("ledger_pricing_row", pricingRow)
		}
		if !estimatedOK {
			observability.L().Warn("ledger: pricing estimate failed, fail-open",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
			)
			estimated = 0
		}
		c.Set("ledger_estimated_cost", int64(estimated))

		// 3. Redis 未初始化 → fail-open（测试环境和启动早期常见）
		if a.Redis == nil {
			c.Set("ledger_reserved", int64(0))
			c.Set("ledger_trust_quota_skipped", true)
			c.Next()
			return
		}

		// 4. 整数预占
		ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
		defer cancel()
		reservedAfter, skipped, available, blocked, err := ledger.ReservesV2(ctx, a.Redis, tenantID, estimated)
		if err != nil {
			// Redis 抖动 → 告警日志 + fail-open，不阻塞请求
			observability.L().Warn("ledger: reserves failed, fail-open",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.Error(err),
			)
			c.Set("ledger_reserved", int64(0))
			c.Set("ledger_trust_quota_skipped", true)
			c.Next()
			return
		}

		if blocked {
			apperr.Abort(c, http.StatusPaymentRequired, "billing_error", "insufficient_balance",
				fmt.Sprintf("insufficient balance: available=%s USD, estimated=%s USD",
					available, estimated))
			return
		}

		if skipped {
			c.Set("ledger_reserved", int64(0))
			c.Set("ledger_trust_quota_skipped", true)
		} else {
			c.Set("ledger_reserved", int64(reservedAfter))
			c.Set("ledger_trust_quota_skipped", false)
		}
		c.Next()
	}
}

// estimateRequestCost 用定价引擎估算本次请求成本（μUSD）。
//
// 读取并恢复 body，后续 handler 仍可正常解析。
// 返回 ok=false 表示估算失败，调用方应 fail-open。
func estimateRequestCost(c *gin.Context, a *app.App, tenantID int64) (ledger.MicroUSD, *pricing.ModelRow, bool) {
	body, err := readAndRestoreBody(c)
	if err != nil {
		return 0, nil, false
	}

	var meta struct {
		Model string `json:"model"`
		N     int64  `json:"n"`
		Count int64  `json:"count"`
	}
	if err := json.Unmarshal(body, &meta); err != nil || meta.Model == "" {
		return 0, nil, false
	}

	units := meta.N
	if units == 0 {
		units = meta.Count
	}
	tokens := int64(ratelimit.EstimateTokens(body))
	input := &pricing.EstimateInput{
		InputTokens:  tokens,
		OutputTokens: tokens / 2,
		UnitCount:    units,
	}
	engine := a.Pricing
	if engine == nil {
		engine = pricing.Default
	}

	// 优先使用 models 表的定价快照（含 pricing_model / pricing_config）
	if a.PricingResolver != nil {
		if row, err := a.PricingResolver.ResolveModelPricing(c.Request.Context(), tenantID, meta.Model); err == nil && row != nil {
			if cost, err := engine.Estimate(row, input); err == nil {
				return cost, row, true
			}
		}
	}

	// models 无记录 → 回退 abilities 单价（linear）
	if a.Resolver != nil {
		if attempts, err := a.Resolver.ResolveAttempts(c.Request.Context(), tenantID, meta.Model, 1); err == nil && len(attempts) > 0 {
			row := &pricing.ModelRow{
				Model:         meta.Model,
				Provider:      attempts[0].Provider,
				PricingModel:  pricing.PriceLinear,
				InputPrice1k:  ledger.FromDollars(attempts[0].InputPrice1k),
				OutputPrice1k: ledger.FromDollars(attempts[0].OutputPrice1k),
				PriceVersion:  1,
			}
			if cost, err := engine.Estimate(row, input); err == nil {
				return cost, row, true
			}
		}
	}

	// 定价链路整体不可用：保留旧版本固定预占，避免完全失去保护
	if a.PricingResolver == nil && a.Resolver == nil {
		return reservedAmountV1, nil, true
	}
	return 0, nil, false
}
