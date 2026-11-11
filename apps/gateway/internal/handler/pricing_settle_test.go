package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/pricing"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type stubPricingResolver struct {
	row *pricing.ModelRow
	err error
}

func (s *stubPricingResolver) ResolveModelPricing(context.Context, int64, string) (*pricing.ModelRow, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.row, nil
}

func TestSettleCostUsesPricingSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)

	a := &app.App{
		Pricing: pricing.Default,
		PricingResolver: &stubPricingResolver{row: &pricing.ModelRow{
			Model: "grad", PricingModel: pricing.PriceGraduated,
			PricingConfig: json.RawMessage(`{"tiers":[{"max_tokens":10000,"input_price_1k":1500,"output_price_1k":6000}]}`),
			PriceVersion:  9,
		}},
	}
	attempt := routing.Ability{Provider: "openai", InputPrice1k: 99, OutputPrice1k: 99}

	cost, version := settleCost(c, a, 1, "grad", attempt, 4000, 1000)
	require.Equal(t, ledger.MicroUSD(12000), cost)
	require.Equal(t, 9, version, "usage_logs 必须记录本次使用的价格版本")
}

// A8：预占时写入 context 的价格快照优先于结算时重新解析
func TestSettleCostPrefersQuotaSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)

	// 预占快照：v1 价格
	c.Set("ledger_pricing_row", &pricing.ModelRow{
		Model: "m", PricingModel: pricing.PriceLinear,
		InputPrice1k: 1500, OutputPrice1k: 6000, PriceVersion: 1,
	})

	// 结算时 DB 已经改价为 v2 —— 必须仍然使用 v1
	a := &app.App{
		Pricing: pricing.Default,
		PricingResolver: &stubPricingResolver{row: &pricing.ModelRow{
			Model: "m", PricingModel: pricing.PriceLinear,
			InputPrice1k: 3000, OutputPrice1k: 12000, PriceVersion: 2,
		}},
	}

	cost, version := settleCost(c, a, 1, "m", routing.Ability{}, 1000, 500)
	require.Equal(t, ledger.MicroUSD(4500), cost, "必须按预占快照 v1 结算")
	require.Equal(t, 1, version)
}

func TestSettleCostFallsBackToAbilityPrices(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)

	a := &app.App{
		Pricing:         pricing.Default,
		PricingResolver: &stubPricingResolver{err: errors.New("model not found")},
	}
	attempt := routing.Ability{
		Provider: "openai", InputPrice1k: 0.0015, OutputPrice1k: 0.006,
	}

	cost, version := settleCost(c, a, 1, "ability-only-model", attempt, 1000, 500)
	require.Equal(t, ledger.MicroUSD(4500), cost)
	require.Equal(t, 1, version)
}
