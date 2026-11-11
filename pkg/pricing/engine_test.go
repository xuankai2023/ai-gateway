package pricing

import (
	"encoding/json"
	"testing"

	"ai-gateway/pkg/ledger"
	"github.com/stretchr/testify/require"
)

// A4：linear，1000 输入 + 500 输出，价格 $0.0015 / $0.006 per 1K，精确等于 4500 μUSD
func TestA4Linear(t *testing.T) {
	row := &ModelRow{
		Model:         "gpt-test",
		PricingModel:  PriceLinear,
		InputPrice1k:  ledger.FromDollars(0.0015),
		OutputPrice1k: ledger.FromDollars(0.006),
		PriceVersion:  1,
	}
	cost, err := Default.Estimate(row, &EstimateInput{InputTokens: 1000, OutputTokens: 500})
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(4500), cost)
	require.InDelta(t, 0.0045, cost.Dollars(), 1e-12)
}

func TestLinearRoundingTruncates(t *testing.T) {
	// 1 token × 1500 μUSD/1K = 1.5 μUSD，整数截断为 1
	cost := EstimateLinear(1, 0, 1500, 0)
	require.Equal(t, ledger.MicroUSD(1), cost)
}

// A5：graduated，5K 走 tier0，20K 走 tier1；边界值与最高档
func TestA5Graduated(t *testing.T) {
	cfg := json.RawMessage(`{
		"tiers": [
			{"max_tokens": 10000,  "input_price_1k": 1500, "output_price_1k": 6000},
			{"max_tokens": 50000,  "input_price_1k": 1200, "output_price_1k": 4800},
			{"max_tokens": 0,      "input_price_1k": 900,  "output_price_1k": 3600}
		]
	}`)
	row := &ModelRow{Model: "grad", PricingModel: PriceGraduated, PricingConfig: cfg, PriceVersion: 1}

	t.Run("5K tokens → tier0", func(t *testing.T) {
		cost, err := Default.Estimate(row, &EstimateInput{InputTokens: 4000, OutputTokens: 1000})
		require.NoError(t, err)
		// 4000*1500/1000 + 1000*6000/1000 = 6000 + 6000 = 12000
		require.Equal(t, ledger.MicroUSD(12000), cost)
	})

	t.Run("20K tokens → tier1", func(t *testing.T) {
		cost, err := Default.Estimate(row, &EstimateInput{InputTokens: 10000, OutputTokens: 10000})
		require.NoError(t, err)
		// 10000*1200/1000 + 10000*4800/1000 = 12000 + 48000 = 60000
		require.Equal(t, ledger.MicroUSD(60000), cost)
	})

	t.Run("边界 10000 → 仍 tier0", func(t *testing.T) {
		cost, err := Default.Estimate(row, &EstimateInput{InputTokens: 10000, OutputTokens: 0})
		require.NoError(t, err)
		require.Equal(t, ledger.MicroUSD(15000), cost)
	})

	t.Run("10001 → tier1", func(t *testing.T) {
		cost, err := Default.Estimate(row, &EstimateInput{InputTokens: 10001, OutputTokens: 0})
		require.NoError(t, err)
		require.Equal(t, ledger.MicroUSD(12001), cost)
	})

	t.Run("超过最高阈值 → 无限档", func(t *testing.T) {
		cost, err := Default.Estimate(row, &EstimateInput{InputTokens: 60000, OutputTokens: 0})
		require.NoError(t, err)
		require.Equal(t, ledger.MicroUSD(54000), cost)
	})

	t.Run("空配置报错", func(t *testing.T) {
		_, err := Default.Estimate(&ModelRow{PricingModel: PriceGraduated}, &EstimateInput{InputTokens: 1})
		require.Error(t, err)
	})
}

// A6：package，2500 tokens / block 1000 → 3 blocks
func TestA6PackageCeil(t *testing.T) {
	row := &ModelRow{
		Model:        "pkg",
		PricingModel: PricePackage,
		PricingConfig: json.RawMessage(
			`{"block_size_tokens": 1000, "price_per_block": 20000}`),
	}

	cases := []struct {
		total int64
		want  ledger.MicroUSD
	}{
		{0, 0},
		{1, 20000},
		{1000, 20000},
		{1001, 40000},
		{2500, 60000},
	}
	for _, tc := range cases {
		// 把 total 拆成输入 / 输出不影响包块结果
		in := tc.total / 2
		out := tc.total - in
		cost, err := Default.Estimate(row, &EstimateInput{InputTokens: in, OutputTokens: out})
		require.NoError(t, err, "total=%d", tc.total)
		require.Equal(t, tc.want, cost, "total=%d", tc.total)
	}

	t.Run("block_size 非法报错", func(t *testing.T) {
		bad := &ModelRow{PricingModel: PricePackage, PricingConfig: json.RawMessage(`{"block_size_tokens":0,"price_per_block":1}`)}
		_, err := Default.Estimate(bad, &EstimateInput{InputTokens: 1})
		require.Error(t, err)
	})
}

// A7：per_unit，4 次 × $0.02 = 80000 μUSD
func TestA7PerUnit(t *testing.T) {
	row := &ModelRow{
		Model:         "image",
		PricingModel:  PricePerUnit,
		PricingConfig: json.RawMessage(`{"price_per_unit": 20000, "unit_field": "n"}`),
	}
	cost, err := Default.Estimate(row, &EstimateInput{UnitCount: 4})
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(80000), cost)

	cost, err = Default.Estimate(row, &EstimateInput{UnitCount: 0})
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), cost)
}

// A8：价格版本快照 —— 旧 ModelRow 不受后续改价影响，且版本号被透传
func TestA8PriceVersionSnapshot(t *testing.T) {
	v1 := &ModelRow{
		Model: "m", PricingModel: PriceLinear,
		InputPrice1k: 1500, OutputPrice1k: 6000, PriceVersion: 1,
	}
	before, err := Default.Settle(v1, &ActualUsage{InputTokens: 1000, OutputTokens: 500})
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(4500), before)
	require.Equal(t, 1, v1.PriceVersion)

	// 管理面改价 → 新版本快照
	v2 := &ModelRow{
		Model: "m", PricingModel: PriceLinear,
		InputPrice1k: 3000, OutputPrice1k: 12000, PriceVersion: 2,
	}

	// 旧请求继续用 v1 结算（同一版本）
	after, err := Default.Settle(v1, &ActualUsage{InputTokens: 1000, OutputTokens: 500})
	require.NoError(t, err)
	require.Equal(t, before, after)

	// 新请求用 v2
	newCost, err := Default.Settle(v2, &ActualUsage{InputTokens: 1000, OutputTokens: 500})
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(9000), newCost)
	require.Equal(t, 2, v2.PriceVersion)
}

func TestEngineValidation(t *testing.T) {
	_, err := Default.Estimate(nil, &EstimateInput{})
	require.Error(t, err)

	_, err = Default.Estimate(&ModelRow{PricingModel: PriceModel("unknown")}, &EstimateInput{})
	require.Error(t, err)

	_, err = Default.Estimate(&ModelRow{PricingModel: PriceLinear}, &EstimateInput{InputTokens: -1})
	require.Error(t, err)

	// 空 PricingModel 归一化为 linear
	cost, err := Default.Estimate(&ModelRow{InputPrice1k: 1500}, &EstimateInput{InputTokens: 1000})
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(1500), cost)
}
