package pricing

import (
	"encoding/json"
	"fmt"

	"ai-gateway/pkg/ledger"
)

// PerUnitConfig 按次计价配置。
//
//	{"price_per_unit": 20000, "unit_field": "n"}
type PerUnitConfig struct {
	PricePerUnit ledger.MicroUSD `json:"price_per_unit"`
	UnitField    string          `json:"unit_field"`
}

func estimatePerUnit(raw json.RawMessage, unitCount int64) (ledger.MicroUSD, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("pricing: per_unit config is empty")
	}
	var cfg PerUnitConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return 0, fmt.Errorf("pricing: invalid per_unit config: %w", err)
	}
	if cfg.PricePerUnit < 0 {
		return 0, fmt.Errorf("pricing: per_unit price must be >= 0")
	}
	return ledger.MicroUSD(unitCount) * cfg.PricePerUnit, nil
}
