package pricing

import (
	"encoding/json"
	"fmt"

	"ai-gateway/pkg/ledger"
)

// GraduatedTier 阶梯档位。
//
//	{"max_tokens": 10000, "input_price_1k": 1500, "output_price_1k": 6000}
//
// max_tokens = 0 表示最高档（无上限）。
type GraduatedTier struct {
	MaxTokens     int64           `json:"max_tokens"`
	InputPrice1k  ledger.MicroUSD `json:"input_price_1k"`
	OutputPrice1k ledger.MicroUSD `json:"output_price_1k"`
}

type graduatedConfig struct {
	Tiers []GraduatedTier `json:"tiers"`
}

func estimateGraduated(raw json.RawMessage, inputTokens, outputTokens int64) (ledger.MicroUSD, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("pricing: graduated config is empty")
	}
	var cfg graduatedConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return 0, fmt.Errorf("pricing: invalid graduated config: %w", err)
	}
	if len(cfg.Tiers) == 0 {
		return 0, fmt.Errorf("pricing: graduated config has no tiers")
	}

	total := inputTokens + outputTokens
	chosen := cfg.Tiers[len(cfg.Tiers)-1]
	for _, tier := range cfg.Tiers {
		if tier.MaxTokens == 0 || total <= tier.MaxTokens {
			chosen = tier
			break
		}
	}
	return EstimateLinear(inputTokens, outputTokens, chosen.InputPrice1k, chosen.OutputPrice1k), nil
}
