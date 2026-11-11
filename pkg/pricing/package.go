package pricing

import (
	"encoding/json"
	"fmt"

	"ai-gateway/pkg/ledger"
)

// PackageConfig 包块定价配置。
//
//	{"block_size_tokens": 1000, "price_per_block": 20000}
type PackageConfig struct {
	BlockSizeTokens int64           `json:"block_size_tokens"`
	PricePerBlock   ledger.MicroUSD `json:"price_per_block"`
}

func estimatePackage(raw json.RawMessage, inputTokens, outputTokens int64) (ledger.MicroUSD, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("pricing: package config is empty")
	}
	var cfg PackageConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return 0, fmt.Errorf("pricing: invalid package config: %w", err)
	}
	if cfg.BlockSizeTokens <= 0 {
		return 0, fmt.Errorf("pricing: package block_size_tokens must be > 0")
	}
	total := inputTokens + outputTokens
	if total == 0 {
		return 0, nil
	}
	blocks := (total + cfg.BlockSizeTokens - 1) / cfg.BlockSizeTokens
	return ledger.MicroUSD(blocks) * cfg.PricePerBlock, nil
}
