package pricing

import "ai-gateway/pkg/ledger"

// EstimateLinear 线性计费：tokens / 1000 × 单价，整数截断。
func EstimateLinear(inputTokens, outputTokens int64, inputPrice1k, outputPrice1k ledger.MicroUSD) ledger.MicroUSD {
	return ledger.MicroUSD(inputTokens)*inputPrice1k/1000 +
		ledger.MicroUSD(outputTokens)*outputPrice1k/1000
}
