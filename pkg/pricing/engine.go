// Package pricing 定价引擎：输入估算 / 实际 usage → 最终成本（μUSD）
//
// 支持四种定价模型：
//   - linear     按 token 线性计费（每 1K token 单价）
//   - graduated  阶梯定价（按单请求 token 总量选档，全量按该档单价）
//   - package    包块定价（按 block_size_tokens 向上取整）
//   - per_unit   按次计价（输入里的 unit_count × price_per_unit）
package pricing

import (
	"encoding/json"
	"errors"
	"fmt"

	"ai-gateway/pkg/ledger"
)

// PriceModel 定价模型
type PriceModel string

const (
	PriceLinear    PriceModel = "linear"
	PriceGraduated PriceModel = "graduated"
	PricePackage   PriceModel = "package"
	PricePerUnit   PriceModel = "per_unit"
)

// ModelRow models 表的一行定价快照。
//
// 注意：该结构一旦从 DB 读出即为不可变快照。调用方必须使用同一个价格版本完成
// 「预占估算 → 结算」，中间即使 models 表价格变更也不影响本次请求（A8）。
type ModelRow struct {
	Model         string
	Provider      string
	PricingModel  PriceModel
	PricingConfig json.RawMessage

	InputPrice1k  ledger.MicroUSD
	OutputPrice1k ledger.MicroUSD
	PriceVersion  int
}

// EstimateInput 预占 / 结算输入。
type EstimateInput struct {
	InputTokens  int64
	OutputTokens int64
	UnitCount    int64
}

// ActualUsage 上游返回的真实 usage（字段含义同 EstimateInput）。
type ActualUsage = EstimateInput

// Engine 定价引擎（无状态，可并发使用）。
type Engine struct{}

// Default 默认引擎。
var Default = &Engine{}

// NormalizeModel 空值归一化为 linear。
func NormalizeModel(p PriceModel) PriceModel {
	if p == "" {
		return PriceLinear
	}
	return p
}

// Estimate 预占估算。
func (e *Engine) Estimate(model *ModelRow, input *EstimateInput) (ledger.MicroUSD, error) {
	return e.calc(model, input)
}

// Settle 用上游真实 usage 结算（A8：只使用 model 快照内的价格版本）。
func (e *Engine) Settle(model *ModelRow, actual *ActualUsage) (ledger.MicroUSD, error) {
	return e.calc(model, actual)
}

func (e *Engine) calc(model *ModelRow, input *EstimateInput) (ledger.MicroUSD, error) {
	if model == nil {
		return 0, errors.New("pricing: model row is nil")
	}
	if input == nil {
		input = &EstimateInput{}
	}
	if input.InputTokens < 0 || input.OutputTokens < 0 || input.UnitCount < 0 {
		return 0, errors.New("pricing: usage must be non-negative")
	}

	switch NormalizeModel(model.PricingModel) {
	case PriceLinear:
		return EstimateLinear(input.InputTokens, input.OutputTokens, model.InputPrice1k, model.OutputPrice1k), nil
	case PriceGraduated:
		return estimateGraduated(model.PricingConfig, input.InputTokens, input.OutputTokens)
	case PricePackage:
		return estimatePackage(model.PricingConfig, input.InputTokens, input.OutputTokens)
	case PricePerUnit:
		return estimatePerUnit(model.PricingConfig, input.UnitCount)
	default:
		return 0, fmt.Errorf("pricing: unsupported pricing model %q", model.PricingModel)
	}
}
