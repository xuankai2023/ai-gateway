// Package routing 渠道路由 + 重试循环
//
// 🔴 New API 借鉴：retry 复用 = 换渠道 + 降级
//
// 倒排索引查询优势：
//   正排 models 表：O(渠道数 × 模型数) 全表扫描
//   倒排 abilities 表：(tenant_id, model_name, priority) 复合索引 O(1) 命中
//
// retry 循环伪代码：
//   priorities := [100, 80, 50, 0]
//   for retry := 0; retry <= maxRetry; retry++ {
//       target := priorities[retry]
//       channels := 同层 enabled 渠道
//       picked := weightedRandom(channels)
//       err := callProvider(picked)
//       if err == nil { return }
//       decision := DecideRelayRetry(err)
//       asyncDisable(picked.id)
//       if decision == stop { break }
//   }
package routing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/db"
)

// Ability 一条渠道记录（abilities 表的 Go 结构）
type Ability struct {
	ID             int64   // abilities.id（用于 asyncDisable 禁用）
	Provider       string  // "openai" / "anthropic" / "volcengine"
	ProviderModel  string  // 上游真实模型名，如 "gpt-4o-mini"
	InputPrice1k   float64 // 本渠道输入 token 单价（每 1000）
	OutputPrice1k  float64 // 本渠道输出 token 单价（每 1000）
	Priority       int     // 优先级层（100=最高）
	Weight         int     // 同层权重
	FallbackModel  string  // 熔断兜底模型（可选）
}

// PriorityTier 一个优先级层的所有渠道（同层内加权随机）
type PriorityTier struct {
	Priority int
	Channels []Ability
}

// ErrNoAbilities abilities 表查不到可用渠道（abilities 表完全没数据时不返回此错误，走 fallback）
var ErrNoAbilities = errors.New("routing: no enabled abilities available")

// ResolveAbilities 对外导出接口：查 abilities 表拿渠道分层
// 如果 abilities 表不存在（旧环境未跑迁移）→ 返回 found=false，handler 走 fallback
func ResolveAbilities(ctx context.Context, tenantID int64, modelName string) ([]PriorityTier, bool, error) {
	return resolveAbilitiesDB(ctx, tenantID, modelName)
}

// resolveAbilitiesDB 从 abilities 表查渠道并按 priority 分层
// 返回值：
//   - tiers：按 priority 降序排列的分层结果
//   - found：表中是否有该 tenant+model 的记录（用于决定是否 fallback 到 models 表）
func resolveAbilitiesDB(ctx context.Context, tenantID int64, modelName string) ([]PriorityTier, bool, error) {
	pool := db.GetPool()
	if pool == nil {
		return nil, false, errors.New("db pool not initialized")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	rows, err := pool.QueryContext(queryCtx, `
		SELECT id, provider, provider_model, input_price_1k, output_price_1k,
		       priority, weight, COALESCE(fallback_model, '')
		FROM abilities
		WHERE tenant_id = ? AND model_name = ? AND enabled = 1
		ORDER BY priority DESC, weight ASC
	`, tenantID, modelName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil // abilities 表没数据 → 返回 found=false → 上层走 fallback
		}
		return nil, false, fmt.Errorf("abilities query failed: %w", err)
	}
	defer rows.Close()

	var all []Ability
	for rows.Next() {
		var a Ability
		var fallback string
		if err := rows.Scan(&a.ID, &a.Provider, &a.ProviderModel,
			&a.InputPrice1k, &a.OutputPrice1k,
			&a.Priority, &a.Weight, &fallback); err != nil {
			return nil, false, fmt.Errorf("abilities scan failed: %w", err)
		}
		a.FallbackModel = fallback

		// 🔴 熔断过滤：跳过熔断器 OPEN 的 provider（临时不可用，30s 后自动恢复）
		if circuitbreaker.IsOpen(a.Provider) {
			continue
		}

		all = append(all, a)
	}

	if len(all) == 0 {
		return nil, false, nil // 有这条 model_name 的记录但全被禁用 → 也走 fallback
	}

	// 按 priority 分层
	tierMap := make(map[int][]Ability)
	for _, a := range all {
		tierMap[a.Priority] = append(tierMap[a.Priority], a)
	}

	// 按 priority 降序组装
	tiers := make([]PriorityTier, 0, len(tierMap))
	for priority, channels := range tierMap {
		tiers = append(tiers, PriorityTier{Priority: priority, Channels: channels})
	}
	// 降序排序（最高优先级在最前）
	for i := 0; i < len(tiers); i++ {
		for j := i + 1; j < len(tiers); j++ {
			if tiers[j].Priority > tiers[i].Priority {
				tiers[i], tiers[j] = tiers[j], tiers[i]
			}
		}
	}

	return tiers, true, nil
}

// WeightedPick 在同一层渠道里按权重随机选一个
// weight 为 0 视为 1（防饿死）；全零则等概率
func WeightedPick(channels []Ability) *Ability {
	if len(channels) == 0 {
		return nil
	}
	if len(channels) == 1 {
		return &channels[0]
	}

	// 权重求和（0 权重当 1 处理）
	var total int
	for i := range channels {
		w := channels[i].Weight
		if w <= 0 {
			w = 1
		}
		total += w
	}

	r := rand.Intn(total)
	var cumulative int
	for i := range channels {
		w := channels[i].Weight
		if w <= 0 {
			w = 1
		}
		cumulative += w
		if r < cumulative {
			return &channels[i]
		}
	}
	return &channels[len(channels)-1]
}

// AsyncDisableChannel 异步禁用一个渠道（写 DB enabled=0，不阻塞当前请求）
func AsyncDisableChannel(abilityID int64) {
	pool := db.GetPool()
	if pool == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.ExecContext(ctx,
			"UPDATE abilities SET enabled = 0 WHERE id = ?", abilityID)
	}()
}

// --- 重试决策 ---

// RetryDecision 错误决策结果
type RetryDecision int

const (
	RetryStop    RetryDecision = iota // 不可重试，放弃
	RetryRetry                        // 可重试，换下一个渠道
	RetryFallback                     // 特殊：有 fallback_model，切到兜底
)

// RetryError 携带上游原始错误 + HTTP 状态码（便于 DecideRelayRetry 判断）
type RetryError struct {
	StatusCode int
	Message    string
	Retryable  bool // 显式标记是否可重试
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("upstream error [%d] %s", e.StatusCode, e.Message)
}

// DecideRelayRetry 🔴 New API 借鉴，按优先级排列 9 个 return
// 只看错误元标记和状态码，不写业务判断
func DecideRelayRetry(err error, retry, maxRetry int) RetryDecision {
	if err == nil {
		return RetryStop
	}

	// 1. retry 次数耗尽
	if retry >= maxRetry {
		return RetryStop
	}

	// 2. 带显式 Retryable 标记的错误
	if re, ok := err.(*RetryError); ok {
		// 不可重试标记
		if !re.Retryable {
			return RetryStop
		}
		// 3. 渠道级错误（401/403）→ 禁用该渠道 + retry
		if re.StatusCode == 401 || re.StatusCode == 403 {
			return RetryRetry
		}
		// 4. 可重试码（429 / 500-503 / 505 / 525-599）
		if re.StatusCode == 429 ||
			(re.StatusCode >= 500 && re.StatusCode <= 503) ||
			re.StatusCode == 505 ||
			(re.StatusCode >= 525 && re.StatusCode <= 599) {
			return RetryRetry
		}
		// 5. 其他已知 4xx → stop
		return RetryStop
	}

	// 6. 兜底：未知类型错误 → stop
	return RetryStop
}

// IsRetryableStatusCode 判断一个 HTTP 状态码是否可重试（供 Handler 构造 RetryError 用）
func IsRetryableStatusCode(code int) bool {
	switch code {
	case 429:
		return true
	case 500, 501, 502, 503, 505:
		return true
	}
	return code >= 525
}
