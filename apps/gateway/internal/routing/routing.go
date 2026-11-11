// Package routing 渠道路由 + 尝试列表展开
//
// 🔴 New API 借鉴：retry 复用 = 换渠道 + 降级
//
// 倒排索引查询优势：
//
//	正排 models 表：O(渠道数 × 模型数) 全表扫描
//	倒排 abilities 表：(tenant_id, model_name, priority) 复合索引 O(1) 命中
//
// 尝试列表示意（重试次数按渠道数计算，每渠道单次请求最多尝试一次）：
//
//	tiers    := [priority=100:{A,B}, priority=80:{C}, priority=50:{D}]
//	attempts := PlanAttempts(tiers, 4)
//	          → 层间按 priority 降序、层内按 weight 加权洗牌展开
//	          → 例如 [B, A, C]（截断到 maxAttempts）
//	for i, attempt := range attempts {
//	    err := callProvider(attempt)
//	    if err == nil { return }
//	    if DecideRelayRetry(err, i, len(attempts)) == stop { break }
//	}
package routing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"

	"ai-gateway/pkg/async"
	"ai-gateway/pkg/broadcast"
	"ai-gateway/pkg/cache"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/provider"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

// Router 渠道路由器（阶段 14：包级单例收敛为实例，随 App 生命周期创建）
//
// 聚合：DB 连接、abilities 本地缓存、熔断管理器、跨节点广播总线、异步执行器。
// 同一进程可起多个互不干扰的 Router（缓存 / 熔断器 / 广播全部实例级）。
type Router struct {
	db       *sql.DB
	breakers *circuitbreaker.Manager
	bus      *broadcast.Bus  // nil = 仅本节点失效，不跨节点广播
	exec     *async.Executor // nil 时内部自建
	cache    *cache.Cache[abilitiesEntry]
}

// abilitiesTTL 路由缓存条目的过期时间
const abilitiesTTL = 30 * time.Second

// abilitiesEntry abilities 缓存值（命名空间 abilities:{tenant_id}:{model}）
//
//	tiers：熔断过滤前的全量分层渠道（熔断状态每请求实时过滤，不冻结进缓存）
//	found：abilities 表是否有该 tenant+model 的可用记录；false 也缓存（防空查每请求打 DB）
type abilitiesEntry struct {
	tiers []PriorityTier
	found bool
}

// NewRouter 创建路由器；exec 为 nil 时内部自建独立 Executor
func NewRouter(db *sql.DB, breakers *circuitbreaker.Manager, bus *broadcast.Bus, exec *async.Executor) *Router {
	if breakers == nil {
		breakers = circuitbreaker.NewManager(circuitbreaker.Options{})
	}
	if exec == nil {
		exec = &async.Executor{}
	}
	c := cache.New[abilitiesEntry](cache.Options{
		Name:       "abilities",
		MaxEntries: 10000,
		DefaultTTL: abilitiesTTL,
	})
	observability.RegisterCache("abilities", c)
	return &Router{db: db, breakers: breakers, bus: bus, exec: exec, cache: c}
}

// Close 关闭路由缓存（App 生命周期收尾调用）
func (r *Router) Close() { r.cache.Close() }

// InvalidateAllAbilitiesCache 失效本节点全部 abilities 路由缓存
// 跨节点事件处理器会调用此方法（其他节点收到 channel_disabled/cooldown/cache_invalidate 后）
func (r *Router) InvalidateAllAbilitiesCache() {
	r.cache.InvalidatePrefix("")
}

func abilitiesCacheKey(tenantID int64, modelName string) string {
	return fmt.Sprintf("%d:%s", tenantID, modelName)
}

// Ability 一条渠道记录（abilities 表的 Go 结构）
type Ability struct {
	ID            int64   // abilities.id（用于 asyncDisable 禁用）
	Provider      string  // "openai" / "anthropic" / "volcengine"
	ProviderModel string  // 上游真实模型名，如 "gpt-4o-mini"
	InputPrice1k  float64 // 本渠道输入 token 单价（每 1000）
	OutputPrice1k float64 // 本渠道输出 token 单价（每 1000）
	Priority      int     // 优先级层（100=最高）
	Weight        int     // 同层权重
	FallbackModel string  // 熔断兜底模型（可选）
}

// PriorityTier 一个优先级层的所有渠道（同层内加权随机）
type PriorityTier struct {
	Priority int
	Channels []Ability
}

// ErrNoAbilities abilities 表查不到可用渠道（abilities 表完全没数据时不返回此错误，走 fallback）
var ErrNoAbilities = errors.New("routing: no enabled abilities available")

// ErrNoAvailableChannel 所有候选（含 fallback）都被熔断/冷却过滤
var ErrNoAvailableChannel = errors.New("routing: no available channel")

// ResolveAbilities 对外导出接口：查 abilities 表拿渠道分层
// 如果 abilities 表不存在（旧环境未跑迁移）→ 返回 found=false，handler 走 fallback
func (r *Router) ResolveAbilities(ctx context.Context, tenantID int64, modelName string) ([]PriorityTier, bool, error) {
	return r.resolveAbilitiesDB(ctx, tenantID, modelName)
}

// resolveAbilitiesDB 从 abilities 表查渠道并按 priority 分层（缓存优先，miss 才打 DB）
// 返回值：
//   - tiers：按 priority 降序排列的分层结果（已按当前熔断状态过滤）
//   - found：表中是否有该 tenant+model 的记录（用于决定是否 fallback 到 models 表）
//
// 调用方不得原地修改返回的 tiers（缓存命中时多请求共享底层切片）
func (r *Router) resolveAbilitiesDB(ctx context.Context, tenantID int64, modelName string) ([]PriorityTier, bool, error) {
	key := abilitiesCacheKey(tenantID, modelName)

	// 缓存优先：命中后按当前熔断状态实时过滤（熔断不冻结进缓存，保证 OPEN 立即换路）
	if hit, ok := r.cache.Get(key); ok {
		tiers, anyLeft := r.filterBreakeredTiers(hit.tiers)
		if !hit.found || !anyLeft {
			return nil, false, nil
		}
		return tiers, true, nil
	}

	pool := r.db
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
		  AND (disabled_until IS NULL OR disabled_until <= NOW(3))
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
		all = append(all, a)
	}

	if len(all) == 0 {
		// 否定结果也缓存：abilities 无记录的 tenant+model 不再每请求空查 DB
		r.cache.Set(key, abilitiesEntry{found: false})
		return nil, false, nil
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

	r.cache.Set(key, abilitiesEntry{tiers: tiers, found: true})

	filtered, anyLeft := r.filterBreakeredTiers(tiers)
	if !anyLeft {
		return nil, false, nil // 记录存在但全被熔断 → 走 fallback
	}
	return filtered, true, nil
}

// filterBreakeredTiers 过滤掉所有端点都熔断的 provider（不修改入参，返回新切片）
// 返回值 anyLeft=false 表示过滤后无任何可用渠道
func (r *Router) filterBreakeredTiers(tiers []PriorityTier) (filtered []PriorityTier, anyLeft bool) {
	out := make([]PriorityTier, 0, len(tiers))
	for _, t := range tiers {
		var chans []Ability
		for _, a := range t.Channels {
			// 🔴 熔断过滤：跳过所有端点都熔断的 provider（临时不可用，超时后自动恢复）
			if !r.breakers.IsProviderOpen(a.Provider) {
				chans = append(chans, a)
			}
		}
		if len(chans) > 0 {
			out = append(out, PriorityTier{Priority: t.Priority, Channels: chans})
		}
	}
	return out, len(out) > 0
}

// randIntn 随机源；测试可替换为固定种子
var randIntn = rand.Intn

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

	r := randIntn(total)
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

// PlanAttempts 把分层渠道展开成有序的尝试列表
//
// 规则：
//   - 层间按 priority 降序（tiers 已由 ResolveAbilities 排好序）
//   - 层内按 weight 加权洗牌（不放回抽取，权重语义与 WeightedPick 一致）
//   - 每个渠道在结果中至多出现一次（单次请求每渠道最多尝试一次）
//   - maxAttempts > 0 时截断到该长度；<= 0 不截断
func PlanAttempts(tiers []PriorityTier, maxAttempts int) []Ability {
	var total int
	for i := range tiers {
		total += len(tiers[i].Channels)
	}

	attempts := make([]Ability, 0, total)
	for i := range tiers {
		remaining := make([]Ability, len(tiers[i].Channels))
		copy(remaining, tiers[i].Channels)
		for len(remaining) > 0 {
			picked := WeightedPick(remaining)
			attempts = append(attempts, *picked)
			remaining = removeAbility(remaining, picked.ID)
		}
	}

	if maxAttempts > 0 && len(attempts) > maxAttempts {
		attempts = attempts[:maxAttempts]
	}
	return attempts
}

// removeAbility 移除候选中指定 ID 的渠道（不放回抽取用）
func removeAbility(list []Ability, id int64) []Ability {
	for i := range list {
		if list[i].ID == id {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// 渠道健康策略
const (
	// FailureThreshold 连续失败达到该次数后进入冷却
	FailureThreshold = 5
	// cooldownDuration 冷却时长，到期后重新参与路由
	cooldownDuration = 30 * time.Second
	// reasonMaxLen disabled_reason 字段长度
	reasonMaxLen = 255
)

// disableChannelQuery 永久禁用渠道
const disableChannelQuery = "UPDATE abilities SET enabled = 0, disabled_reason = ? WHERE id = ?"

// recordSuccessQuery 成功清零：仅当行确实存在失败/冷却状态时才命中
// （稳态下每个成功请求都会走这里，守卫子句保证 RowsAffected=0 → 不失效缓存，防缓存被成功流量清空）
const recordSuccessQuery = `UPDATE abilities SET
		consecutive_failures = 0,
		disabled_until = NULL,
		disabled_reason = NULL
	WHERE id = ? AND (consecutive_failures > 0 OR disabled_until IS NOT NULL OR disabled_reason IS NOT NULL)`

// AsyncDisableChannel 异步永久禁用一个渠道（鉴权失败 / 上游模型不存在）
// 写库成功后失效本节点 abilities 缓存，下一次请求即不再路由到该渠道
func (r *Router) AsyncDisableChannel(abilityID int64, reason string) {
	if abilityID <= 0 {
		return
	}
	r.updateAbilityAsync(true, disableChannelQuery, truncateReason(reason), abilityID)
}

// AsyncRecordFailure 异步记录一次失败：连续失败达到阈值后进入冷却
// 写库成功后失效本节点 abilities 缓存，冷却状态下一次请求即生效
func (r *Router) AsyncRecordFailure(abilityID int64, reason string) {
	if abilityID <= 0 {
		return
	}
	r.updateAbilityAsync(true, fmt.Sprintf(`
		UPDATE abilities SET
			consecutive_failures = consecutive_failures + 1,
			disabled_reason = ?,
			disabled_until = CASE
				WHEN consecutive_failures + 1 >= %d THEN DATE_ADD(NOW(3), INTERVAL %d SECOND)
				ELSE disabled_until
			END
		WHERE id = ?`, FailureThreshold, int(cooldownDuration.Seconds())),
		truncateReason(reason), abilityID)
}

// AsyncRecordSuccess 异步清零连续失败计数并解除冷却
func (r *Router) AsyncRecordSuccess(abilityID int64) {
	if abilityID <= 0 {
		return
	}
	r.updateAbilityAsync(true, recordSuccessQuery, abilityID)
}

// updateAbilityAsync 注册到本路由器的异步执行器执行渠道健康更新，不阻塞当前请求
// （任务进 Router.exec 的 WaitGroup，优雅退出时由 App 关停序列统一排空）
// invalidate=true 且更新确实命中行时，失效本节点 abilities 缓存命名空间
func (r *Router) updateAbilityAsync(invalidate bool, query string, args ...interface{}) {
	if r.db == nil {
		return
	}
	r.exec.Go(func() { r.execAbilityUpdate(invalidate, query, args...) })
}

// execAbilityUpdate 同步执行渠道健康更新（从 updateAbilityAsync 拆出便于测试）
//
// 失效纪律：
//   - 执行失败不失效（缓存维持现状，TTL 兜底）
//   - RowsAffected=0 不失效（行状态未变化，如稳态下的成功清零）
//   - 失效粒度为整个 abilities 命名空间（abilityID → tenant+model 的映射需额外查库，
//     而禁用/冷却是低频事件，全清代价可接受；跨节点同步由 broadcast.Bus 负责）
func (r *Router) execAbilityUpdate(invalidate bool, query string, args ...interface{}) {
	pool := r.db
	if pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := pool.ExecContext(ctx, query, args...)
	if err != nil || !invalidate {
		return
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return
	}
	r.cache.InvalidatePrefix("")

	// 跨节点广播：把本地状态变更通知其他节点同步失效缓存
	if r.bus != nil {
		var evType string
		switch {
		case query == disableChannelQuery:
			evType = broadcast.TypeChannelDisabled
		case query == recordSuccessQuery:
			// 成功清零不广播（其他节点缓存自然过期即可，或等下次该节点自己写库）
			return
		default:
			// AsyncRecordFailure 生成的冷却 SQL（包含 FailureThreshold 与 cooldownDuration）
			evType = broadcast.TypeChannelCooldown
		}

		var abilityID int64
		if len(args) > 0 {
			if id, ok := args[len(args)-1].(int64); ok {
				abilityID = id
			}
		}

		r.exec.Go(func() {
			bctx, bcancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer bcancel()
			if perr := r.bus.Publish(bctx, broadcast.Event{
				Type:      evType,
				AbilityID: abilityID,
			}); perr != nil {
				observability.L().Warn("broadcast publish failed",
					zap.String("type", evType),
					zap.Int64("ability_id", abilityID),
					zap.Error(perr),
				)
			}
		})
	}
}

func truncateReason(reason string) string {
	if len(reason) <= reasonMaxLen {
		return reason
	}
	return reason[:reasonMaxLen]
}

// --- 重试决策 ---

// RetryDecision 错误决策结果
type RetryDecision int

const (
	RetryStop     RetryDecision = iota // 不可重试，放弃
	RetryRetry                         // 可重试，换下一个渠道
	RetryFallback                      // 特殊：有 fallback_model，切到兜底
)

// DecideRelayRetry 根据结构化错误决定是否换下一个渠道
//
// 决策只看错误分类与熔断器状态，不做任何字符串匹配：
//   - nil → stop
//   - 客户端取消 → stop（客户端已放弃，重试无意义）
//   - 重试次数耗尽 → stop
//   - 熔断打开（ErrOpenState / ErrTooManyRequests）→ retry（该渠道暂不可用，换下一个）
//   - provider.UpstreamError → 按 Kind 派生的 Retryable()
//   - 超时（context.DeadlineExceeded / net.Error.Timeout）→ retry
//   - 其他未知错误 → stop
func DecideRelayRetry(err error, retry, maxRetry int) RetryDecision {
	if err == nil {
		return RetryStop
	}

	// 1. 客户端已断开
	if errors.Is(err, context.Canceled) {
		return RetryStop
	}

	// 2. retry 次数耗尽
	if retry >= maxRetry {
		return RetryStop
	}

	// 3. 熔断打开：该渠道不可用，换下一个
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return RetryRetry
	}

	// 4. 结构化上游错误：是否换渠道完全由 Kind 决定
	var upstreamErr *provider.UpstreamError
	if errors.As(err, &upstreamErr) {
		if upstreamErr.Retryable() {
			return RetryRetry
		}
		return RetryStop
	}

	// 5. 超时
	if errors.Is(err, context.DeadlineExceeded) {
		return RetryRetry
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return RetryRetry
	}

	// 6. 未知类型错误
	return RetryStop
}
