// Package provider 多 key 池（阶段 8 新组件）
//
// 一个渠道（abilities 表一条记录）可以配多把 key（channel_keys 表）。
// ChannelFactory 把 DB 里的 key 池加载成内存结构，每次 relay attempt 从 KeyPool
// 按加权轮询选一把 key，再动态构造该 key 对应的 Provider 实例来调用上游。
//
// 隔离策略：
//   - 同 key 连续失败 3 次 → 60s 冷却（只影响这把 key，不影响同渠道其他 key）
//   - 熔断 key 从 provider+base_url 细化到 provider+base_url+channel_id（relay 层改）
//
// key_ref 契约（只存引用，不落明文）：
//
//	env://VAR_NAME     → os.Getenv("VAR_NAME")
//	vault://path       → Vault KV v2 读取（阶段 16 已接入，main 经 SetKeyRefResolver 注入；
//	                     未配置 vault.addr 时解析为空并 warn 跳过）
package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ai-gateway/pkg/observability"
	"go.uber.org/zap"
)

// KeyPool 管理一个渠道的多把 key
//
// 维护一组 KeyEntry 并提供加权轮询的 Select 方法；
// 连续失败达到 FailureThreshold 后该 key 进入冷却（DisableDuration）。
//
// 每把 key 的 Provider 实例在首次选中时惰性构造并缓存（Provider 接口无状态，
// 缓存安全；不同 key 的 Provider 实例独立，互不影响）。
type KeyPool struct {
	mu      sync.RWMutex
	entries []*KeyEntry

	// roundRobinCursor 加权轮询游标（原子递增，取模 entries 数量得到起始位置，
	// 避免每次都从 0 开始导致权重靠前的 key 始终被先选）
	roundRobinCursor atomic.Int64
}

// KeyEntry 单把 key 的运行时状态
type KeyEntry struct {
	KeyRef              string     // env://VAR / vault://path
	APICKey             string     // 启动时解析 key_ref 拿到的真实密钥（仅内存）
	Weight              int        // 权重，0 视为 1
	Enabled             bool       // Admin 侧禁用开关
	ConsecutiveFailures int        // 连续失败计数
	DisabledUntil       *time.Time // 冷却截止（nil = 正常）
	DisabledReason      string     // 冷却原因（调试）
}

const (
	// KeyFailureThreshold 同 key 连续失败达到该次数后进入冷却
	KeyFailureThreshold = 3
	// KeyDisableDuration 单 key 冷却时长（到期后自动恢复）
	KeyDisableDuration = 60 * time.Second
)

// NewKeyPool 构造一个空的 KeyPool
func NewKeyPool() *KeyPool {
	return &KeyPool{}
}

// SetEntries 加载（或替换）一个渠道的全部 key 配置
// entries 的顺序不影响 Select 结果（Select 内部会随机起始位置 + 跳过 disabled）
func (kp *KeyPool) SetEntries(entries []*KeyEntry) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.entries = entries
}

// Entries 返回当前所有 key 条目副本（只读用途）
func (kp *KeyPool) Entries() []*KeyEntry {
	kp.mu.RLock()
	defer kp.mu.RUnlock()
	out := make([]*KeyEntry, len(kp.entries))
	copy(out, kp.entries)
	return out
}

// Select 选出下一把可用的 key
//
// 策略：round-robin 游标起始位置 + 跳过 disabled / 冷却中的 key
// 所有权重为 0 的 key 视为权重 1（防饿死）
//
// 返回 (entry, true) 表示选中；返回 (nil, false) 表示没有可用 key（全部禁用或冷却）
func (kp *KeyPool) Select() (*KeyEntry, bool) {
	kp.mu.RLock()
	entries := kp.entries
	kp.mu.RUnlock()

	if len(entries) == 0 {
		return nil, false
	}

	// 跳过 disabled 与冷却中的 key（一次遍历找全部可用 key）
	type weightedEntry struct {
		e     *KeyEntry
		start int
		end   int
	}
	var available []weightedEntry
	var total int
	now := time.Now()
	for _, e := range entries {
		if !e.Enabled {
			continue
		}
		if e.DisabledUntil != nil && now.Before(*e.DisabledUntil) {
			continue
		}
		w := e.Weight
		if w <= 0 {
			w = 1
		}
		available = append(available, weightedEntry{e: e, start: total, end: total + w})
		total += w
	}
	if len(available) == 0 || total == 0 {
		return nil, false
	}

	// round-robin：游标原子递增，取模后映射到加权区间
	// 这种实现比纯加权随机更稳定（每把 key 的选择频率严格按权重）
	cursor := kp.roundRobinCursor.Add(1) - 1 // 第一笔调用返回 0
	target := int(cursor % int64(total))

	for _, we := range available {
		if target >= we.start && target < we.end {
			return we.e, true
		}
	}
	// 理论上不会落到这里，保底返回第一个可用 key
	return available[0].e, true
}

// RecordSuccess 某把 key 成功 → 清零连续失败计数 + 解除冷却
func (kp *KeyPool) RecordSuccess(keyRef string) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	for _, e := range kp.entries {
		if e.KeyRef == keyRef {
			if e.ConsecutiveFailures != 0 || e.DisabledUntil != nil {
				e.ConsecutiveFailures = 0
				e.DisabledUntil = nil
				e.DisabledReason = ""
			}
			return
		}
	}
}

// RecordFailure 某把 key 失败 → 累计 + 达到阈值则进入冷却
func (kp *KeyPool) RecordFailure(keyRef, reason string) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	for _, e := range kp.entries {
		if e.KeyRef == keyRef {
			e.ConsecutiveFailures++
			if e.ConsecutiveFailures >= KeyFailureThreshold && e.DisabledUntil == nil {
				end := time.Now().Add(KeyDisableDuration)
				e.DisabledUntil = &end
				e.DisabledReason = reason
			}
			return
		}
	}
}

// --- provider type → baseURL 映射（App 组装时从 config 注入到 Registry） ---

// SetBaseURLs 注入 provider type → baseURL 映射
// 供 GetForChannel 内部查询（ChannelProvider 构造时需要 baseURL）
func (r *Registry) SetBaseURLs(mapping map[string]string) {
	r.baseURLsMu.Lock()
	defer r.baseURLsMu.Unlock()
	for k, v := range mapping {
		r.baseURLs[k] = v
	}
}

// --- ChannelFactory：按 abilityID 索引 KeyPool + Provider 实例 ---

// ChannelConfig 渠道（ability）的完整配置
type ChannelConfig struct {
	AbilityID     int64
	ProviderType  string
	ProviderModel string
	BaseURL       string // 启动时解析自 config.providers
	KeyPool       *KeyPool
	ProviderCache sync.Map // key_ref → Provider（按选中 key 动态构造）
	registry      *Registry
}

// LoadChannelKeys 从 channel_keys 表加载所有 key 配置
//
// 传入 DB pool；共享 HTTP clients 由 SetClients 注入。
// 成功返回后 GetForChannel 即可生效；失败则 channel_keys 不启用，
// relay fallback 到原有 Registry.Get 流程（向后兼容）。
func (r *Registry) LoadChannelKeys(ctx context.Context, pool *sql.DB) error {
	if pool == nil {
		return errors.New("db pool is nil")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := pool.QueryContext(queryCtx, `
		SELECT ck.channel_id, ck.key_ref, ck.weight, ck.enabled,
		       ck.consecutive_failures, ck.disabled_until, ck.disabled_reason,
		       a.provider, a.provider_model
		FROM channel_keys ck
		INNER JOIN abilities a ON a.id = ck.channel_id
		WHERE ck.enabled = 1 AND a.enabled = 1
	`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 表未迁移或无种子 → 不启用多 key 池，不报错
			return nil
		}
		return fmt.Errorf("load channel_keys failed: %w", err)
	}
	defer rows.Close()

	// 按 channel_id 分组
	type group struct {
		abilityID     int64
		providerType  string
		providerModel string
		entries       []*KeyEntry
	}
	groups := make(map[int64]*group)

	for rows.Next() {
		var (
			abilityID      int64
			keyRef         string
			weight         int
			enabled        bool
			consecutive    int
			disabledUntil  sql.NullTime
			disabledReason sql.NullString
			providerType   string
			providerModel  string
		)
		if err := rows.Scan(&abilityID, &keyRef, &weight, &enabled,
			&consecutive, &disabledUntil, &disabledReason,
			&providerType, &providerModel); err != nil {
			return fmt.Errorf("scan channel_key failed: %w", err)
		}
		apiKey := r.resolveKeyRef(keyRef)
		if apiKey == "" {
			observability.L().Warn("channel_key resolve empty, skipping",
				zap.Int64("channel_id", abilityID), zap.String("key_ref", keyRef))
			continue
		}

		entry := &KeyEntry{
			KeyRef:              keyRef,
			APICKey:             apiKey,
			Weight:              weight,
			Enabled:             enabled,
			ConsecutiveFailures: consecutive,
		}
		if disabledUntil.Valid {
			t := disabledUntil.Time
			entry.DisabledUntil = &t
			entry.DisabledReason = disabledReason.String
		}

		g, ok := groups[abilityID]
		if !ok {
			g = &group{abilityID: abilityID, providerType: providerType, providerModel: providerModel}
			groups[abilityID] = g
		}
		g.entries = append(g.entries, entry)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("channel_keys rows error: %w", err)
	}

	// 注册每个渠道的 ChannelConfig
	count := 0
	for abilityID, g := range groups {
		r.baseURLsMu.RLock()
		baseURL := r.baseURLs[g.providerType]
		r.baseURLsMu.RUnlock()
		cfg := &ChannelConfig{
			AbilityID:     abilityID,
			ProviderType:  g.providerType,
			ProviderModel: g.providerModel,
			BaseURL:       baseURL,
			KeyPool:       NewKeyPool(),
			registry:      r,
		}
		cfg.KeyPool.SetEntries(g.entries)
		r.channelConfigs.Store(abilityID, cfg)
		count++
	}

	observability.L().Info("channel_keys loaded",
		zap.Int("channels", count),
		zap.Int("keys", func() int {
			total := 0
			for _, g := range groups {
				total += len(g.entries)
			}
			return total
		}()))
	return nil
}

// GetForChannel 获取指定渠道的 Provider（带多 key 池）
//
// abilityID <= 0 或找不到 channel_keys 记录 → 返回 nil, false（让调用方 fallback）。
// ChannelConfig 在 LoadChannelKeys 时已包含 providerType + baseURL，调用方无需额外提供。
func (r *Registry) GetForChannel(abilityID int64) (Provider, bool) {
	if abilityID <= 0 {
		return nil, false
	}
	v, ok := r.channelConfigs.Load(abilityID)
	if !ok {
		return nil, false
	}
	cfg := v.(*ChannelConfig)
	return &channelProvider{cfg: cfg}, true
}

// ReloadChannelKeys 启动后再次加载（Admin 侧变更 key 池后热更用，阶段 15）
// 当前阶段仅提供 API，实际触发由 Admin 负责
func (r *Registry) ReloadChannelKeys(ctx context.Context, pool *sql.DB) error {
	// 清空旧配置后重新加载
	r.channelConfigs.Range(func(key, _ any) bool {
		r.channelConfigs.Delete(key)
		return true
	})
	return r.LoadChannelKeys(ctx, pool)
}

// --- channelProvider：包装 Provider 接口，内部每次调用选 key ---

// channelProvider 一个渠道的 Provider 代理
// 每次 ChatCompletion/ChatCompletionStream 调用时，从 KeyPool 选一把可用的 key，
// 惰性构造（或复用缓存）该 key 对应的 Provider 实例，然后委托调用。
type channelProvider struct {
	cfg *ChannelConfig
}

// Name 返回渠道级标识（日志/指标用，含 channel_id 便于定位）
func (cp *channelProvider) Name() string {
	return cp.cfg.ProviderType
}

// --- ChatCompletion 代理 ---

func (cp *channelProvider) ChatCompletion(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	p, entry, err := cp.pickProvider()
	if err != nil {
		return nil, err
	}
	resp, callErr := p.ChatCompletion(ctx, req)
	cp.recordResult(entry, callErr)
	return resp, callErr
}

func (cp *channelProvider) ChatCompletionStream(ctx context.Context, req *ChatRequest) (<-chan StreamChunk, <-chan error) {
	p, entry, err := cp.pickProvider()
	if err != nil {
		ch := make(chan error, 1)
		ch <- err
		close(ch)
		chunkCh := make(chan StreamChunk)
		close(chunkCh)
		return chunkCh, ch
	}
	chunkCh, errCh := p.(Streamer).ChatCompletionStream(ctx, req)
	// 包装 error channel，让我们能在调用结束后 recordResult
	// 但由于是流式，我们会在 errChan 收到第一个 error / close 时记录
	// 这里先只记录成功/失败在 errChan 上
	go func() {
		for {
			select {
			case err, ok := <-errCh:
				if !ok {
					// 流正常关闭（errChan close = 没有流式错误）
					cp.cfg.KeyPool.RecordSuccess(entry.KeyRef)
					return
				}
				if err != nil {
					cp.cfg.KeyPool.RecordFailure(entry.KeyRef, err.Error())
				}
				return
			case <-ctx.Done():
				cp.cfg.KeyPool.RecordFailure(entry.KeyRef, ctx.Err().Error())
				return
			}
		}
	}()
	return chunkCh, errCh
}

func (cp *channelProvider) Embedding(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	p, entry, err := cp.pickProvider()
	if err != nil {
		return nil, err
	}
	resp, callErr := p.Embedding(ctx, req)
	cp.recordResult(entry, callErr)
	return resp, callErr
}

func (cp *channelProvider) Moderation(ctx context.Context, req *ModerationRequest) (*ModerationResponse, error) {
	p, entry, err := cp.pickProvider()
	if err != nil {
		return nil, err
	}
	resp, callErr := p.Moderation(ctx, req)
	cp.recordResult(entry, callErr)
	return resp, callErr
}

func (cp *channelProvider) Cost(providerModel string, inputTokens, outputTokens int64) (float64, error) {
	p, _, err := cp.pickProvider()
	if err != nil {
		return 0, err
	}
	return p.Cost(providerModel, inputTokens, outputTokens)
}

func (cp *channelProvider) BaseURL() string {
	return cp.cfg.BaseURL
}

// pickProvider 从 KeyPool 选一把 key 并获取/构造对应的 Provider 实例
func (cp *channelProvider) pickProvider() (Provider, *KeyEntry, error) {
	entry, ok := cp.cfg.KeyPool.Select()
	if !ok {
		return nil, nil, fmt.Errorf("no enabled key in key pool for channel %d", cp.cfg.AbilityID)
	}

	// 先查缓存
	if cached, loaded := cp.cfg.ProviderCache.Load(entry.KeyRef); loaded {
		return cached.(Provider), entry, nil
	}

	// 首次构造：通过 Registry 注册的渠道构造器创建实例
	p, err := cp.cfg.registry.constructProvider(cp.cfg.ProviderType, cp.cfg.BaseURL, entry.APICKey)
	if err != nil {
		cp.cfg.KeyPool.RecordFailure(entry.KeyRef, err.Error())
		return nil, nil, err
	}
	cp.cfg.ProviderCache.Store(entry.KeyRef, p)
	return p, entry, nil
}

// recordResult 记录非流式调用的结果（成功/失败）
func (cp *channelProvider) recordResult(entry *KeyEntry, err error) {
	if entry == nil {
		return
	}
	if err == nil {
		cp.cfg.KeyPool.RecordSuccess(entry.KeyRef)
		return
	}
	// 鉴权失败（401/403/404）→ 永久失效（该 key 的凭证有误）
	var ue *UpstreamError
	if errors.As(err, &ue) && ue.Permanent() {
		cp.cfg.KeyPool.RecordFailure(entry.KeyRef, ue.Error())
		// 鉴权失败是凭证本身有问题，直接标记该 key 失效（永不自动恢复）
		cp.disablePermanently(entry, ue.Error())
		return
	}
	cp.cfg.KeyPool.RecordFailure(entry.KeyRef, err.Error())
}

// disablePermanently 鉴权失败后把该 key 永久禁用，Admin 侧需手动修复
func (cp *channelProvider) disablePermanently(entry *KeyEntry, reason string) {
	cp.cfg.KeyPool.mu.Lock()
	defer cp.cfg.KeyPool.mu.Unlock()
	for _, e := range cp.cfg.KeyPool.entries {
		if e.KeyRef == entry.KeyRef {
			e.Enabled = false
			e.DisabledReason = reason
			observability.L().Warn("key permanently disabled due to auth failure",
				zap.String("key_ref", e.KeyRef),
				zap.String("reason", reason))
			return
		}
	}
}

// --- 辅助：按 providerType + baseURL + apiKey 构造 Provider 实例 ---

// providerConstructor 保存每个 provider type 的构造器签名
// 启动时 App 组装（registerProviders）调用 RegisterChannelConstructor 注册
type providerConstructor func(baseURL, apiKey string, clients *Clients) Provider

// RegisterChannelConstructor 注册一个 provider type 的构造器，供 ChannelProvider 惰性构造用
func (r *Registry) RegisterChannelConstructor(providerType string, ctor providerConstructor) {
	r.constructorsMu.Lock()
	defer r.constructorsMu.Unlock()
	r.constructors[providerType] = ctor
}

// constructProvider 按 providerType + baseURL + apiKey 构造 Provider 实例
func (r *Registry) constructProvider(providerType, baseURL, apiKey string) (Provider, error) {
	r.constructorsMu.RLock()
	ctor, ok := r.constructors[providerType]
	r.constructorsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no constructor registered for provider type %q", providerType)
	}
	return ctor(baseURL, apiKey, r.clients.Load()), nil
}

// SetClients 注入共享 HTTP client 对（channelProvider 惰性构造时需要）
func (r *Registry) SetClients(c *Clients) { r.clients.Store(c) }

// Clients 返回共享 HTTP client 对（未注入时为 nil）
func (r *Registry) Clients() *Clients { return r.clients.Load() }

// --- key_ref 解析 ---

// ResolveKeyRef 包级默认解析（env:// + 纯字符串透传）
// main 构造组合解析器（env://+vault://）时以它为 env/fallback 分支
func ResolveKeyRef(ref string) string { return resolveKeyRef(ref) }

// resolveKeyRef 解析 key_ref 为真实密钥（启动时从环境变量/Vault 读取，只在内存中留存）
func resolveKeyRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, "env://") {
		name := strings.TrimPrefix(ref, "env://")
		return os.Getenv(name)
	}
	if strings.HasPrefix(ref, "vault://") {
		// 阶段 16 接入 Vault，当前实现返回空并 warn
		return ""
	}
	return ref // 纯字符串 fallback（仅测试用，生产不应落明文）
}

// ClearChannelConfigs 清空所有渠道配置（测试辅助 / Reload 前置）
func (r *Registry) ClearChannelConfigs() {
	r.channelConfigs.Range(func(key, _ any) bool {
		r.channelConfigs.Delete(key)
		return true
	})
}
