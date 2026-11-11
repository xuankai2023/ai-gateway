// Package relay 上游调用中继
//
// 承载候选计划解析、逐渠道尝试、熔断、非流式/流式调用。
// Handler 只负责请求解析、响应编解码和结果输出。
//
// 尝试规则：
//   - 候选计划由 Router.ResolveAbilities + PlanAttempts 展开（abilities 优先，models 表兜底）
//   - 每个渠道单次请求最多尝试一次，失败后按 routing.DecideRelayRetry 决策是否换下一个
//   - 每次 attempt 用熔断器包裹；流式只包裹“建连 + 等待首帧”，首帧成功即视为成功
//
// 阶段 14：包级全局态（dbPool / AttemptTimeout / ResolveAttemptsFn / modelCache）
// 收敛为 Relay 实例字段，随 App 生命周期创建，同进程可多实例互不干扰。
package relay

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"ai-gateway/apps/gateway/internal/diagnostics"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/cache"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/pricing"
	"ai-gateway/pkg/provider"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// DefaultAttemptTimeout 单次渠道 attempt 的默认硬上限（App 组装时按 gateway.attempt_timeout 覆盖）
const DefaultAttemptTimeout = 30 * time.Second

// timeout_kind span 属性取值：本次 attempt 被哪层超时切断
const (
	timeoutKindNone    = "none"    // 非超时结束（成功或普通失败）
	timeoutKindAttempt = "attempt" // 单次 attempt 上限触发
	timeoutKindRequest = "request" // 请求级上限（request_timeout / stream_max_duration）触发
)

// ErrModelNotFound models 表没有该租户的模型映射
var ErrModelNotFound = errors.New("model not found for this tenant")

// Relay 上游调用中继（阶段 14：实例化，聚合路由 / 熔断 / Provider 注册表 / 兜底缓存）
type Relay struct {
	router         *routing.Router
	breakers       *circuitbreaker.Manager
	providers      *provider.Registry
	db             *sql.DB
	attemptTimeout time.Duration
	modelCache     *cache.Cache[modelRoute] // 兜底路由本地缓存（命名空间 model:{tenant_id}:{model}）
}

// modelTTL 兜底路由缓存条目的过期时间
const modelTTL = 30 * time.Second

// New 创建中继；attemptTimeout<=0 回落 DefaultAttemptTimeout
// ErrModelNotFound 等否定结果不缓存：新上架模型下一次请求即可用
func New(router *routing.Router, breakers *circuitbreaker.Manager, providers *provider.Registry,
	db *sql.DB, attemptTimeout time.Duration) *Relay {
	if breakers == nil {
		breakers = circuitbreaker.NewManager(circuitbreaker.Options{})
	}
	if attemptTimeout <= 0 {
		attemptTimeout = DefaultAttemptTimeout
	}
	mc := cache.New[modelRoute](cache.Options{
		Name:       "model",
		MaxEntries: 10000,
		DefaultTTL: modelTTL,
	})
	observability.RegisterCache("model", mc)
	return &Relay{
		router:         router,
		breakers:       breakers,
		providers:      providers,
		db:             db,
		attemptTimeout: attemptTimeout,
		modelCache:     mc,
	}
}

// Close 关闭兜底路由缓存（App 生命周期收尾调用）
func (r *Relay) Close() { r.modelCache.Close() }

// InvalidateModelCache 全清兜底路由缓存（命名空间 model:*）
// Admin 变更 models 表 / 广播 cache_invalidate 后调用，下一次解析即回源 DB
func (r *Relay) InvalidateModelCache() { r.modelCache.InvalidatePrefix("") }

// Result 一次请求的最终 attempt 结果（非流式 / 流式共用）
type Result struct {
	Attempt    routing.Ability        // 最终命中的渠道（全部失败时为最后一次尝试的渠道）
	Response   *provider.ChatResponse // 非流式成功响应
	FirstChunk provider.StreamChunk   // 流式首帧
	Chunks     <-chan provider.StreamChunk
	Errs       <-chan error
	Attempts   int   // 候选渠道总数
	Err        error // 全部失败时的最后一个错误；客户端断开时为 context.Canceled
	cancel     context.CancelFunc

	// AttemptsTrace 每次渠道尝试的诊断快照（诊断包用；按尝试顺序，含成功跳）
	AttemptsTrace []diagnostics.Attempt
}

// Close 释放流式派生 context
func (r *Result) Close() {
	if r.cancel != nil {
		r.cancel()
	}
}

// --- 候选计划 ---

// ResolveAttempts 解析本次请求的有序尝试列表
//
// abilities 表有可用渠道 → PlanAttempts 展开（层间降序 + 层内加权洗牌 + 截断）
// abilities 无可用渠道 → models 表单渠道兜底，ID=0 表示非 abilities 渠道
func (r *Relay) ResolveAttempts(ctx context.Context, tenantID int64, modelName string, maxAttempts int) ([]routing.Ability, error) {
	if r.router != nil {
		tiers, abilitiesFound, err := r.router.ResolveAbilities(ctx, tenantID, modelName)
		if err != nil {
			return nil, fmt.Errorf("routing error: %w", err)
		}
		if abilitiesFound {
			if attempts := routing.PlanAttempts(tiers, maxAttempts); len(attempts) > 0 {
				return attempts, nil
			}
		}
	}

	route, err := r.resolveModel(ctx, tenantID, modelName)
	if err != nil {
		return nil, err
	}
	// fallback 候选同样做熔断过滤，避免绕过 abilities 的熔断判断
	if r.breakers.IsProviderOpen(route.providerType) {
		return nil, routing.ErrNoAvailableChannel
	}
	return []routing.Ability{{
		ID:            0,
		Provider:      route.providerType,
		ProviderModel: route.providerModel,
		InputPrice1k:  route.inputPrice1k.Dollars(),
		OutputPrice1k: route.outputPrice1k.Dollars(),
	}}, nil
}

// ResolveModelPricing 返回 models 表的定价快照（复用 modelCache，不额外打 DB）。
func (r *Relay) ResolveModelPricing(ctx context.Context, tenantID int64, modelName string) (*pricing.ModelRow, error) {
	route, err := r.resolveModel(ctx, tenantID, modelName)
	if err != nil {
		return nil, err
	}
	return &pricing.ModelRow{
		Model:         modelName,
		Provider:      route.providerType,
		PricingModel:  pricing.NormalizeModel(route.pricingModel),
		PricingConfig: route.pricingConfig,
		InputPrice1k:  route.inputPrice1k,
		OutputPrice1k: route.outputPrice1k,
		PriceVersion:  route.priceVersion,
	}, nil
}

// modelRoute 模型路由解析结果（models 表 fallback 用）
type modelRoute struct {
	providerType  string
	providerModel string
	inputPrice1k  ledger.MicroUSD
	outputPrice1k ledger.MicroUSD
	pricingModel  pricing.PriceModel
	pricingConfig []byte
	priceVersion  int
}

// resolveModel 查 models 表（fallback），缓存优先，miss 才打 DB
func (r *Relay) resolveModel(ctx context.Context, tenantID int64, modelName string) (*modelRoute, error) {
	key := fmt.Sprintf("%d:%s", tenantID, modelName)
	if route, ok := r.modelCache.Get(key); ok {
		route := route // 拷贝出参，避免调用方共享缓存引用
		return &route, nil
	}

	pool := r.db
	if pool == nil {
		return nil, errors.New("db pool not initialized")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	route := &modelRoute{}
	var pricingModel string
	err := pool.QueryRowContext(queryCtx, `
		SELECT provider, provider_model, input_price_1k, output_price_1k,
		       COALESCE(pricing_model, 'linear'), pricing_config, price_version
		FROM models
		WHERE tenant_id = ? AND model_name = ? AND is_active = 1
	`, tenantID, modelName).Scan(&route.providerType, &route.providerModel,
		&route.inputPrice1k, &route.outputPrice1k,
		&pricingModel, &route.pricingConfig, &route.priceVersion)
	route.pricingModel = pricing.PriceModel(pricingModel)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrModelNotFound
		}
		return nil, fmt.Errorf("model resolve failed: %w", err)
	}

	r.modelCache.Set(key, *route)
	return route, nil
}

// --- 非流式调用 ---

// NonStream 按尝试列表逐个调用上游，成功即返回
func (r *Relay) NonStream(ctx context.Context, attempts []routing.Ability, req *provider.ChatRequest) *Result {
	res := &Result{Attempts: len(attempts)}

	for i := range attempts {
		attempt := attempts[i]
		res.Attempt = attempt

		attemptCtx, span := startAttempt(ctx, attempt, i, false)
		p, err := r.getProvider(attempt)
		if err != nil {
			finishAttempt(span, err, timeoutKindNone)
			res.Err = err
			res.AttemptsTrace = append(res.AttemptsTrace, buildAttemptTrace(attempt, i, "", "unknown", 0, err))
			continue
		}

		// 诊断快照采样点：熔断前状态 + base_url（不含 key）
		bKey := breakerKey(attempt.Provider, p, attempt.ID)
		breakerBefore := circuitbreaker.StateString(r.breakers.State(bKey))
		baseURL := ""
		if e, ok := p.(provider.Endpoint); ok {
			baseURL = e.BaseURL()
		}
		callStart := time.Now()

		// 单次 attempt 硬上限：慢上游被切断后按 KindTimeout 换下一个渠道
		callCtx, cancelCall := context.WithTimeout(attemptCtx, r.attemptTimeout)
		creq := requestFor(req, attempt.ProviderModel)
		result, callErr := r.breakers.Call(bKey, func() (interface{}, error) {
			return p.ChatCompletion(callCtx, creq)
		})
		cancelCall()
		finishAttempt(span, callErr, timeoutKindOf(ctx, callCtx, callErr))
		res.AttemptsTrace = append(res.AttemptsTrace,
			buildAttemptTrace(attempt, i, baseURL, breakerBefore, time.Since(callStart).Milliseconds(), callErr))

		if callErr == nil {
			res.Response = result.(*provider.ChatResponse)
			res.Err = nil
			r.recordSuccess(attempt.ID)
			return res
		}

		res.Err = callErr
		r.recordFailure(attempt.ID, callErr)
		if routing.DecideRelayRetry(callErr, i, len(attempts)) == routing.RetryStop {
			return res
		}
		observability.RecordRetry(req.Model, attempt.Provider)
	}

	if res.Err == nil {
		res.Err = errors.New("relay: no attempts configured")
	}
	return res
}

// --- 流式调用 ---

// Stream 按尝试列表逐个建连并等待首帧，首帧成功即返回可转发的流
//
// 只重试“建连 + 首帧”阶段；一旦开始转发不再换渠道（客户端已经看到流式输出）。
func (r *Relay) Stream(ctx context.Context, attempts []routing.Ability, req *provider.ChatRequest) *Result {
	res := &Result{Attempts: len(attempts)}

	for i := range attempts {
		attempt := attempts[i]
		res.Attempt = attempt

		attemptCtx, span := startAttempt(ctx, attempt, i, true)
		p, err := r.getProvider(attempt)
		if err != nil {
			finishAttempt(span, err, timeoutKindNone)
			res.Err = err
			res.AttemptsTrace = append(res.AttemptsTrace, buildAttemptTrace(attempt, i, "", "unknown", 0, err))
			continue
		}

		streamer, ok := p.(provider.Streamer)
		if !ok {
			// Provider 不支持流式：只跳过当前渠道，不写 enabled=0
			err = errors.New("provider does not support streaming")
			finishAttempt(span, err, timeoutKindNone)
			res.Err = err
			res.AttemptsTrace = append(res.AttemptsTrace, buildAttemptTrace(attempt, i, "", "unknown", 0, err))
			continue
		}

		// 诊断快照采样点：熔断前状态 + base_url（不含 key）
		bKey := breakerKey(attempt.Provider, p, attempt.ID)
		breakerBefore := circuitbreaker.StateString(r.breakers.State(bKey))
		baseURL := ""
		if e, ok := p.(provider.Endpoint); ok {
			baseURL = e.BaseURL()
		}
		callStart := time.Now()

		// streamCtx 是流的全生命周期 ctx（受请求级 stream_max_duration 约束），
		// probeCtx 只约束“建连 + 等待首帧”这一段 attempt 上限，
		// 首帧到达后 stopProbe，流继续活到 streamCtx 结束。
		streamCtx, cancel := context.WithCancel(attemptCtx)
		probeCtx, stopProbe := context.WithTimeout(attemptCtx, r.attemptTimeout)
		probeStart := time.Now() // TTFT 起点：attempt 开始即计时（建连 + 等首帧）
		var (
			chunkChan  <-chan provider.StreamChunk
			errChan    <-chan error
			firstChunk provider.StreamChunk
			connected  bool
		)

		creq := requestFor(req, attempt.ProviderModel)
		_, callErr := r.breakers.Call(bKey, func() (interface{}, error) {
			chunkChan, errChan = streamer.ChatCompletionStream(streamCtx, creq)
			fc, ok, probeErr := probeFirstChunk(probeCtx, chunkChan, errChan)
			if probeErr != nil {
				return nil, probeErr
			}
			if !ok {
				// probeCtx 到期有两种来源：attempt 级截止 / 请求级截止，
				// 统一按可重试的上游超时归类（换下一个渠道或最终 504）；
				// 客户端主动断开是 Canceled，保持原语义（不再换渠道）
				if errors.Is(probeCtx.Err(), context.DeadlineExceeded) {
					return nil, provider.NewNetworkError(attempt.Provider, context.DeadlineExceeded)
				}
				return nil, nil // 客户端已断开
			}
			firstChunk, connected = fc, true
			return fc, nil
		})
		stopProbe()
		finishAttempt(span, callErr, timeoutKindOf(ctx, probeCtx, callErr))
		// 客户端已断开（callErr=nil 且未连接）不产生诊断快照
		if callErr != nil || connected {
			res.AttemptsTrace = append(res.AttemptsTrace,
				buildAttemptTrace(attempt, i, baseURL, breakerBefore, time.Since(callStart).Milliseconds(), callErr))
		}

		if callErr != nil {
			cancel() // 首帧前失败：取消派生 context，停止上游请求
			res.Err = callErr
			r.recordFailure(attempt.ID, callErr)
			if routing.DecideRelayRetry(callErr, i, len(attempts)) == routing.RetryStop {
				return res
			}
			observability.RecordRetry(req.Model, attempt.Provider)
			continue
		}
		if !connected {
			cancel()
			res.Err = context.Canceled
			return res
		}

		// TTFT 埋点：建连到首个 token 到达的耗时（relay probe 成功即视为首 token 到达）
		observability.RecordTTFT(attempt.Provider, time.Since(probeStart).Milliseconds())
		r.recordSuccess(attempt.ID)
		res.FirstChunk = firstChunk
		res.Chunks = chunkChan
		res.Errs = errChan
		res.Err = nil
		res.cancel = cancel
		return res
	}

	if res.Err == nil {
		res.Err = errors.New("relay: no attempts configured")
	}
	return res
}

// probeFirstChunk 等待上游第一个 chunk，判断建连是否成功
//
// 返回值：
//   - connected=true：上游已开始吐流
//   - connected=false + err：建连失败，交给 DecideRelayRetry 决策
//   - connected=false + err=nil：客户端已断开
func probeFirstChunk(ctx context.Context, chunkChan <-chan provider.StreamChunk,
	errChan <-chan error) (provider.StreamChunk, bool, error) {
	for {
		select {
		case firstChunk, ok := <-chunkChan:
			if ok {
				return firstChunk, true, nil
			}
			// chunk 流已关闭：优先取已缓冲的上游错误
			select {
			case streamErr, errOpen := <-errChan:
				if errOpen && streamErr != nil {
					return provider.StreamChunk{}, false, streamErr
				}
			default:
			}
			return provider.StreamChunk{}, false, errors.New("stream closed without first chunk")

		case streamErr, ok := <-errChan:
			if ok && streamErr != nil {
				return provider.StreamChunk{}, false, streamErr
			}
			// errChan 已关闭且无错误：以 chunkChan 为准继续等待，避免误判
			errChan = nil

		case <-ctx.Done():
			return provider.StreamChunk{}, false, nil
		}
	}
}

// --- 内部辅助 ---

// buildAttemptTrace 按调用结果生成一次渠道尝试的诊断快照（诊断包用）
func buildAttemptTrace(attempt routing.Ability, index int, baseURL, breakerStateBefore string,
	durationMs int64, callErr error) diagnostics.Attempt {

	tr := diagnostics.Attempt{
		Attempt:            index + 1,
		ChannelID:          attempt.ID,
		Provider:           attempt.Provider,
		BaseURL:            baseURL,
		Status:             "success",
		DurationMs:         durationMs,
		BreakerStateBefore: strings.ToLower(breakerStateBefore),
	}
	if callErr != nil {
		tr.Status = "failed"
		var upstreamErr *provider.UpstreamError
		if errors.As(callErr, &upstreamErr) {
			tr.ErrorKind = string(upstreamErr.Kind)
		} else {
			tr.ErrorKind = string(provider.KindUnknown)
		}
		tr.ErrorMessage = callErr.Error()
	}
	return tr
}

// startAttempt 创建一次 attempt 的 child span
func startAttempt(ctx context.Context, attempt routing.Ability, index int, stream bool) (context.Context, trace.Span) {
	return observability.T().Start(ctx, "relay.attempt",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("provider", attempt.Provider),
			attribute.String("provider_model", attempt.ProviderModel),
			attribute.Int64("channel_id", attempt.ID),
			attribute.Int("attempt", index+1),
			attribute.Bool("stream", stream),
		),
	)
}

// finishAttempt 记录 attempt 结果（含 timeout_kind 归类）并结束 span
func finishAttempt(span trace.Span, err error, timeoutKind string) {
	statusCode := http.StatusOK
	if err != nil {
		statusCode = statusCodeOf(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.SetAttributes(
		attribute.Int("status_code", statusCode),
		attribute.String("timeout_kind", timeoutKind),
	)
	span.End()
}

// timeoutKindOf 判定本次 attempt 由哪层超时切断：
// 父 ctx（请求级）先到期优先归 request（子 ctx 会继承父的截止，先看父才能归因正确）；
// 仅 boundCtx（attempt 级）到期归 attempt；非超时失败归 none
func timeoutKindOf(parentCtx, boundCtx context.Context, err error) string {
	if err == nil {
		return timeoutKindNone
	}
	if errors.Is(parentCtx.Err(), context.DeadlineExceeded) {
		return timeoutKindRequest
	}
	if errors.Is(boundCtx.Err(), context.DeadlineExceeded) {
		return timeoutKindAttempt
	}
	return timeoutKindNone
}

// statusCodeOf 从结构化错误中提取上游状态码；非 HTTP 错误视为 502
func statusCodeOf(err error) int {
	var upstreamErr *provider.UpstreamError
	if errors.As(err, &upstreamErr) && upstreamErr.StatusCode > 0 {
		return upstreamErr.StatusCode
	}
	return http.StatusBadGateway
}

// requestFor 复制请求并把 model 替换成渠道的上游模型名
func requestFor(req *provider.ChatRequest, providerModel string) *provider.ChatRequest {
	creq := *req
	creq.Model = providerModel
	return &creq
}

// breakerKey 熔断 key = provider type + base_url + channel_id（阶段 8 细化）
// abilityID = 0（models 表 fallback）时不追加 channel_id，保持原粒度
func breakerKey(providerType string, p provider.Provider, abilityID int64) string {
	baseURL := ""
	if e, ok := p.(provider.Endpoint); ok {
		baseURL = e.BaseURL()
	}
	key := circuitbreaker.Key(providerType, baseURL)
	if abilityID > 0 {
		key = fmt.Sprintf("%s:%d", key, abilityID)
	}
	return key
}

// getProvider 渠道级 Provider 获取：优先查多 key 池（channel_keys），fallback 到 Registry.Get
//
// 向后兼容：attempt.ID = 0（models 表 fallback 渠道）或 channel_keys 表为空/未迁移，
// 直接走 Registry.Get 按 provider type 创建单 key Provider 实例。
func (r *Relay) getProvider(attempt routing.Ability) (provider.Provider, error) {
	if r.providers == nil {
		return nil, fmt.Errorf("provider registry not initialized")
	}
	if p, ok := r.providers.GetForChannel(attempt.ID); ok {
		return p, nil
	}
	return r.providers.Get(attempt.Provider)
}

// recordFailure 按错误归类更新渠道健康状态
//   - KindAuth（401/403/404：鉴权失败、上游模型不存在）→ 永久禁用
//   - 其余可重试归类（429/5xx/超时/网络错误等瞬态失败）→ 累计连续失败，达到阈值后冷却
//   - 非 UpstreamError（本地错误、熔断器自身错误）→ 不计入渠道健康
func (r *Relay) recordFailure(abilityID int64, err error) {
	if abilityID <= 0 || r.router == nil {
		return
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		return
	}
	if upstreamErr.Permanent() {
		r.router.AsyncDisableChannel(abilityID, upstreamErr.Error())
		return
	}
	r.router.AsyncRecordFailure(abilityID, upstreamErr.Error())
}

// recordSuccess 成功即清零连续失败计数
func (r *Relay) recordSuccess(abilityID int64) {
	if r.router != nil {
		r.router.AsyncRecordSuccess(abilityID)
	}
}
