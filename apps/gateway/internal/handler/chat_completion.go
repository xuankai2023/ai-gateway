// Package handler AI 请求处理器
//
// ChatCompletion 链路：
//
//	gin 路由 → Auth 中间件塞 tenant_id → Handler 解析请求
//	→ App.Resolver.ResolveAttempts（abilities 倒排索引优先，models 表兜底）
//	→ App.Relay.NonStream / App.Relay.Stream 逐渠道尝试（熔断 + 重试决策在 relay 内）
//	→ Handler 只做响应编解码、usage_logs 与指标输出
//
// 阶段 14：handler 改为 func(app *app.App) gin.HandlerFunc 形式，
// 路由解析接口注入（测试可替换 fake），不再引用任何包级全局态。
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/diagnostics"
	"ai-gateway/apps/gateway/internal/middleware"
	"ai-gateway/apps/gateway/internal/rag"
	"ai-gateway/apps/gateway/internal/relay"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/apps/gateway/internal/usagelog"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/pricing"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/requestid"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// chatCompletionRequest OpenAI 兼容请求体
type chatCompletionRequest struct {
	Model       string    `json:"model" binding:"required"`
	Messages    []message `json:"messages" binding:"required,min=1"`
	Temperature *float32  `json:"temperature,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	Stream      bool      `json:"stream,omitempty"`
}

type message struct {
	Role    string `json:"role" binding:"required"`
	Content string `json:"content" binding:"required"`
}

// defaultStreamMaxDuration 流式转发总时长兜底默认值（与 pkg/config 默认一致）
const defaultStreamMaxDuration = 300 * time.Second

// 请求校验硬上限（阶段 5）：非法请求在 relay 之前早拦，不消耗渠道与上游配额。
// 总请求体尺寸由全局 bodyLimit（MaxBytesReader）统一兜底，此处不重复检查。
const (
	maxMessages    = 100       // messages 条数上限
	maxContentSize = 512 << 10 // 单条 content ≤ 512KB
)

// modelNamePattern 模型名白名单字符集（1~128 字符）
var modelNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._:/-]{1,128}$`)

// validationError 请求校验错误（code 对外暴露，msg 进 error.message）
type validationError struct {
	code string
	msg  string
}

func (e *validationError) Error() string { return e.msg }

// validateChatRequest 请求体加固校验；通过返回 nil
func validateChatRequest(req *chatCompletionRequest) *validationError {
	if !modelNamePattern.MatchString(req.Model) {
		return &validationError{"invalid_model", "invalid model name"}
	}
	if len(req.Messages) > maxMessages {
		return &validationError{"too_many_messages",
			fmt.Sprintf("too many messages: %d (max %d)", len(req.Messages), maxMessages)}
	}
	for i := range req.Messages {
		if len(req.Messages[i].Content) > maxContentSize {
			return &validationError{"content_too_large",
				fmt.Sprintf("messages[%d].content exceeds %d bytes", i, maxContentSize)}
		}
	}
	return nil
}

// requestScope 一次请求的上下文（收敛成结构体，避免长参数列表在收尾函数间层层透传）
type requestScope struct {
	TenantID       int64
	APIKeyID       int64
	ProjectID      int64
	AssistantID    int64
	Model          string // 客户端请求模型（响应回填用）
	RouteModel     string // 实际路由模型（灰度 / 渠道映射）
	RetrievedCount int
	RetrievalMs    int64
	Citations      []rag.Citation
}

// ChatCompletion Handler 入口（阶段 14：func(app) 形式）
// 阶段 15：maxAttempts / streamMaxDuration 每请求读取 a.Hot 热更新值（不重进程生效）；
// a.Hot 缺失或零值时回落静态配置，streamMaxDuration 再回落内置兜底
func ChatCompletion(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		maxAttempts := 0
		streamMaxDuration := time.Duration(0)
		if a.Hot != nil {
			maxAttempts = a.Hot.MaxAttempts()
			streamMaxDuration = a.Hot.StreamMaxDuration()
		}
		if maxAttempts <= 0 && a.Config != nil {
			maxAttempts = a.Config.Gateway.MaxAttempts
		}
		if streamMaxDuration <= 0 && a.Config != nil {
			_, _, streamMaxDuration = a.Config.Gateway.TimeoutDurations()
		}
		if streamMaxDuration <= 0 {
			streamMaxDuration = defaultStreamMaxDuration
		}
		start := time.Now()

		// 1. auth 检查
		raw, exists := c.Get("tenant_id")
		tenantID, ok := raw.(int64)
		if !exists || !ok {
			apperr.Write(c, http.StatusUnauthorized, "authentication_error", "unauthorized", "unauthorized")
			return
		}
		raw, exists = c.Get("api_key_id")
		apiKeyID, ok := raw.(int64)
		if !exists || !ok {
			apperr.Write(c, http.StatusUnauthorized, "authentication_error", "unauthorized", "unauthorized")
			return
		}

		// 2. 解析请求体
		var req chatCompletionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			apperr.Write(c, http.StatusBadRequest, "invalid_request_error", "invalid_request_body", "invalid request body: "+err.Error())
			return
		}

		// 2.1 请求校验加固（条数 / 单条尺寸 / 模型名字符集）
		if verr := validateChatRequest(&req); verr != nil {
			apperr.Write(c, http.StatusBadRequest, "invalid_request_error", verr.code, verr.msg)
			return
		}

		// 3. 助手解析（对外模型名 → 项目）：命中后强制校验 API Key 所属项目
		//    项目归属只信认证中间件注入的 project_id，绝不从请求体读取
		var asst *rag.Assistant
		if a.RAG != nil {
			resolved, err := a.RAG.ResolveAssistant(requestContext(c), tenantID, req.Model)
			switch {
			case err == nil:
				asst = resolved
			case errors.Is(err, rag.ErrAssistantNotFound):
				// 普通模型：继续走现有渠道路由
			case errors.Is(err, rag.ErrProjectInactive):
				apperr.Write(c, http.StatusForbidden, "permission_error", "project_disabled", "project disabled")
				return
			default:
				// 检索配置异常不阻断对话：降级为普通模型
				observability.L().Warn("rag: resolve assistant failed, fallback to plain model",
					zap.String("model", req.Model), zap.Error(err))
			}
		}
		projectID := c.GetInt64("project_id")
		if asst != nil {
			if projectID <= 0 {
				apperr.Write(c, http.StatusForbidden, "permission_error", "project_required",
					fmt.Sprintf("model %q requires an API key bound to a project", req.Model))
				return
			}
			if projectID != asst.ProjectID {
				apperr.Write(c, http.StatusForbidden, "permission_error", "project_scope_mismatch",
					fmt.Sprintf("model %q belongs to another project", req.Model))
				return
			}
		}

		// 4. 解析候选计划（灰度中间件命中时按 target_model 路由，响应仍回填客户端请求模型）
		routeModel := req.Model
		if v, ok := c.Get("target_model"); ok {
			if tm, ok := v.(string); ok && tm != "" {
				routeModel = tm
			}
		}
		attempts, err := a.Resolver.ResolveAttempts(requestContext(c), tenantID, routeModel, maxAttempts)
		if err != nil {
			status := http.StatusInternalServerError
			errType := "internal_error"
			code := "routing_error"
			switch {
			case errors.Is(err, relay.ErrModelNotFound):
				status = http.StatusNotFound
				errType = "model_error"
				code = "model_not_found"
			case errors.Is(err, routing.ErrNoAvailableChannel):
				status = http.StatusBadGateway
				errType = "upstream_error"
				code = "no_available_channel"
			}
			apperr.Write(c, status, errType, code, err.Error())
			return
		}

		// 5. RAG 检索拼装（命中助手时）：失败 / 超时降级为无资料回答，不影响 relay
		scope := requestScope{
			TenantID: tenantID, APIKeyID: apiKeyID, ProjectID: projectID,
			Model: req.Model, RouteModel: routeModel,
		}
		creq := buildChatRequest(req)
		if asst != nil {
			scope.AssistantID = asst.ID
			rc, ragErr := a.RAG.Assemble(requestContext(c), asst, creq.Messages)
			if ragErr != nil {
				observability.L().Warn("rag: assemble failed, fallback to no-context",
					zap.Int64("project_id", asst.ProjectID), zap.Error(ragErr))
			}
			if rc != nil {
				creq.Messages = rc.Messages
				scope.Citations = rc.Citations
				scope.RetrievedCount = rc.RetrievedCount
				scope.RetrievalMs = rc.Elapsed.Milliseconds()
			}
		}

		// 6. 非流式 / 流式统一走 relay
		if req.Stream {
			runStream(c, a, attempts, creq, scope, start, streamMaxDuration)
		} else {
			runNonStream(c, a, attempts, creq, scope, start)
		}
	}
}

// requestContext 取中间件注入的 otel_ctx（span 上下文），缺失时退回请求原始 context
func requestContext(c *gin.Context) context.Context {
	if v, ok := c.Get("otel_ctx"); ok {
		if ctx, ok := v.(context.Context); ok {
			return ctx
		}
	}
	return c.Request.Context()
}

// buildChatRequest 构造平台中立请求（model 由 relay 按渠道替换）
func buildChatRequest(req chatCompletionRequest) *provider.ChatRequest {
	creq := &provider.ChatRequest{
		Model: req.Model,
	}
	for _, m := range req.Messages {
		creq.Messages = append(creq.Messages, provider.Message{Role: m.Role, Content: m.Content})
	}
	if req.Temperature != nil {
		creq.Temperature = *req.Temperature
	}
	if req.MaxTokens != nil {
		creq.MaxTokens = *req.MaxTokens
	}
	return creq
}

// runNonStream 非流式调用 + 结果输出
func runNonStream(c *gin.Context, a *app.App, attempts []routing.Ability,
	creq *provider.ChatRequest, scope requestScope, start time.Time) {

	res := a.Relay.NonStream(requestContext(c), attempts, creq)
	if res.Err != nil {
		logExhausted(c, a, scope, res.Attempt.Provider, res.Attempts, res.Err, start)
		recordDiagnostics(c, a, scope, false, creq, res, exhaustedStatus(res.Err), res.Err, 0, start)
		return
	}

	attempt, resp := res.Attempt, res.Response
	middleware.AddRequestAttrs(c, scope.TenantID, scope.Model, attempt.Provider)

	cost, priceVersion := settleCost(c, a, scope.TenantID, scope.RouteModel, attempt, resp.InputTokens, resp.OutputTokens)
	c.Set("ledger_final_cost", int64(cost))
	logAndRecord(c, a, scope, attempt.Provider,
		resp.InputTokens, resp.OutputTokens, cost, priceVersion, "success", "", start)
	recordDiagnostics(c, a, scope, false, creq, res, http.StatusOK, nil, int64(cost), start)
	c.JSON(http.StatusOK, openaiCompletionResponse(resp, scope.Model, scope.Citations))
}

// runStream 流式调用 + SSE 输出
func runStream(c *gin.Context, a *app.App, attempts []routing.Ability,
	creq *provider.ChatRequest, scope requestScope, start time.Time, streamMaxDuration time.Duration) {

	res := a.Relay.Stream(requestContext(c), attempts, creq)
	if res.Err != nil {
		if errors.Is(res.Err, context.Canceled) {
			return // 客户端已断开，静默退出
		}
		logExhausted(c, a, scope, res.Attempt.Provider, res.Attempts, res.Err, start)
		recordDiagnostics(c, a, scope, true, creq, res, exhaustedStatus(res.Err), res.Err, 0, start)
		return
	}
	defer res.Close()

	middleware.AddRequestAttrs(c, scope.TenantID, scope.Model, res.Attempt.Provider)
	handleStreamConnected(c, a, creq, res, scope, start, streamMaxDuration)
}

// handleStreamConnected 上游连接成功后的 SSE 转发
// streamMaxDuration 是转发总时长兜底：超时按流式错误收尾（错误帧 + [DONE]），
// 防上游半死连接把转发 goroutine 与连接永久挂住
func handleStreamConnected(c *gin.Context, a *app.App, creq *provider.ChatRequest,
	res *relay.Result, scope requestScope, start time.Time, streamMaxDuration time.Duration) {

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		apperr.Write(c, http.StatusInternalServerError, "internal_error", "stream_unsupported", "streaming not supported")
		return
	}

	respID := "chatcmpl-" + time.Now().Format("20060102150405")
	created := time.Now().Unix()

	// 发第一个 chunk
	fmt.Fprintf(c.Writer, "data: %s\n\n", buildStreamFrame(respID, created, scope.Model, res.FirstChunk, scope.Citations))
	flusher.Flush()

	var finalInputTokens, finalOutputTokens int64
	var streamErr error
	var doneSent bool

	// 转发总时长兜底：中间件的 stream_max_duration ctx 是第一道防线，
	// 本定时器从转发起算独立计时，双重保险防挂死
	maxTimer := time.NewTimer(streamMaxDuration)
	defer maxTimer.Stop()

	if res.FirstChunk.IsDone {
		finalInputTokens = res.FirstChunk.InputTokens
		finalOutputTokens = res.FirstChunk.OutputTokens
		goto STREAM_END
	}

	for {
		select {
		case chunk, ok := <-res.Chunks:
			if !ok {
				goto STREAM_END
			}
			if chunk.IsDone {
				finalInputTokens = chunk.InputTokens
				finalOutputTokens = chunk.OutputTokens
			}
			fmt.Fprintf(c.Writer, "data: %s\n\n", buildStreamFrame(respID, created, scope.Model, chunk, scope.Citations))
			flusher.Flush()

		case err, ok := <-res.Errs:
			if ok && err != nil {
				streamErr = err
				errBody, _ := json.Marshal(apperr.Body(c, "upstream_error", "stream_upstream_error", "upstream provider error"))
				fmt.Fprintf(c.Writer, "data: %s\n\n", errBody)
				fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
				flusher.Flush()
				doneSent = true
			}
			goto STREAM_END

		case <-maxTimer.C:
			streamErr = errStreamMaxDuration
			errBody, _ := json.Marshal(apperr.Body(c, "upstream_error", "stream_max_duration_exceeded", "stream max duration exceeded"))
			fmt.Fprintf(c.Writer, "data: %s\n\n", errBody)
			fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
			flusher.Flush()
			doneSent = true
			goto STREAM_END

		case <-c.Request.Context().Done():
			goto STREAM_END
		}
	}

STREAM_END:
	if !doneSent {
		fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
		flusher.Flush()
	}

	status := "success"
	errMsg := ""
	if streamErr != nil {
		status = "error"
		errMsg = streamErr.Error()
	}
	cost, priceVersion := settleCost(c, a, scope.TenantID, scope.RouteModel, res.Attempt, finalInputTokens, finalOutputTokens)
	c.Set("ledger_final_cost", int64(cost))
	logAndRecord(c, a, scope, res.Attempt.Provider,
		finalInputTokens, finalOutputTokens, cost, priceVersion, status, errMsg, start)
	// 诊断快照：流式在收尾时落库（status_code 恒 200——SSE 头已发出，错误经错误帧传递）
	recordDiagnostics(c, a, scope, true, creq, res, http.StatusOK, streamErr, int64(cost), start)
}

// errStreamMaxDuration 流式转发总时长兜底触发
var errStreamMaxDuration = errors.New("stream max duration exceeded")

// exhaustedStatus 全部渠道耗尽时的对外状态码（与 logExhausted 的映射一致）
func exhaustedStatus(lastErr error) int {
	var upstreamErr *provider.UpstreamError
	if errors.As(lastErr, &upstreamErr) && upstreamErr.Kind == provider.KindTimeout {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

// recordDiagnostics 请求级诊断包落库（采样 / 异步 / 脱敏在 Recorder 内完成）。
// trace_id 由 Trace 中间件注入；request_digest 只存请求体重序列化后的 BodyDigest
// 输出（len + sha256 前缀），不落明文。
func recordDiagnostics(c *gin.Context, a *app.App, scope requestScope, stream bool,
	creq *provider.ChatRequest, res *relay.Result, statusCode int, finalErr error,
	costMicro int64, start time.Time) {

	if a.Diagnostics == nil || res == nil {
		return
	}
	traceID := c.GetString("trace_id")
	if traceID == "" {
		return
	}

	digest := ""
	if body, err := json.Marshal(creq); err == nil {
		digest = observability.BodyDigest(body)
	}

	rec := diagnostics.Record{
		TraceID:       traceID,
		TenantID:      scope.TenantID,
		Model:         scope.Model,
		Stream:        stream,
		RequestDigest: digest,
		Attempts:      res.AttemptsTrace,
		StatusCode:    statusCode,
		TotalCost:     costMicro,
		DurationMs:    time.Since(start).Milliseconds(),
	}
	if finalErr != nil {
		var upstreamErr *provider.UpstreamError
		if errors.As(finalErr, &upstreamErr) {
			rec.ErrorType = string(upstreamErr.Kind)
		} else {
			rec.ErrorType = string(provider.KindUnknown)
		}
		rec.ErrorMessage = finalErr.Error()
	}
	a.Diagnostics.Record(rec)
}

// logExhausted 所有渠道都失败后的统一收尾：写 usage_logs + 返回 502/504
// 超时类失败（attempt/request 层超时切断）映射 504，其余上游失败映射 502
func logExhausted(c *gin.Context, a *app.App, scope requestScope, providerType string,
	attemptCount int, lastErr error, start time.Time) {
	if providerType == "" {
		providerType = "unknown"
	}
	logAndRecord(c, a, scope, providerType,
		0, 0, 0, 0, "error", fmt.Sprintf("all %d attempts exhausted: %v", attemptCount, lastErr), start)

	var upstreamErr *provider.UpstreamError
	if errors.As(lastErr, &upstreamErr) && upstreamErr.Kind == provider.KindTimeout {
		apperr.Write(c, http.StatusGatewayTimeout, "upstream_error", "upstream_timeout", "upstream providers timed out")
		return
	}
	apperr.Write(c, http.StatusBadGateway, "upstream_error", "upstream_exhausted", "all upstream providers exhausted")
}

// --- 计费 / 日志 ---

// settleCost 用定价引擎按上游真实 usage 结算（A8：全程使用同一价格版本快照）。
//
// models 表无记录（纯 abilities 渠道）时回退 abilities 上的线性单价。
func settleCost(c *gin.Context, a *app.App, tenantID int64, modelName string,
	attempt routing.Ability, inputTokens, outputTokens int64) (ledger.MicroUSD, int) {

	engine := a.Pricing
	if engine == nil {
		engine = pricing.Default
	}

	// A8：优先复用 Ledger 中间件在预占时写入的同一份价格快照，避免请求中途改价
	var row *pricing.ModelRow
	if v, ok := c.Get("ledger_pricing_row"); ok {
		row, _ = v.(*pricing.ModelRow)
	}
	if row == nil && a.PricingResolver != nil {
		if resolved, err := a.PricingResolver.ResolveModelPricing(requestContext(c), tenantID, modelName); err == nil {
			row = resolved
		}
	}
	if row == nil {
		row = &pricing.ModelRow{
			Model:         modelName,
			Provider:      attempt.Provider,
			PricingModel:  pricing.PriceLinear,
			InputPrice1k:  ledger.FromDollars(attempt.InputPrice1k),
			OutputPrice1k: ledger.FromDollars(attempt.OutputPrice1k),
			PriceVersion:  1,
		}
	}
	if row.PriceVersion <= 0 {
		row.PriceVersion = 1
	}

	cost, err := engine.Settle(row, &pricing.ActualUsage{
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
	})
	if err != nil {
		// 定价配置损坏时退回 abilities 线性单价，保证账单不因配置错误丢失
		cost = pricing.EstimateLinear(inputTokens, outputTokens, row.InputPrice1k, row.OutputPrice1k)
	}
	return cost, row.PriceVersion
}

// logAndRecord 投递用量日志（App 级异步批量写库，宁丢不堵响应路径）+ 更新 Prometheus 指标
func logAndRecord(c *gin.Context, a *app.App, scope requestScope, providerType string,
	inputTokens, outputTokens int64, cost ledger.MicroUSD, priceVersion int, status, errorMessage string, start time.Time) {

	// 从 context 读取账本中间件设置的预占金额（ReservedCost，μUSD）
	var reservedCostP *ledger.MicroUSD
	if v, ok := c.Get("ledger_reserved"); ok {
		if r, ok := v.(int64); ok {
			m := ledger.MicroUSD(r)
			reservedCostP = &m
		}
	}
	// FinalCost 就是 handler 本次计算出的 cost
	finalCost := cost
	finalCostP := &finalCost

	// request_id 优先取中间件预生成的，fallback 到本地生成
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = requestid.New()
	}

	if a.Usage != nil {
		a.Usage.Add(usagelog.Entry{
			RequestID:      requestID,
			TenantID:       scope.TenantID,
			APIKeyID:       scope.APIKeyID,
			ProjectID:      scope.ProjectID,
			AssistantID:    scope.AssistantID,
			Model:          scope.Model,
			Provider:       providerType,
			InputTokens:    inputTokens,
			OutputTokens:   outputTokens,
			Cost:           cost,
			ReservedCost:   reservedCostP,
			FinalCost:      finalCostP,
			PriceVersion:   priceVersion,
			LatencyMs:      time.Since(start).Milliseconds(),
			Status:         status,
			ErrorMessage:   errorMessage,
			RetrievedCount: scope.RetrievedCount,
			RetrievalMs:    int(scope.RetrievalMs),
		})
	}

	// Prometheus 指标
	observability.RecordRequest(
		fmt.Sprintf("%d", scope.TenantID),
		scope.Model, providerType, status,
		time.Since(start).Milliseconds(), cost.Dollars(),
	)
}

// --- 响应构造 ---

// openaiCompletionResponse 非流式响应；citations 非空时挂在 message 上（客户端可直接渲染引用）
func openaiCompletionResponse(resp *provider.ChatResponse, model string, citations []rag.Citation) gin.H {
	message := gin.H{
		"role":    "assistant",
		"content": resp.Content,
	}
	if len(citations) > 0 {
		message["citations"] = citations
	}
	return gin.H{
		"id":      "chatcmpl-" + time.Now().Format("20060102150405"),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []gin.H{
			{
				"index":         0,
				"message":       message,
				"finish_reason": "stop",
			},
		},
		"usage": gin.H{
			"prompt_tokens":     resp.InputTokens,
			"completion_tokens": resp.OutputTokens,
			"total_tokens":      resp.InputTokens + resp.OutputTokens,
		},
	}
}

func buildStreamFrame(id string, created int64, model string, chunk provider.StreamChunk, citations []rag.Citation) string {
	var finishReason *string
	if chunk.IsDone {
		s := "stop"
		finishReason = &s
	}

	choice := map[string]interface{}{
		"index": 0,
		"delta": map[string]interface{}{
			"content": chunk.Content,
		},
		"finish_reason": finishReason,
	}
	// 引用只在最后一个 finish_reason=stop 的 chunk 下发一次
	if chunk.IsDone && len(citations) > 0 {
		choice["citations"] = citations
	}

	frame := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []map[string]interface{}{choice},
	}

	if chunk.IsDone && chunk.InputTokens > 0 {
		frame["usage"] = map[string]interface{}{
			"prompt_tokens":     chunk.InputTokens,
			"completion_tokens": chunk.OutputTokens,
			"total_tokens":      chunk.InputTokens + chunk.OutputTokens,
		}
	}

	b, _ := json.Marshal(frame)
	return string(b)
}
