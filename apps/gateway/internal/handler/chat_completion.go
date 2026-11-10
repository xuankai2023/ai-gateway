// Package handler AI 请求处理器
//
// ChatCompletion 链路（改造后：abilities 优先 + retry 循环 + models 表 fallback）：
//
//	gin 路由 → Auth 中间件塞 tenant_id → Handler 解析请求
//	→ resolveAbilitiesDB（倒排索引）
//	    找到渠道 → retry 循环（retry 复用 = 换渠道 + 降级）
//	        每轮 retry 同时是优先级下标 → tiers[retry]
//	        WeightedPick 选渠道 → Factory.Get → 调上游
//	        失败 → DecideRelayRetry + AsyncDisableChannel → 下一轮
//	    全失败 → 返回最后一次错误
//	→ 找不到渠道 → resolveModel（正排 models 表 fallback，保证 P0 向后兼容）
//	→ 非流式：ChatCompletion → 算 cost → 写 usage_logs → 返回
//	→ 流式：ChatCompletionStream → 逐帧 SSE 写客户端 → 累积 usage → 算 cost → 写 usage_logs
package handler

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"ai-gateway/apps/gateway/internal/middleware"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/db"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/provider"
	"github.com/gin-gonic/gin"
)

const (
	// maxRetryPerRequest 单次请求最大重试次数（= abilities 层数，try all tiers）
	maxRetryPerRequest = 4
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

// modelRoute 模型路由解析结果（fallback 用：models 表单渠道）
type modelRoute struct {
	providerType  string
	providerModel string
	inputPrice1k  float64
	outputPrice1k float64
}

// ---- 测试注入点 ----

var resolveModelFn = resolveModel

// ---- Handler 入口 ----

// ChatCompletion Handler 入口（改造后：abilities 优先 + retry 循环 + models 表 fallback）
func ChatCompletion() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// 1. auth 检查
		raw, exists := c.Get("tenant_id")
		tenantID, ok := raw.(int64)
		if !exists || !ok {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{"message": "unauthorized", "type": "authentication_error"},
			})
			return
		}
		raw, exists = c.Get("api_key_id")
		apiKeyID, ok := raw.(int64)
		if !exists || !ok {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{"message": "unauthorized", "type": "authentication_error"},
			})
			return
		}

		// 2. 解析请求体
		var req chatCompletionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"message": "invalid request body: " + err.Error(), "type": "invalid_request_error"},
			})
			return
		}

		// 3. abilities 倒排索引优先查询
		tiers, abilitiesFound, err := routing.ResolveAbilities(c.Request.Context(), tenantID, req.Model)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"message": "routing error: " + err.Error(), "type": "internal_error"},
			})
			return
		}

		if abilitiesFound && len(tiers) > 0 {
			// ✅ 有 abilities：走 retry 循环
			if req.Stream {
				handleStreamWithRetry(c, tiers, req, tenantID, apiKeyID, start)
			} else {
				handleNonStreamWithRetry(c, tiers, req, tenantID, apiKeyID, start)
			}
			return
		}

		// 🔄 fallback：abilities 没数据（或全禁用）→ 老的 models 表单渠道
		route, err := resolveModelFn(c.Request.Context(), tenantID, req.Model)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, ErrModelNotFound) {
				status = http.StatusNotFound
			}
			c.JSON(status, gin.H{
				"error": gin.H{"message": err.Error(), "type": "model_error"},
			})
			return
		}

		p, err := provider.Factory.Get(route.providerType)
		if err != nil {
			logAndRecord(c, tenantID, apiKeyID, req.Model, route.providerType,
				0, 0, 0, "error", "provider not registered: "+route.providerType, start)
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"message": "provider not registered: " + route.providerType, "type": "config_error"},
			})
			return
		}

		creq := buildChatRequest(route.providerModel, req)

		if req.Stream {
			handleStream(c, p, creq, tenantID, apiKeyID, req.Model, route.providerType,
				route.inputPrice1k, route.outputPrice1k, start)
		} else {
			handleNonStream(c, p, creq, tenantID, apiKeyID, req.Model, route.providerType,
				route.inputPrice1k, route.outputPrice1k, start)
		}
	}
}

// buildChatRequest 构造平台中立请求
func buildChatRequest(providerModel string, req chatCompletionRequest) *provider.ChatRequest {
	creq := &provider.ChatRequest{
		Model: providerModel,
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

// --- retry 循环（非流式） ---

func handleNonStreamWithRetry(c *gin.Context, tiers []routing.PriorityTier,
	req chatCompletionRequest, tenantID, apiKeyID int64, start time.Time) {

	maxRetry := min(maxRetryPerRequest, len(tiers))
	var lastErr error
	var lastAbility *routing.Ability

	for retry := 0; retry < maxRetry; retry++ {
		tier := tiers[retry]
		picked := routing.WeightedPick(tier.Channels)
		if picked == nil {
			continue // 这层没可用渠道，跳过
		}

		lastAbility = picked
		p, err := provider.Factory.Get(picked.Provider)
		if err != nil {
			lastErr = err
			routing.AsyncDisableChannel(picked.ID)
			continue
		}

		// 补充 span 属性：租户 + 模型 + 选中的渠道
		middleware.AddRequestAttrs(c, tenantID, req.Model, picked.Provider)

		creq := buildChatRequest(picked.ProviderModel, req)

		// 🔴 CircuitBreaker：用 circuitbreaker.Call 包裹上游调用
		// gobreaker 自动根据 callErr 是否为 nil 来更新熔断计数器
		result, callErr := circuitbreaker.Call(picked.Provider, func() (interface{}, error) {
			return p.ChatCompletion(c.Request.Context(), creq)
		})

		if callErr == nil {
			// ✅ 成功：熔断器自动重置计数 + 用本渠道自己的价格算 cost
			resp := result.(*provider.ChatResponse)
			cost := calcCost(resp.InputTokens, resp.OutputTokens, picked.InputPrice1k, picked.OutputPrice1k)
			logAndRecord(c, tenantID, apiKeyID, req.Model, picked.Provider,
				resp.InputTokens, resp.OutputTokens, cost, "success", "", start)
			c.JSON(http.StatusOK, openaiCompletionResponse(resp, req.Model))
			return
		}

		// ❌ 失败：熔断器自动 +1 + 异步禁用渠道
		lastErr = callErr
		routing.AsyncDisableChannel(picked.ID)

		decision := routing.DecideRelayRetry(callErr, retry, maxRetry)
		if decision == routing.RetryStop {
			break // 不可重试，直接退出循环返回错误
		}
		// RetryRetry → 自然进入下一轮（tiers[retry+1] 更低优先级）
	}

	// 所有层都失败
	providerType := "unknown"
	if lastAbility != nil {
		providerType = lastAbility.Provider
	}
	logAndRecord(c, tenantID, apiKeyID, req.Model, providerType,
		0, 0, 0, "error", fmt.Sprintf("all %d tiers exhausted: %v", maxRetry, lastErr), start)
	c.JSON(http.StatusBadGateway, gin.H{
		"error": gin.H{"message": "all upstream providers exhausted", "type": "upstream_error"},
	})
}

// --- retry 循环（流式） ---

// 流式 retry 规则：只尝试"上游初始连接阶段"的重试。
// HTTP 200 + SSE headers 已经发出后，不再 retry（客户端已经看到流式了，不能回头）。
// 如果上游在初始连接阶段（还没返回第一个 chunk）就报错 → 立刻切下一个渠道。

func handleStreamWithRetry(c *gin.Context, tiers []routing.PriorityTier,
	req chatCompletionRequest, tenantID, apiKeyID int64, start time.Time) {

	maxRetry := min(maxRetryPerRequest, len(tiers))
	var lastErr error
	var lastAbility *routing.Ability

	for retry := 0; retry < maxRetry; retry++ {
		tier := tiers[retry]
		picked := routing.WeightedPick(tier.Channels)
		if picked == nil {
			continue
		}

		lastAbility = picked
		p, err := provider.Factory.Get(picked.Provider)
		if err != nil {
			lastErr = err
			routing.AsyncDisableChannel(picked.ID)
			continue
		}

		streamer, ok := p.(provider.Streamer)
		if !ok {
			routing.AsyncDisableChannel(picked.ID)
			continue // 这个 provider 不支持流式，跳过
		}

		creq := buildChatRequest(picked.ProviderModel, req)
		chunkChan, errChan := streamer.ChatCompletionStream(c.Request.Context(), creq)

		// 尝试读取第一个 chunk 来判断上游连接是否成功
		select {
		case firstChunk, ok := <-chunkChan:
			if !ok {
				// 上游直接失败
				streamErr, _ := <-errChan
				if streamErr == nil {
					streamErr = errors.New("stream closed without first chunk")
				}
				lastErr = streamErr
				routing.AsyncDisableChannel(picked.ID)
				decision := routing.DecideRelayRetry(streamErr, retry, maxRetry)
				if decision == routing.RetryStop {
					goto STREAM_FAIL
				}
				continue // try next tier
			}
			// 🔑 上游连接成功，开始 SSE 转发——不再 retry
			handleStreamConnected(c, chunkChan, errChan, firstChunk,
				picked, req, tenantID, apiKeyID, start)
			return

		case streamErr, ok := <-errChan:
			if ok && streamErr != nil {
				lastErr = streamErr
				routing.AsyncDisableChannel(picked.ID)
				decision := routing.DecideRelayRetry(streamErr, retry, maxRetry)
				if decision == routing.RetryStop {
					goto STREAM_FAIL
				}
				continue // try next tier
			}

		case <-c.Request.Context().Done():
			goto STREAM_FAIL
		}
	}

STREAM_FAIL:
	providerType := "unknown"
	if lastAbility != nil {
		providerType = lastAbility.Provider
	}
	logAndRecord(c, tenantID, apiKeyID, req.Model, providerType,
		0, 0, 0, "error", fmt.Sprintf("all %d tiers exhausted: %v", maxRetry, lastErr), start)
	c.JSON(http.StatusBadGateway, gin.H{
		"error": gin.H{"message": "all upstream providers exhausted", "type": "upstream_error"},
	})
}

// handleStreamConnected 上游连接成功后的 SSE 转发（不会 retry）
func handleStreamConnected(c *gin.Context, chunkChan <-chan provider.StreamChunk,
	errChan <-chan error, firstChunk provider.StreamChunk,
	picked *routing.Ability, req chatCompletionRequest,
	tenantID, apiKeyID int64, start time.Time) {

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{"message": "streaming not supported", "type": "internal_error"},
		})
		return
	}

	respID := "chatcmpl-" + time.Now().Format("20060102150405")
	created := time.Now().Unix()

	// 发第一个 chunk
	fmt.Fprintf(c.Writer, "data: %s\n\n", buildStreamFrame(respID, created, req.Model, firstChunk))
	flusher.Flush()

	var finalInputTokens, finalOutputTokens int64
	var streamErr error
	var doneSent bool

	if firstChunk.IsDone {
		finalInputTokens = firstChunk.InputTokens
		finalOutputTokens = firstChunk.OutputTokens
		goto STREAM_END
	}

	for {
		select {
		case chunk, ok := <-chunkChan:
			if !ok {
				goto STREAM_END
			}
			if chunk.IsDone {
				finalInputTokens = chunk.InputTokens
				finalOutputTokens = chunk.OutputTokens
			}
			fmt.Fprintf(c.Writer, "data: %s\n\n", buildStreamFrame(respID, created, req.Model, chunk))
			flusher.Flush()

		case err, ok := <-errChan:
			if ok && err != nil {
				streamErr = err
				fmt.Fprintf(c.Writer, "data: %s\n\n", `{"error":{"message":"upstream provider error","type":"upstream_error"}}`)
				fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
				flusher.Flush()
				doneSent = true
			}
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
	cost := calcCost(finalInputTokens, finalOutputTokens, picked.InputPrice1k, picked.OutputPrice1k)
	logAndRecord(c, tenantID, apiKeyID, req.Model, picked.Provider,
		finalInputTokens, finalOutputTokens, cost, status, errMsg, start)
}

// --- fallback handlers（models 表单渠道，单调用） ---

// handleNonStream 非流式处理（fallback：models 表单渠道）
func handleNonStream(c *gin.Context, p provider.Provider, creq *provider.ChatRequest,
	tenantID, apiKeyID int64, modelAlias, providerType string,
	inputPrice1k, outputPrice1k float64, start time.Time) {

	resp, err := p.ChatCompletion(c.Request.Context(), creq)
	if err != nil {
		logAndRecord(c, tenantID, apiKeyID, modelAlias, providerType,
			0, 0, 0, "error", err.Error(), start)
		c.JSON(http.StatusBadGateway, gin.H{
			"error": gin.H{"message": "upstream provider error", "type": "upstream_error"},
		})
		return
	}

	cost := calcCost(resp.InputTokens, resp.OutputTokens, inputPrice1k, outputPrice1k)
	logAndRecord(c, tenantID, apiKeyID, modelAlias, providerType,
		resp.InputTokens, resp.OutputTokens, cost, "success", "", start)
	c.JSON(http.StatusOK, openaiCompletionResponse(resp, modelAlias))
}

// handleStream 流式处理（fallback：models 表单渠道）
func handleStream(c *gin.Context, p provider.Provider, creq *provider.ChatRequest,
	tenantID, apiKeyID int64, modelAlias, providerType string,
	inputPrice1k, outputPrice1k float64, start time.Time) {

	streamer, ok := p.(provider.Streamer)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{"message": "provider does not support streaming", "type": "not_supported_error"},
		})
		return
	}

	chunkChan, errChan := streamer.ChatCompletionStream(c.Request.Context(), creq)

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{"message": "streaming not supported", "type": "internal_error"},
		})
		return
	}

	var finalInputTokens, finalOutputTokens int64
	var streamErr error
	var doneSent bool
	respID := "chatcmpl-" + time.Now().Format("20060102150405")
	created := time.Now().Unix()

	for {
		select {
		case chunk, ok := <-chunkChan:
			if !ok {
				goto FINISH
			}
			if chunk.IsDone {
				finalInputTokens = chunk.InputTokens
				finalOutputTokens = chunk.OutputTokens
			}
			frame := buildStreamFrame(respID, created, modelAlias, chunk)
			fmt.Fprintf(c.Writer, "data: %s\n\n", frame)
			flusher.Flush()

		case err, ok := <-errChan:
			if ok && err != nil {
				streamErr = err
				fmt.Fprintf(c.Writer, "data: %s\n\n", `{"error":{"message":"upstream provider error","type":"upstream_error"}}`)
				fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
				flusher.Flush()
				doneSent = true
			}
			goto FINISH

		case <-c.Request.Context().Done():
			goto FINISH
		}
	}

FINISH:
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
	cost := calcCost(finalInputTokens, finalOutputTokens, inputPrice1k, outputPrice1k)
	logAndRecord(c, tenantID, apiKeyID, modelAlias, providerType,
		finalInputTokens, finalOutputTokens, cost, status, errMsg, start)
}

// --- Routing wrapper + DB 操作 ---

// ResolveAbilitiesSafe 路由层到 handler 的桥接
func ResolveAbilitiesSafe(ctx context.Context, tenantID int64, modelName string) ([]routing.PriorityTier, bool, error) {
	return routing.ResolveAbilities(ctx, tenantID, modelName)
}
var ErrModelNotFound = errors.New("model not found for this tenant")

// resolveModel 查 models 表（fallback）
func resolveModel(ctx context.Context, tenantID int64, modelName string) (*modelRoute, error) {
	pool := db.GetPool()
	if pool == nil {
		return nil, errors.New("db pool not initialized")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	route := &modelRoute{}
	err := pool.QueryRowContext(queryCtx, `
		SELECT provider, provider_model, input_price_1k, output_price_1k
		FROM models
		WHERE tenant_id = ? AND model_name = ? AND is_active = 1
	`, tenantID, modelName).Scan(&route.providerType, &route.providerModel,
		&route.inputPrice1k, &route.outputPrice1k)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrModelNotFound
		}
		return nil, fmt.Errorf("model resolve failed: %w", err)
	}
	return route, nil
}

// --- 计费 / 日志 ---

// calcCost 计费公式
func calcCost(inputTokens, outputTokens int64, inputPrice1k, outputPrice1k float64) float64 {
	inputCost := float64(inputTokens) * inputPrice1k / 1000.0
	outputCost := float64(outputTokens) * outputPrice1k / 1000.0
	return inputCost + outputCost
}

// logAndRecord 同步写 usage_logs + 更新 Prometheus 指标
func logAndRecord(c *gin.Context, tenantID, apiKeyID int64, model, providerType string,
	inputTokens, outputTokens int64, cost float64, status, errorMessage string, start time.Time) {

	pool := db.GetPool()
	if pool != nil {
		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		requestID := newUUID()
		latencyMs := time.Since(start).Milliseconds()

		_, _ = pool.ExecContext(queryCtx, `
			INSERT INTO usage_logs
				(request_id, tenant_id, api_key_id, model, provider, input_tokens, output_tokens, cost, latency_ms, status, error_message)
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, requestID, tenantID, apiKeyID, model, providerType,
			inputTokens, outputTokens, cost, latencyMs, status, errorMessage)
	}

	// Prometheus 指标
	observability.RecordRequest(
		fmt.Sprintf("%d", tenantID),
		model, providerType, status,
		time.Since(start).Milliseconds(), cost,
	)
}

// --- 响应构造 ---

func openaiCompletionResponse(resp *provider.ChatResponse, model string) gin.H {
	return gin.H{
		"id":      "chatcmpl-" + time.Now().Format("20060102150405"),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []gin.H{
			{
				"index": 0,
				"message": gin.H{
					"role":    "assistant",
					"content": resp.Content,
				},
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

func buildStreamFrame(id string, created int64, model string, chunk provider.StreamChunk) string {
	var finishReason *string
	if chunk.IsDone {
		s := "stop"
		finishReason = &s
	}

	frame := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"delta": map[string]interface{}{
					"content": chunk.Content,
				},
				"finish_reason": finishReason,
			},
		},
	}

	if chunk.IsDone && chunk.InputTokens > 0 {
		frame["usage"] = map[string]interface{}{
			"prompt_tokens":      chunk.InputTokens,
			"completion_tokens":  chunk.OutputTokens,
			"total_tokens":       chunk.InputTokens + chunk.OutputTokens,
		}
	}

	b, _ := json.Marshal(frame)
	return string(b)
}

// newUUID 生成 v4 风格 UUID
func newUUID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return hex.EncodeToString(buf[:4]) + "-" +
		hex.EncodeToString(buf[4:6]) + "-" +
		hex.EncodeToString(buf[6:8]) + "-" +
		hex.EncodeToString(buf[8:10]) + "-" +
		hex.EncodeToString(buf[10:])
}
