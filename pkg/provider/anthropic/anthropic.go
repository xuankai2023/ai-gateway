// Package anthropic Anthropic Claude Provider
//
// 使用官方 Messages API（POST /v1/messages），与 OpenAI 协议不同：
//  - Header 用 x-api-key + anthropic-version
//  - 请求体有 system 字段 / max_tokens 必填 / stream 布尔
//  - 响应体 content 是 block 数组（text / tool_use），usage 字段名 input_tokens / output_tokens
//  - 流式 SSE 事件类型：message_start → content_block_delta* → message_delta → message_stop
//
// 实现策略：参考 OpenAI Provider 的完整度——ChatCompletion + ChatCompletionStream + Name 三个完整实现；
// Embedding / Moderation / Cost 返回 ErrNotImplemented（与 OpenAI 保持一致的阶段取舍）。
package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"ai-gateway/pkg/provider"
)

// Provider Anthropic Claude 供应商
type Provider struct {
	baseURL   string
	apiKey    string
	client    *http.Client
	anthropicVersion string
}

// New 创建 Anthropic Provider
// baseURL 官方是 https://api.anthropic.com，也可以兼容代理端点
func New(baseURL, apiKey string) *Provider {
	return &Provider{
		baseURL:          strings.TrimRight(baseURL, "/"),
		apiKey:           apiKey,
		anthropicVersion: "2023-06-01",
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

// Name 供应商标识
func (p *Provider) Name() string {
	return "anthropic"
}

// --- ChatCompletion 非流式 ---

// anthropicMessage Claude Messages API 的单条消息
type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// anthropicRequest Claude Messages API 请求体
type anthropicRequest struct {
	Model       string             `json:"model"`
	Messages    []anthropicMessage `json:"messages"`
	System      string             `json:"system,omitempty"`
	Temperature *float32           `json:"temperature,omitempty"`
	MaxTokens   int                `json:"max_tokens"` // Claude 必填
	Stream      bool               `json:"stream"`
}

// anthropicContentBlock content 块（非流式）
type anthropicContentBlock struct {
	Type string `json:"type"` // "text" / "tool_use"
	Text string `json:"text"`
}

// anthropicUsage usage 统计
type anthropicUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// anthropicResponse Claude Messages API 响应体（非流式）
type anthropicResponse struct {
	ID           string                  `json:"id"`
	Type         string                  `json:"type"` // 应该是 "message"
	Role         string                  `json:"role"`
	Content      []anthropicContentBlock `json:"content"`
	Model        string                  `json:"model"`
	StopReason   string                  `json:"stop_reason"`
	Usage        anthropicUsage          `json:"usage"`
}

// anthropicErrorResponse 错误响应体
type anthropicErrorResponse struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// ChatCompletion 聊天补全（非流式）
func (p *Provider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	start := time.Now()

	// 1. 分离 system prompt：Claude API 把 system 从 messages 里拆出来
	var systemPrompt string
	var messages []anthropicMessage
	for _, m := range req.Messages {
		if m.Role == "system" {
			systemPrompt = m.Content
			continue
		}
		messages = append(messages, anthropicMessage{Role: m.Role, Content: m.Content})
	}

	body := anthropicRequest{
		Model:     req.Model,
		Messages:  messages,
		System:    systemPrompt,
		Stream:    false,
		MaxTokens: req.MaxTokens,
	}
	if body.MaxTokens <= 0 {
		body.MaxTokens = 1024 // Claude 必填，给个合理默认
	}
	if req.Temperature > 0 {
		t := req.Temperature
		body.Temperature = &t
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("anthropic: marshal request: %w", err)
	}

	// 2. 发起 HTTP 请求
	url := p.baseURL + "/v1/messages"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", p.anthropicVersion)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic: http call failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("anthropic: read response body: %w", err)
	}

	// 3. 错误处理
	if resp.StatusCode >= 400 {
		var errResp anthropicErrorResponse
		if e := json.Unmarshal(respBody, &errResp); e == nil && errResp.Error.Message != "" {
			return nil, fmt.Errorf("anthropic: upstream error [%s] %s", errResp.Error.Type, errResp.Error.Message)
		}
		return nil, fmt.Errorf("anthropic: upstream returned status %d: %s", resp.StatusCode, string(respBody))
	}

	// 4. 解析成功响应
	var parsed anthropicResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("anthropic: unmarshal response: %w", err)
	}

	// 拼接所有 text block（忽略 tool_use 等非 text 块）
	var content strings.Builder
	for _, block := range parsed.Content {
		if block.Type == "text" {
			content.WriteString(block.Text)
		}
	}

	result := &provider.ChatResponse{
		Content:      content.String(),
		InputTokens:  parsed.Usage.InputTokens,
		OutputTokens: parsed.Usage.OutputTokens,
		LatencyMs:    time.Since(start).Milliseconds(),
		Provider:     p.Name(),
	}
	return result, nil
}

// --- ChatCompletionStream 流式（实现 provider.Streamer 可选接口） ---

// anthropicSSEEvent Claude 流式 SSE 事件的通用 envelope
type anthropicSSEEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"-"` // 原始数据后续解析
}

// contentBlockDeltaEvent content_block_delta 事件
type contentBlockDeltaEvent struct {
	Type  string `json:"type"`  // "content_block_delta"
	Index int    `json:"index"`
	Delta struct {
		Type string `json:"type"` // "text_delta"
		Text string `json:"text"`
	} `json:"delta"`
}

// messageDeltaEvent message_delta 事件（含最终 usage）
type messageDeltaEvent struct {
	Type       string         `json:"type"` // "message_delta"
	Usage      anthropicUsage `json:"usage"`
	StopReason string         `json:"stop_reason"`
}

// messageStartEvent message_start 事件（含初始 usage）
type messageStartEvent struct {
	Type    string         `json:"type"` // "message_start"
	Message struct {
		Usage anthropicUsage `json:"usage"`
	} `json:"message"`
}

// ChatCompletionStream 流式聊天补全（SSE 逐帧推送）
func (p *Provider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (chunkChan <-chan provider.StreamChunk, errChan <-chan error) {
	chunks := make(chan provider.StreamChunk, 8)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		// 1. 构造流式请求体
		var systemPrompt string
		var messages []anthropicMessage
		for _, m := range req.Messages {
			if m.Role == "system" {
				systemPrompt = m.Content
				continue
			}
			messages = append(messages, anthropicMessage{Role: m.Role, Content: m.Content})
		}

		body := anthropicRequest{
			Model:     req.Model,
			Messages:  messages,
			System:    systemPrompt,
			Stream:    true,
			MaxTokens: req.MaxTokens,
		}
		if body.MaxTokens <= 0 {
			body.MaxTokens = 1024
		}
		if req.Temperature > 0 {
			t := req.Temperature
			body.Temperature = &t
		}

		payload, err := json.Marshal(body)
		if err != nil {
			errs <- fmt.Errorf("anthropic stream: marshal request: %w", err)
			return
		}

		// 2. 发起流式 HTTP 请求
		url := p.baseURL + "/v1/messages"
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			errs <- fmt.Errorf("anthropic stream: build request: %w", err)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("x-api-key", p.apiKey)
		httpReq.Header.Set("anthropic-version", p.anthropicVersion)
		httpReq.Header.Set("Accept", "text/event-stream")

		resp, err := p.client.Do(httpReq)
		if err != nil {
			errs <- fmt.Errorf("anthropic stream: http call failed: %w", err)
			return
		}
		defer resp.Body.Close()

		// 3. 上游错误处理
		if resp.StatusCode >= 400 {
			respBody, _ := io.ReadAll(resp.Body)
			var errResp anthropicErrorResponse
			if e := json.Unmarshal(respBody, &errResp); e == nil && errResp.Error.Message != "" {
				errs <- fmt.Errorf("anthropic stream: upstream error [%s] %s", errResp.Error.Type, errResp.Error.Message)
			} else {
				errs <- fmt.Errorf("anthropic stream: upstream returned status %d", resp.StatusCode)
			}
			return
		}

		// 4. 逐帧读取 SSE，累积 usage
		var finalUsage anthropicUsage
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

		// SSE 事件可能跨多行（data: 后面有 JSON），需要累积直到空行
		var eventBuffer bytes.Buffer

		for scanner.Scan() {
			line := scanner.Text()

			if line == "" {
				// 空行 = 事件分隔符，处理累积的事件
				if eventBuffer.Len() > 0 {
					payload := eventBuffer.Bytes()
					eventBuffer.Reset()
					p.parseAndForwardEvent(payload, &finalUsage, chunks, ctx)
				}
				continue
			}

			// 只收集 data: 开头的行，跳过 event: / 注释等
			if strings.HasPrefix(line, "data: ") {
				eventBuffer.WriteString(strings.TrimPrefix(line, "data: "))
				eventBuffer.WriteByte('\n')
			}
		}

		// 循环结束后，处理可能剩余的最后一个事件
		if eventBuffer.Len() > 0 {
			payload := eventBuffer.Bytes()
			eventBuffer.Reset()
			p.parseAndForwardEvent(payload, &finalUsage, chunks, ctx)
		}

		if err := scanner.Err(); err != nil {
			errs <- fmt.Errorf("anthropic stream: scanner error: %w", err)
		}
	}()

	return chunks, errs
}

// parseAndForwardEvent 解析一个完整的 SSE 事件并转发给 channel
func (p *Provider) parseAndForwardEvent(payload []byte, finalUsage *anthropicUsage, chunks chan<- provider.StreamChunk, ctx context.Context) {
	// 先提取 type 字段
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return // 跳过解析失败
	}

	switch envelope.Type {
	case "content_block_delta":
		var evt contentBlockDeltaEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return
		}
		if evt.Delta.Type != "text_delta" {
			return // 只处理文本增量
		}
		select {
		case chunks <- provider.StreamChunk{Content: evt.Delta.Text}:
		case <-ctx.Done():
			return
		}

	case "message_delta":
		var evt messageDeltaEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return
		}
		*finalUsage = evt.Usage

	case "message_start":
		var evt messageStartEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return
		}
		// 初始 usage 先记着，message_delta 会覆盖成最终值
		*finalUsage = evt.Message.Usage

	case "message_stop":
		// 最后一帧，带 usage
		select {
		case chunks <- provider.StreamChunk{
			IsDone:       true,
			InputTokens:  finalUsage.InputTokens,
			OutputTokens: finalUsage.OutputTokens,
		}:
		case <-ctx.Done():
		}

	case "content_block_start", "content_block_stop":
		// 不需要处理
	}
}

// --- 以下方法本阶段未实现（与 OpenAI Provider 保持一致） ---

// Embedding 向量嵌入（未实现）
func (p *Provider) Embedding(ctx context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

// Moderation 内容审核（未实现）
func (p *Provider) Moderation(ctx context.Context, req *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

// Cost 成本计算（未实现，后续读 models 表配置）
func (p *Provider) Cost(providerModel string, inputTokens, outputTokens int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}
