// Package openai OpenAI 兼容 Provider
//
// OpenAI 协议同时覆盖 DeepSeek / 火山方舟 / 自建 vLLM 等兼容服务。
// 本阶段只实现 ChatCompletion 非流式调用；流式/Embedding/Moderation/Cost 后续补齐。
package openai

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

// Provider OpenAI 兼容供应商
type Provider struct {
	baseURL string
	apiKey  string
	clients *provider.Clients
}

// New 创建 OpenAI 兼容 Provider
// clients 由 main 启动时统一构造注入（非流式带 attempt 总超时，流式无总超时）
func New(baseURL, apiKey string, clients *provider.Clients) *Provider {
	return &Provider{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		clients: clients,
	}
}

// Name 供应商标识
func (p *Provider) Name() string {
	return "openai"
}

// BaseURL 上游端点（熔断 key 用）
func (p *Provider) BaseURL() string {
	return p.baseURL
}

// --- ChatCompletion 非流式 ---

// openaiRequest OpenAI /chat/completions 请求体（只包含当前阶段用到的字段）
type openaiRequest struct {
	Model       string          `json:"model"`
	Messages    []openaiMessage `json:"messages"`
	Temperature *float32        `json:"temperature,omitempty"`
	MaxTokens   *int            `json:"max_tokens,omitempty"`
	Stream      bool            `json:"stream"`
}

type openaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openaiResponse OpenAI /chat/completions 响应体
type openaiResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

// openaiErrorResponse OpenAI 错误响应体
type openaiErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// ChatCompletion 聊天补全（非流式）
func (p *Provider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	start := time.Now()

	// 1. 构造请求体
	body := openaiRequest{
		Model:  req.Model,
		Stream: false, // P0 #2 只实现非流式
	}
	for _, m := range req.Messages {
		body.Messages = append(body.Messages, openaiMessage{Role: m.Role, Content: m.Content})
	}
	if req.Temperature > 0 {
		t := req.Temperature
		body.Temperature = &t
	}
	if req.MaxTokens > 0 {
		m := req.MaxTokens
		body.MaxTokens = &m
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai: marshal request: %w", err)
	}

	// 2. 发起 HTTP 请求
	url := p.baseURL + "/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.clients.NonStream.Do(httpReq)
	if err != nil {
		return nil, provider.NewNetworkError(p.Name(), err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, provider.NewNetworkError(p.Name(), err)
	}

	// 3. 错误处理
	if resp.StatusCode >= 400 {
		var errResp openaiErrorResponse
		if e := json.Unmarshal(respBody, &errResp); e == nil && errResp.Error.Message != "" {
			return nil, provider.NewUpstreamError(p.Name(), resp.StatusCode, errResp.Error.Type, errResp.Error.Message)
		}
		return nil, provider.NewUpstreamError(p.Name(), resp.StatusCode, "http_error", string(respBody))
	}

	// 4. 解析成功响应
	var parsed openaiResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, provider.NewInvalidResponseError(p.Name(), resp.StatusCode, "unmarshal response: "+err.Error())
	}

	if len(parsed.Choices) == 0 {
		return nil, provider.NewInvalidResponseError(p.Name(), resp.StatusCode, "empty choices in response")
	}

	result := &provider.ChatResponse{
		Content:   parsed.Choices[0].Message.Content,
		LatencyMs: time.Since(start).Milliseconds(),
		Provider:  p.Name(),
	}
	if parsed.Usage != nil {
		result.InputTokens = parsed.Usage.PromptTokens
		result.OutputTokens = parsed.Usage.CompletionTokens
	}

	return result, nil
}

// --- ChatCompletionStream 流式（实现 provider.Streamer 可选接口） ---

// openaiStreamChunk OpenAI SSE 流式帧结构
type openaiStreamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
			Role    string `json:"role"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

// ChatCompletionStream 流式聊天补全（SSE 逐帧推送）
// 返回只读 channel：chunkChan 发帧，errChan 发最多一个 error，都会在完成时 close
func (p *Provider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (chunkChan <-chan provider.StreamChunk, errChan <-chan error) {
	chunks := make(chan provider.StreamChunk, 8)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		// 1. 构造流式请求体
		body := openaiRequest{
			Model:  req.Model,
			Stream: true,
		}
		for _, m := range req.Messages {
			body.Messages = append(body.Messages, openaiMessage{Role: m.Role, Content: m.Content})
		}
		if req.Temperature > 0 {
			t := req.Temperature
			body.Temperature = &t
		}
		if req.MaxTokens > 0 {
			m := req.MaxTokens
			body.MaxTokens = &m
		}

		payload, err := json.Marshal(body)
		if err != nil {
			errs <- fmt.Errorf("openai stream: marshal request: %w", err)
			return
		}

		// 2. 发起流式 HTTP 请求
		url := p.baseURL + "/chat/completions"
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			errs <- fmt.Errorf("openai stream: build request: %w", err)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
		httpReq.Header.Set("Accept", "text/event-stream")

		resp, err := p.clients.Stream.Do(httpReq)
		if err != nil {
			errs <- provider.NewNetworkError(p.Name(), err)
			return
		}
		defer resp.Body.Close()

		// 3. 上游错误处理
		if resp.StatusCode >= 400 {
			respBody, _ := io.ReadAll(resp.Body)
			var errResp openaiErrorResponse
			if e := json.Unmarshal(respBody, &errResp); e == nil && errResp.Error.Message != "" {
				errs <- provider.NewUpstreamError(p.Name(), resp.StatusCode, errResp.Error.Type, errResp.Error.Message)
			} else {
				errs <- provider.NewUpstreamError(p.Name(), resp.StatusCode, "http_error", string(respBody))
			}
			return
		}

		// 4. 逐帧读取 SSE
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // 支持最大 4MB 帧

		for scanner.Scan() {
			line := scanner.Text()

			// 跳过空行和注释行
			if line == "" || strings.HasPrefix(line, ":") {
				continue
			}

			// 去掉 "data: " 前缀
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")

			// [DONE] 是上游正常结束标记
			if data == "[DONE]" {
				return
			}

			// 解析 JSON 帧
			var frame openaiStreamChunk
			if err := json.Unmarshal([]byte(data), &frame); err != nil {
				continue // 跳过解析失败的帧
			}

			if len(frame.Choices) == 0 {
				continue
			}

			// 构造 StreamChunk
			chunk := provider.StreamChunk{
				Content: frame.Choices[0].Delta.Content,
			}

			// 含 finish_reason 的帧 = 最后一帧，带 usage
			if frame.Choices[0].FinishReason != nil && *frame.Choices[0].FinishReason != "" {
				chunk.IsDone = true
				if frame.Usage != nil {
					chunk.InputTokens = frame.Usage.PromptTokens
					chunk.OutputTokens = frame.Usage.CompletionTokens
				}
			}

			// 非阻塞发送（监听 ctx.Done 止损）
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				// 🔴 断开即止损：客户端放弃了 → 立即停止读上游
				return
			}

			if chunk.IsDone {
				return
			}
		}

		// Scanner 错误（可能是上游断了或网络问题）
		if err := scanner.Err(); err != nil {
			errs <- provider.NewNetworkError(p.Name(), err)
		}
	}()

	return chunks, errs
}

// --- 以下方法本阶段未实现 ---

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
