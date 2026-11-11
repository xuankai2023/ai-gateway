// Package volcengine 火山方舟 Provider
//
// 火山方舟的 Chat Completions endpoint 是 OpenAI 兼容协议：
//   - baseURL 官方示例：https://ark.cn-beijing.volces.com/api/v3
//   - 请求路径：POST /chat/completions
//   - 鉴权：Bearer {ARK_API_KEY}
//   - 流式：SSE 格式，data: {...} + [DONE] 结束标记
//   - 响应：choices[0].message.content + usage.prompt_tokens / completion_tokens
//
// 实现策略：结构对齐 OpenAI Provider——ChatCompletion 非流式 + ChatCompletionStream 流式 SSE + Name；
// Embedding / Moderation / Cost 返回 ErrNotImplemented。
package volcengine

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

// Provider 火山方舟供应商
type Provider struct {
	baseURL string
	apiKey  string
	clients *provider.Clients
}

// New 创建火山方舟 Provider
// baseURL 官方默认：https://ark.cn-beijing.volces.com/api/v3
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
	return "volcengine"
}

// BaseURL 上游端点（熔断 key 用）
func (p *Provider) BaseURL() string {
	return p.baseURL
}

// --- 请求 / 响应结构（与 OpenAI 协议对齐） ---

type volcRequest struct {
	Model       string        `json:"model"`
	Messages    []volcMessage `json:"messages"`
	Temperature *float32      `json:"temperature,omitempty"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
	Stream      bool          `json:"stream"`
}

type volcMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type volcResponse struct {
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

type volcErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

type volcStreamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

// --- ChatCompletion 非流式 ---

// ChatCompletion 聊天补全（非流式）
func (p *Provider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	start := time.Now()

	body := volcRequest{
		Model:  req.Model,
		Stream: false,
	}
	for _, m := range req.Messages {
		body.Messages = append(body.Messages, volcMessage{Role: m.Role, Content: m.Content})
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
		return nil, fmt.Errorf("volcengine: marshal request: %w", err)
	}

	url := p.baseURL + "/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("volcengine: build request: %w", err)
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

	if resp.StatusCode >= 400 {
		var errResp volcErrorResponse
		if e := json.Unmarshal(respBody, &errResp); e == nil && errResp.Error.Message != "" {
			return nil, provider.NewUpstreamError(p.Name(), resp.StatusCode, errResp.Error.Type, errResp.Error.Message)
		}
		return nil, provider.NewUpstreamError(p.Name(), resp.StatusCode, "http_error", string(respBody))
	}

	var parsed volcResponse
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

// --- ChatCompletionStream 流式 SSE ---

// ChatCompletionStream 流式聊天补全
func (p *Provider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (chunkChan <-chan provider.StreamChunk, errChan <-chan error) {
	chunks := make(chan provider.StreamChunk, 8)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body := volcRequest{
			Model:  req.Model,
			Stream: true,
		}
		for _, m := range req.Messages {
			body.Messages = append(body.Messages, volcMessage{Role: m.Role, Content: m.Content})
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
			errs <- fmt.Errorf("volcengine stream: marshal request: %w", err)
			return
		}

		url := p.baseURL + "/chat/completions"
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			errs <- fmt.Errorf("volcengine stream: build request: %w", err)
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

		if resp.StatusCode >= 400 {
			respBody, _ := io.ReadAll(resp.Body)
			var errResp volcErrorResponse
			if e := json.Unmarshal(respBody, &errResp); e == nil && errResp.Error.Message != "" {
				errs <- provider.NewUpstreamError(p.Name(), resp.StatusCode, errResp.Error.Type, errResp.Error.Message)
			} else {
				errs <- provider.NewUpstreamError(p.Name(), resp.StatusCode, "http_error", string(respBody))
			}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

		for scanner.Scan() {
			line := scanner.Text()

			if line == "" || strings.HasPrefix(line, ":") {
				continue
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")

			if data == "[DONE]" {
				return
			}

			var frame volcStreamChunk
			if err := json.Unmarshal([]byte(data), &frame); err != nil {
				continue
			}
			if len(frame.Choices) == 0 {
				continue
			}

			chunk := provider.StreamChunk{
				Content: frame.Choices[0].Delta.Content,
			}

			if frame.Choices[0].FinishReason != nil && *frame.Choices[0].FinishReason != "" {
				chunk.IsDone = true
				if frame.Usage != nil {
					chunk.InputTokens = frame.Usage.PromptTokens
					chunk.OutputTokens = frame.Usage.CompletionTokens
				}
			}

			select {
			case chunks <- chunk:
			case <-ctx.Done():
				return
			}

			if chunk.IsDone {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			errs <- provider.NewNetworkError(p.Name(), err)
		}
	}()

	return chunks, errs
}

// --- 未实现的方法（与 OpenAI / Anthropic 保持一致） ---

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
