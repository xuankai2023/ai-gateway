package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-gateway/pkg/provider"
)

// --- 测试辅助 ---

// roundTripFunc 让测试可以构造 NewClients 覆盖不到的场景（如响应体读取失败）
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// readErrorBody 模拟响应体读取中途 IO 错误
type readErrorBody struct{}

func (readErrorBody) Read([]byte) (int, error) { return 0, errors.New("read boom") }
func (readErrorBody) Close() error             { return nil }

// clientsWith 用自定义 Transport 组装一对 client（绕开重试与超时干扰）
func clientsWith(rt http.RoundTripper) *provider.Clients {
	return &provider.Clients{
		NonStream: &http.Client{Transport: rt},
		Stream:    &http.Client{Transport: rt},
	}
}

// drainStream 收干流式 channel，返回拼接文本、全部帧与全部错误
func drainStream(t *testing.T, chunks <-chan provider.StreamChunk, errs <-chan error) (string, []provider.StreamChunk, []error) {
	t.Helper()
	var content strings.Builder
	var gotChunks []provider.StreamChunk
	var gotErrs []error

	for chunks != nil || errs != nil {
		select {
		case c, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			gotChunks = append(gotChunks, c)
			content.WriteString(c.Content)
		case e, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if e != nil {
				gotErrs = append(gotErrs, e)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("stream did not close within 3s")
		}
	}
	return content.String(), gotChunks, gotErrs
}

// sseFrames 拼装 anthropic 风格 SSE（event 行被实现忽略，只取 data 行）
func sseFrames(payloads ...string) string {
	var b strings.Builder
	for _, p := range payloads {
		b.WriteString("event: message\n")
		b.WriteString("data: ")
		b.WriteString(p)
		b.WriteString("\n\n")
	}
	return b.String()
}

// --- 基础契约 ---

func TestProviderMetadata(t *testing.T) {
	p := New("https://api.anthropic.com/", "sk-ant-test", provider.NewClients(30*time.Second))

	if got := p.Name(); got != "anthropic" {
		t.Errorf("Name = %q, want anthropic", got)
	}
	// 构造时应去掉尾部斜杠，避免拼出 //v1/messages
	if got := p.BaseURL(); got != "https://api.anthropic.com" {
		t.Errorf("BaseURL = %q, want trimmed url", got)
	}
}

func TestUnimplementedMethods(t *testing.T) {
	p := New("https://api.anthropic.com", "sk-ant-test", provider.NewClients(30*time.Second))
	ctx := context.Background()

	if _, err := p.Embedding(ctx, &provider.EmbeddingRequest{}); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Embedding err = %v, want ErrNotImplemented", err)
	}
	if _, err := p.Moderation(ctx, &provider.ModerationRequest{}); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Moderation err = %v, want ErrNotImplemented", err)
	}
	if _, err := p.Cost("claude-3-5-sonnet", 1, 1); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Cost err = %v, want ErrNotImplemented", err)
	}
}

// --- 非流式 ---

func TestChatCompletion_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path = %s, want /v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("x-api-key"); got != "sk-ant-key" {
			t.Errorf("x-api-key = %q", got)
		}
		if got := r.Header.Get("anthropic-version"); got != "2023-06-01" {
			t.Errorf("anthropic-version = %q, want 2023-06-01", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}

		var req anthropicRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.Model != "claude-3-5-sonnet" {
			t.Errorf("model = %q", req.Model)
		}
		// system 必须从 messages 中拆到顶层 system 字段
		if req.System != "你是助手" {
			t.Errorf("system = %q, want 你是助手", req.System)
		}
		if len(req.Messages) != 1 || req.Messages[0].Role != "user" || req.Messages[0].Content != "你好" {
			t.Errorf("messages = %+v", req.Messages)
		}
		// max_tokens 未指定时回落 1024（Claude 必填）
		if req.MaxTokens != 1024 {
			t.Errorf("max_tokens = %d, want 1024", req.MaxTokens)
		}
		// temperature 未指定时不下发
		if req.Temperature != nil {
			t.Errorf("temperature should be omitted, got %v", *req.Temperature)
		}
		if req.Stream {
			t.Error("stream should be false")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":   "msg_123",
			"type": "message",
			"role": "assistant",
			"content": []map[string]any{
				{"type": "text", "text": "你好"},
				{"type": "tool_use", "text": "ignored"},
				{"type": "text", "text": "世界"},
			},
			"model":       "claude-3-5-sonnet",
			"stop_reason": "end_turn",
			"usage":       map[string]any{"input_tokens": 11, "output_tokens": 7},
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	resp, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model: "claude-3-5-sonnet",
		Messages: []provider.Message{
			{Role: "system", Content: "你是助手"},
			{Role: "user", Content: "你好"},
		},
	})
	if err != nil {
		t.Fatalf("ChatCompletion returned error: %v", err)
	}
	if resp.Content != "你好世界" {
		t.Errorf("Content = %q, want 你好世界（tool_use 块应被忽略）", resp.Content)
	}
	if resp.InputTokens != 11 || resp.OutputTokens != 7 {
		t.Errorf("usage = (%d, %d), want (11, 7)", resp.InputTokens, resp.OutputTokens)
	}
	if resp.Provider != "anthropic" {
		t.Errorf("Provider = %q", resp.Provider)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("LatencyMs = %d, want >= 0", resp.LatencyMs)
	}
}

func TestChatCompletion_ForwardsSamplingOptions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req anthropicRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.MaxTokens != 256 {
			t.Errorf("max_tokens = %d, want 256", req.MaxTokens)
		}
		if req.Temperature == nil || *req.Temperature != 0.4 {
			t.Errorf("temperature not forwarded: %v", req.Temperature)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:       "claude-3-5-haiku",
		Messages:    []provider.Message{{Role: "user", Content: "hi"}},
		Temperature: 0.4,
		MaxTokens:   256,
	})
	if err != nil {
		t.Fatalf("ChatCompletion returned error: %v", err)
	}
}

func TestChatCompletion_UpstreamStatus(t *testing.T) {
	tests := []struct {
		status    int
		errType   string
		kind      provider.ErrorKind
		retryable bool
		permanent bool
	}{
		{status: http.StatusUnauthorized, errType: "authentication_error", kind: provider.KindAuth, retryable: true, permanent: true},
		{status: http.StatusNotFound, errType: "not_found_error", kind: provider.KindAuth, retryable: true, permanent: true},
		{status: http.StatusTooManyRequests, errType: "rate_limit_error", kind: provider.KindRateLimit, retryable: true, permanent: false},
		{status: http.StatusInternalServerError, errType: "api_error", kind: provider.KindServer, retryable: true, permanent: false},
		{status: 529, errType: "overloaded_error", kind: provider.KindOverloaded, retryable: true, permanent: false},
		{status: http.StatusBadRequest, errType: "invalid_request_error", kind: provider.KindUnknown, retryable: false, permanent: false},
	}

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				json.NewEncoder(w).Encode(map[string]any{
					"type": "error",
					"error": map[string]any{
						"type":    tt.errType,
						"message": "upstream says no",
					},
				})
			}))
			defer ts.Close()

			p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
			_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
				Model:    "claude-3-5-sonnet",
				Messages: []provider.Message{{Role: "user", Content: "hi"}},
			})

			var upstreamErr *provider.UpstreamError
			if !errors.As(err, &upstreamErr) {
				t.Fatalf("err = %v, want *provider.UpstreamError", err)
			}
			if upstreamErr.StatusCode != tt.status {
				t.Errorf("StatusCode = %d, want %d", upstreamErr.StatusCode, tt.status)
			}
			if upstreamErr.Type != tt.errType {
				t.Errorf("Type = %q, want %q", upstreamErr.Type, tt.errType)
			}
			if upstreamErr.Kind != tt.kind {
				t.Errorf("Kind = %q, want %q", upstreamErr.Kind, tt.kind)
			}
			if upstreamErr.Retryable() != tt.retryable {
				t.Errorf("Retryable() = %v, want %v", upstreamErr.Retryable(), tt.retryable)
			}
			if upstreamErr.Permanent() != tt.permanent {
				t.Errorf("Permanent() = %v, want %v", upstreamErr.Permanent(), tt.permanent)
			}
		})
	}
}

func TestChatCompletion_NonJSONErrorBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, "upstream gateway exploded")
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Type != "http_error" {
		t.Errorf("Type = %q, want http_error", upstreamErr.Type)
	}
	if upstreamErr.Kind != provider.KindServer {
		t.Errorf("Kind = %q, want server", upstreamErr.Kind)
	}
}

func TestChatCompletion_InvalidResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "not-json")
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Type != "invalid_response" {
		t.Errorf("Type = %q, want invalid_response", upstreamErr.Type)
	}
	if upstreamErr.Kind != provider.KindServer {
		t.Errorf("Kind = %q, want server", upstreamErr.Kind)
	}
	if !upstreamErr.Retryable() {
		t.Error("invalid response should be retryable")
	}
}

func TestChatCompletion_NetworkError(t *testing.T) {
	p := New("http://127.0.0.1:1", "sk-ant-key", provider.NewClients(2*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Kind != provider.KindServer {
		t.Errorf("Kind = %q, want server", upstreamErr.Kind)
	}
	if upstreamErr.StatusCode != 0 {
		t.Errorf("StatusCode = %d, want 0 for network error", upstreamErr.StatusCode)
	}
}

func TestChatCompletion_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(20*time.Millisecond))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Kind != provider.KindTimeout {
		t.Errorf("Kind = %q, want timeout", upstreamErr.Kind)
	}
	if !upstreamErr.Retryable() {
		t.Error("timeout should be retryable")
	}
}

func TestChatCompletion_ResponseBodyReadError(t *testing.T) {
	clients := clientsWith(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       readErrorBody{},
		}, nil
	}))

	p := New("http://upstream.invalid", "sk-ant-key", clients)
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Type != "network_error" {
		t.Errorf("Type = %q, want network_error", upstreamErr.Type)
	}
}

// --- 流式 ---

func TestChatCompletionStream_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path = %s, want /v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("Accept = %q, want text/event-stream", got)
		}

		var req anthropicRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if !req.Stream {
			t.Error("stream should be true")
		}
		if req.System != "系统提示" {
			t.Errorf("system = %q", req.System)
		}
		if req.MaxTokens != 1024 {
			t.Errorf("max_tokens = %d, want 1024", req.MaxTokens)
		}
		if len(req.Messages) != 1 {
			t.Errorf("messages = %+v", req.Messages)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseFrames(
			`{"type":"message_start","message":{"usage":{"input_tokens":12,"output_tokens":1}}}`,
			`{"type":"content_block_start","index":0}`,
			// 非文本增量必须被忽略
			`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","text":"思考"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"你"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"好"}}`,
			`{"type":"message_delta","usage":{"input_tokens":12,"output_tokens":5},"stop_reason":"end_turn"}`,
			`{"type":"message_stop"}`,
		))
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model: "claude-3-5-sonnet",
		Messages: []provider.Message{
			{Role: "system", Content: "系统提示"},
			{Role: "user", Content: "你好"},
		},
	})

	content, got, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 0 {
		t.Fatalf("unexpected stream errors: %v", gotErrs)
	}
	if content != "你好" {
		t.Errorf("content = %q, want 你好", content)
	}
	if len(got) != 3 {
		t.Fatalf("chunk count = %d, want 3 (2 增量 + 1 结束帧)", len(got))
	}
	last := got[len(got)-1]
	if !last.IsDone {
		t.Error("last chunk should be IsDone")
	}
	if last.InputTokens != 12 || last.OutputTokens != 5 {
		t.Errorf("final usage = (%d, %d), want (12, 5)", last.InputTokens, last.OutputTokens)
	}
}

func TestChatCompletionStream_UpstreamError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]any{
			"type":  "error",
			"error": map[string]any{"type": "rate_limit_error", "message": "slow down"},
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	_, _, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 1 {
		t.Fatalf("err count = %d, want 1", len(gotErrs))
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(gotErrs[0], &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", gotErrs[0])
	}
	if upstreamErr.StatusCode != http.StatusTooManyRequests || upstreamErr.Kind != provider.KindRateLimit {
		t.Errorf("err = %+v, want 429 rate_limit", upstreamErr)
	}
}

func TestChatCompletionStream_NonJSONUpstreamError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "service unavailable")
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	_, _, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 1 {
		t.Fatalf("err count = %d, want 1", len(gotErrs))
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(gotErrs[0], &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", gotErrs[0])
	}
	if upstreamErr.Type != "http_error" || upstreamErr.Kind != provider.KindOverloaded {
		t.Errorf("err = %+v, want http_error + overloaded", upstreamErr)
	}
}

func TestChatCompletionStream_MalformedFrames(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseFrames(
			`not-json`,
			`{"type":"unknown_event","foo":"bar"}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta"}}`,
			`{"type":"message_stop"}`,
		))
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	content, got, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 0 {
		t.Fatalf("malformed frames should be skipped, got errors: %v", gotErrs)
	}
	if content != "" {
		t.Errorf("content = %q, want empty", content)
	}
	// 空 text_delta 仍会产生一个空增量帧，随后是唯一的结束帧
	if len(got) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(got))
	}
	if !got[len(got)-1].IsDone {
		t.Error("last chunk should be IsDone")
	}
}

func TestChatCompletionStream_ContextCanceled(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseFrames(
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"部分"}}`,
		))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer ts.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	p := New(ts.URL, "sk-ant-key", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(ctx, &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	select {
	case _, ok := <-chunks:
		if !ok {
			t.Fatal("chunks closed before first delta")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first delta not received")
	}

	cancel()

	// 取消后两条 channel 必须收敛关闭，不能挂住转发 goroutine
	deadline := time.After(3 * time.Second)
	for chunks != nil || errs != nil {
		select {
		case _, ok := <-chunks:
			if !ok {
				chunks = nil
			}
		case _, ok := <-errs:
			if !ok {
				errs = nil
			}
		case <-deadline:
			t.Fatal("stream channels did not close after context cancel")
		}
	}
}

func TestChatCompletionStream_ScannerError(t *testing.T) {
	clients := clientsWith(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       readErrorBody{},
		}, nil
	}))

	p := New("http://upstream.invalid", "sk-ant-key", clients)
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	_, _, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 1 {
		t.Fatalf("err count = %d, want 1", len(gotErrs))
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(gotErrs[0], &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", gotErrs[0])
	}
	if upstreamErr.Type != "network_error" {
		t.Errorf("Type = %q, want network_error", upstreamErr.Type)
	}
}
