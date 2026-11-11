package volcengine

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

// clientsWith 用自定义 Transport 组装一对 client
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

// sseBody 拼装 OpenAI 兼容的 SSE 行（逐行扫描，不依赖空行分隔）
func sseBody(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

// --- 基础契约 ---

func TestProviderMetadata(t *testing.T) {
	p := New("https://ark.cn-beijing.volces.com/api/v3/", "ark-test", provider.NewClients(30*time.Second))

	if got := p.Name(); got != "volcengine" {
		t.Errorf("Name = %q, want volcengine", got)
	}
	if got := p.BaseURL(); got != "https://ark.cn-beijing.volces.com/api/v3" {
		t.Errorf("BaseURL = %q, want trimmed url", got)
	}
}

func TestUnimplementedMethods(t *testing.T) {
	p := New("https://ark.cn-beijing.volces.com/api/v3", "ark-test", provider.NewClients(30*time.Second))
	ctx := context.Background()

	if _, err := p.Embedding(ctx, &provider.EmbeddingRequest{}); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Embedding err = %v, want ErrNotImplemented", err)
	}
	if _, err := p.Moderation(ctx, &provider.ModerationRequest{}); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Moderation err = %v, want ErrNotImplemented", err)
	}
	if _, err := p.Cost("doubao-pro-32k", 1, 1); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Cost err = %v, want ErrNotImplemented", err)
	}
}

// --- 非流式 ---

func TestChatCompletion_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %s, want /chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer ark-test" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}

		var req volcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.Model != "doubao-pro-32k" {
			t.Errorf("model = %q", req.Model)
		}
		if req.Stream {
			t.Error("stream should be false")
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Content != "你好" {
			t.Errorf("messages = %+v", req.Messages)
		}
		if req.Temperature == nil || *req.Temperature != 0.7 {
			t.Errorf("temperature not forwarded: %v", req.Temperature)
		}
		if req.MaxTokens == nil || *req.MaxTokens != 512 {
			t.Errorf("max_tokens not forwarded: %v", req.MaxTokens)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message":       map[string]any{"role": "assistant", "content": "你好！"},
					"finish_reason": "stop",
				},
			},
			"usage": map[string]any{
				"prompt_tokens":     9,
				"completion_tokens": 4,
				"total_tokens":      13,
			},
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	resp, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model: "doubao-pro-32k",
		Messages: []provider.Message{
			{Role: "system", Content: "你是助手"},
			{Role: "user", Content: "你好"},
		},
		Temperature: 0.7,
		MaxTokens:   512,
	})
	if err != nil {
		t.Fatalf("ChatCompletion returned error: %v", err)
	}
	if resp.Content != "你好！" {
		t.Errorf("Content = %q, want 你好！", resp.Content)
	}
	if resp.InputTokens != 9 || resp.OutputTokens != 4 {
		t.Errorf("usage = (%d, %d), want (9, 4)", resp.InputTokens, resp.OutputTokens)
	}
	if resp.Provider != "volcengine" {
		t.Errorf("Provider = %q", resp.Provider)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("LatencyMs = %d, want >= 0", resp.LatencyMs)
	}
}

func TestChatCompletion_OmitsUnsetOptions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Errorf("decode request: %v", err)
		}
		// temperature / max_tokens 未设置时不应出现在请求体里
		if _, ok := raw["temperature"]; ok {
			t.Error("temperature should be omitted when 0")
		}
		if _, ok := raw["max_tokens"]; ok {
			t.Error("max_tokens should be omitted when 0")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	resp, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("ChatCompletion returned error: %v", err)
	}
	// usage 缺省时必须安全回落 0，而不是 nil 解引用
	if resp.InputTokens != 0 || resp.OutputTokens != 0 {
		t.Errorf("usage = (%d, %d), want (0, 0)", resp.InputTokens, resp.OutputTokens)
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
		{status: http.StatusNotFound, errType: "model_not_found", kind: provider.KindAuth, retryable: true, permanent: true},
		{status: http.StatusTooManyRequests, errType: "rate_limit_error", kind: provider.KindRateLimit, retryable: true, permanent: false},
		{status: http.StatusInternalServerError, errType: "server_error", kind: provider.KindServer, retryable: true, permanent: false},
		{status: http.StatusServiceUnavailable, errType: "overloaded_error", kind: provider.KindOverloaded, retryable: true, permanent: false},
		{status: http.StatusBadRequest, errType: "invalid_request_error", kind: provider.KindUnknown, retryable: false, permanent: false},
	}

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{
						"code":    tt.status,
						"type":    tt.errType,
						"message": "upstream says no",
					},
				})
			}))
			defer ts.Close()

			p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
			_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
				Model:    "doubao-pro-32k",
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

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Type != "http_error" {
		t.Errorf("Type = %q, want http_error", upstreamErr.Type)
	}
}

func TestChatCompletion_InvalidResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "not-json")
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Type != "invalid_response" || upstreamErr.Kind != provider.KindServer {
		t.Errorf("err = %+v, want invalid_response + server", upstreamErr)
	}
}

func TestChatCompletion_EmptyChoices(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[],"usage":null}`)
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Type != "invalid_response" {
		t.Errorf("Type = %q, want invalid_response", upstreamErr.Type)
	}
}

func TestChatCompletion_NetworkError(t *testing.T) {
	p := New("http://127.0.0.1:1", "ark-test", provider.NewClients(2*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Kind != provider.KindServer || upstreamErr.StatusCode != 0 {
		t.Errorf("err = %+v, want network error", upstreamErr)
	}
}

func TestChatCompletion_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(20*time.Millisecond))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err = %v, want *provider.UpstreamError", err)
	}
	if upstreamErr.Kind != provider.KindTimeout {
		t.Errorf("Kind = %q, want timeout", upstreamErr.Kind)
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

	p := New("http://upstream.invalid", "ark-test", clients)
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
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
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %s, want /chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("Accept = %q, want text/event-stream", got)
		}

		var req volcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if !req.Stream {
			t.Error("stream should be true")
		}
		if req.Temperature == nil || *req.Temperature != 0.5 {
			t.Errorf("temperature not forwarded: %v", req.Temperature)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody(
			`data: {"choices":[{"delta":{"content":"你"},"finish_reason":null}]}`,
			`data: {"choices":[{"delta":{"content":"好"},"finish_reason":null}]}`,
			`data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`,
			`data: [DONE]`,
		))
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:       "doubao-pro-32k",
		Messages:    []provider.Message{{Role: "user", Content: "你好"}},
		Temperature: 0.5,
	})

	content, got, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 0 {
		t.Fatalf("unexpected stream errors: %v", gotErrs)
	}
	if content != "你好" {
		t.Errorf("content = %q, want 你好", content)
	}
	if len(got) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(got))
	}
	last := got[len(got)-1]
	if !last.IsDone {
		t.Error("last chunk should be IsDone")
	}
	if last.InputTokens != 3 || last.OutputTokens != 2 {
		t.Errorf("final usage = (%d, %d), want (3, 2)", last.InputTokens, last.OutputTokens)
	}
}

func TestChatCompletionStream_SkipsNoiseAndStopsAtDone(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody(
			`: keep-alive`,
			`event: ping`,
			`data: not-json`,
			`data: {"choices":[]}`,
			`data: {"choices":[{"delta":{"content":"ok"},"finish_reason":null}]}`,
			`data: [DONE]`,
			// [DONE] 之后的数据必须被丢弃
			`data: {"choices":[{"delta":{"content":"after-done"},"finish_reason":null}]}`,
		))
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})

	content, got, gotErrs := drainStream(t, chunks, errs)
	if len(gotErrs) != 0 {
		t.Fatalf("noise lines should be skipped, got errors: %v", gotErrs)
	}
	if content != "ok" {
		t.Errorf("content = %q, want ok", content)
	}
	if len(got) != 1 || got[0].IsDone {
		t.Errorf("chunks = %+v, want single non-done chunk", got)
	}
}

func TestChatCompletionStream_UpstreamError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 429, "type": "rate_limit_error", "message": "slow down"},
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
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

	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
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

func TestChatCompletionStream_ContextCanceled(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody(
			`data: {"choices":[{"delta":{"content":"部分"},"finish_reason":null}]}`,
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
	p := New(ts.URL, "ark-test", provider.NewClients(5*time.Second))
	chunks, errs := p.ChatCompletionStream(ctx, &provider.ChatRequest{
		Model:    "doubao-pro-32k",
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

	p := New("http://upstream.invalid", "ark-test", clients)
	chunks, errs := p.ChatCompletionStream(context.Background(), &provider.ChatRequest{
		Model:    "doubao-pro-32k",
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
