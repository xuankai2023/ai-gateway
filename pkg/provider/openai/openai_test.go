package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ai-gateway/pkg/provider"
)

func TestName(t *testing.T) {
	if got := New("https://api.openai.com/v1", "sk-test", provider.NewClients(30*time.Second)).Name(); got != "openai" {
		t.Errorf("Name = %q, want openai", got)
	}
}

func TestUnimplementedMethods(t *testing.T) {
	p := New("https://api.openai.com/v1", "sk-test", provider.NewClients(30*time.Second))
	ctx := context.Background()

	if _, err := p.Embedding(ctx, &provider.EmbeddingRequest{}); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Embedding err = %v, want ErrNotImplemented", err)
	}
	if _, err := p.Moderation(ctx, &provider.ModerationRequest{}); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Moderation err = %v, want ErrNotImplemented", err)
	}
	if _, err := p.Cost("gpt-4o", 1, 1); !errors.Is(err, provider.ErrNotImplemented) {
		t.Errorf("Cost err = %v, want ErrNotImplemented", err)
	}
}

func TestChatCompletion_Success(t *testing.T) {
	// mock 上游服务
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 验证请求头
		if r.Header.Get("Authorization") != "Bearer sk-test-key" {
			t.Errorf("Authorization header mismatch")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type header mismatch")
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %s, want /chat/completions", r.URL.Path)
		}

		// 验证请求体
		var req openaiRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Model != "gpt-4o-mini" {
			t.Errorf("model = %s, want gpt-4o-mini", req.Model)
		}
		if req.Stream != false {
			t.Errorf("stream should be false")
		}
		if len(req.Messages) != 1 || req.Messages[0].Content != "你好" {
			t.Errorf("messages mismatch: %+v", req.Messages)
		}

		// 返回成功响应
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]any{
						"role":    "assistant",
						"content": "你好！有什么可以帮你的吗？",
					},
					"finish_reason": "stop",
				},
			},
			"usage": map[string]any{
				"prompt_tokens":     10,
				"completion_tokens": 18,
				"total_tokens":      28,
			},
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-test-key", provider.NewClients(30*time.Second))
	resp, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "你好"}},
	})
	if err != nil {
		t.Fatalf("ChatCompletion returned error: %v", err)
	}

	if resp.Content != "你好！有什么可以帮你的吗？" {
		t.Errorf("Content = %q, want %q", resp.Content, "你好！有什么可以帮你的吗？")
	}
	if resp.InputTokens != 10 {
		t.Errorf("InputTokens = %d, want 10", resp.InputTokens)
	}
	if resp.OutputTokens != 18 {
		t.Errorf("OutputTokens = %d, want 18", resp.OutputTokens)
	}
	if resp.Provider != "openai" {
		t.Errorf("Provider = %q, want openai", resp.Provider)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("LatencyMs should be >= 0")
	}
}

func TestChatCompletion_UpstreamError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Incorrect API key provided",
				"type":    "invalid_authentication_error",
			},
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-wrong-key", provider.NewClients(30*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("err type = %T, want *provider.UpstreamError", err)
	}
	if upstreamErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("StatusCode = %d, want 401", upstreamErr.StatusCode)
	}
	if upstreamErr.Type != "invalid_authentication_error" {
		t.Errorf("Type = %q, want invalid_authentication_error", upstreamErr.Type)
	}
	if upstreamErr.Kind != provider.KindAuth {
		t.Errorf("Kind = %q, want auth", upstreamErr.Kind)
	}
	if !upstreamErr.Retryable() {
		t.Error("401 should be retryable on another channel")
	}
	if !upstreamErr.Permanent() {
		t.Error("401 should permanently disable the channel")
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
		{status: http.StatusUnauthorized, errType: "auth_error", kind: provider.KindAuth, retryable: true, permanent: true},
		{status: http.StatusTooManyRequests, errType: "rate_limit_error", kind: provider.KindRateLimit, retryable: true, permanent: false},
		{status: http.StatusInternalServerError, errType: "server_error", kind: provider.KindServer, retryable: true, permanent: false},
		{status: http.StatusBadRequest, errType: "invalid_request_error", kind: provider.KindUnknown, retryable: false, permanent: false},
	}

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{"message": "upstream says no", "type": tt.errType},
				})
			}))
			defer ts.Close()

			p := New(ts.URL, "sk-test", provider.NewClients(30*time.Second))
			_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
				Model:    "gpt-4o-mini",
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

func TestChatCompletion_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-test", provider.NewClients(20*time.Millisecond))

	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
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
	if upstreamErr.Permanent() {
		t.Error("timeout should not permanently disable the channel")
	}
}

func TestChatCompletion_InvalidResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not-json"))
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-test", provider.NewClients(30*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
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

func TestChatCompletion_EmptyChoices(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{},
			"usage":   nil,
		})
	}))
	defer ts.Close()

	p := New(ts.URL, "sk-test", provider.NewClients(30*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error for empty choices, got nil")
	}
}

func TestChatCompletion_NetworkError(t *testing.T) {
	// 指向一个不存在的地址
	p := New("http://127.0.0.1:1", "sk-test", provider.NewClients(30*time.Second))
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected network error, got nil")
	}
}
