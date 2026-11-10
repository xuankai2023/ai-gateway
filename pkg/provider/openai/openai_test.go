package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-gateway/pkg/provider"
)

func TestName(t *testing.T) {
	if got := New("https://api.openai.com/v1", "sk-test").Name(); got != "openai" {
		t.Errorf("Name = %q, want openai", got)
	}
}

func TestUnimplementedMethods(t *testing.T) {
	p := New("https://api.openai.com/v1", "sk-test")
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

	p := New(ts.URL, "sk-test-key")
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
	if resp.LatencyMs <= 0 {
		t.Errorf("LatencyMs should be > 0")
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

	p := New(ts.URL, "sk-wrong-key")
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
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

	p := New(ts.URL, "sk-test")
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
	p := New("http://127.0.0.1:1", "sk-test")
	_, err := p.ChatCompletion(context.Background(), &provider.ChatRequest{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected network error, got nil")
	}
}
