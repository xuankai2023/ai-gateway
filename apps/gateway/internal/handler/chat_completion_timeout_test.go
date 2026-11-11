package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/provider"
)

// fakeDripStreamer 无限滴流的假流式 Provider：首帧立即到达，之后每 10ms 一帧直到 ctx 取消
type fakeDripStreamer struct {
	name string
}

func (f *fakeDripStreamer) Name() string { return f.name }

func (f *fakeDripStreamer) ChatCompletion(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeDripStreamer) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeDripStreamer) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeDripStreamer) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

func (f *fakeDripStreamer) ChatCompletionStream(ctx context.Context, _ *provider.ChatRequest) (<-chan provider.StreamChunk, <-chan error) {
	chunks := make(chan provider.StreamChunk, 16)
	errs := make(chan error, 1)
	go func() {
		defer close(chunks)
		defer close(errs)
		chunks <- provider.StreamChunk{Content: "first"}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case chunks <- provider.StreamChunk{Content: "x"}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return chunks, errs
}

func TestChatCompletion_AllAttemptsTimeoutReturns504(t *testing.T) {
	timeoutErr := provider.NewNetworkError("fake-timeout", context.DeadlineExceeded)

	r, registry := newChatTestRouter(t, []routing.Ability{
		{ID: 0, Provider: "fake-timeout", ProviderModel: "model-a"},
	})
	registerFake(registry, "fake-timeout", &fakeProvider{name: "fake-timeout", err: timeoutErr})

	w := doChatRequest(t, r)
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504（超时类失败映射）, body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", resp)
	}
	if errObj["type"] != "upstream_error" || errObj["code"] != "upstream_timeout" {
		t.Errorf("error 契约不对: %v", errObj)
	}
	if id, _ := resp["request_id"].(string); id == "" {
		t.Errorf("request_id 缺失: %v", resp)
	} else if w.Header().Get("X-Request-Id") != id {
		t.Errorf("X-Request-Id = %q, 与 request_id = %q 不一致", w.Header().Get("X-Request-Id"), id)
	}
}

func TestChatCompletion_NonTimeoutFailureStillReturns502(t *testing.T) {
	r, registry := newChatTestRouter(t, []routing.Ability{
		{ID: 0, Provider: "fake-500", ProviderModel: "model-a"},
	})
	registerFake(registry, "fake-500", &fakeProvider{
		name: "fake-500",
		err:  provider.NewUpstreamError("fake-500", 500, "server_error", "boom"),
	})

	w := doChatRequest(t, r)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502（非超时失败不映射 504）, body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "upstream_exhausted") {
		t.Errorf("body 应含 upstream_exhausted: %s", w.Body.String())
	}
}

func TestChatCompletionStream_MaxDurationBackstop(t *testing.T) {
	streamer := &fakeDripStreamer{name: "fake-drip"}

	// 流式总时长兜底 80ms
	a, registry := newHandlerTestApp(t, resolveFunc(func(context.Context, int64, string, int) ([]routing.Ability, error) {
		return []routing.Ability{{ID: 0, Provider: "fake-drip", ProviderModel: "model-a"}}, nil
	}), func(cfg *config.Config) {
		cfg.Gateway.StreamMaxDuration = "80ms"
	})
	registerFake(registry, "fake-drip", streamer)
	r := mountChat(a)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	start := time.Now()
	r.ServeHTTP(w, req)
	elapsed := time.Since(start)

	body := w.Body.String()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200（SSE 正常建连）, body=%s", w.Code, body)
	}
	if !strings.Contains(body, "data: ") {
		t.Error("响应应包含已转发的数据帧")
	}
	if !strings.Contains(body, "stream_max_duration_exceeded") {
		t.Errorf("响应应包含 stream_max_duration_exceeded 错误帧: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Errorf("响应应以 [DONE] 收尾: %q", body)
	}
	if elapsed > 2*time.Second {
		t.Errorf("兜底触发耗时 %v, 应在 80ms 量级", elapsed)
	}
}

func TestChatCompletionStream_NormalEndNotKilled(t *testing.T) {
	// 正常短流（远小于兜底时长）应完整结束，不被 max-duration 误杀
	finite := &fakeFiniteStreamer{name: "fake-finite", frames: []string{"a", "b", "c"}}

	a, registry := newHandlerTestApp(t, resolveFunc(func(context.Context, int64, string, int) ([]routing.Ability, error) {
		return []routing.Ability{{ID: 0, Provider: "fake-finite", ProviderModel: "model-a"}}, nil
	}), func(cfg *config.Config) {
		cfg.Gateway.StreamMaxDuration = "1m"
	})
	registerFake(registry, "fake-finite", finite)
	r := mountChat(a)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	body := w.Body.String()
	if strings.Contains(body, "stream_max_duration_exceeded") {
		t.Errorf("正常短流不应触发兜底: %s", body)
	}
	for _, frame := range []string{"a", "b", "c"} {
		if !strings.Contains(body, frame) {
			t.Errorf("响应应包含帧 %q: %s", frame, body)
		}
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Errorf("正常结束应以 [DONE] 收尾: %q", body)
	}
}

// fakeFiniteStreamer 有限帧流式假 Provider
type fakeFiniteStreamer struct {
	name   string
	frames []string
}

func (f *fakeFiniteStreamer) Name() string { return f.name }

func (f *fakeFiniteStreamer) ChatCompletion(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeFiniteStreamer) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeFiniteStreamer) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeFiniteStreamer) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

func (f *fakeFiniteStreamer) ChatCompletionStream(ctx context.Context, _ *provider.ChatRequest) (<-chan provider.StreamChunk, <-chan error) {
	chunks := make(chan provider.StreamChunk, len(f.frames)+1)
	errs := make(chan error, 1)
	for _, frame := range f.frames {
		chunks <- provider.StreamChunk{Content: frame}
	}
	chunks <- provider.StreamChunk{IsDone: true, InputTokens: 2, OutputTokens: 3}
	close(chunks)
	close(errs)
	return chunks, errs
}
