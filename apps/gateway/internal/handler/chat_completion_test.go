package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/relay"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/provider"
	"github.com/gin-gonic/gin"
)

// resolveFunc 函数适配器，实现 app.AttemptResolver（测试注入 fake 路由计划）
type resolveFunc func(context.Context, int64, string, int) ([]routing.Ability, error)

func (f resolveFunc) ResolveAttempts(ctx context.Context, tenantID int64, model string, maxAttempts int) ([]routing.Ability, error) {
	return f(ctx, tenantID, model, maxAttempts)
}

// newHandlerTestApp 构造 handler 测试 App：独立熔断器 / Provider 注册表 / 真实 Relay，
// Resolver 注入 fake；tune 可覆盖 Gateway 配置（如 StreamMaxDuration / AttemptTimeout 字符串）
func newHandlerTestApp(t *testing.T, resolver app.AttemptResolver, tune func(*config.Config)) (*app.App, *provider.Registry) {
	t.Helper()
	cfg := config.Default()
	if tune != nil {
		tune(cfg)
	}
	_, attemptTimeout, _ := cfg.Gateway.TimeoutDurations()

	registry := provider.NewRegistry()
	r := relay.New(nil, circuitbreaker.NewManager(circuitbreaker.Options{}), registry, nil, attemptTimeout)
	t.Cleanup(r.Close)

	return &app.App{Config: cfg, Relay: r, Resolver: resolver}, registry
}

// mountChat 挂载带 tenant 注入的 chat 路由；extra 可追加 context 注入（如灰度命中态）
func mountChat(a *app.App, extra ...func(*gin.Context)) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Set("api_key_id", int64(2))
		for _, fn := range extra {
			fn(c)
		}
		c.Next()
	})
	r.POST("/v1/chat/completions", ChatCompletion(a))
	return r
}

// newChatTestRouter 常用组装：固定路由计划 + 默认配置，返回引擎与注册表（注册 fake provider 用）
func newChatTestRouter(t *testing.T, attempts []routing.Ability) (*gin.Engine, *provider.Registry) {
	t.Helper()
	a, registry := newHandlerTestApp(t, resolveFunc(func(context.Context, int64, string, int) ([]routing.Ability, error) {
		return attempts, nil
	}), nil)
	return mountChat(a), registry
}

// fakeProvider 可注入结果的假 Provider（不实现 Streamer）
type fakeProvider struct {
	name     string
	response *provider.ChatResponse
	err      error
	gotModel string
}

func (f *fakeProvider) ChatCompletion(_ context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	f.gotModel = req.Model
	if f.err != nil {
		return nil, f.err
	}
	resp := *f.response
	resp.Provider = f.name
	return &resp, nil
}

func (f *fakeProvider) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeProvider) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeProvider) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

func (f *fakeProvider) Name() string { return f.name }

// registerFake 把 fake provider 注册进指定注册表（实例级，替代原全局注册表钩子）
func registerFake(registry *provider.Registry, providerType string, p provider.Provider) {
	registry.Register(providerType, func() provider.Provider { return p })
}

func doChatRequest(t *testing.T, r *gin.Engine) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"model":    "test-model",
		"messages": []map[string]string{{"role": "user", "content": "hi"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestChatCompletion_SwitchChannelSuccess(t *testing.T) {
	first := &fakeProvider{name: "fake-switch-a", err: provider.NewUpstreamError("fake-switch-a", 500, "server_error", "boom")}
	second := &fakeProvider{name: "fake-switch-b", response: &provider.ChatResponse{
		Content: "hello from b", InputTokens: 3, OutputTokens: 5,
	}}

	r, registry := newChatTestRouter(t, []routing.Ability{
		{ID: 0, Provider: "fake-switch-a", ProviderModel: "model-a"},
		{ID: 0, Provider: "fake-switch-b", ProviderModel: "model-b"},
	})
	registerFake(registry, "fake-switch-a", first)
	registerFake(registry, "fake-switch-b", second)

	w := doChatRequest(t, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, w.Body.String())
	}
	if second.gotModel != "model-b" {
		t.Errorf("provider model = %q, want model-b", second.gotModel)
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("hello from b")) {
		t.Errorf("response body = %s", w.Body.String())
	}
}

func TestChatCompletion_AllChannelsFail(t *testing.T) {
	first := &fakeProvider{name: "fake-fail-a", err: provider.NewUpstreamError("fake-fail-a", 500, "server_error", "boom a")}
	second := &fakeProvider{name: "fake-fail-b", err: provider.NewUpstreamError("fake-fail-b", 500, "server_error", "boom b")}

	r, registry := newChatTestRouter(t, []routing.Ability{
		{ID: 0, Provider: "fake-fail-a", ProviderModel: "model-a"},
		{ID: 0, Provider: "fake-fail-b", ProviderModel: "model-b"},
	})
	registerFake(registry, "fake-fail-a", first)
	registerFake(registry, "fake-fail-b", second)

	w := doChatRequest(t, r)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body=%s", w.Code, w.Body.String())
	}

	// 统一错误契约
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", resp)
	}
	if errObj["type"] != "upstream_error" || errObj["code"] != "upstream_exhausted" {
		t.Errorf("error 契约不对: %v", errObj)
	}
	if id, _ := resp["request_id"].(string); id == "" {
		t.Errorf("request_id 缺失: %v", resp)
	}
}

func TestChatCompletion_InvalidBodyContract(t *testing.T) {
	r, _ := newChatTestRouter(t, nil)

	body := bytes.NewReader([]byte(`{"model":`)) // 非法 JSON
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", resp)
	}
	if errObj["type"] != "invalid_request_error" || errObj["code"] != "invalid_request_body" {
		t.Errorf("error 契约不对: %v", errObj)
	}
	if id, _ := resp["request_id"].(string); id == "" {
		t.Errorf("request_id 缺失: %v", resp)
	} else if w.Header().Get("X-Request-Id") != id {
		t.Errorf("X-Request-Id = %q, 与 request_id = %q 不一致", w.Header().Get("X-Request-Id"), id)
	}
}

func TestChatCompletion_FallbackModel(t *testing.T) {
	// models 表兜底渠道：ID=0，没有 abilities 记录
	fallback := &fakeProvider{name: "fake-fallback", response: &provider.ChatResponse{
		Content: "fallback answer", InputTokens: 1, OutputTokens: 2,
	}}

	r, registry := newChatTestRouter(t, []routing.Ability{
		{ID: 0, Provider: "fake-fallback", ProviderModel: "fallback-model"},
	})
	registerFake(registry, "fake-fallback", fallback)

	w := doChatRequest(t, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("fallback answer")) {
		t.Errorf("response body = %s", w.Body.String())
	}
}

// TestChatCompletion_GrayTargetModel 灰度中间件命中后，handler 按 target_model 解析路由，
// 响应仍回填客户端请求模型（阶段 13）
func TestChatCompletion_GrayTargetModel(t *testing.T) {
	gray := &fakeProvider{name: "fake-gray", response: &provider.ChatResponse{
		Content: "gray answer", InputTokens: 1, OutputTokens: 2,
	}}

	// 记录 resolver 实际收到的 model
	var gotRouteModel string
	a, registry := newHandlerTestApp(t, resolveFunc(func(_ context.Context, _ int64, model string, _ int) ([]routing.Ability, error) {
		gotRouteModel = model
		return []routing.Ability{{ID: 0, Provider: "fake-gray", ProviderModel: "gray-upstream-model"}}, nil
	}), nil)
	registerFake(registry, "fake-gray", gray)

	// 模拟 Gray 中间件命中后的 context 状态
	r := mountChat(a, func(c *gin.Context) {
		c.Set("target_model", "gpt-4o-gray")
		c.Set("is_gray", true)
	})

	w := doChatRequest(t, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, w.Body.String())
	}
	if gotRouteModel != "gpt-4o-gray" {
		t.Errorf("路由解析 model = %q, want gpt-4o-gray（target_model）", gotRouteModel)
	}
	if gray.gotModel != "gray-upstream-model" {
		t.Errorf("上游 model = %q, want gray-upstream-model", gray.gotModel)
	}
	// 响应 model 仍是客户端请求模型
	if !bytes.Contains(w.Body.Bytes(), []byte(`"model":"test-model"`)) {
		t.Errorf("响应 model 应回填客户端请求模型: %s", w.Body.String())
	}
}
