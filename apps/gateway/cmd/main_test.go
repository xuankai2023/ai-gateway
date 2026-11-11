package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/provider"
)

// newProbeTestApp 构造探针测试用 App（DB/Redis 为 nil，验证就绪探针的依赖检查分支）
func newProbeTestApp(t *testing.T) *app.App {
	t.Helper()
	a := app.New(app.Options{Config: config.Default()})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})
	return a
}

// TestLivezAlwaysOK 存活探针：不查依赖，未就绪/关停中也返回 200
func TestLivezAlwaysOK(t *testing.T) {
	ready.Store(false)
	t.Cleanup(func() { ready.Store(false) })

	w := httptest.NewRecorder()
	newRouter(newProbeTestApp(t)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("livez status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"status":"alive"`) {
		t.Errorf("livez body 应包含 status alive: %s", w.Body.String())
	}
}

// TestReadyzNotReady 就绪探针：ready=false（关停中）立即 503 摘流量
func TestReadyzNotReady(t *testing.T) {
	ready.Store(false)

	w := httptest.NewRecorder()
	newRouter(newProbeTestApp(t)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"reason":"shutting_down"`) {
		t.Errorf("readyz body 应包含 shutting_down: %s", w.Body.String())
	}
}

// TestReadyzWithoutPool 就绪探针：ready 后 DB 未初始化返回 503
func TestReadyzWithoutPool(t *testing.T) {
	ready.Store(true)
	t.Cleanup(func() { ready.Store(false) })

	w := httptest.NewRecorder()
	newRouter(newProbeTestApp(t)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"db":"uninitialized"`) {
		t.Errorf("readyz body 应说明 DB 未初始化: %s", w.Body.String())
	}
}

// TestHealthzAlias 旧 /healthz 保留为 /readyz 别名（一个版本后删除）
func TestHealthzAlias(t *testing.T) {
	ready.Store(true)
	t.Cleanup(func() { ready.Store(false) })

	w := httptest.NewRecorder()
	newRouter(newProbeTestApp(t)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz status = %d, want 503（与 readyz 一致）", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"db":"uninitialized"`) {
		t.Errorf("healthz body 应与 readyz 一致: %s", w.Body.String())
	}
}

func TestBodyLimitUnifiedContract(t *testing.T) {
	// Content-Length 超限 → 413；Trace 先于 bodyLimit，响应必带 request_id 且与响应头一致
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
	req.ContentLength = maxBodySize + 1
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	newRouter(newProbeTestApp(t)).ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413, body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", body)
	}
	if errObj["type"] != "invalid_request_error" || errObj["code"] != "request_body_too_large" {
		t.Errorf("error 契约不对: %v", errObj)
	}
	id, _ := body["request_id"].(string)
	if id == "" {
		t.Error("request_id 缺失（Trace 应先于 bodyLimit 注入 trace_id）")
	}
	if header := w.Header().Get("X-Request-Id"); header != id {
		t.Errorf("X-Request-Id = %q, 与 request_id = %q 不一致", header, id)
	}
}

func TestRegisterProvidersUnknownType(t *testing.T) {
	registry := provider.NewRegistry()
	registry.SetClients(provider.NewClients(30 * time.Second))

	err := registerProviders([]config.ProviderConfig{{Type: "foo"}}, registry, nil)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "unknown provider type: foo") {
		t.Errorf("错误信息不符合预期: %v", err)
	}
}

func TestRegisterProvidersOpenAI(t *testing.T) {
	t.Setenv("TEST_OPENAI_KEY", "sk-test")

	registry := provider.NewRegistry()
	registry.SetClients(provider.NewClients(30 * time.Second))

	err := registerProviders([]config.ProviderConfig{{
		Type:      "openai",
		BaseURL:   "https://api.openai.com/v1",
		APIKeyEnv: "TEST_OPENAI_KEY",
	}}, registry, nil)
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	p, err := registry.Get("openai")
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if p.Name() != "openai" {
		t.Errorf("Name = %q, want openai", p.Name())
	}
	if !contains(registry.AllRegistered(), "openai") {
		t.Errorf("AllRegistered 缺少 openai: %v", registry.AllRegistered())
	}
}

func contains(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}
