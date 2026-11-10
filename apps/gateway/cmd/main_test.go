package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-gateway/pkg/config"
	"ai-gateway/pkg/db"
	"ai-gateway/pkg/provider"
)

func TestHealthzWithoutPool(t *testing.T) {
	db.Close()

	w := httptest.NewRecorder()
	newRouter().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"db":"uninitialized"`) {
		t.Errorf("body 应说明 DB 未初始化: %s", w.Body.String())
	}
}

func TestRegisterProvidersUnknownType(t *testing.T) {
	err := registerProviders([]config.ProviderConfig{{Type: "foo"}})
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "unknown provider type: foo") {
		t.Errorf("错误信息不符合预期: %v", err)
	}
}

func TestRegisterProvidersOpenAI(t *testing.T) {
	t.Setenv("TEST_OPENAI_KEY", "sk-test")

	err := registerProviders([]config.ProviderConfig{{
		Type:      "openai",
		BaseURL:   "https://api.openai.com/v1",
		APIKeyEnv: "TEST_OPENAI_KEY",
	}})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	p, err := provider.Factory.Get("openai")
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if p.Name() != "openai" {
		t.Errorf("Name = %q, want openai", p.Name())
	}
	if !contains(provider.Factory.AllRegistered(), "openai") {
		t.Errorf("AllRegistered 缺少 openai: %v", provider.Factory.AllRegistered())
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
