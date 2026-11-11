package vault

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newFakeVault 起一个 KV v2 假服务：/v1/{path} → {"data":{"data":{...}}}
func newFakeVault(t *testing.T, secrets map[string]map[string]any, wantToken string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if wantToken != "" && r.Header.Get("X-Vault-Token") != wantToken {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []string{"permission denied"}})
			return
		}
		path := r.URL.Path[len("/v1/"):]
		data, ok := secrets[path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []string{"not found"}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestReadSecretAPIKeyField(t *testing.T) {
	srv := newFakeVault(t, map[string]map[string]any{
		"secret/data/ai-gateway/openai-1": {"api_key": "sk-real-from-vault"},
	}, "test-token")

	c := New(srv.URL, "test-token", time.Second)
	got, err := c.ReadSecret(context.Background(), "secret/data/ai-gateway/openai-1")
	if err != nil {
		t.Fatalf("ReadSecret 失败: %v", err)
	}
	if got != "sk-real-from-vault" {
		t.Errorf("got %q, want sk-real-from-vault", got)
	}
}

func TestReadSecretValueFallback(t *testing.T) {
	srv := newFakeVault(t, map[string]map[string]any{
		"secret/data/k": {"value": "v-secret"},
	}, "")

	c := New(srv.URL, "", time.Second)
	got, err := c.ReadSecret(context.Background(), "secret/data/k")
	if err != nil || got != "v-secret" {
		t.Fatalf("got %q err=%v, want v-secret", got, err)
	}
}

func TestReadSecretNotFound(t *testing.T) {
	srv := newFakeVault(t, map[string]map[string]any{}, "tok")
	c := New(srv.URL, "tok", time.Second)
	if _, err := c.ReadSecret(context.Background(), "secret/data/missing"); err == nil {
		t.Fatal("缺失路径应报错")
	}
}

func TestReadSecretForbidden(t *testing.T) {
	srv := newFakeVault(t, map[string]map[string]any{}, "right-token")
	c := New(srv.URL, "wrong-token", time.Second)
	if _, err := c.ReadSecret(context.Background(), "secret/data/k"); err == nil {
		t.Fatal("token 错误应报错")
	}
}

// 未配置 addr 的零值客户端必须报错而不是 panic（调用方按"解析失败跳过"处理）
func TestReadSecretUnconfigured(t *testing.T) {
	var c *Client
	if _, err := c.ReadSecret(context.Background(), "x"); err == nil {
		t.Fatal("nil client 应报错")
	}
}
