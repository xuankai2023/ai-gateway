package handler

// 阶段 4 验收级端到端测试：真实 HTTP Transport + 真实 openai Provider + 真实 relay/handler 链路，
// 只把上游替换成 httptest 慢/流式服务器，验证分层超时闭环：
//  1. 慢上游非流式：attempt_timeout 内 504
//  2. 流式长回答：跨过 attempt_timeout 与 request_timeout 不被误杀
//  3. 请求级超时：request_timeout 切断慢响应
//
// 阶段 14：全局钩子（包级 attempt 上限 / 解析函数 / 全局注册表）全部拆除，
// 改为 App 组装：Relay 实例带 attemptTimeout，Resolver 注入 fake，Provider 注册表实例级。

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/middleware"
	"ai-gateway/apps/gateway/internal/relay"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/provider/openai"
	"github.com/gin-gonic/gin"

	"ai-gateway/apps/gateway/internal/app"
)

// newE2ERouter 组装带 Timeout 中间件的测试路由（真实 provider 注入实例级注册表）
func newE2ERouter(t *testing.T, providerType string, clients *provider.Clients, baseURL string,
	requestTimeout, attemptTimeout, streamMax time.Duration) *gin.Engine {
	t.Helper()

	cfg := config.Default()
	cfg.Gateway.RequestTimeout = requestTimeout.String()
	cfg.Gateway.AttemptTimeout = attemptTimeout.String()
	cfg.Gateway.StreamMaxDuration = streamMax.String()

	registry := provider.NewRegistry()
	real := openai.New(baseURL, "sk-e2e", clients)
	registry.Register(providerType, func() provider.Provider { return real })

	r := relay.New(nil, circuitbreaker.NewManager(circuitbreaker.Options{}), registry, nil, attemptTimeout)
	t.Cleanup(r.Close)

	a := &app.App{
		Config: cfg,
		Relay:  r,
		Resolver: resolveFunc(func(context.Context, int64, string, int) ([]routing.Ability, error) {
			return []routing.Ability{{ID: 0, Provider: providerType, ProviderModel: "gpt-4o-mini"}}, nil
		}),
	}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Set("api_key_id", int64(2))
		c.Next()
	})
	v1 := engine.Group("/v1")
	v1.Use(middleware.Timeout(a))
	v1.POST("/chat/completions", ChatCompletion(a))
	return engine
}

func doE2ERequest(r *gin.Engine, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestE2E_SlowUpstream_NonStream504WithinAttemptTimeout(t *testing.T) {
	// 上游 sleep 10s（模拟验收场景的 sleep 120s），attempt_timeout 300ms（模拟 30s）
	// stall 通道由测试主动关闭，保证 httptest Close 不等满上游 sleep
	stall := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-stall:
		}
	}))
	defer func() { close(stall); upstream.Close() }()

	clients := provider.NewClients(300 * time.Millisecond)
	r := newE2ERouter(t, "e2e-slow-openai", clients, upstream.URL, 5*time.Second, 300*time.Millisecond, 10*time.Second)

	start := time.Now()
	w := doE2ERequest(r, `{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}`)
	elapsed := time.Since(start)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504, body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "upstream_timeout") {
		t.Errorf("body 应含 upstream_timeout: %s", w.Body.String())
	}
	// 应在 attempt_timeout(300ms) 量级被切断，远小于上游 sleep 10s 与 request_timeout 5s
	if elapsed < 200*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("504 耗时 %v, 应在 ~300ms（attempt_timeout）量级", elapsed)
	}
}

func TestE2E_LongStreamSurvivesBeyondAttemptAndRequestTimeout(t *testing.T) {
	// 上游流式持续 ~3s：首帧立即，之后每 100ms 一帧共 30 帧，最后 finish_reason + [DONE]
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for i := 0; i < 30; i++ {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x%d\"}}]}\n\n", i)
			flusher.Flush()
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":30}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	clients := provider.NewClients(300 * time.Millisecond) // 非流式总超时不应误伤流式
	// request_timeout 800ms < 流总时长 3s：若流式被错用 request_timeout 会被误杀
	// attempt_timeout 1s 只约束建连+首帧（首帧立即到达），不约束整流
	r := newE2ERouter(t, "e2e-stream-openai", clients, upstream.URL, 800*time.Millisecond, time.Second, 10*time.Second)

	start := time.Now()
	w := doE2ERequest(r, `{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	elapsed := time.Since(start)

	body := w.Body.String()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, body)
	}
	if !strings.Contains(body, "x29") {
		t.Errorf("应收到末尾帧 x29（流跑满 ~3s）: %s", body[len(body)-min(len(body), 400):])
	}
	if strings.Contains(body, "stream_max_duration_exceeded") {
		t.Errorf("3s 长回答不应被 10s 兜底误杀: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Errorf("应以 [DONE] 收尾: %q", body[len(body)-min(len(body), 200):])
	}
	// 证明流跨过了 request_timeout(800ms) 与 attempt_timeout(1s) 仍在继续
	if elapsed < 2500*time.Millisecond {
		t.Errorf("流式耗时 %v < 2.5s, 疑似被超时层提前切断", elapsed)
	}
}

func TestE2E_RequestTimeoutCutsSlowResponse(t *testing.T) {
	// 上游 2s 才响应：attempt_timeout(5s) 与非流式 client 总超时(5s) 都不是约束，
	// request_timeout(400ms) 应先行切断
	stall := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-stall:
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"late"}}]}`)
	}))
	defer func() { close(stall); upstream.Close() }()

	clients := provider.NewClients(5 * time.Second)
	r := newE2ERouter(t, "e2e-req-openai", clients, upstream.URL, 400*time.Millisecond, 5*time.Second, 10*time.Second)

	start := time.Now()
	w := doE2ERequest(r, `{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}`)
	elapsed := time.Since(start)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504（request_timeout 切断）, body=%s", w.Code, w.Body.String())
	}
	if elapsed < 300*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("504 耗时 %v, 应在 ~400ms（request_timeout）量级而非上游 2s", elapsed)
	}
}
