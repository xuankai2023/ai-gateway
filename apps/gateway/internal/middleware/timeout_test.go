package middleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/config"
	"github.com/gin-gonic/gin"
)

// deadlineProbe 记录下游看到的 ctx 截止时间（request ctx 与 otel_ctx 两份）
type deadlineProbe struct {
	reqDeadline   time.Duration // c.Request.Context() 的剩余时限
	otelDeadline  time.Duration // otel_ctx 的剩余时限
	otelIsTimeout bool          // otel_ctx 与 request ctx 是否为同一个（带超时的）ctx
	bodyAfter     string        // 下游读到的 body（验证探读后回填）
}

// timeoutTestApp 用指定超时字符串构造测试 App（阶段 14：超时取自 a.Config）
func timeoutTestApp(requestTimeout, streamMaxDuration string) *app.App {
	cfg := config.Default()
	cfg.Gateway.RequestTimeout = requestTimeout
	cfg.Gateway.StreamMaxDuration = streamMaxDuration
	return &app.App{Config: cfg}
}

func newTimeoutRouter(requestTimeout, streamMaxDuration string, probe *deadlineProbe) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(timeoutTestApp(requestTimeout, streamMaxDuration)))
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		record(c, probe)
		c.Status(http.StatusOK)
	})
	r.GET("/v1/models", func(c *gin.Context) {
		record(c, probe)
		c.Status(http.StatusOK)
	})
	return r
}

func record(c *gin.Context, probe *deadlineProbe) {
	if d, ok := c.Request.Context().Deadline(); ok {
		probe.reqDeadline = time.Until(d)
	}
	if v, exists := c.Get("otel_ctx"); exists {
		if octx, ok := v.(context.Context); ok {
			if d, ok := octx.Deadline(); ok {
				probe.otelDeadline = time.Until(d)
			}
			probe.otelIsTimeout = octx == c.Request.Context()
		}
	}
	if c.Request.Body != nil {
		body, _ := io.ReadAll(c.Request.Body)
		probe.bodyAfter = string(body)
	}
}

func TestTimeout_NonStreamChatUsesRequestTimeout(t *testing.T) {
	probe := &deadlineProbe{}
	r := newTimeoutRouter("2s", "30s", probe)

	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if probe.reqDeadline < 1500*time.Millisecond || probe.reqDeadline > 2*time.Second {
		t.Errorf("非流式 request ctx 剩余时限 = %v, want ~2s（request_timeout）", probe.reqDeadline)
	}
	if probe.bodyAfter != string(body) {
		t.Errorf("body 探读后未回填: got %q", probe.bodyAfter)
	}
}

func TestTimeout_StreamChatUsesStreamMaxDuration(t *testing.T) {
	probe := &deadlineProbe{}
	r := newTimeoutRouter("2s", "30s", probe)

	body := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if probe.reqDeadline < 29*time.Second || probe.reqDeadline > 30*time.Second {
		t.Errorf("流式 request ctx 剩余时限 = %v, want ~30s（stream_max_duration）", probe.reqDeadline)
	}
	if probe.bodyAfter != string(body) {
		t.Errorf("body 探读后未回填: got %q", probe.bodyAfter)
	}
}

func TestTimeout_OtelCtxCarriesTimeout(t *testing.T) {
	probe := &deadlineProbe{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// 模拟 Trace 中间件先注入 otel_ctx（携带下游想保留的 span 上下文）
	r.Use(func(c *gin.Context) {
		c.Set("otel_ctx", c.Request.Context())
		c.Next()
	})
	r.Use(Timeout(timeoutTestApp("2s", "30s")))
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		record(c, probe)
		c.Status(http.StatusOK)
	})

	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if probe.otelDeadline < 1500*time.Millisecond || probe.otelDeadline > 2*time.Second {
		t.Errorf("otel_ctx 剩余时限 = %v, want ~2s（handler 经 requestContext() 拿到超时 ctx）", probe.otelDeadline)
	}
	if !probe.otelIsTimeout {
		t.Error("otel_ctx 与 request ctx 不是同一个 ctx（两份应都带超时）")
	}
}

func TestTimeout_NonChatPathUsesRequestTimeout(t *testing.T) {
	probe := &deadlineProbe{}
	r := newTimeoutRouter("2s", "30s", probe)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if probe.reqDeadline < 1500*time.Millisecond || probe.reqDeadline > 2*time.Second {
		t.Errorf("非 chat 路径剩余时限 = %v, want ~2s（request_timeout）", probe.reqDeadline)
	}
}

func TestTimeout_InvalidJSONFallsBackToRequestTimeout(t *testing.T) {
	probe := &deadlineProbe{}
	r := newTimeoutRouter("2s", "30s", probe)

	body := []byte(`{"model":`) // 非法 JSON
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if probe.reqDeadline < 1500*time.Millisecond || probe.reqDeadline > 2*time.Second {
		t.Errorf("非法 JSON 剩余时限 = %v, want ~2s（按非流式处理，400 留给 handler）", probe.reqDeadline)
	}
	if probe.bodyAfter != string(body) {
		t.Errorf("body 探读后未回填: got %q", probe.bodyAfter)
	}
}

func TestTimeout_ZeroOptionsUseDefaults(t *testing.T) {
	probe := &deadlineProbe{}
	r := newTimeoutRouter("", "", probe)

	body := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if probe.reqDeadline < 299*time.Second || probe.reqDeadline > 300*time.Second {
		t.Errorf("零值配置流式剩余时限 = %v, want ~300s（fallbackStreamMaxDuration）", probe.reqDeadline)
	}
}

func TestTimeout_UpstreamSlowRequestCtxExpires(t *testing.T) {
	// 端到端语义：下游卡在 ctx 上，request_timeout 到期后应被切断
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Timeout(timeoutTestApp("60ms", "1m")))
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		select {
		case <-c.Request.Context().Done():
			c.Status(http.StatusGatewayTimeout)
		case <-time.After(10 * time.Second):
			c.Status(http.StatusOK)
		}
	})

	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	start := time.Now()
	r.ServeHTTP(w, req)
	elapsed := time.Since(start)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504（request_timeout 切断下游）", w.Code)
	}
	if elapsed > time.Second {
		t.Errorf("切断耗时 %v, 应在 60ms 左右", elapsed)
	}
}
