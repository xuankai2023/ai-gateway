package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/ledger"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// newIdemTest 构建 miniredis + App（Redis 挂到 App，阶段 14 注入形式）
func newIdemTest(t *testing.T) (*miniredis.Miniredis, *app.App) {
	t.Helper()
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { cli.Close() })
	return s, newTestApp(t, nil, cli)
}

// buildIdemEngine 组装测试链路：Trace → tenant → Idempotency → handler
func buildIdemEngine(a *app.App, h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Set("api_key_id", int64(2))
		c.Next()
	})
	r.Use(Idempotency(a))
	r.POST("/v1/chat/completions", h)
	return r
}

// doIdemRequest 带幂等键的 POST
func doIdemRequest(engine *gin.Engine, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(idempotencyHeader, key)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

const idemBody = `{"model":"demo","messages":[{"role":"user","content":"hello"}]}`

// 验收口径 1：同 key 重试 5 次只调上游一次，后续全部回放快照
func TestIdempotencyReplaysSnapshot(t *testing.T) {
	_, a := newIdemTest(t)

	var calls atomic.Int64
	engine := buildIdemEngine(a, func(c *gin.Context) {
		n := calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"n": n, "content": "hello"})
	})

	first := doIdemRequest(engine, "key-1", idemBody)
	if first.Code != http.StatusOK {
		t.Fatalf("首个请求 status = %d, body=%s", first.Code, first.Body.String())
	}
	if first.Header().Get("Idempotent-Replay") != "" {
		t.Error("首个请求不应带 Idempotent-Replay")
	}

	for i := 0; i < 4; i++ {
		w := doIdemRequest(engine, "key-1", idemBody)
		if w.Code != http.StatusOK {
			t.Fatalf("重试 %d status = %d", i+2, w.Code)
		}
		if w.Header().Get("Idempotent-Replay") != "true" {
			t.Errorf("重试 %d 缺少 Idempotent-Replay: true", i+2)
		}
		if w.Body.String() != first.Body.String() {
			t.Errorf("重试 %d 响应与首个不一致: %q vs %q", i+2, w.Body.String(), first.Body.String())
		}
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("上游调用 %d 次, want 1", got)
	}
}

// 验收口径 2：并发同 key 恰有一个进入 handler，其余 409
func TestIdempotencyConcurrentConflict(t *testing.T) {
	_, a := newIdemTest(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int64
	engine := buildIdemEngine(a, func(c *gin.Context) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	const total = 10
	results := make(chan int, total)
	for i := 0; i < total; i++ {
		go func() { results <- doIdemRequest(engine, "same-key", idemBody).Code }()
	}

	// 等首个请求进入 handler（占位成功），其余 9 个应立即 409
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("首个请求未进入 handler")
	}

	conflicts := 0
	deadline := time.After(3 * time.Second)
	for conflicts < total-1 {
		select {
		case code := <-results:
			if code != http.StatusConflict {
				t.Fatalf("并发同 key 应 409, got %d", code)
			}
			conflicts++
		case <-deadline:
			t.Fatalf("只收到 %d 个 409，其余请求疑似被阻塞", conflicts)
		}
	}

	close(release)
	last := <-results
	if last != http.StatusOK {
		t.Fatalf("首个请求 status = %d, want 200", last)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler 调用 %d 次, want 1", got)
	}
}

// 流式请求带幂等键 → 400（不做快照）
func TestIdempotencyRejectsStream(t *testing.T) {
	_, a := newIdemTest(t)

	var calls atomic.Int64
	engine := buildIdemEngine(a, func(c *gin.Context) {
		calls.Add(1)
		c.Status(http.StatusOK)
	})

	w := doIdemRequest(engine, "stream-key", `{"model":"demo","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	assertIdemErrorCode(t, w, "idempotency_stream_unsupported")
	if calls.Load() != 0 {
		t.Error("流式幂等请求不应进入 handler")
	}
}

// 首请求失败释放占位：同 key 重试可以重新执行（不会被错误缓存）
func TestIdempotencyFailureReleasesPlaceholder(t *testing.T) {
	_, a := newIdemTest(t)

	var calls atomic.Int64
	engine := buildIdemEngine(a, func(c *gin.Context) {
		if calls.Add(1) == 1 {
			c.JSON(http.StatusBadGateway, gin.H{"error": "upstream down"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	first := doIdemRequest(engine, "retry-key", idemBody)
	if first.Code != http.StatusBadGateway {
		t.Fatalf("首次 status = %d, want 502", first.Code)
	}

	second := doIdemRequest(engine, "retry-key", idemBody)
	if second.Code != http.StatusOK {
		t.Fatalf("失败后重试 status = %d, want 200", second.Code)
	}
	if second.Header().Get("Idempotent-Replay") != "" {
		t.Error("失败释放后的重试应是真实执行，不应回放")
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("handler 调用 %d 次, want 2", got)
	}
}

// 验收口径 3：同 key 重试 5 次，账本只有一次 settle（余额只扣一次）
func TestIdempotencyLedgerSettlesOnce(t *testing.T) {
	s, a := newIdemTest(t)

	// 账本与幂等共用同一个 miniredis；余额 100 USD = 100,000,000 μUSD > trust_quota(10 USD)
	s.HSet(ledger.TenantKey(1), "balance", "100000000")

	var calls atomic.Int64
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Set("api_key_id", int64(2))
		c.Next()
	})
	r.Use(Idempotency(a))
	r.Use(Ledger(a))
	r.POST("/v1/chat/completions", LedgerCapture(a), func(c *gin.Context) {
		calls.Add(1)
		c.Set("ledger_final_cost", int64(20000))
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	for i := 0; i < 5; i++ {
		w := doIdemRequest(r, "ledger-key", idemBody)
		if w.Code != http.StatusOK {
			t.Fatalf("第 %d 次 status = %d", i+1, w.Code)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler 调用 %d 次, want 1", got)
	}
	balance, reserved, _, err := ledger.GetBalance(context.Background(), a.Redis, 1)
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// 预占 0.01（10,000）、实际 0.02（20,000）→ 差额补扣 10,000：100 - 0.01 = 99.99 USD
	if balance != ledger.MicroUSD(99_990_000) {
		t.Errorf("balance = %s USD, want 99.99（只结算一次）", balance)
	}
	if reserved != 0 {
		t.Errorf("reserved = %s USD, want 0", reserved)
	}
}

// assertIdemErrorCode 统一错误契约断言
func assertIdemErrorCode(t *testing.T, w *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v", err)
	}
	errObj, _ := body["error"].(map[string]any)
	if errObj["code"] != wantCode {
		t.Errorf("error.code = %v, want %s", errObj["code"], wantCode)
	}
	if body["request_id"] == "" || body["request_id"] == nil {
		t.Error("request_id 缺失")
	}
}

// 未带幂等键 → 完全透传，不占位不产生任何 Redis key
func TestIdempotencyNoKeyPassthrough(t *testing.T) {
	s, a := newIdemTest(t)

	var calls atomic.Int64
	engine := buildIdemEngine(a, func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	if w := doIdemRequest(engine, "", idemBody); w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if w := doIdemRequest(engine, "", idemBody); w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("无幂等键应每次真实执行, handler = %d, want 2", got)
	}
	if keys := s.Keys(); len(keys) != 0 {
		t.Errorf("无幂等键不应写 Redis, got keys=%v", keys)
	}
}
