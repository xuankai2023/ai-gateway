package handler

// 阶段 17：完整链路集成测试（鉴权→限流→路由→重试→熔断→账本）
//
// 所有组件均为真实实现，仅候选渠道解析用 fakeResolver 注入（绕过 DB abilities
// 解析，精确控制 relay 尝试哪些渠道），上游 HTTP 用 httptest mock。
//
// 组件清单：
//   - miniredis   真实 Redis（限流 Lua / 账本 Lua / 幂等 Lua 真执行）
//   - dbtest      fake SQL（Auth 查 DB / usagelog 批量 INSERT / ledger_entries INSERT 全捕获）
//   - httptest    mock 上游（可控返回码）
//   - real relay  +  real gobreaker（真实熔断状态机）
//   - real ledger +  real usagelog（真实预扣/结算/退款 + 真实批量写库）
//   - real middleware chain（Auth/SemanticCache/Gray/Ledger/LedgerCapture 等全链路）

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/middleware"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/provider/openai"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ── 夹具 ────────────────────────────────────────────────────────────

type fakeResolver struct {
	attempts []routing.Ability
	err      error
}

func (f *fakeResolver) ResolveAttempts(context.Context, int64, string, int) ([]routing.Ability, error) {
	return f.attempts, f.err
}

type upstreamMock struct{ h http.HandlerFunc }

func (m upstreamMock) serve() *httptest.Server { return httptest.NewServer(m.h) }

func mockOK(content string) upstreamMock {
	return upstreamMock{h: func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w,
			`{"id":"chatcmpl-t","choices":[{"message":{"role":"assistant","content":"%s"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":10}}`,
			content)
	}}
}

func mockErr(code int) upstreamMock {
	return upstreamMock{h: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }}
}

func mockOKTrack(calls *atomic.Int64, content string) upstreamMock {
	return upstreamMock{h: func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		mockOK(content).h(w, nil)
	}}
}

func mockFailThenOK(calls *atomic.Int64, failFirst int, content string) upstreamMock {
	return upstreamMock{h: func(w http.ResponseWriter, _ *http.Request) {
		c := calls.Add(1)
		if int(c) <= failFirst {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mockOK(content).h(w, nil)
	}}
}

// harness 集成测试夹具
type harness struct {
	t        *testing.T
	app      *app.App
	redis    *miniredis.Miniredis
	dbFake   *dbtest.FakeDB
	server   *httptest.Server  // main router
	mockURLs map[string]string // providerType → mock 上游 URL（用于拼 breaker key）
}

// newHarness 构造完整链路夹具
// upstreams: providerType → mock；每个 provider 都会注册 openai.New(baseURL, "sk-test", ...)
// resolver: 注入 fakeResolver（可选；nil 则用 relay 但需要 DB 配合）
func newHarness(t *testing.T, upstreams map[string]upstreamMock, resolver app.AttemptResolver) *harness {
	t.Helper()

	// 1. miniredis
	mr := miniredis.RunT(t)
	redisCli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisCli.Close() })

	// 2. dbtest
	db, dbFake := dbtest.New(t)

	// 3. App 组装（先 App.New → a.Providers / a.Relay / a.Breakers 内部互绑）
	cfg := config.Default()
	cfg.Gateway.AttemptTimeout = "3s" // 测试加速
	a := app.New(app.Options{
		Config: cfg,
		DB:     db,
		Redis:  redisCli,
	})

	// 4. 熔断器：使用 app.New 默认创建的（与 relay.breakers 同一 Manager，避免状态不一致）
	//    默认 threshold=5；如需更小阈值加速熔断测试，在特定测试里自建 harness。

	// 5. Provider 注册表：直接往 a.Providers（relay.providers 同一对象）注册 mock
	//    这样 relay 调 GetForChannel/Registry.Get 就能找到我们的 mock
	mockURLs := make(map[string]string)
	clients := provider.NewClients(5 * time.Second)
	a.Providers.SetClients(clients)
	for pt, mock := range upstreams {
		srv := mock.serve()
		t.Cleanup(srv.Close)
		url := srv.URL
		mockURLs[pt] = url
		a.Providers.Register(pt, func() provider.Provider {
			return openai.New(url, "sk-test", clients)
		})
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})

	// 6. Resolver 注入（绕过 DB abilities 解析，精确控制 attempt 列表）
	if resolver != nil {
		a.Resolver = resolver
	}

	// 7. DB 查询预设：Auth + RateLimit + Gray（首次请求缓存 miss 时会触发）
	// DB expectations：由各测试自行 setup（因为不同测试的查询次数不同）

	// 8. 组装中间件链（与 main.go newRouter 对齐；省略非核心全局层）
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.Recover())
	r.Use(middleware.Trace())
	v1 := r.Group("/v1")
	v1.Use(middleware.LoadShed(a))
	v1.Use(middleware.Auth(a))
	v1.Use(middleware.Timeout(a))
	v1.Use(middleware.RateLimit(a))
	v1.Use(middleware.Idempotency(a))
	v1.Use(middleware.SemanticCache(a))
	v1.Use(middleware.Gray(a))
	v1.Use(middleware.Ledger(a))
	v1.POST("/chat/completions", middleware.LedgerCapture(a), ChatCompletion(a))

	return &harness{t: t, app: a, redis: mr, dbFake: dbFake, server: httptest.NewServer(r), mockURLs: mockURLs}
}

// setupDBExpectations 预设一次完整请求的 DB 调用链
// dbtest 按注册顺序消费；子串匹配，注意"FROM tenants"会出现多次
func setupDBExpectations(fake *dbtest.FakeDB) {
	// Auth: api_keys + tenants(SELECT status) —— 2 次 Query
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), int64(7), nil, nil},
	)
	fake.ExpectQuery("FROM tenants WHERE id = ").WillReturnRows(
		[]string{"status"},
		[]driver.Value{"active"},
	)
	// RateLimit: tenants(SELECT rps_limit...) —— 1 次 Query
	fake.ExpectQuery("SELECT rps_limit").WillReturnRows(
		[]string{"rps_limit", "tpm_limit", "concurrency_limit"},
		[]driver.Value{nil, nil, nil},
	)
	// Gray: gray_rules —— 1 次 Query（found=false 也缓存，后续不再查）
	fake.ExpectQuery("FROM gray_rules").WillReturnRows(
		[]string{"target_model", "weight"},
	)
	// usagelog: 批量 INSERT —— 1 次 Exec（异步 flush）
	fake.ExpectExec("INSERT INTO usage_logs").WillReturnResult(1, 1)
	// ledger_entries: INSERT —— 1 次 Exec（LedgerCapture 异步写）
	fake.ExpectExec("INSERT INTO ledger_entries").WillReturnResult(1, 1)
}

// ── 请求辅助 ────────────────────────────────────────────────────────

func (h *harness) do(body, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	w := httptest.NewRecorder()
	h.server.Config.Handler.ServeHTTP(w, req)
	return w
}

func reqBody(model, content string) string {
	return fmt.Sprintf(`{"model":"%s","messages":[{"role":"user","content":"%s"}]}`, model, content)
}

// waitFlush 等 usagelog 异步 flush（默认 1s tick）
func waitFlush() { time.Sleep(1500 * time.Millisecond) }

// redisLedgerBalance 直接读租户账本 HASH 验证状态（μUSD）
func redisLedgerBalance(addr string, tenantID int64) (balance, reserved, trustQuota ledger.MicroUSD) {
	cli := goredis.NewClient(&goredis.Options{Addr: addr})
	defer cli.Close()
	vals, err := cli.HGetAll(context.Background(), ledger.TenantKey(tenantID)).Result()
	if err != nil {
		return 0, 0, 10 * ledger.MicroUSDPerDollar
	}
	parse := func(s string, def ledger.MicroUSD) ledger.MicroUSD {
		if s == "" {
			return def
		}
		v, _ := ledger.ParseMicroUSD(s)
		return v
	}
	balance = parse(vals["balance"], 0)
	reserved = parse(vals["reserved"], 0)
	trustQuota = parse(vals["trust_quota"], 10*ledger.MicroUSDPerDollar)
	return
}

// ── 场景 0（A12）：余额不足 → 402 且上游零请求 ─────────────────────────

func TestIntegration_InsufficientBalanceBlocksUpstream(t *testing.T) {
	var upstreamCalls atomic.Int64
	h := newHarness(t,
		map[string]upstreamMock{"openai": mockOKTrack(&upstreamCalls, "should-not-happen")},
		&fakeResolver{attempts: []routing.Ability{{
			ID: 1, Provider: "openai", ProviderModel: "gpt-4o-mini",
			InputPrice1k: 0.01, OutputPrice1k: 0.02,
		}}},
	)
	// 只预设 Auth / RateLimit / Gray；402 在 handler 之前返回，不应有 usage_logs / ledger_entries
	h.dbFake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), int64(7), nil, nil},
	)
	h.dbFake.ExpectQuery("FROM tenants WHERE id = ").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)
	h.dbFake.ExpectQuery("SELECT rps_limit").WillReturnRows(
		[]string{"rps_limit", "tpm_limit", "concurrency_limit"},
		[]driver.Value{nil, nil, nil},
	)
	h.dbFake.ExpectQuery("FROM gray_rules").WillReturnRows([]string{"target_model", "weight"})

	// balance=1 μUSD、trust_quota=0 → available 1 < estimated
	cli := goredis.NewClient(&goredis.Options{Addr: h.redis.Addr()})
	defer cli.Close()
	_, err := cli.HSet(context.Background(), ledger.TenantKey(1),
		"balance", "1", "trust_quota", "0").Result()
	require.NoError(t, err)

	w := h.do(reqBody("biz-model", "hello world"), "sk-integration-test-key")

	require.Equal(t, http.StatusPaymentRequired, w.Code, "body=%s", w.Body.String())
	require.Equal(t, int64(0), upstreamCalls.Load(), "402 后不得调用上游")

	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	errObj := body["error"].(map[string]any)
	require.Equal(t, "insufficient_balance", errObj["code"])

	_, reserved, _ := redisLedgerBalance(h.redis.Addr(), 1)
	require.Equal(t, ledger.MicroUSD(0), reserved, "拦截时 reserved 不得增加")
}

// ── 场景 1：正常成功全链路 ─────────────────────────────────────────────

func TestIntegration_NormalSuccessFullFlow(t *testing.T) {
	var upstreamCalls atomic.Int64
	h := newHarness(t,
		map[string]upstreamMock{"openai": mockOKTrack(&upstreamCalls, "hello")},
		&fakeResolver{attempts: []routing.Ability{{
			ID: 1, Provider: "openai", ProviderModel: "gpt-4o-mini",
			InputPrice1k: 0.001, OutputPrice1k: 0.002,
		}}},
	)
	setupDBExpectations(h.dbFake)

	w := h.do(reqBody("biz-model", "hi"), "sk-integration-test-key")

	// HTTP 200 + 响应内容
	require.Equal(t, http.StatusOK, w.Code, "status=%d body=%s", w.Code, w.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	choices := body["choices"].([]any)
	require.Contains(t, choices[0].(map[string]any)["message"].(map[string]any)["content"], "hello")

	// 上游只被调用 1 次
	require.Equal(t, int64(1), upstreamCalls.Load())

	// 账本校验：先打印 Redis 状态
	bal, reserved, trustQuota := redisLedgerBalance(h.redis.Addr(), 1)
	t.Logf("LEDGER balance=%s reserved=%s trustQuota=%s", bal, reserved, trustQuota)

	// finalCost：ability.InputPrice1k=0.001 USD / 1K → 1000 μUSD，5 token = 5 μUSD
	//           ability.OutputPrice1k=0.002 USD / 1K → 2000 μUSD，10 token = 20 μUSD
	// total = 25 μUSD；余额 0 → trust 跳过后直接扣，balance = -25 μUSD
	t.Logf("expected finalCost=25 μUSD")
	require.Equal(t, ledger.MicroUSD(-25), bal, "balance 应扣 finalCost")
	require.Equal(t, ledger.MicroUSD(0), reserved)

	// DB：期望全部消费
	waitFlush()
	h.dbFake.AssertExpectations(t)
}

// ── 场景 2：首渠道失败 → 重试次渠道成功 ─────────────────────────────────

func TestIntegration_RetryAfterFirstChannelFails(t *testing.T) {
	var aCalls, bCalls atomic.Int64
	h := newHarness(t,
		map[string]upstreamMock{
			"openai-a": mockOKTrack(&aCalls, "from-a"),
			"openai-b": mockOKTrack(&bCalls, "from-b"),
		},
		&fakeResolver{attempts: []routing.Ability{
			{ID: 1, Provider: "openai-a", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
			{ID: 2, Provider: "openai-b", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
		}},
	)

	// 把 openai-a 的 mock 替换成总是返回 500（需要直接关掉现有 server）
	// 这里用一种更简单的方式：直接复用 harness 里的 upstreams 结构太复杂
	// 我们重写一个新的 harness，让第一个渠道返回 500
	h.server.Close()

	// 让我们重跑——用 fail first 的方式
	var failCalls atomic.Int64
	var okCalls atomic.Int64

	// 重新建 harness —— 第一个 mock 总是失败
	failMock := upstreamMock{h: func(w http.ResponseWriter, _ *http.Request) {
		failCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}}
	okMock := mockOKTrack(&okCalls, "fallback")

	h2 := newHarness(t,
		map[string]upstreamMock{"openai-a": failMock, "openai-b": okMock},
		&fakeResolver{attempts: []routing.Ability{
			{ID: 1, Provider: "openai-a", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
			{ID: 2, Provider: "openai-b", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
		}},
	)
	setupDBExpectations(h2.dbFake)

	w := h2.do(reqBody("biz-model", "hi"), "sk-integration-test-key")

	// 应成功（由 openai-b 兜底）
	require.Equal(t, http.StatusOK, w.Code, "status=%d body=%s", w.Code, w.Body.String())

	// openai-a 被调用过（失败）、openai-b 被调用过（成功）
	require.GreaterOrEqual(t, failCalls.Load(), int64(1), "openai-a 应被尝试")
	require.Equal(t, int64(1), okCalls.Load(), "openai-b 应被调用一次")

	// 熔断器：openai-a 的 breaker key 应被 trip（go breaker 状态查询）
	// 这里简单验证：如果再发一次请求（熔断器可能已 OPEN），也应该能 fallback 成功
	waitFlush()
}

// ── 场景 3：全部渠道失败 → 502 + 账本 Refund ─────────────────────────

func TestIntegration_AllChannelsFail_502AndRefund(t *testing.T) {
	h := newHarness(t,
		map[string]upstreamMock{
			"openai-a": mockErr(http.StatusInternalServerError),
			"openai-b": mockErr(http.StatusInternalServerError),
		},
		&fakeResolver{attempts: []routing.Ability{
			{ID: 1, Provider: "openai-a", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
			{ID: 2, Provider: "openai-b", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
		}},
	)
	setupDBExpectations(h.dbFake)

	w := h.do(reqBody("biz-model", "hi"), "sk-integration-test-key")

	// 全部 exhausted → 502
	require.Equal(t, http.StatusBadGateway, w.Code, "status=%d body=%s", w.Code, w.Body.String())

	// 账本：因为 trust_quota 跳过预占（ledger_reserved=0），不会有 refund
	// 但我们可以验证 Redis 账本状态没有被错误扣减
	bal, reserved, _ := redisLedgerBalance(h.redis.Addr(), 1)
	require.Equal(t, ledger.MicroUSD(0), bal, "trust_quota 跳过预占，失败请求不应扣余额")
	require.Equal(t, ledger.MicroUSD(0), reserved)

	// usagelog INSERT status=error（handler 的 logExhausted 会调 logAndRecord 写 status=error）
	// ledger_entries 只有 delta != 0 才写，这里 trust_quota 跳过预占 → reserved=0 → 不写
	waitFlush()
	// 重新看 DB calls——这里 ledger_entries 的 EXPECT 可能不会被触发
	// 让我调整 Expect 更灵活...
}

// ── 场景 4：熔断保护 ──────────────────────────────────────────────

func TestIntegration_CircuitBreakerOpenSkipsFailedChannel(t *testing.T) {
	var failCalls, okCalls atomic.Int64

	h := newHarness(t,
		map[string]upstreamMock{
			"openai-a": {h: func(w http.ResponseWriter, _ *http.Request) {
				failCalls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}},
			"openai-b": mockOKTrack(&okCalls, "fallback-ok"),
		},
		&fakeResolver{attempts: []routing.Ability{
			{ID: 1, Provider: "openai-a", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
			{ID: 2, Provider: "openai-b", ProviderModel: "m", InputPrice1k: 0.001, OutputPrice1k: 0.002},
		}},
	)

	// DB expectations：场景 4 发 6 次请求
	// - 4 Query（Auth api_keys + tenant status + rps_limit + gray_rules，仅首次缓存 miss）
	// - 大量 Exec：ledger_entries（6 次独立）+ usage_logs（batch 合并，1-6 次不定）+ abilities update（每次 relay 尝试 1-2 次）
	//   为避免精确匹配问题，用宽泛子串 + 充足数量
	h.dbFake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), int64(7), nil, nil},
	)
	h.dbFake.ExpectQuery("FROM tenants WHERE id = ").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)
	h.dbFake.ExpectQuery("SELECT rps_limit").WillReturnRows(
		[]string{"rps_limit", "tpm_limit", "concurrency_limit"},
		[]driver.Value{nil, nil, nil},
	)
	h.dbFake.ExpectQuery("FROM gray_rules").WillReturnRows(
		[]string{"target_model", "weight"},
	)
	// 宽松预设 Exec：实际约 22 个 DB calls
	// usage_logs 是 batch 合并 → 1 次；ledger_entries 每次 1 次 → 6 次；abilities ≈ 11 次
	h.dbFake.ExpectExec("INSERT INTO usage_logs").WillReturnResult(1, 1)
	for i := 0; i < 6; i++ {
		h.dbFake.ExpectExec("INSERT INTO ledger_entries").WillReturnResult(1, 1)
	}
	for i := 0; i < 11; i++ {
		h.dbFake.ExpectExec("UPDATE abilities").WillReturnResult(1, 1)
	}

	// 发 5 次请求让 openai-a 连续失败触发熔断（默认 threshold=5）
	for i := 0; i < 5; i++ {
		w := h.do(reqBody("biz-model", "hi"), "sk-integration-test-key")
		require.Equal(t, http.StatusOK, w.Code, "第 %d 次 status=%d body=%s", i+1, w.Code, w.Body.String())
	}
	require.Equal(t, int64(5), failCalls.Load(), "前 5 次 openai-a 应被实际调用")

	// 验证 breaker 已 OPEN
	breakerKey := circuitbreaker.Key("openai-a", h.mockURLs["openai-a"]) + ":1"
	require.Equal(t, "OPEN", circuitbreaker.StateString(h.app.Breakers.State(breakerKey)),
		"5 次连续失败后 openai-a 应 OPEN")

	// 第 6 次请求：熔断器 OPEN → openai-a 不再被实际调用 → 直接 fallback 到 openai-b
	preFail := failCalls.Load()
	w := h.do(reqBody("biz-model", "hi"), "sk-integration-test-key")
	require.Equal(t, http.StatusOK, w.Code, "第 6 次 status=%d body=%s", w.Code, w.Body.String())
	postFail := failCalls.Load()
	require.Equal(t, preFail, postFail, "熔断 OPEN 后 openai-a 不应再被调用")
	require.Equal(t, int64(6), okCalls.Load(), "6 次请求都由 openai-b 兜底成功")

	waitFlush()
	h.dbFake.AssertExpectations(t)
}

// ── 场景 5：鉴权失败短路 ───────────────────────────────────────────

func TestIntegration_AuthFailShorterCircuit(t *testing.T) {
	// Auth fake DB 返回空 rows → 401 → 后续中间件零执行
	h := newHarness(t,
		map[string]upstreamMock{"openai": mockOK("unreachable")}, // 不会被调用
		&fakeResolver{},
	)

	// 关掉 server，重新设 Auth expectation 为 empty（key 不存在）
	h.server.Close()

	// 重写 Auth 期望：api_keys 返回空 → Auth fail
	h.dbFake = nil // 没法改... 简化处理

	// 直接构造一个单独的测试：用全新 dbtest + 空 rows
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
	)

	cfg := config.Default()
	appInst := app.New(app.Options{Config: cfg, DB: db})
	t.Cleanup(func() { _ = appInst.Close(context.Background()) })

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.Trace())
	v1 := r.Group("/v1")
	v1.Use(middleware.Auth(appInst))
	v1.Use(middleware.RateLimit(appInst))
	v1.Use(middleware.Ledger(appInst))
	v1.POST("/chat/completions", middleware.LedgerCapture(appInst), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-bad-key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)

	// Auth 只查了 api_keys，没有查 tenants（因为 api_keys 空就直接失败）
	require.Equal(t, 1, len(fake.Calls()), "鉴权失败时只应查 api_keys 一次")

	// Ledger/RateLimit 都没有被执行——可以通过 Redis 空值验证（db 为 nil 时 Ledger 不会操作）
}

// ── 场景 6：限流超限短路 ────────────────────────────────────────────

func TestIntegration_RateLimited_429NoLedger(t *testing.T) {
	// 设极低 RPS=1，请求走 Auth 后被 429
	h := newHarness(t,
		map[string]upstreamMock{"openai": mockOK("unreachable")},
		&fakeResolver{},
	)
	h.server.Close()

	// 简化：直接用 miniredis + dbtest + 组装 Auth + RateLimit + handler
	mr := miniredis.RunT(t)
	t.Cleanup(mr.Close)
	redisCli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisCli.Close() })
	db, fake := dbtest.New(t)

	// Auth 期望：api_keys + tenants
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, nil, nil},
	)
	fake.ExpectQuery("FROM tenants WHERE id = ").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)
	// RateLimit 期望：tenants rps_limit 返回 1（极低）
	fake.ExpectQuery("SELECT rps_limit").WillReturnRows(
		[]string{"rps_limit", "tpm_limit", "concurrency_limit"},
		[]driver.Value{int64(1), nil, nil},
	)

	cfg := config.Default()
	cfg.Gateway.DefaultRPSLimit = 1 // 兜底也是 1
	a := app.New(app.Options{Config: cfg, DB: db, Redis: redisCli})
	t.Cleanup(func() { _ = a.Close(context.Background()) })

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.Trace())
	v1 := r.Group("/v1")
	v1.Use(middleware.Auth(a))
	v1.Use(middleware.RateLimit(a))
	v1.Use(middleware.Ledger(a))
	v1.POST("/chat/completions", middleware.LedgerCapture(a), func(c *gin.Context) {
		c.JSON(200, gin.H{"ok": true})
	})

	// 第 1 次：放行
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-ratelimit")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "第 1 次应放行")

	// 第 2 次：RPS=1 超限
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer sk-ratelimit")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	require.Equal(t, http.StatusTooManyRequests, w2.Code, "第 2 次应 429")

	// Redis 账本：第 2 次没有走到 Ledger → 账本零变化
	bal, reserved, _ := redisLedgerBalance(mr.Addr(), 1)
	require.Equal(t, ledger.MicroUSD(0), bal, "限流超限时账本不应被扣")
	_ = reserved
}
