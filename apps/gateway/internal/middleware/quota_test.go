package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/pricing"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type fakePricingResolver struct {
	row *pricing.ModelRow
	err error
}

func (f *fakePricingResolver) ResolveModelPricing(context.Context, int64, string) (*pricing.ModelRow, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.row, nil
}

type fakeAttemptResolver struct {
	attempts []routing.Ability
	err      error
}

func (f *fakeAttemptResolver) ResolveAttempts(context.Context, int64, string, int) ([]routing.Ability, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.attempts, nil
}

func newQuotaTestApp(t *testing.T, row *pricing.ModelRow, resolverAttempts []routing.Ability) (*app.App, *miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })

	a := newTestApp(t, nil, cli)
	a.Pricing = pricing.Default
	if row != nil {
		a.PricingResolver = &fakePricingResolver{row: row}
	}
	if resolverAttempts != nil {
		a.Resolver = &fakeAttemptResolver{attempts: resolverAttempts}
	}
	return a, mr, cli
}

// runQuota 走完整 gin 链路：tenant context → Ledger → handler
func runQuota(t *testing.T, a *app.App, body string, called *bool) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Next()
	})
	r.Use(Ledger(a))
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		if called != nil {
			*called = true
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func linearRow() *pricing.ModelRow {
	return &pricing.ModelRow{
		Model:         "demo",
		PricingModel:  pricing.PriceLinear,
		InputPrice1k:  1500,
		OutputPrice1k: 3000,
		PriceVersion:  7,
	}
}

// estimateFor 与中间件相同的估算口径：tokens + tokens/2
func estimateFor(t *testing.T, body string) ledger.MicroUSD {
	t.Helper()
	// "hello" → 4 + 5/4 = 5 输入 token
	tokens := int64(5)
	return pricing.EstimateLinear(tokens, tokens/2, 1500, 3000)
}

func TestLedgerQuotaReservesEstimatedCost(t *testing.T) {
	row := linearRow()
	a, _, cli := newQuotaTestApp(t, row, nil)
	// balance 100 USD > trust 10 USD → 走预占
	_, err := cli.HSet(context.Background(), ledger.TenantKey(1), "balance", "100000000").Result()
	require.NoError(t, err)

	body := `{"model":"demo","messages":[{"role":"user","content":"hello"}]}`
	called := false
	w := runQuota(t, a, body, &called)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, called)

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, estimateFor(t, body), reserved)
}

// A2 端到端：余额不足 → 402 insufficient_balance，handler 不被调用，reserved 不变
func TestLedgerQuota402WhenInsufficient(t *testing.T) {
	row := linearRow()
	a, _, cli := newQuotaTestApp(t, row, nil)
	// balance 1 μUSD、trust=0 → available 1 < estimated 13
	_, err := cli.HSet(context.Background(), ledger.TenantKey(1),
		"balance", "1", "trust_quota", "0").Result()
	require.NoError(t, err)

	body := `{"model":"demo","messages":[{"role":"user","content":"hello"}]}`
	called := false
	w := runQuota(t, a, body, &called)

	require.Equal(t, http.StatusPaymentRequired, w.Code, w.Body.String())
	require.False(t, called, "402 后不得进入 handler")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	errObj := resp["error"].(map[string]any)
	require.Equal(t, "billing_error", errObj["type"])
	require.Equal(t, "insufficient_balance", errObj["code"])

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), reserved, "被拦截时不得修改 reserved")
}

func TestLedgerQuotaTrustQuotaSkips(t *testing.T) {
	a, _, cli := newQuotaTestApp(t, linearRow(), nil)
	_, err := cli.HSet(context.Background(), ledger.TenantKey(1), "balance", "5000000").Result()
	require.NoError(t, err)

	called := false
	w := runQuota(t, a, `{"model":"demo","messages":[]}`, &called)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, called)

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), reserved)
}

// 定价估算失败 → fail-open（不预占、不阻断）
func TestLedgerQuotaEstimateFailureFailOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	a := newTestApp(t, nil, cli)
	a.PricingResolver = &fakePricingResolver{err: errors.New("model not found")}
	a.Resolver = nil

	_, err := cli.HSet(context.Background(), ledger.TenantKey(1), "balance", "100000000").Result()
	require.NoError(t, err)

	called := false
	w := runQuota(t, a, `{"model":"missing","messages":[]}`, &called)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, called)

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), reserved)
}

// models 无记录时回退 abilities 单价估算
func TestLedgerQuotaFallsBackToAbilities(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	a := newTestApp(t, nil, cli)
	a.PricingResolver = &fakePricingResolver{err: errors.New("model not found")}
	a.Resolver = &fakeAttemptResolver{attempts: []routing.Ability{{
		Provider: "openai", InputPrice1k: 0.0015, OutputPrice1k: 0.003,
	}}}
	_, err := cli.HSet(context.Background(), ledger.TenantKey(1), "balance", "100000000").Result()
	require.NoError(t, err)

	body := `{"model":"demo","messages":[{"role":"user","content":"hello"}]}`
	w := runQuota(t, a, body, nil)
	require.Equal(t, http.StatusOK, w.Code)

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, estimateFor(t, body), reserved)
}

// 无任何定价链路 → 保留 v1 固定 0.01 USD 预占，防止完全失去保护
func TestLedgerQuotaNoPricingChainUsesDefault(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	a := newTestApp(t, nil, cli)

	_, err := cli.HSet(context.Background(), ledger.TenantKey(1), "balance", "100000000").Result()
	require.NoError(t, err)

	w := runQuota(t, a, `{"model":"demo","messages":[]}`, nil)
	require.Equal(t, http.StatusOK, w.Code)

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(10_000), reserved)
}

func TestLedgerQuotaRedisNilFailOpen(t *testing.T) {
	a := newTestApp(t, nil, nil)

	called := false
	w := runQuota(t, a, `{"model":"demo","messages":[]}`, &called)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, called)
}

// 中间件读取请求体后必须恢复，handler 仍能解析
func TestLedgerQuotaRestoresBody(t *testing.T) {
	a, _, _ := newQuotaTestApp(t, linearRow(), nil)
	body := `{"model":"demo","messages":[{"role":"user","content":"hello"}]}`
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	c.Set("tenant_id", int64(1))

	Ledger(a)(c)

	restored, err := io.ReadAll(c.Request.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(restored))
}
