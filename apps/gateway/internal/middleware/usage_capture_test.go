package middleware

import (
	"context"
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/ledger"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newRedisClient(t *testing.T, mr *miniredis.Miniredis) *goredis.Client {
	t.Helper()
	cli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// runCapture 构造最小链路：tenant context → LedgerCapture → handler
func runCapture(t *testing.T, a *app.App, reserved, finalCost int64, status int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Set("request_id", "req-test")
		// LedgerCapture 在 c.Next() 之前读取 reserved —— 必须由上游中间件写入
		if reserved >= 0 {
			c.Set("ledger_reserved", reserved)
		}
		c.Next()
	})
	r.POST("/v1/chat/completions", LedgerCapture(a), func(c *gin.Context) {
		if finalCost >= 0 {
			c.Set("ledger_final_cost", finalCost)
		}
		c.Status(status)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, status, w.Code)
}

func namedInt(t *testing.T, v driver.NamedValue) int64 {
	t.Helper()
	n, ok := v.Value.(int64)
	require.True(t, ok, "参数类型 = %T, want int64", v.Value)
	return n
}

func waitAsync(t *testing.T, a *app.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, a.Async.Wait(ctx))
}

func TestLedgerCaptureSettleExactInteger(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := newRedisClient(t, mr)
	db, fake := dbtest.New(t)
	a := newTestApp(t, db, cli)

	_, err := cli.HSet(context.Background(), ledger.TenantKey(1),
		"balance", "100000000", "reserved", "10000").Result()
	require.NoError(t, err)

	fake.ExpectExec("INSERT INTO ledger_entries").WillReturnResult(1, 1)
	fake.ExpectQuery("FROM credit_lots").WillReturnRows([]string{"id", "remaining"}) // 无批次

	runCapture(t, a, 10000, 25000, http.StatusOK)
	waitAsync(t, a)

	bal, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	// 100 - (25000-10000)/1e6 = 99.985 USD
	require.Equal(t, ledger.MicroUSD(99_985_000), bal)
	require.Equal(t, ledger.MicroUSD(0), reserved)

	calls := fake.Calls()
	require.Len(t, calls, 2)
	require.Contains(t, calls[0].SQL, "INSERT INTO ledger_entries")
	require.Equal(t, int64(15000), namedInt(t, calls[0].Args[2]), "delta = final - reserved")
	fake.AssertExpectations(t)
}

func TestLedgerCaptureRefundOnFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := newRedisClient(t, mr)
	db, fake := dbtest.New(t)
	a := newTestApp(t, db, cli)

	_, err := cli.HSet(context.Background(), ledger.TenantKey(1),
		"balance", "100000000", "reserved", "10000").Result()
	require.NoError(t, err)

	fake.ExpectExec("INSERT INTO ledger_entries").WillReturnResult(1, 1)

	runCapture(t, a, 10000, -1, http.StatusInternalServerError)
	waitAsync(t, a)

	_, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), reserved)

	calls := fake.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, int64(-10000), namedInt(t, calls[0].Args[2]), "退款 delta 为负")
	fake.AssertExpectations(t)
}

// trust_quota 跳过预占（reserved=0）时，成功结算仍会记一条 settle 流水并扣批次
func TestLedgerCaptureTrustSkipSettles(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := newRedisClient(t, mr)
	db, fake := dbtest.New(t)
	a := newTestApp(t, db, cli)

	// 余额 0，无预占
	fake.ExpectExec("INSERT INTO ledger_entries").WillReturnResult(1, 1)
	fake.ExpectQuery("FROM credit_lots").WillReturnRows(
		[]string{"id", "remaining"},
		[]driver.Value{int64(1), int64(50000)},
	)
	fake.ExpectExec("UPDATE credit_lots SET remaining").WillReturnResult(0, 1)

	runCapture(t, a, 0, 3000, http.StatusOK)
	waitAsync(t, a)

	bal, reserved, _, err := ledger.GetBalance(context.Background(), cli, 1)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(-3000), bal)
	require.Equal(t, ledger.MicroUSD(0), reserved)

	calls := fake.Calls()
	require.Len(t, calls, 3)
	fake.AssertExpectations(t)
}

func TestLedgerCaptureNoopWhenNothingToSettle(t *testing.T) {
	mr := miniredis.RunT(t)
	cli := newRedisClient(t, mr)
	db, fake := dbtest.New(t)
	a := newTestApp(t, db, cli)

	runCapture(t, a, 0, 0, http.StatusOK)
	waitAsync(t, a)

	require.Empty(t, fake.Calls(), "无预占且 finalCost=0 时不得产生 DB 写入")
}
