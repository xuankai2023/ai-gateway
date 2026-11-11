package middleware

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/usagelog"
	"ai-gateway/pkg/async"
	"ai-gateway/pkg/config"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// newTestApp 构造最小可用 App（阶段 14：中间件测试统一入口）
// db/redisCli 按需传入（可为 nil）；Usage 默认挂 nil-DB 写入器（队列丢弃不报错）
func newTestApp(t *testing.T, db *sql.DB, redisCli *goredis.Client) *app.App {
	t.Helper()
	return &app.App{
		Config: config.Default(),
		DB:     db,
		Redis:  redisCli,
		Async:  &async.Executor{},
		Usage:  usagelog.New(nil, usagelog.Options{}),
	}
}

// runMiddleware 在 gin 测试上下文中执行一次中间件
func runMiddleware(t *testing.T, mw gin.HandlerFunc, setup func(c *gin.Context)) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if setup != nil {
		setup(c)
	}
	mw(c)
	return w, c
}
