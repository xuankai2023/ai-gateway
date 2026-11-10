package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLoggerLogsStartAndEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	core, logs := observer.New(zapcore.InfoLevel)

	r := gin.New()
	r.Use(Trace(), LoggerWith(zap.New(core)))
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ping?x=1", nil))

	entries := logs.All()
	if len(entries) != 2 {
		t.Fatalf("期望 2 条日志，实际 %d 条", len(entries))
	}
	if entries[0].Message != "request_start" || entries[1].Message != "request_end" {
		t.Errorf("日志顺序错误: %q, %q", entries[0].Message, entries[1].Message)
	}

	end := entries[1].ContextMap()
	if got := end["status"]; got != int64(http.StatusOK) {
		t.Errorf("status = %v, want 200", got)
	}
	if got := end["path"]; got != "/ping" {
		t.Errorf("path = %v, want /ping", got)
	}
	if _, ok := end["latency_ms"]; !ok {
		t.Error("request_end 缺少 latency_ms")
	}
	if got := end["trace_id"]; got == "" {
		t.Error("request_end 缺少 trace_id")
	}
	if got := entries[0].ContextMap()["query"]; got != "x=1" {
		t.Errorf("query = %v, want x=1", got)
	}
}
