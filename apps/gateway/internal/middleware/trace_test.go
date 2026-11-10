package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestTraceGeneratesTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, c.GetString("trace_id"))
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	traceID := w.Body.String()
	if len(traceID) != 32 {
		t.Fatalf("trace_id 长度 = %d, want 32, got=%q", len(traceID), traceID)
	}
	if got := w.Header().Get(traceIDHeader); got != traceID {
		t.Errorf("响应头 %s = %q, want %q", traceIDHeader, got, traceID)
	}
}

func TestTraceReusesIncomingTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, c.GetString("trace_id"))
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(traceIDHeader, "incoming-trace")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if got := w.Body.String(); got != "incoming-trace" {
		t.Errorf("trace_id = %q, want incoming-trace", got)
	}
	if got := w.Header().Get(traceIDHeader); got != "incoming-trace" {
		t.Errorf("响应头 = %q, want incoming-trace", got)
	}
}

func TestNewTraceIDUnique(t *testing.T) {
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id := newTraceID()
		if _, ok := seen[id]; ok {
			t.Fatalf("trace_id 重复: %s", id)
		}
		seen[id] = struct{}{}
	}
}
