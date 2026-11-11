package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRecoverReturns500JSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Recover())
	r.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}

	// 统一错误契约：{"error":{"message","type","code"},"request_id"}
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失或类型不对: %v", body)
	}
	if errObj["type"] != "internal_error" {
		t.Errorf("error.type = %v, want internal_error", errObj["type"])
	}
	if errObj["code"] != "internal_panic" {
		t.Errorf("error.code = %v, want internal_panic", errObj["code"])
	}
	if errObj["message"] != "internal error" {
		t.Errorf("error.message = %v, want internal error", errObj["message"])
	}
	// Recover 在 Trace 外层，trace_id 可能缺失 → apperr 兜底生成，必须非空
	if id, _ := body["request_id"].(string); id == "" {
		t.Errorf("request_id 缺失: %v", body)
	} else if w.Header().Get("X-Request-Id") != id {
		t.Errorf("X-Request-Id header = %q, 与 body.request_id = %q 不一致", w.Header().Get("X-Request-Id"), id)
	}
}

func TestRecoverPassesNormalRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Recover())
	r.GET("/ok", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ok", nil))

	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Errorf("正常请求被影响: status=%d body=%q", w.Code, w.Body.String())
	}
}
