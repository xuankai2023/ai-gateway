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
	if body["code"] != float64(500) {
		t.Errorf("code = %v, want 500", body["code"])
	}
	if body["message"] != "internal error" {
		t.Errorf("message = %v, want internal error", body["message"])
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
