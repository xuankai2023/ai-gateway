package middleware

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// buildGzipEngine 组装测试链路：Gzip → 各类响应 handler
func buildGzipEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip())
	r.GET("/json", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": strings.Repeat("a", 1000)})
	})
	r.GET("/string", func(c *gin.Context) {
		c.String(http.StatusOK, "hello gzip")
	})
	r.GET("/sse", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Writer.WriteHeader(http.StatusOK)
		fmt.Fprintf(c.Writer, "data: chunk-1\n\n")
		c.Writer.Flush()
		fmt.Fprintf(c.Writer, "data: chunk-2\n\n")
		c.Writer.Flush()
	})
	return r
}

func gunzip(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader 失败: %v", err)
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("解压失败: %v", err)
	}
	return string(raw)
}

func TestGzipCompressesJSON(t *testing.T) {
	engine := buildGzipEngine()
	req := httptest.NewRequest(http.MethodGet, "/json", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ce := w.Header().Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", ce)
	}
	if v := w.Header().Get("Vary"); v != "Accept-Encoding" {
		t.Errorf("Vary = %q, want Accept-Encoding", v)
	}
	if body := gunzip(t, w); !strings.Contains(body, strings.Repeat("a", 100)) {
		t.Errorf("解压后响应体不完整: %.50s...", body)
	}
}

// TestGzipBypassesSSE 流式响应不压缩（帧即时下发，不被压缩缓冲）
func TestGzipBypassesSSE(t *testing.T) {
	engine := buildGzipEngine()
	req := httptest.NewRequest(http.MethodGet, "/sse", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if ce := w.Header().Get("Content-Encoding"); ce != "" {
		t.Fatalf("SSE 不应压缩，Content-Encoding = %q", ce)
	}
	if !strings.Contains(w.Body.String(), "data: chunk-1") || !strings.Contains(w.Body.String(), "data: chunk-2") {
		t.Errorf("SSE 原始帧应原样下发: %q", w.Body.String())
	}
}

func TestGzipNoAcceptEncoding(t *testing.T) {
	engine := buildGzipEngine()
	req := httptest.NewRequest(http.MethodGet, "/json", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if ce := w.Header().Get("Content-Encoding"); ce != "" {
		t.Fatalf("未声明 gzip 时不应压缩，Content-Encoding = %q", ce)
	}
	if !strings.Contains(w.Body.String(), "msg") {
		t.Errorf("响应体应为原始 JSON: %.50s", w.Body.String())
	}
}

func TestGzipQZeroRejected(t *testing.T) {
	engine := buildGzipEngine()
	req := httptest.NewRequest(http.MethodGet, "/json", nil)
	req.Header.Set("Accept-Encoding", "gzip;q=0")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if ce := w.Header().Get("Content-Encoding"); ce != "" {
		t.Fatalf("gzip;q=0 不应压缩，Content-Encoding = %q", ce)
	}
}

// TestGzipWriteString 覆写路径：c.String 走 WriteString，也必须进压缩
func TestGzipWriteString(t *testing.T) {
	engine := buildGzipEngine()
	req := httptest.NewRequest(http.MethodGet, "/string", nil)
	req.Header.Set("Accept-Encoding", "deflate, gzip")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if ce := w.Header().Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", ce)
	}
	if body := gunzip(t, w); body != "hello gzip" {
		t.Errorf("解压后 = %q, want %q", body, "hello gzip")
	}
}
