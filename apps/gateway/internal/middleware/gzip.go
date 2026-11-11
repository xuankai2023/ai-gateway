// GzipMiddleware 非流式响应 gzip 压缩
//
// 仅当请求头 Accept-Encoding 含 gzip（q>0）时接管 writer；
// 首次写响应头时按 Content-Type 决策：SSE（text/event-stream）旁路压缩——
// 流式帧必须即时下发，不能被压缩缓冲；下游已设置 Content-Encoding 时不重复压缩。
package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

var gzipWriterPool = sync.Pool{New: func() any { return gzip.NewWriter(io.Discard) }}

// Gzip 响应压缩中间件（非 SSE 且客户端接受 gzip 时生效）
func Gzip() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !acceptsGzip(c.GetHeader("Accept-Encoding")) {
			c.Next()
			return
		}
		c.Header("Vary", "Accept-Encoding")

		gz := gzipWriterPool.Get().(*gzip.Writer)
		w := &gzipResponseWriter{ResponseWriter: c.Writer, gz: gz}
		c.Writer = w
		defer func() {
			if w.use {
				_ = w.gz.Close() // 写出 gzip trailer
			}
			w.gz.Reset(io.Discard)
			gzipWriterPool.Put(w.gz)
		}()
		c.Next()
	}
}

// acceptsGzip 解析 Accept-Encoding：显式 gzip 且 q>0 才接受
func acceptsGzip(header string) bool {
	for _, part := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.TrimSpace(name) != "gzip" {
			continue
		}
		q := 1.0
		if params != "" {
			v, ok := strings.CutPrefix(strings.TrimSpace(params), "q=")
			if !ok {
				continue
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				continue
			}
			q = f
		}
		if q > 0 {
			return true
		}
	}
	return false
}

// gzipResponseWriter 延迟决策的压缩 writer：在首个 Write/Flush/WriteHeaderNow 时
// 定夺是否压缩（此时响应头必然已设置完毕；gin 的 WriteHeader 只记录状态码，
// 头部真正序列化发生在 WriteHeaderNow/首个 Write，故 WriteHeader 不做决策）。
// 约定：响应 Content-Type 必须在首个写出/Flush 之前设置（gin 渲染与 SSE 转发均如此）。
type gzipResponseWriter struct {
	gin.ResponseWriter
	gz      *gzip.Writer
	decided bool
	use     bool
}

// decide 按响应特征决定是否压缩；只执行一次
func (w *gzipResponseWriter) decide() {
	w.decided = true
	if strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
		return // SSE 不压缩
	}
	if w.Header().Get("Content-Encoding") != "" {
		return // 已有编码，不重复压缩
	}
	if code := w.Status(); code == http.StatusNoContent || code == http.StatusNotModified {
		return // 无响应体
	}
	w.use = true
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Del("Content-Length") // 压缩后长度未知，改走 chunked
	w.gz.Reset(w.ResponseWriter)
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	if !w.decided {
		w.decide()
	}
	if w.use {
		return w.gz.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

// WriteString 必须覆写：gin 的默认实现直连底层 writer，会绕过压缩
func (w *gzipResponseWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// WriteHeaderNow 覆写：gin 会直接调它序列化响应头，必须先完成决策
func (w *gzipResponseWriter) WriteHeaderNow() {
	if !w.decided {
		w.decide()
	}
	w.ResponseWriter.WriteHeaderNow()
}

// Flush 触发决策（SSE 旁路时流式转发依赖此接口）；压缩中先把 gzip 缓冲冲下去
func (w *gzipResponseWriter) Flush() {
	if !w.decided {
		w.decide()
	}
	if w.use {
		_ = w.gz.Flush()
	}
	w.ResponseWriter.Flush()
}
