// responseCapture 响应体捕获 writer（幂等快照 / 语义缓存写回共用）
//
// 嵌在 gin.ResponseWriter 外层：下游 handler 写出的字节先记入内存缓冲区，
// 再原样透传给底层 writer（可能已被 Gzip 包装），因此捕获到的是未压缩的原始响应体，
// 回放时由底层 writer 按当前请求的 Accept-Encoding 重新决策压缩。
package middleware

import (
	"bytes"

	"github.com/gin-gonic/gin"
)

// responseCapture 记录写入的响应体字节
type responseCapture struct {
	gin.ResponseWriter
	buf bytes.Buffer
}

func newResponseCapture(w gin.ResponseWriter) *responseCapture {
	return &responseCapture{ResponseWriter: w}
}

// Write 记录并透传
func (w *responseCapture) Write(b []byte) (int, error) {
	w.buf.Write(b)
	return w.ResponseWriter.Write(b)
}

// WriteString 覆写：gin 默认实现直连底层 writer，会绕过捕获
func (w *responseCapture) WriteString(s string) (int, error) {
	w.buf.WriteString(s)
	return w.ResponseWriter.Write([]byte(s))
}

// Bytes 返回已捕获的响应体副本
func (w *responseCapture) Bytes() []byte {
	return w.buf.Bytes()
}
