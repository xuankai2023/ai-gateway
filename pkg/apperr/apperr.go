// Package apperr 对外统一错误响应契约
//
// 所有 4xx/5xx 响应一律为：
//
//	{"error": {"message": "...", "type": "...", "code": "..."}, "request_id": "..."}
//
// request_id 复用 Trace 中间件注入的 trace_id（响应头 X-Request-Id 同源），
// 与日志中的 trace_id 一致，客户端报错可直接定位单次请求。
package apperr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
)

// HeaderRequestID 请求 ID 响应头（与 Trace 中间件同源）
const HeaderRequestID = "X-Request-Id"

// traceIDContextKey 与 middleware.Trace 写入 gin.Context 的 key 一致
const traceIDContextKey = "trace_id"

// Error 对外错误对象
type Error struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

// envelope 顶层错误响应
type envelope struct {
	Error     *Error `json:"error"`
	RequestID string `json:"request_id"`
}

// Write 以统一契约写出错误响应
func Write(c *gin.Context, status int, errType, code, message string) {
	id := requestID(c)
	c.JSON(status, envelope{Error: &Error{Message: message, Type: errType, Code: code}, RequestID: id})
}

// Abort 以统一契约写出错误响应并中断后续处理（等价 AbortWithStatusJSON）
func Abort(c *gin.Context, status int, errType, code, message string) {
	id := requestID(c)
	c.AbortWithStatusJSON(status, envelope{Error: &Error{Message: message, Type: errType, Code: code}, RequestID: id})
}

// Body 构造统一契约的错误响应体（供 SSE 错误帧等非 JSON 响应场景复用）
func Body(c *gin.Context, errType, code, message string) map[string]any {
	id := requestID(c)
	return map[string]any{
		"error":      map[string]string{"message": message, "type": errType, "code": code},
		"request_id": id,
	}
}

// requestID 取本请求的 request_id（= trace_id）；上游中间件缺失时生成兜底 ID 并回写，
// 保证任何错误响应都带可关联的 request_id 且与后续日志的 trace_id 一致
func requestID(c *gin.Context) string {
	if id := c.GetString(traceIDContextKey); id != "" {
		return id
	}
	id := newRequestID()
	c.Set(traceIDContextKey, id)
	c.Header(HeaderRequestID, id)
	return id
}

// newRequestID 生成 32 位十六进制随机串（与 Trace 中间件兜底逻辑一致）
func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}
