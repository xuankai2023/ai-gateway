package middleware

import (
	"bytes"
	"io"

	"github.com/gin-gonic/gin"
)

// readAndRestoreBody 读取请求体后立即回填，供下游中间件与 handler 重复读取。
//
// bodyLimit 已用 MaxBytesReader 限制读取上限；Body 为 nil 时返回 nil。
func readAndRestoreBody(c *gin.Context) ([]byte, error) {
	if c.Request.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}
