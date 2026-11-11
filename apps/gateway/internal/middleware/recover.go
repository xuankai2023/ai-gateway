// RecoverMiddleware
//
// 位置：第 1 层（最外层）
// 依赖：observability.Logger
//
// 职责：
//   - recover 所有后续中间件 / Handler 的 panic
//   - 统一返回 apperr 错误格式（500 internal_error）
//   - 记录 panic 日志（带 stacktrace）
//   - 更新 Prometheus 错误指标
//
// 为什么在最外层？任何层级 panic 都能兜住，不会让进程挂。
package middleware

import (
	"net/http"
	"runtime/debug"

	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Recover 兜底 panic，统一返回 500 JSON，进程不退出
func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				observability.L().Error("panic recovered",
					zap.Any("panic", r),
					zap.String("trace_id", c.GetString("trace_id")),
					zap.String("method", c.Request.Method),
					zap.String("path", c.Request.URL.Path),
					zap.String("stack", string(debug.Stack())),
				)
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "internal_panic", "internal error")
			}
		}()
		c.Next()
	}
}
