// LoggerMiddleware
//
// 位置：第 3 层
// 依赖：observability.Logger + gin.Context（需已有 trace_id）
//
// 职责：
//   - 从 ctx 取 trace_id，创建带 trace_id 的子 logger
//   - gin.Context.Set("logger", subLogger)
//   - 请求进入时打 "request_start" 日志
//   - 请求退出时打 "request_end" 日志（包含 latency_ms / status / cost / cache_hit）
//
// 为什么在 Trace 之后？日志必须带 trace_id，TraceMiddleware 先塞进去。
package middleware

import (
	"time"

	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Logger 请求开始 / 结束日志，使用全局 logger
func Logger() gin.HandlerFunc {
	return LoggerWith(observability.L())
}

// LoggerWith 使用指定 logger，便于测试与后续按请求注入子 logger
func LoggerWith(l *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		l.Info("request_start",
			zap.String("trace_id", c.GetString("trace_id")),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
		)

		c.Next()

		fields := []zap.Field{
			zap.String("trace_id", c.GetString("trace_id")),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Int64("latency_ms", time.Since(start).Milliseconds()),
		}
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("error", c.Errors.String()))
		}
		l.Info("request_end", fields...)
	}
}
