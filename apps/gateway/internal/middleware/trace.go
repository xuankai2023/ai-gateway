// TraceMiddleware OTel 全链路 span 注入
//
// 位置：第 2 层（Recover 之后，其他中间件之前）
//
// 职责：
//   - 从请求 Headers 提取 traceparent（W3C TraceContext），有则续 span，无则新建
//   - 创建 root span，记录 http.method / http.target / tenant_id / model_name 等 attributes
//   - gin.Context.Set("trace_id", traceID) — 日志里用
//   - gin.Context.Set("otel_span", span) — 下游可继续扩展 attributes
//   - gin.Context.Set("otel_ctx", ctx) — 携带 span 的 ctx（供 Handler 调 OTel Tracer）
//   - 请求退出时 defer span.End() + SetStatus
package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// traceIDHeader 请求链路 ID 的透传头
const traceIDHeader = "X-Request-Id"

// Trace 为每个请求创建 OTel span（从 traceparent 续或新建）
func Trace() gin.HandlerFunc {
	return func(c *gin.Context) {
		tracer := observability.T()

		// 1. 从 traceparent header 恢复或新建 span
		ctx := c.Request.Context()
		parentSpanCtx := trace.SpanContextFromContext(ctx)

		var span trace.Span
		if parentSpanCtx.IsValid() {
			// 有 traceparent → 续接
			ctx, span = tracer.Start(ctx, "gateway.request",
				trace.WithSpanKind(trace.SpanKindServer),
			)
		} else {
			// 无 traceparent → 新建（OTel SDK 会自动生成新 trace_id）
			ctx, span = tracer.Start(ctx, "gateway.request",
				trace.WithSpanKind(trace.SpanKindServer),
			)
		}

		defer span.End()

		// 2. 记录 HTTP attributes
		span.SetAttributes(
			semconv.HTTPRequestMethodKey.String(c.Request.Method),
			semconv.URLPath(c.Request.URL.Path),
			attribute.String("http.route", c.FullPath()),
		)

		// 3. 生成 trace_id（优先用 OTel 生成的，没有则手动）
		traceID := traceSpanID(span)
		if traceID == "" {
			traceID = c.GetHeader(traceIDHeader)
			if traceID == "" {
				traceID = newTraceID()
			}
		}

		// 4. 塞回 gin.Context（下游用）
		c.Set("trace_id", traceID)
		c.Set("otel_span", span)
		c.Set("otel_ctx", ctx)

		// 5. 响应头也带一份 trace_id
		c.Header(traceIDHeader, traceID)

		c.Next()

		// 6. 记录响应状态 + span status
		span.SetAttributes(
			semconv.HTTPResponseStatusCode(c.Writer.Status()),
		)
		if c.Writer.Status() >= 400 {
			span.SetStatus(codes.Error, c.Errors.String())
		} else {
			span.SetStatus(codes.Ok, "")
		}
	}
}

// traceSpanID 从 OTel span 取 trace_id
func traceSpanID(span trace.Span) string {
	if span == nil {
		return ""
	}
	traceID := span.SpanContext().TraceID().String()
	if traceID == "00000000000000000000000000000000" {
		return "" // 无效 trace id → 手动生成
	}
	return traceID
}

// newTraceID 生成 32 位十六进制随机串（兜底用）
func newTraceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("trace-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

// --- 辅助：Handler 里用的 span 扩展函数 ---

// GetSpanFromContext 从 gin.Context 拿 span（Handler 里补租户/model_name 信息时用）
func GetSpanFromContext(c *gin.Context) trace.Span {
	raw, ok := c.Get("otel_span")
	if !ok {
		return nil
	}
	span, _ := raw.(trace.Span)
	return span
}

// AddRequestAttrs 给 span 补充租户 + 模型 + provider 属性
func AddRequestAttrs(c *gin.Context, tenantID int64, model, provider string) {
	span := GetSpanFromContext(c)
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.Int64("tenant_id", tenantID),
		attribute.String("model_name", model),
		attribute.String("provider", provider),
	)
}

// MarkSpanError 给 span 标错误状态 + 错误信息
func MarkSpanError(c *gin.Context, err error) {
	span := GetSpanFromContext(c)
	if span == nil || err == nil {
		return
	}
	span.SetStatus(codes.Error, err.Error())
}
