// Package observability 可观测性封装
//
// tracer.go  OTel Tracer 全链路 span 注入
// meter.go   Prometheus 核心指标（5 个）
// logger.go  zap 结构化日志（带 trace_id 字段）
package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const serviceName = "ai-gateway"

// globalTracer 全局 Tracer 实例
var globalTracer trace.Tracer = trace.NewNoopTracerProvider().Tracer(serviceName)

// tp 当前运行的 TracerProvider（用于 Shutdown）
var tp *sdktrace.TracerProvider

// InitTracer 初始化 OTel TracerProvider
// 生产环境可通过 OTEL_TRACES_EXPORTER / OTEL_EXPORTER_OTLP_ENDPOINT 环境变量切换 exporter
// 这里用最朴素的方式：TracerProvider + 全局注册，保证 span 创建 + context propagation 正常
func InitTracer(ctx context.Context) error {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("dev"),
		),
	)
	if err != nil {
		return err
	}

	tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// 不加 exporter → span 只存在于内存里用于 context 传递
		// 生产环境用：sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(otlpExporter))
	)

	otel.SetTracerProvider(tp)
	globalTracer = otel.Tracer(serviceName)
	return nil
}

// T 返回全局 Tracer
func T() trace.Tracer {
	return globalTracer
}

// ShutdownTracer flush & close
func ShutdownTracer(ctx context.Context) {
	if tp != nil {
		_ = tp.Shutdown(ctx)
	}
}
