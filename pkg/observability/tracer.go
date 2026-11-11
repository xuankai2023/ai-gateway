// Package observability 可观测性封装
//
// tracer.go  OTel Tracer 全链路 span 注入
// meter.go   Prometheus 核心指标
// logger.go  zap 结构化日志（带 trace_id 字段）
package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const serviceName = "ai-gateway"

// TracerOptions OTel TracerProvider 初始化参数
type TracerOptions struct {
	Enabled     bool    // 关闭时保持 Noop，不建立任何 exporter 连接
	Endpoint    string  // OTLP gRPC endpoint，如 otel-collector:4317
	Insecure    bool    // true 表示明文 gRPC
	SampleRatio float64 // 采样率 [0, 1]
}

// globalTracer 全局 Tracer 实例
var globalTracer trace.Tracer = trace.NewNoopTracerProvider().Tracer(serviceName)

// tp 当前运行的 TracerProvider（用于 Shutdown）
var tp *sdktrace.TracerProvider

// InitTracer 初始化 OTel TracerProvider
// 未启用或未配置 exporter 时保持 Noop，不产生额外开销
func InitTracer(ctx context.Context, opts TracerOptions) error {
	if !opts.Enabled {
		return nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("dev"),
		),
	)
	if err != nil {
		return err
	}

	exporterOpts := []otlptracegrpc.Option{}
	if opts.Endpoint != "" {
		exporterOpts = append(exporterOpts, otlptracegrpc.WithEndpoint(opts.Endpoint))
	}
	if opts.Insecure {
		exporterOpts = append(exporterOpts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, exporterOpts...)
	if err != nil {
		return err
	}

	tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(opts.SampleRatio))),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
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
