// Meter Prometheus 核心指标定义
//
// 5 个核心指标：
//   gateway_requests_total{tenant,model,provider,status}         Counter 总请求数
//   gateway_request_duration_ms{tenant,model,provider}           Histogram 请求延迟
//   gateway_cost_total{tenant,model,provider}                    Counter 累计成本
//   gateway_cache_hit_ratio{tenant,model}                        Gauge 语义缓存命中率（后续 cache 中间件更新）
//   gateway_circuit_breaker_events{model,event}                   Counter 熔断事件（后续 CB 中间件更新）
//
// 暴露方式：在 config.gateway.listen_metrics 端口启动单独的 /metrics HTTP handler
// 注册入口见 apps/gateway/cmd/main.go newMetricsServer()
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- 指标定义 ---

var (
	// GatewayRequestsTotal 总请求数
	GatewayRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Total number of AI gateway requests",
		},
		[]string{"tenant", "model", "provider", "status"},
	)

	// GatewayRequestDuration 请求延迟（毫秒）
	GatewayRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_request_duration_ms",
			Help:    "Request latency in milliseconds",
			Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		},
		[]string{"tenant", "model", "provider"},
	)

	// GatewayCostTotal 累计成本（元）
	GatewayCostTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_cost_total",
			Help: "Accumulated cost of AI requests (CNY)",
		},
		[]string{"tenant", "model", "provider"},
	)

	// GatewayCacheHitRatio 语义缓存命中率（百分比 0-100）
	GatewayCacheHitRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_cache_hit_ratio",
			Help: "Semantic cache hit ratio (0-100)",
		},
		[]string{"tenant", "model"},
	)

	// GatewayCircuitBreakerEvents 熔断事件计数
	GatewayCircuitBreakerEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_circuit_breaker_events",
			Help: "Circuit breaker state change events",
		},
		[]string{"model", "event"},
	)
)

// --- 便捷函数 ---

// RecordRequest 在 Handler 里调用：记录一次完整请求的指标
func RecordRequest(tenant, model, provider, status string, latencyMs int64, cost float64) {
	labels := prometheus.Labels{"tenant": tenant, "model": model, "provider": provider}

	GatewayRequestsTotal.With(prometheus.Labels{
		"tenant":   tenant,
		"model":    model,
		"provider": provider,
		"status":   status,
	}).Inc()

	GatewayRequestDuration.With(labels).Observe(float64(latencyMs))

	if cost > 0 {
		GatewayCostTotal.With(labels).Add(cost)
	}
}

// RecordCircuitBreaker 熔断事件计数
func RecordCircuitBreaker(model, event string) {
	GatewayCircuitBreakerEvents.With(prometheus.Labels{
		"model": model, "event": event,
	}).Inc()
}
