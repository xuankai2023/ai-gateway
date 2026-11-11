// Meter Prometheus 核心指标定义
//
// 指标清单（阶段 0~6 已落地的保留，阶段 7 新增的在下方）：
//
// 已有：
//
//	gateway_requests_total{tenant,model,provider,status}         Counter
//	gateway_request_duration_ms{tenant,model,provider}           Histogram
//	gateway_cost_total{tenant,model,provider}                    Counter
//	gateway_retries_total{model,provider}                        Counter
//	gateway_circuit_breaker_events{provider,event}               Counter
//	gateway_rate_limited_total{tenant,kind}                      Counter   (阶段 3)
//	gateway_in_flight_requests                                   Gauge     (阶段 5)
//	gateway_usagelog_dropped_total                               Counter   (阶段 6)
//
// 阶段 7 新增：
//
//	gateway_upstream_ttft_ms{provider}                           Histogram  流式首帧延迟（LLM 网关核心体验指标）
//	gateway_db_pool_connections{state}                           Gauge      MySQL 连接池状态（max/open/inuse/idle）
//	gateway_db_pool_wait_seconds                                 Counter    MySQL 因连接耗尽导致的等待总时长（秒）
//	gateway_redis_pool_connections{state}                         Gauge      Redis 连接池状态（max/total/idle/active）
//	gateway_cache_hits_total{namespace}                          Counter    进程内缓存命中计数
//	gateway_cache_misses_total{namespace}                         Counter    进程内缓存未命中计数
//	gateway_cache_size{namespace}                                Gauge      当前缓存条目数
//
// 暴露方式：在 config.gateway.listen_metrics 端口启动单独的 /metrics HTTP handler
// 注册入口见 apps/gateway/cmd/main.go newMetricsServer()
package observability

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"

	"ai-gateway/pkg/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	goredis "github.com/redis/go-redis/v9"
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

	// GatewayRetriesTotal 重试次数（每次换渠道 +1）
	GatewayRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_retries_total",
			Help: "Total number of channel retries",
		},
		[]string{"model", "provider"},
	)

	// GatewayCircuitBreakerEvents 熔断事件计数
	GatewayCircuitBreakerEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_circuit_breaker_events",
			Help: "Circuit breaker state change events",
		},
		[]string{"provider", "event"},
	)

	// GatewayRateLimitedTotal 限流拒绝计数（kind = rps / tpm / concurrency）
	GatewayRateLimitedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_rate_limited_total",
			Help: "Requests rejected by rate limiting",
		},
		[]string{"tenant", "kind"},
	)

	// GatewayInFlightRequests 全局在途请求数（load_shed 中间件埋点）
	GatewayInFlightRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_in_flight_requests",
			Help: "In-flight requests currently being processed",
		},
	)

	// GatewayUsageLogDroppedTotal 用量日志丢弃计数（异步队列满时宁丢不堵）
	GatewayUsageLogDroppedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_usagelog_dropped_total",
			Help: "Usage log entries dropped because the async queue is full or closed",
		},
	)

	// GatewaySemanticCacheHits 语义缓存命中计数（layer = exact / semantic）
	GatewaySemanticCacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_semantic_cache_hits_total",
			Help: "Semantic cache hits by layer",
		},
		[]string{"layer"},
	)

	// GatewaySemanticCacheMisses 语义缓存未命中计数
	GatewaySemanticCacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_semantic_cache_misses_total",
			Help: "Semantic cache misses",
		},
	)

	// GatewayGrayRequests 灰度命中计数（阶段 13：model=原模型，target=灰度目标模型）
	GatewayGrayRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_gray_requests_total",
			Help: "Requests routed to a gray target model",
		},
		[]string{"model", "target"},
	)

	// GatewayRAGRetrievalMs RAG 检索耗时（毫秒）
	GatewayRAGRetrievalMs = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_rag_retrieval_ms",
			Help:    "RAG retrieval latency in milliseconds",
			Buckets: []float64{5, 10, 25, 50, 100, 200, 400, 800, 1600},
		},
		[]string{"project"},
	)

	// GatewayRAGEmptyTotal 检索结果为空（未召回任何资料）
	GatewayRAGEmptyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_rag_empty_total",
			Help: "RAG retrievals that returned no document",
		},
		[]string{"project"},
	)

	// GatewayRAGFallbackTotal 检索失败 / 超时降级为无资料回答
	GatewayRAGFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_rag_fallback_total",
			Help: "RAG retrievals degraded to no-context answers",
		},
		[]string{"project"},
	)

	// --- 阶段 7 新增 ---

	// GatewayUpstreamTTFT 流式首帧延迟（Time To First Token）
	// relay.Stream probe 成功后埋点；LLM 网关核心体验指标
	GatewayUpstreamTTFT = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_upstream_ttft_ms",
			Help:    "Time to first token (TTFT) for streaming requests in milliseconds",
			Buckets: []float64{50, 100, 200, 300, 500, 800, 1200, 2000, 3000, 5000},
		},
		[]string{"provider"},
	)

	// GatewayDBPool MySQL 连接池状态（state = max_open / open / in_use / idle / wait_count）
	GatewayDBPool = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_db_pool_connections",
			Help: "MySQL connection pool state",
		},
		[]string{"state"},
	)

	// GatewayDBPoolWaitSeconds MySQL 因连接耗尽导致的等待总时长（秒）
	GatewayDBPoolWaitSeconds = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_db_pool_wait_seconds_total",
			Help: "Total wait time in seconds due to exhausted MySQL connections",
		},
	)

	// GatewayRedisPool Redis 连接池状态（state = total / idle / active / wait_count）
	GatewayRedisPool = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_redis_pool_connections",
			Help: "Redis connection pool state",
		},
		[]string{"state"},
	)

	// GatewayCacheHitsTotal 进程内缓存命中计数（周期采集自各命名空间 Stats.Hits）
	GatewayCacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_cache_hits_total",
			Help: "Cache hits by namespace",
		},
		[]string{"namespace"},
	)

	// GatewayCacheMissesTotal 进程内缓存未命中计数（周期采集自各命名空间 Stats.Misses）
	GatewayCacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_cache_misses_total",
			Help: "Cache misses by namespace",
		},
		[]string{"namespace"},
	)

	// GatewayCacheSize 当前缓存条目数（瞬时 Gauge）
	GatewayCacheSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_cache_size",
			Help: "Current cache size by namespace",
		},
		[]string{"namespace"},
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

// RecordRetry 记录一次换渠道重试
func RecordRetry(model, provider string) {
	GatewayRetriesTotal.With(prometheus.Labels{
		"model": model, "provider": provider,
	}).Inc()
}

// RecordCircuitBreaker 熔断事件计数（provider = provider@base_url）
func RecordCircuitBreaker(provider, event string) {
	GatewayCircuitBreakerEvents.With(prometheus.Labels{
		"provider": provider, "event": event,
	}).Inc()
}

// RecordRateLimited 限流拒绝计数（kind = rps / tpm / concurrency）
func RecordRateLimited(tenant, kind string) {
	GatewayRateLimitedTotal.With(prometheus.Labels{
		"tenant": tenant, "kind": kind,
	}).Inc()
}

// IncInFlight / DecInFlight 全局在途请求数增减（load_shed 信号量占用/释放）
func IncInFlight() { GatewayInFlightRequests.Inc() }
func DecInFlight() { GatewayInFlightRequests.Dec() }

// RecordUsageLogDropped 用量日志丢弃计数（队列满/已关闭时调用）
func RecordUsageLogDropped() { GatewayUsageLogDroppedTotal.Inc() }

// RecordSemanticCacheHit 语义缓存命中（layer = exact / semantic）
func RecordSemanticCacheHit(layer string) {
	GatewaySemanticCacheHits.WithLabelValues(layer).Inc()
}

// RecordSemanticCacheMiss 语义缓存未命中
func RecordSemanticCacheMiss() { GatewaySemanticCacheMisses.Inc() }

// RecordTTFT 流式首帧延迟埋点（relay.Stream probe 成功后调用）
func RecordTTFT(provider string, ttftMs int64) {
	GatewayUpstreamTTFT.WithLabelValues(provider).Observe(float64(ttftMs))
}

// RecordGrayRequest 灰度命中计数（model=原模型，target=灰度目标模型）
func RecordGrayRequest(model, target string) {
	GatewayGrayRequests.With(prometheus.Labels{"model": model, "target": target}).Inc()
}

// RecordRAGRetrieval RAG 检索耗时埋点（project 用项目 ID 字符串，避免高基数）
func RecordRAGRetrieval(projectID, ms int64) {
	GatewayRAGRetrievalMs.WithLabelValues(strconv.FormatInt(projectID, 10)).Observe(float64(ms))
}

// RecordRAGEmpty 检索为空计数
func RecordRAGEmpty(projectID int64) {
	GatewayRAGEmptyTotal.WithLabelValues(strconv.FormatInt(projectID, 10)).Inc()
}

// RecordRAGFallback 检索降级计数
func RecordRAGFallback(projectID int64) {
	GatewayRAGFallbackTotal.WithLabelValues(strconv.FormatInt(projectID, 10)).Inc()
}

// --- 周期采集器：DB 池 / Redis 池 / 进程内缓存 ---

// collectorInterval 池与缓存的周期采集间隔
const collectorInterval = 10 * time.Second

// cacheRegistry 所有进程内缓存命名空间的注册表
// 各缓存实例在自己的包初始化时通过 RegisterCache 注册；避免 meter 反向依赖 cache 包
var (
	cacheRegistryMu sync.Mutex
	cacheRegistry   = make(map[string]cacheStatsSource)
)

// cacheStatsSource 周期采集器对缓存实例的抽象（cache.Cache[T].Stats() 签名统一）
type cacheStatsSource interface {
	Stats() cache.Stats
}

// RegisterCache 把一个进程内缓存实例注册到 Prometheus 周期采集器
// 幂等：同名重复注册覆盖（测试里重建缓存时调用）
func RegisterCache(name string, src cacheStatsSource) {
	cacheRegistryMu.Lock()
	defer cacheRegistryMu.Unlock()
	cacheRegistry[name] = src
}

// InitPeriodicCollector 启动池状态 + 缓存状态的周期采集 goroutine
// dbPool 可为 nil（测试场景）；redisClient 可为 nil；ctx cancel 时退出
func InitPeriodicCollector(ctx context.Context, dbPool *sql.DB, redisClient *goredis.Client) {
	go periodicCollector(ctx, dbPool, redisClient)
}

func periodicCollector(ctx context.Context, dbPool *sql.DB, redisClient *goredis.Client) {
	ticker := time.NewTicker(collectorInterval)
	defer ticker.Stop()

	// 缓存 hits/misses 是 Counter 语义：周期增量采集时用"本次快照 - 上次快照"的差值 Add，
	// 保证 Prometheus Counter 单调递增且不会因进程重启或 32 位回绕出现异常。
	// 但进程内 Stats.Hits/Misses 本身就是单调累加的原子量，直接 Add 最终值等价于 Counter。
	// 为避免每轮 Add 巨大差值（进程已运行很久），我们每轮只 Add "差值"。
	var lastCacheHits = make(map[string]int64)
	var lastCacheMisses = make(map[string]int64)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collectDBPool(dbPool)
			collectRedisPool(redisClient)
			collectCaches(lastCacheHits, lastCacheMisses)
		}
	}
}

func collectDBPool(pool *sql.DB) {
	if pool == nil {
		return
	}
	s := pool.Stats()
	GatewayDBPool.WithLabelValues("max_open").Set(float64(s.MaxOpenConnections))
	GatewayDBPool.WithLabelValues("open").Set(float64(s.OpenConnections))
	GatewayDBPool.WithLabelValues("in_use").Set(float64(s.InUse))
	GatewayDBPool.WithLabelValues("idle").Set(float64(s.Idle))
	GatewayDBPool.WithLabelValues("wait_count").Set(float64(s.WaitCount))

	// WaitDuration 是累计等待时长（duration 类型 → 转秒 float64），
	// sql.DB 自己就是累计值，直接 Sub 算差值后 Add。
	// 周期 10s 内 Add 的增量通常很小；首次启动时 Sub 结果可能很大，属预期（进程刚起来时 WaitDuration=0）。
	if s.WaitDuration > 0 {
		// 每轮 Add 增量：记录当前值到一个闭包变量做差（用 CounterGauge 里的原子变量存前值）
		dbWaitSeconds.AddWaitSeconds(s.WaitDuration.Seconds())
	}
}

func collectRedisPool(cli *goredis.Client) {
	if cli == nil {
		return
	}
	ps := cli.PoolStats()
	// go-redis v9 PoolStats 字段：TotalConns / IdleConns / StaleConns / Hits / Misses / Timeouts
	GatewayRedisPool.WithLabelValues("total").Set(float64(ps.TotalConns))
	GatewayRedisPool.WithLabelValues("idle").Set(float64(ps.IdleConns))
	GatewayRedisPool.WithLabelValues("stale").Set(float64(ps.StaleConns))
}

func collectCaches(lastHits, lastMisses map[string]int64) {
	cacheRegistryMu.Lock()
	snap := make(map[string]cache.Stats, len(cacheRegistry))
	for name, src := range cacheRegistry {
		snap[name] = src.Stats()
	}
	cacheRegistryMu.Unlock()

	for name, s := range snap {
		GatewayCacheSize.WithLabelValues(name).Set(float64(s.Size))

		if prev, ok := lastHits[name]; ok {
			if diff := s.Hits - prev; diff > 0 {
				GatewayCacheHitsTotal.WithLabelValues(name).Add(float64(diff))
			}
		}
		lastHits[name] = s.Hits

		if prev, ok := lastMisses[name]; ok {
			if diff := s.Misses - prev; diff > 0 {
				GatewayCacheMissesTotal.WithLabelValues(name).Add(float64(diff))
			}
		}
		lastMisses[name] = s.Misses
	}
}

// --- DB 等待时长增量累加器 ---

// dbWaitSeconds 一个 Counter 包装，做 WaitDuration 的差 + Add
type waitCounter struct {
	mu   sync.Mutex
	last float64
	acc  float64
}

func (c *waitCounter) AddWaitSeconds(curSeconds float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == 0 && c.acc == 0 {
		// 首次启动：不产生巨大的初始 Add（进程刚起来时 WaitDuration 是累计值 0，
		// 但如果 DB 有过历史等待则差值不为 0，那是真实的运行期增量）
		c.last = curSeconds
		return
	}
	diff := curSeconds - c.last
	if diff > 0 {
		GatewayDBPoolWaitSeconds.Add(diff)
		c.acc += diff
	}
	c.last = curSeconds
}

var dbWaitSeconds = &waitCounter{}
