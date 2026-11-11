// LoadShedMiddleware 全局负载卸载（过载快速拒绝）
//
// 位置：/v1 组首层（Auth 之前）——未鉴权流量同样占用在途名额，过载时先挡在这里；
// 探针端点（/livez /readyz）不经过本层，k8s 探活在过载时不被误杀、不引发重启雪崩。
//
// 语义：
//   - semaphore.Weighted 控制全局 in-flight，上限 gateway.max_in_flight（默认 1000）
//   - 获取信号量带 2s 排队超时，超时返回 503 + Retry-After: 1（快速拒绝而非排队挂死）
//   - 每租户 in-flight 软上限与限流中间件的并发计数共用（RateLimit 的
//     rl:conc:{tenant_id}），本层只兜全进程总量，不重复计数
//
// 埋点：gateway_in_flight_requests Gauge 随占用/释放增减。
package middleware

import (
	"context"
	"net/http"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/semaphore"
)

// 内置兜底默认值（配置缺省/非法值时使用）
const (
	fallbackMaxInFlight = 1000
	// DefaultQueueTimeout 获取信号量的默认排队超时
	DefaultQueueTimeout = 2 * time.Second
)

// LoadShed 全局在途信号量中间件（阶段 14：func(app) 形式；
// 上限取自 a.Config.Gateway.MaxInFlight，排队超时可用 a.LoadShedQueueTimeout 覆盖）
func LoadShed(a *app.App) gin.HandlerFunc {
	maxInFlight := int64(fallbackMaxInFlight)
	if a.Config != nil && a.Config.Gateway.MaxInFlight > 0 {
		maxInFlight = int64(a.Config.Gateway.MaxInFlight)
	}
	queueTimeout := a.LoadShedQueueTimeout
	if queueTimeout <= 0 {
		queueTimeout = DefaultQueueTimeout
	}
	sem := semaphore.NewWeighted(maxInFlight)

	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), queueTimeout)
		err := sem.Acquire(ctx, 1)
		cancel()
		if err != nil {
			// 排队超时（含客户端提前断开）：快速拒绝，让调用方退避重试
			c.Header("Retry-After", "1")
			apperr.Abort(c, http.StatusServiceUnavailable, "overloaded_error", "overloaded", "server overloaded, please retry later")
			return
		}
		observability.IncInFlight()
		defer func() {
			sem.Release(1)
			observability.DecInFlight()
		}()
		c.Next()
	}
}
