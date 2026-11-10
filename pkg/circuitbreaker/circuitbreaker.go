// Package circuitbreaker 熔断器（按 provider 维度管理）
//
// 🔴 为什么需要独立的 CircuitBreaker？
//
//  New API 把熔断作为重试循环的副作用（失败 → 异步禁用 → 下次选渠道跳过），
//  适合多渠道热切换场景。但有些场景需要"全局熔断 N 秒再试"：
//    - 上游返回 401/403（渠道级问题，30s 内不会自愈）
//    - 上游连续超时（可能是网络抖动，熔断后让上游恢复）
//
// 我们的策略：
//
//	① 本地熔断（gobreaker v1 Execute 模式，按 provider 维度）
//	   - CLOSED：正常放行
//	   - OPEN：拒绝所有请求（超时 30s 后进入 HALF_OPEN）
//	   - HALF_OPEN：放行 1 个试探请求，成功 → CLOSED，失败 → OPEN
//
//	② 集成点：
//	   - routing.ResolveAbilities 选渠道时跳过 OPEN provider
//	   - Handler retry 循环每次调用用 circuitbreaker.Call 包裹
//
//	③ 后置：etcd Watch 多 Pod 同步熔断状态（单 Pod 本地熔断够用）
//
// 与 abilities.enabled 的区别：
//   abilities.enabled = 0 是永久禁用（运维/运维手动或异步禁用写库）
//   CircuitBreaker OPEN 是临时熔断（自动恢复）
package circuitbreaker

import (
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

const (
	// 默认熔断参数
	// 连续失败 5 次 → OPEN；OPEN 30s 后进入 HALF_OPEN
	defaultFailureThreshold = 5
	defaultTimeout          = 30 * time.Second
)

var (
	global = &registry{}
)

type registry struct {
	mu  sync.Mutex
	cbs map[string]*gobreaker.CircuitBreaker
}

// GetOrCreate 获取某个 provider 的熔断器（不存在则自动创建）
func GetOrCreate(provider string) *gobreaker.CircuitBreaker {
	global.mu.Lock()
	defer global.mu.Unlock()

	if global.cbs == nil {
		global.cbs = make(map[string]*gobreaker.CircuitBreaker)
	}

	if cb, ok := global.cbs[provider]; ok {
		return cb
	}

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "provider:" + provider,
		MaxRequests: 1, // HALF_OPEN 时放行 1 个试探
		Interval:    0, // CLOSED 状态下不周期重置计数（纯连续失败计数）
		Timeout:     defaultTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= defaultFailureThreshold
		},
	})
	global.cbs[provider] = cb
	return cb
}

// IsOpen 快速判断：某个 provider 现在是否熔断
// routing 选渠道时跳过 OPEN provider
func IsOpen(provider string) bool {
	cb := GetOrCreate(provider)
	return cb.State() == gobreaker.StateOpen
}

// IsAvailable 复合判断：provider 可用 = 未熔断
func IsAvailable(provider string) bool {
	return !IsOpen(provider)
}

// Call 用熔断器包裹一次调用
// 返回值：
//   - result：函数返回的第一个值（interface{} 类型，需调用方自己断言）
//   - err：函数返回的 error；如果熔断器 OPEN → 返回 ErrTooManyRequests
//
// 用法：
//   result, err := circuitbreaker.Call("openai", func() (interface{}, error) {
//       return p.ChatCompletion(ctx, req)
//   })
//   if errors.Is(err, gobreaker.ErrTooManyRequests) {
//       // 熔断器触发 → 换渠道
//   }
func Call(provider string, fn func() (interface{}, error)) (interface{}, error) {
	cb := GetOrCreate(provider)
	return cb.Execute(fn)
}

// StateString 熔断器状态名（日志/指标用）
func StateString(state gobreaker.State) string {
	switch state {
	case gobreaker.StateClosed:
		return "CLOSED"
	case gobreaker.StateOpen:
		return "OPEN"
	case gobreaker.StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}
