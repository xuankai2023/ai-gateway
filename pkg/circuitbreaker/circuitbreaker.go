// Package circuitbreaker 熔断器（按 provider + base_url 维度管理）
//
// 🔴 为什么熔断 key 要带 base_url？
//
//	同一个 provider type 可能配置多个上游端点（不同 base_url、不同密钥池）。
//	只按 provider type 熔断会把一个端点的故障扩散到同类型的所有端点。
//
// 策略：
//
//	① 本地熔断（gobreaker Execute 模式）
//	   - CLOSED：正常放行
//	   - OPEN：拒绝所有请求（超时后进入 HALF_OPEN）
//	   - HALF_OPEN：放行 1 个试探请求，成功 → CLOSED，失败 → OPEN
//
//	② 集成点：
//	   - routing.ResolveAbilities 选渠道时按 provider type 检查是否全部端点熔断
//	   - relay 每次 attempt 用 Call 包裹上游调用
//
// 与 abilities.enabled 的区别：
//
//	abilities.enabled = 0 是永久禁用（运维手动或鉴权/模型不存在时写入）
//	CircuitBreaker OPEN 是临时熔断（自动恢复）
package circuitbreaker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ai-gateway/pkg/broadcast"
	"ai-gateway/pkg/observability"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

const (
	// 默认熔断参数：连续失败 5 次 → OPEN；OPEN 30s 后进入 HALF_OPEN
	defaultFailureThreshold = 5
	defaultTimeout          = 30 * time.Second
)

// Options 熔断参数（测试可注入更小的阈值与超时）
type Options struct {
	FailureThreshold int
	Timeout          time.Duration
}

// Manager 管理一组熔断器（key = provider@base_url）
// （阶段 14：实例化，随 App 生命周期创建；跨节点广播经 SetBus 挂接到 App 的 Bus）
type Manager struct {
	mu   sync.Mutex
	cbs  map[string]*gobreaker.CircuitBreaker
	opts Options
	bus  atomic.Pointer[broadcast.Bus] // nil = 仅本节点熔断
}

// NewManager 创建熔断管理器
func NewManager(opts Options) *Manager {
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = defaultFailureThreshold
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	return &Manager{
		cbs:  make(map[string]*gobreaker.CircuitBreaker),
		opts: opts,
	}
}

// SetBus 挂接跨节点广播总线（熔断 OPEN 时向所有节点推送 breaker_open）
func (m *Manager) SetBus(b *broadcast.Bus) { m.bus.Store(b) }

// Key 熔断 key = provider + base_url
func Key(provider, baseURL string) string {
	if baseURL == "" {
		return provider
	}
	return provider + "@" + baseURL
}

// GetOrCreate 获取熔断器（不存在则创建）
func (m *Manager) GetOrCreate(key string) *gobreaker.CircuitBreaker {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, ok := m.cbs[key]; ok {
		return cb
	}

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        key,
		MaxRequests: 1, // HALF_OPEN 时放行 1 个试探
		Interval:    0, // CLOSED 状态下不周期重置计数（纯连续失败计数）
		Timeout:     m.opts.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(m.opts.FailureThreshold)
		},
		OnStateChange: func(_ string, _, to gobreaker.State) {
			observability.RecordCircuitBreaker(key, strings.ToLower(StateString(to)))
			if to == gobreaker.StateOpen {
				if bus := m.bus.Load(); bus != nil {
					go func() {
						bctx, bcancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer bcancel()
						if perr := bus.Publish(bctx, broadcast.Event{
							Type:       broadcast.TypeBreakerOpen,
							BreakerKey: key,
						}); perr != nil {
							observability.L().Warn("broadcast breaker_open failed",
								zap.String("breaker_key", key),
								zap.Error(perr),
							)
						}
					}()
				}
			}
		},
	})
	m.cbs[key] = cb
	return cb
}

// IsOpen 指定 key 是否熔断
func (m *Manager) IsOpen(key string) bool {
	return m.GetOrCreate(key).State() == gobreaker.StateOpen
}

// IsProviderOpen provider type 下所有端点是否都不可用
// 用于 abilities 选渠道（abilities 只记录 provider type，没有 base_url）
func (m *Manager) IsProviderOpen(provider string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix := provider + "@"
	matched := false
	for key, cb := range m.cbs {
		if key != provider && !strings.HasPrefix(key, prefix) {
			continue
		}
		matched = true
		if cb.State() != gobreaker.StateOpen {
			return false
		}
	}
	// 没有任何端点熔断器 = 尚未失败过 = 可用
	return matched
}

// Call 用熔断器包裹一次调用
func (m *Manager) Call(key string, fn func() (interface{}, error)) (interface{}, error) {
	return m.GetOrCreate(key).Execute(fn)
}

// State 返回指定 key 的熔断状态
func (m *Manager) State(key string) gobreaker.State {
	return m.GetOrCreate(key).State()
}

// StateString 熔断状态名（日志/指标用）
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
