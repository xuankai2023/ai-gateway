// Package broadcast 多节点状态同步（Redis Pub/Sub）
//
// 设计：
//
//	单频道 gw:events，消息类型四种：
//	  - channel_disabled   永久禁用（abilities.enabled=0）
//	  - channel_cooldown   进入冷却（disabled_until 已设置）
//	  - breaker_open       熔断器 OPEN（gobreaker.StateOpen）
//	  - cache_invalidate   通用缓存失效（Admin API 写操作后）
//
// 每条事件带 origin_instance_id，订阅端收到自己发布的事件跳过防回环。
// 启动时 Publish 前先全量拉一次 DB 快照（EnsureSnapshot），
// 防止错过订阅前发生的事件（进程在订阅前那段时间是"失明"的）。
//
// 故障策略：Redis 断连 → 日志告警，本地状态不变（容忍陈旧，TTL 兜底）。
package broadcast

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// Channel 统一 Pub/Sub 频道名
	Channel = "gw:events"

	// 事件类型
	TypeChannelDisabled = "channel_disabled"
	TypeChannelCooldown = "channel_cooldown"
	TypeBreakerOpen     = "breaker_open"
	TypeCacheInvalidate = "cache_invalidate"

	// 默认订阅超时（启动时等连接）
	defaultSubscribeTimeout = 5 * time.Second
)

// Event 广播消息
type Event struct {
	Type        string `json:"type"`
	AbilityID   int64  `json:"ability_id,omitempty"`
	BreakerKey  string `json:"breaker_key,omitempty"`
	CachePrefix string `json:"cache_prefix,omitempty"` // 空=全部
	Origin      string `json:"origin"`                 // 发布方实例 ID
	TraceID     string `json:"trace_id,omitempty"`
	PublishedAt int64  `json:"published_at"`
}

// Handler 收到事件时的回调
type Handler func(ctx context.Context, ev Event) error

// Bus 广播总线（每个进程一个实例）
type Bus struct {
	client     *goredis.Client
	instanceID string

	mu       sync.RWMutex
	handlers map[string][]Handler

	pubsub *goredis.PubSub

	snapshotEnsured bool

	closeOnce sync.Once
	done      chan struct{} // Close() 关闭，通知 run() goroutine 退出
}

// Init 创建广播总线；必须在 Redis InitPool 之后调用
func Init(client *goredis.Client) *Bus {
	return &Bus{
		client:     client,
		instanceID: newInstanceID(),
		handlers:   make(map[string][]Handler),
		done:       make(chan struct{}),
	}
}

// InstanceID 返回本实例唯一 ID（日志 / 事件 origin 识别用）
func (b *Bus) InstanceID() string { return b.instanceID }

// On 注册某类事件的处理器；同类型可注册多个，按注册顺序串行调用
func (b *Bus) On(eventType string, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
}

// Publish 发布事件到 Redis 频道；Redis 不可用时返回 error（本地状态已在调用方更新）
func (b *Bus) Publish(ctx context.Context, ev Event) error {
	if b.client == nil {
		return fmt.Errorf("broadcast: redis client not initialized")
	}
	ev.Origin = b.instanceID
	if ev.PublishedAt == 0 {
		ev.PublishedAt = time.Now().UnixNano()
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("broadcast: marshal event: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return b.client.Publish(publishCtx, Channel, string(raw)).Err()
}

// Subscribe 启动订阅循环；返回时订阅 goroutine 已运行，传入 ctx cancel 即停止
func (b *Bus) Subscribe(ctx context.Context) error {
	if b.client == nil {
		return fmt.Errorf("broadcast: redis client not initialized")
	}
	b.mu.Lock()
	if b.pubsub != nil {
		b.mu.Unlock()
		return nil
	}
	// Subscribe 本身会阻塞等待 Redis 服务端确认（SUBSCRIBE 命令返回），
	// 所以返回时已在频道上；传入 ctx 可以被用于超时取消。
	b.pubsub = b.client.Subscribe(ctx, Channel)
	b.mu.Unlock()

	go b.run(ctx)
	return nil
}

// run 订阅消费循环
func (b *Bus) run(ctx context.Context) {
	// RLock 保护读 pubsub，与 Close() 的 pubsub=nil 互斥
	b.mu.RLock()
	ps := b.pubsub
	b.mu.RUnlock()
	if ps == nil {
		// Close() 已经先于 run() 执行，直接退出
		return
	}
	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.done: // Close() 被调用，主动退出
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev Event
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				logWarn("broadcast: unmarshal failed", zap.Error(err))
				continue
			}
			// 跳过自己发的（本进程本地状态已在 Publish 之前更新）
			if ev.Origin == b.instanceID {
				continue
			}
			b.dispatch(ctx, ev)
		}
	}
}

// dispatch 串行调用所有匹配类型的 handler
func (b *Bus) dispatch(ctx context.Context, ev Event) {
	b.mu.RLock()
	handlers := b.handlers[ev.Type]
	b.mu.RUnlock()
	for _, h := range handlers {
		if err := h(ctx, ev); err != nil {
			logWarn("broadcast: handler error",
				zap.Error(err),
				zap.String("type", ev.Type),
				zap.String("origin", ev.Origin),
			)
		}
	}
}

// Close 关闭订阅；幂等
func (b *Bus) Close() {
	b.closeOnce.Do(func() {
		// 1. 通知 run() goroutine 退出（安全地 close channel，只会被调用一次）
		close(b.done)

		b.mu.Lock()
		ps := b.pubsub
		b.pubsub = nil
		b.mu.Unlock()

		// 2. 关闭 PubSub（让 Redis 连接释放）
		if ps != nil {
			_ = ps.Close()
		}
	})
}

// EnsureSnapshot 快照拉取完成通知；保留供后续扩展（快照未完成前事件可缓冲）
func (b *Bus) EnsureSnapshot() {
	b.mu.Lock()
	b.snapshotEnsured = true
	b.mu.Unlock()
}

func newInstanceID() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// --- 日志（避免循环依赖，main 注入，未注入时降级到 stderr） ---

var (
	logMu sync.RWMutex
	logFn func(msg string, fields ...zap.Field)
)

// SetLogger 注入结构化日志函数；main 启动时调用
func SetLogger(fn func(msg string, fields ...zap.Field)) {
	logMu.Lock()
	logFn = fn
	logMu.Unlock()
}

func logWarn(msg string, fields ...zap.Field) {
	logMu.RLock()
	fn := logFn
	logMu.RUnlock()
	if fn != nil {
		fn(msg, fields...)
		return
	}
	fmt.Fprintf(os.Stderr, "[broadcast] %s", msg)
	for _, f := range fields {
		fmt.Fprintf(os.Stderr, " %v", f)
	}
	fmt.Fprintln(os.Stderr)
}
