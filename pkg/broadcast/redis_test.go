package broadcast

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestClient(addr string) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
}

type noopRedisLogger struct{}

func (noopRedisLogger) Printf(ctx context.Context, format string, v ...interface{}) {}

// ensure no-op loggers to avoid stderr noise in tests
func init() {
	SetLogger(func(msg string, fields ...zap.Field) {})
	goredis.SetLogger(noopRedisLogger{})
}

// TestPublishAndSubscribe 同一 miniredis 上 Bus A Publish、Bus B Subscribe，
// 验证 B 收到正确的 TypeChannelDisabled 事件，AbilityID=42，TraceID 非空，
// 且 Origin 等于 A 的 instanceID（即 != B 的 instanceID）。
func TestPublishAndSubscribe(t *testing.T) {
	mr := miniredis.RunT(t)
	client := newTestClient(mr.Addr())

	busA := Init(client)
	busB := Init(client)

	// Bus B 先订阅
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	require.NoError(t, busB.Subscribe(ctxB))

	// 等一下让 PubSub 连上
	time.Sleep(50 * time.Millisecond)

	evCh := make(chan Event, 1)
	busB.On(TypeChannelDisabled, func(ctx context.Context, ev Event) error {
		evCh <- ev
		return nil
	})

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	// A 也需要 Subscribe 以便 Publish 走同一个连接（不是必须，但任务要求每个 bus 有 Subscribe）
	require.NoError(t, busA.Subscribe(ctxA))

	traceID := "trace-abc-123"
	err := busA.Publish(ctxA, Event{
		Type:      TypeChannelDisabled,
		AbilityID: 42,
		TraceID:   traceID,
	})
	require.NoError(t, err)

	select {
	case ev := <-evCh:
		require.Equal(t, TypeChannelDisabled, ev.Type)
		require.Equal(t, int64(42), ev.AbilityID)
		require.Equal(t, traceID, ev.TraceID)
		require.Equal(t, busA.InstanceID(), ev.Origin)
		require.NotEqual(t, busB.InstanceID(), ev.Origin)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event on bus B")
	}
}

// TestOriginSkip Bus A Publish 一条 origin=自己的事件，
// 验证 Bus A 自己的 handler 不会收到这条事件（跳过 self-origin）。
func TestOriginSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	client := newTestClient(mr.Addr())

	busA := Init(client)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	require.NoError(t, busA.Subscribe(ctxA))

	time.Sleep(50 * time.Millisecond)

	var called int32
	handlerCalled := make(chan struct{}, 1)
	busA.On(TypeChannelDisabled, func(ctx context.Context, ev Event) error {
		// 如果是自己的 origin 也会触发，我们用 channel 通知
		select {
		case handlerCalled <- struct{}{}:
		default:
		}
		return nil
	})

	err := busA.Publish(ctxA, Event{
		Type:      TypeChannelDisabled,
		AbilityID: 7,
		TraceID:   "self-origin",
	})
	require.NoError(t, err)
	_ = called

	// 给 PubSub 一点时间传播；handler 应被跳过
	select {
	case <-handlerCalled:
		t.Fatal("self-origin event should not invoke handler on same bus")
	case <-time.After(300 * time.Millisecond):
		// 预期：没收到
	}
}

// TestCacheInvalidateBroadcast cache_invalidate 事件广播。
func TestCacheInvalidateBroadcast(t *testing.T) {
	mr := miniredis.RunT(t)
	client := newTestClient(mr.Addr())

	busA := Init(client)
	busB := Init(client)

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	require.NoError(t, busB.Subscribe(ctxB))
	time.Sleep(50 * time.Millisecond)

	evCh := make(chan Event, 1)
	busB.On(TypeCacheInvalidate, func(ctx context.Context, ev Event) error {
		evCh <- ev
		return nil
	})

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	require.NoError(t, busA.Subscribe(ctxA))

	err := busA.Publish(ctxA, Event{
		Type:        TypeCacheInvalidate,
		CachePrefix: "abilities",
		TraceID:     "cache-001",
	})
	require.NoError(t, err)

	select {
	case ev := <-evCh:
		require.Equal(t, TypeCacheInvalidate, ev.Type)
		require.Equal(t, "abilities", ev.CachePrefix)
		require.Equal(t, busA.InstanceID(), ev.Origin)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cache_invalidate event")
	}
}

// TestHandlerOrder 同一事件类型注册 2 个 handler，验证按注册顺序调用。
func TestHandlerOrder(t *testing.T) {
	mr := miniredis.RunT(t)
	client := newTestClient(mr.Addr())

	busA := Init(client)
	busB := Init(client)

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	require.NoError(t, busB.Subscribe(ctxB))
	time.Sleep(50 * time.Millisecond)

	var orderMu sync.Mutex
	var order []int
	done := make(chan struct{}, 1)

	busB.On(TypeChannelCooldown, func(ctx context.Context, ev Event) error {
		orderMu.Lock()
		order = append(order, 1)
		orderMu.Unlock()
		// 等一下，让第二个 handler 有机会被排队（串行调用，这个函数返回后才会调用下一个）
		return nil
	})
	busB.On(TypeChannelCooldown, func(ctx context.Context, ev Event) error {
		orderMu.Lock()
		order = append(order, 2)
		isDone := len(order) == 2
		orderMu.Unlock()
		if isDone {
			done <- struct{}{}
		}
		return nil
	})

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	require.NoError(t, busA.Subscribe(ctxA))

	err := busA.Publish(ctxA, Event{
		Type:      TypeChannelCooldown,
		AbilityID: 99,
		TraceID:   "order-test",
	})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handlers")
	}

	orderMu.Lock()
	defer orderMu.Unlock()
	require.Equal(t, []int{1, 2}, order)
}

// TestClose Subscribe 后 Close，验证订阅 goroutine 退出。
func TestClose(t *testing.T) {
	mr := miniredis.RunT(t)
	client := newTestClient(mr.Addr())

	bus := Init(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, bus.Subscribe(ctx))
	time.Sleep(50 * time.Millisecond)

	// 发一条，让订阅 goroutine 已经跑起来
	bus.Publish(ctx, Event{Type: TypeBreakerOpen, BreakerKey: "k1"})
	time.Sleep(100 * time.Millisecond)

	bus.Close()

	// 再发一条，确保 Close 后 handler 不会再被触发
	var called bool
	done := make(chan struct{}, 1)
	bus.On(TypeBreakerOpen, func(ctx context.Context, ev Event) error {
		called = true
		done <- struct{}{}
		return nil
	})

	err := bus.Publish(ctx, Event{Type: TypeBreakerOpen, BreakerKey: "k2"})
	// Publish 本身通过 client 发，不依赖 pubsub；允许继续
	_ = err

	select {
	case <-done:
		t.Fatalf("handler called after Close (called=%v)", called)
	case <-time.After(300 * time.Millisecond):
		// 预期：Close 后应该不再分发
	}
}

// TestBusSubscribeInvalidRedis nil client → Subscribe 返回 error。
func TestBusSubscribeInvalidRedis(t *testing.T) {
	bus := Init(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := bus.Subscribe(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "redis client not initialized")
}
