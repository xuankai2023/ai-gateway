package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/provider"
)

// fakeNonStreamProvider 非流式假 Provider（blocked 时挂起直到 ctx 取消，模拟慢上游）
type fakeNonStreamProvider struct {
	name    string
	blocked bool
	resp    *provider.ChatResponse
	err     error
}

func (f *fakeNonStreamProvider) Name() string { return f.name }

func (f *fakeNonStreamProvider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	if f.blocked {
		<-ctx.Done()
		return nil, provider.NewNetworkError(f.name, ctx.Err())
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeNonStreamProvider) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeNonStreamProvider) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeNonStreamProvider) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

// newTimeoutRelay 构造带自定义 attempt 超时的 Relay（实例级字段，替代原全局 AttemptTimeout 钩子）
func newTimeoutRelay(t *testing.T, d time.Duration) (*Relay, *provider.Registry) {
	t.Helper()
	registry := provider.NewRegistry()
	r := New(nil, circuitbreaker.NewManager(circuitbreaker.Options{}), registry, nil, d)
	t.Cleanup(r.Close)
	return r, registry
}

func TestNonStream_AttemptTimeoutSwitchesChannel(t *testing.T) {
	r, registry := newTimeoutRelay(t, 50*time.Millisecond)

	registry.Register("slow", func() provider.Provider { return &fakeNonStreamProvider{name: "slow", blocked: true} })
	registry.Register("fast", func() provider.Provider {
		return &fakeNonStreamProvider{
			name: "fast",
			resp: &provider.ChatResponse{
				Content: "ok", InputTokens: 1, OutputTokens: 1, LatencyMs: 1,
			},
		}
	})

	attempts := []routing.Ability{
		{ID: 1, Provider: "slow", ProviderModel: "gpt-4", Weight: 1},
		{ID: 2, Provider: "fast", ProviderModel: "gpt-4", Weight: 1},
	}

	start := time.Now()
	res := r.NonStream(context.Background(), attempts, &provider.ChatRequest{Model: "gpt-4"})
	elapsed := time.Since(start)
	defer res.Close()

	if res.Err != nil {
		t.Fatalf("Err = %v, want nil（attempt 超时后应换到 fast 成功）", res.Err)
	}
	if res.Attempt.Provider != "fast" {
		t.Errorf("成功渠道 = %q, want fast", res.Attempt.Provider)
	}
	if res.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", res.Attempts)
	}
	// 慢渠道被 50ms attempt 上限切断，整体应在远小于默认 30s 内完成
	if elapsed > 2*time.Second {
		t.Errorf("整体耗时 %v, 应在 50ms attempt 超时量级", elapsed)
	}
}

func TestNonStream_AttemptTimeoutExhausted(t *testing.T) {
	r, registry := newTimeoutRelay(t, 50*time.Millisecond)

	registry.Register("slow-only", func() provider.Provider { return &fakeNonStreamProvider{name: "slow-only", blocked: true} })

	attempts := []routing.Ability{
		{ID: 1, Provider: "slow-only", ProviderModel: "gpt-4", Weight: 1},
	}

	start := time.Now()
	res := r.NonStream(context.Background(), attempts, &provider.ChatRequest{Model: "gpt-4"})
	elapsed := time.Since(start)
	defer res.Close()

	if res.Err == nil {
		t.Fatal("Err = nil, want 超时错误")
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(res.Err, &upstreamErr) {
		t.Fatalf("Err type = %T, want *UpstreamError", res.Err)
	}
	if upstreamErr.Kind != provider.KindTimeout {
		t.Errorf("Kind = %q, want %q", upstreamErr.Kind, provider.KindTimeout)
	}
	if elapsed > 2*time.Second {
		t.Errorf("单渠道 attempt 超时耗时 %v, 应在 50ms 量级", elapsed)
	}
}

func TestNonStream_RequestTimeoutWins(t *testing.T) {
	r, registry := newTimeoutRelay(t, 30*time.Second) // attempt 上限远大于请求级

	registry.Register("slow-req", func() provider.Provider { return &fakeNonStreamProvider{name: "slow-req", blocked: true} })

	attempts := []routing.Ability{
		{ID: 1, Provider: "slow-req", ProviderModel: "gpt-4", Weight: 1},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	res := r.NonStream(ctx, attempts, &provider.ChatRequest{Model: "gpt-4"})
	elapsed := time.Since(start)
	defer res.Close()

	var upstreamErr *provider.UpstreamError
	if !errors.As(res.Err, &upstreamErr) || upstreamErr.Kind != provider.KindTimeout {
		t.Fatalf("Err = %v, want KindTimeout 的 UpstreamError（请求级超时切断）", res.Err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("请求级超时耗时 %v, 应在 60ms 量级", elapsed)
	}
}

func TestStream_ConnectTimeoutSwitchesChannel(t *testing.T) {
	r, registry := newTimeoutRelay(t, 50*time.Millisecond)

	registry.Register("s-low", func() provider.Provider { return &fakeStreamProvider{name: "s-low", block: true} })
	registry.Register("s-fast", func() provider.Provider {
		return &fakeStreamProvider{name: "s-fast", chunks: []provider.StreamChunk{{Content: "hello"}, {Content: "world"}}}
	})

	attempts := []routing.Ability{
		{ID: 1, Provider: "s-low", ProviderModel: "m", Weight: 1},
		{ID: 2, Provider: "s-fast", ProviderModel: "m", Weight: 1},
	}

	start := time.Now()
	res := r.Stream(context.Background(), attempts, &provider.ChatRequest{Model: "m", Stream: true})
	elapsed := time.Since(start)
	defer res.Close()

	if res.Err != nil {
		t.Fatalf("Err = %v, want nil（建连超时后应换到 s-fast）", res.Err)
	}
	if res.Attempt.Provider != "s-fast" {
		t.Errorf("成功渠道 = %q, want s-fast", res.Attempt.Provider)
	}
	if res.FirstChunk.Content != "hello" {
		t.Errorf("FirstChunk.Content = %q, want hello", res.FirstChunk.Content)
	}
	if elapsed > 2*time.Second {
		t.Errorf("建连超时耗时 %v, 应在 50ms 量级", elapsed)
	}
}

func TestStream_ConnectTimeoutExhausted(t *testing.T) {
	r, registry := newTimeoutRelay(t, 50*time.Millisecond)

	registry.Register("s-only", func() provider.Provider { return &fakeStreamProvider{name: "s-only", block: true} })

	attempts := []routing.Ability{
		{ID: 1, Provider: "s-only", ProviderModel: "m", Weight: 1},
	}

	res := r.Stream(context.Background(), attempts, &provider.ChatRequest{Model: "m", Stream: true})
	defer res.Close()

	// 建连+首帧超时应归类为可重试的上游超时（最终由 handler 映射 504），
	// 而不是被误判为客户端断开
	var upstreamErr *provider.UpstreamError
	if !errors.As(res.Err, &upstreamErr) {
		t.Fatalf("Err type = %T, want *UpstreamError", res.Err)
	}
	if upstreamErr.Kind != provider.KindTimeout {
		t.Errorf("Kind = %q, want %q", upstreamErr.Kind, provider.KindTimeout)
	}
	if !upstreamErr.Retryable() {
		t.Error("建连超时应 Retryable（换渠道重试）")
	}
}

func TestStream_ClientCanceledStillDetected(t *testing.T) {
	r, registry := newTimeoutRelay(t, 30*time.Second)

	registry.Register("s-cancel", func() provider.Provider { return &fakeStreamProvider{name: "s-cancel", block: true} })

	attempts := []routing.Ability{
		{ID: 1, Provider: "s-cancel", ProviderModel: "m", Weight: 1},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	res := r.Stream(ctx, attempts, &provider.ChatRequest{Model: "m", Stream: true})
	defer res.Close()

	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled（客户端断开不误判为上游超时）", res.Err)
	}
}

func TestTimeoutKindOf(t *testing.T) {
	parent := context.Background()
	parentTimeout, cancelParent := context.WithTimeout(parent, 10*time.Millisecond)
	defer cancelParent()
	bound, cancelBound := context.WithTimeout(parentTimeout, time.Hour)
	defer cancelBound()

	if got := timeoutKindOf(parentTimeout, bound, nil); got != timeoutKindNone {
		t.Errorf("err=nil 时 kind = %q, want none", got)
	}
	if got := timeoutKindOf(parentTimeout, bound, errors.New("boom")); got != timeoutKindNone {
		t.Errorf("普通错误 kind = %q, want none", got)
	}

	// attempt 级先到期
	attemptBound, cancelA := context.WithTimeout(parent, 10*time.Millisecond)
	defer cancelA()
	time.Sleep(20 * time.Millisecond)
	if got := timeoutKindOf(parent, attemptBound, context.DeadlineExceeded); got != timeoutKindAttempt {
		t.Errorf("attempt 级到期 kind = %q, want attempt", got)
	}

	// 请求级到期（子 ctx 继承父的截止，应先归 request）
	time.Sleep(30 * time.Millisecond) // parentTimeout(10ms) 已到期，bound 随之到期
	if got := timeoutKindOf(parentTimeout, bound, context.DeadlineExceeded); got != timeoutKindRequest {
		t.Errorf("请求级到期 kind = %q, want request", got)
	}
}
