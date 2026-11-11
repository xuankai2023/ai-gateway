package relay

import (
	"context"
	"errors"
	"testing"

	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/provider"
)

// newTestRelay 构造实例级 Relay（注册表 / 熔断器独立，router=nil 跳过渠道健康回写）
func newTestRelay(t *testing.T) (*Relay, *provider.Registry) {
	t.Helper()
	registry := provider.NewRegistry()
	r := New(nil, circuitbreaker.NewManager(circuitbreaker.Options{}), registry, nil, DefaultAttemptTimeout)
	t.Cleanup(r.Close)
	return r, registry
}

type fakeStreamProvider struct {
	name     string
	chunks   []provider.StreamChunk
	err      error
	block    bool
	gotModel string
}

func (f *fakeStreamProvider) ChatCompletion(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeStreamProvider) ChatCompletionStream(_ context.Context, req *provider.ChatRequest) (<-chan provider.StreamChunk, <-chan error) {
	f.gotModel = req.Model
	if f.block {
		return make(chan provider.StreamChunk), make(chan error)
	}
	chunks := make(chan provider.StreamChunk, len(f.chunks))
	errs := make(chan error, 1)
	if f.err != nil {
		errs <- f.err
	} else {
		for _, c := range f.chunks {
			chunks <- c
		}
	}
	close(chunks)
	close(errs)
	return chunks, errs
}

func (f *fakeStreamProvider) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeStreamProvider) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeStreamProvider) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

func (f *fakeStreamProvider) Name() string { return f.name }

func TestStream_SwitchesChannelBeforeFirstChunk(t *testing.T) {
	failing := &fakeStreamProvider{name: "stream-fail", err: provider.NewUpstreamError("stream-fail", 500, "server_error", "boom")}
	ok := &fakeStreamProvider{name: "stream-ok", chunks: []provider.StreamChunk{{Content: "hello"}}}
	r, registry := newTestRelay(t)
	registry.Register("stream-fail", func() provider.Provider { return failing })
	registry.Register("stream-ok", func() provider.Provider { return ok })

	res := r.Stream(context.Background(), []routing.Ability{
		{Provider: "stream-fail", ProviderModel: "model-fail"},
		{Provider: "stream-ok", ProviderModel: "model-ok"},
	}, &provider.ChatRequest{Model: "biz-model", Messages: []provider.Message{{Role: "user", Content: "hi"}}})
	defer res.Close()

	if res.Err != nil {
		t.Fatalf("Err = %v, want nil", res.Err)
	}
	if res.FirstChunk.Content != "hello" {
		t.Errorf("FirstChunk.Content = %q", res.FirstChunk.Content)
	}
	if ok.gotModel != "model-ok" {
		t.Errorf("gotModel = %q, want model-ok", ok.gotModel)
	}
}

func TestStream_AllChannelsFail(t *testing.T) {
	a := &fakeStreamProvider{name: "stream-all-a", err: provider.NewUpstreamError("stream-all-a", 500, "server_error", "a")}
	b := &fakeStreamProvider{name: "stream-all-b", err: provider.NewUpstreamError("stream-all-b", 500, "server_error", "b")}
	r, registry := newTestRelay(t)
	registry.Register("stream-all-a", func() provider.Provider { return a })
	registry.Register("stream-all-b", func() provider.Provider { return b })

	res := r.Stream(context.Background(), []routing.Ability{
		{Provider: "stream-all-a", ProviderModel: "model-a"},
		{Provider: "stream-all-b", ProviderModel: "model-b"},
	}, &provider.ChatRequest{Model: "biz-model"})

	if res.Err == nil {
		t.Fatal("Err = nil, want upstream error")
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(res.Err, &upstreamErr) {
		t.Fatalf("Err type = %T, want *provider.UpstreamError", res.Err)
	}
}

func TestStream_ClientCanceled(t *testing.T) {
	blocked := &fakeStreamProvider{name: "stream-blocked", block: true}
	r, registry := newTestRelay(t)
	registry.Register("stream-blocked", func() provider.Provider { return blocked })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := r.Stream(ctx, []routing.Ability{{Provider: "stream-blocked", ProviderModel: "m"}},
		&provider.ChatRequest{Model: "biz-model"})
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", res.Err)
	}
}
