// AttemptsTrace 诊断快照测试（relay 层每次渠道尝试的记录）
package relay

import (
	"context"
	"testing"

	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/provider"
)

// fakeChatProvider 非流式 fake（成功 / 失败可配）
type fakeChatProvider struct {
	name string
	err  error
	resp *provider.ChatResponse
}

func (f *fakeChatProvider) ChatCompletion(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeChatProvider) ChatCompletionStream(context.Context, *provider.ChatRequest) (<-chan provider.StreamChunk, <-chan error) {
	return nil, nil
}

func (f *fakeChatProvider) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeChatProvider) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (f *fakeChatProvider) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

func (f *fakeChatProvider) Name() string { return f.name }

// TestNonStream_AttemptsTrace 失败 + 成功两跳：trace 含每次的渠道 / 状态 / 错误归类 / 熔断状态
func TestNonStream_AttemptsTrace(t *testing.T) {
	failing := &fakeChatProvider{name: "trace-fail", err: provider.NewUpstreamError("trace-fail", 429, "rate_limit", "slow down")}
	ok := &fakeChatProvider{name: "trace-ok", resp: &provider.ChatResponse{Content: "hi"}}
	r, registry := newTestRelay(t)
	registry.Register("trace-fail", func() provider.Provider { return failing })
	registry.Register("trace-ok", func() provider.Provider { return ok })

	res := r.NonStream(context.Background(), []routing.Ability{
		{ID: 101, Provider: "trace-fail", ProviderModel: "m1"},
		{ID: 102, Provider: "trace-ok", ProviderModel: "m2"},
	}, &provider.ChatRequest{Model: "biz-model"})

	if res.Err != nil {
		t.Fatalf("Err = %v", res.Err)
	}
	if len(res.AttemptsTrace) != 2 {
		t.Fatalf("AttemptsTrace 长度 = %d, want 2", len(res.AttemptsTrace))
	}

	first := res.AttemptsTrace[0]
	if first.Attempt != 1 || first.ChannelID != 101 || first.Provider != "trace-fail" {
		t.Errorf("第一跳元数据错误: %+v", first)
	}
	if first.Status != "failed" {
		t.Errorf("第一跳 status = %q, want failed", first.Status)
	}
	if first.ErrorKind != "rate_limit" {
		t.Errorf("第一跳 error_kind = %q, want rate_limit", first.ErrorKind)
	}
	if first.ErrorMessage == "" {
		t.Error("第一跳 error_message 为空")
	}
	if first.BreakerStateBefore != "closed" {
		t.Errorf("第一跳 breaker_state_before = %q, want closed", first.BreakerStateBefore)
	}

	second := res.AttemptsTrace[1]
	if second.Attempt != 2 || second.ChannelID != 102 || second.Status != "success" {
		t.Errorf("第二跳错误: %+v", second)
	}
	if second.ErrorKind != "" || second.ErrorMessage != "" {
		t.Errorf("成功跳不应有错误字段: %+v", second)
	}
}

// TestNonStream_AttemptsTrace_AllFail 全部失败：trace 记录所有跳
func TestNonStream_AttemptsTrace_AllFail(t *testing.T) {
	a := &fakeChatProvider{name: "tf-a", err: provider.NewUpstreamError("tf-a", 500, "server_error", "a")}
	b := &fakeChatProvider{name: "tf-b", err: provider.NewUpstreamError("tf-b", 500, "server_error", "b")}
	r, registry := newTestRelay(t)
	registry.Register("tf-a", func() provider.Provider { return a })
	registry.Register("tf-b", func() provider.Provider { return b })

	res := r.NonStream(context.Background(), []routing.Ability{
		{ID: 1, Provider: "tf-a", ProviderModel: "m"},
		{ID: 2, Provider: "tf-b", ProviderModel: "m"},
	}, &provider.ChatRequest{Model: "biz-model"})

	if res.Err == nil {
		t.Fatal("应全部失败")
	}
	if len(res.AttemptsTrace) != 2 {
		t.Fatalf("AttemptsTrace 长度 = %d, want 2", len(res.AttemptsTrace))
	}
	for i, tr := range res.AttemptsTrace {
		if tr.Status != "failed" || tr.ErrorKind != "server" {
			t.Errorf("第 %d 跳: %+v", i, tr)
		}
	}
}

// TestStream_AttemptsTrace 流式：首跳失败换渠道，trace 记录两跳
func TestStream_AttemptsTrace(t *testing.T) {
	failing := &fakeStreamProvider{name: "strace-fail", err: provider.NewUpstreamError("strace-fail", 500, "server_error", "boom")}
	ok := &fakeStreamProvider{name: "strace-ok", chunks: []provider.StreamChunk{{Content: "hello"}}}
	r, registry := newTestRelay(t)
	registry.Register("strace-fail", func() provider.Provider { return failing })
	registry.Register("strace-ok", func() provider.Provider { return ok })

	res := r.Stream(context.Background(), []routing.Ability{
		{ID: 201, Provider: "strace-fail", ProviderModel: "m1"},
		{ID: 202, Provider: "strace-ok", ProviderModel: "m2"},
	}, &provider.ChatRequest{Model: "biz-model"})
	defer res.Close()

	if res.Err != nil {
		t.Fatalf("Err = %v", res.Err)
	}
	if len(res.AttemptsTrace) != 2 {
		t.Fatalf("AttemptsTrace 长度 = %d, want 2", len(res.AttemptsTrace))
	}
	if res.AttemptsTrace[0].Status != "failed" || res.AttemptsTrace[1].Status != "success" {
		t.Errorf("trace 状态错误: %+v", res.AttemptsTrace)
	}
}
