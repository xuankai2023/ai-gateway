package routing

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"ai-gateway/pkg/provider"
	"github.com/sony/gobreaker"
)

func TestDecideRelayRetry(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		retry    int
		maxRetry int
		want     RetryDecision
	}{
		{name: "nil", err: nil, retry: 0, maxRetry: 3, want: RetryStop},
		{name: "client canceled", err: context.Canceled, retry: 0, maxRetry: 3, want: RetryStop},
		{name: "retry exhausted", err: provider.NewUpstreamError("openai", 500, "server_error", "boom"), retry: 3, maxRetry: 3, want: RetryStop},
		{name: "circuit open", err: gobreaker.ErrOpenState, retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "circuit too many requests", err: gobreaker.ErrTooManyRequests, retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "retryable 500", err: provider.NewUpstreamError("openai", 500, "server_error", "boom"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "retryable 429", err: provider.NewUpstreamError("openai", 429, "rate_limit", "slow down"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "overloaded 503", err: provider.NewUpstreamError("anthropic", 503, "overloaded_error", "busy"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "overloaded 529", err: provider.NewUpstreamError("anthropic", 529, "overloaded_error", "busy"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "timeout 408", err: provider.NewUpstreamError("openai", 408, "timeout_error", "slow"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "channel-level 401", err: provider.NewUpstreamError("openai", 401, "auth_error", "bad key"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "channel-level 404 model", err: provider.NewUpstreamError("openai", 404, "model_not_found", "no model"), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "request-level 400", err: provider.NewUpstreamError("openai", 400, "invalid_request_error", "bad body"), retry: 0, maxRetry: 3, want: RetryStop},
		{name: "request-level 422", err: provider.NewUpstreamError("openai", 422, "invalid_request_error", "bad params"), retry: 0, maxRetry: 3, want: RetryStop},
		{name: "deadline exceeded", err: context.DeadlineExceeded, retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "wrapped network error", err: provider.NewNetworkError("openai", errors.New("connection reset")), retry: 0, maxRetry: 3, want: RetryRetry},
		{name: "unknown error", err: errors.New("weird"), retry: 0, maxRetry: 3, want: RetryStop},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DecideRelayRetry(tt.err, tt.retry, tt.maxRetry); got != tt.want {
				t.Errorf("DecideRelayRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanAttempts_OrderAndTruncate(t *testing.T) {
	tiers := []PriorityTier{
		{Priority: 100, Channels: []Ability{
			{ID: 1, Provider: "openai", Weight: 3},
			{ID: 2, Provider: "anthropic", Weight: 1},
		}},
		{Priority: 50, Channels: []Ability{
			{ID: 3, Provider: "volcengine", Weight: 1},
		}},
	}

	got := PlanAttempts(tiers, 0)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	// 层间按 priority 降序：高优先级两个渠道一定排在 volcengine 前
	if got[0].ID > 2 || got[1].ID > 2 {
		t.Errorf("high priority tier should come first, got %+v", got)
	}
	if got[2].ID != 3 {
		t.Errorf("last attempt = %d, want 3", got[2].ID)
	}

	if truncated := PlanAttempts(tiers, 2); len(truncated) != 2 {
		t.Errorf("truncated len = %d, want 2", len(truncated))
	}
	if unlimited := PlanAttempts(tiers, -1); len(unlimited) != 3 {
		t.Errorf("unlimited len = %d, want 3", len(unlimited))
	}
}

func TestPlanAttempts_FixedSeedDeterministic(t *testing.T) {
	oldRand := randIntn
	defer func() { randIntn = oldRand }()

	tiers := []PriorityTier{{Priority: 100, Channels: []Ability{
		{ID: 1, Weight: 5},
		{ID: 2, Weight: 3},
		{ID: 3, Weight: 1},
		{ID: 4, Weight: 1},
	}}}

	seed := func() { randIntn = rand.New(rand.NewSource(42)).Intn }
	seed()
	first := PlanAttempts(tiers, 0)
	seed()
	second := PlanAttempts(tiers, 0)

	if len(first) != 4 || len(second) != 4 {
		t.Fatalf("unexpected lengths: %d / %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("fixed seed produced different order: %v vs %v", ids(first), ids(second))
		}
	}
	seen := map[int64]bool{}
	for _, a := range first {
		if seen[a.ID] {
			t.Fatalf("channel %d appeared more than once", a.ID)
		}
		seen[a.ID] = true
	}
}

func ids(attempts []Ability) []int64 {
	out := make([]int64, len(attempts))
	for i := range attempts {
		out[i] = attempts[i].ID
	}
	return out
}
