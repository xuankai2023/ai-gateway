package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

func TestStateTransitions(t *testing.T) {
	m := NewManager(Options{FailureThreshold: 2, Timeout: 10 * time.Millisecond})
	key := Key("openai", "https://api.openai.com/v1")
	fail := func() (interface{}, error) { return nil, errors.New("boom") }

	// CLOSED → OPEN：连续失败达到阈值
	for i := 0; i < 2; i++ {
		if _, err := m.Call(key, fail); err == nil {
			t.Fatalf("call %d should fail", i)
		}
	}
	if !m.IsOpen(key) {
		t.Fatalf("state = %s, want OPEN", StateString(m.State(key)))
	}

	// OPEN 状态直接拒绝请求
	if _, err := m.Call(key, func() (interface{}, error) { return nil, nil }); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("err = %v, want ErrOpenState", err)
	}

	// OPEN → HALF_OPEN：超时后试探
	time.Sleep(15 * time.Millisecond)
	if got := m.State(key); got != gobreaker.StateHalfOpen {
		t.Fatalf("state = %s, want HALF_OPEN", StateString(got))
	}

	// HALF_OPEN → CLOSED：试探成功
	if _, err := m.Call(key, func() (interface{}, error) { return "ok", nil }); err != nil {
		t.Fatalf("half-open probe failed: %v", err)
	}
	if got := m.State(key); got != gobreaker.StateClosed {
		t.Fatalf("state = %s, want CLOSED", StateString(got))
	}
}

func TestIsProviderOpen(t *testing.T) {
	m := NewManager(Options{FailureThreshold: 1, Timeout: time.Minute})
	fail := func() (interface{}, error) { return nil, errors.New("boom") }

	// 无熔断器 = 可用
	if m.IsProviderOpen("openai") {
		t.Fatal("provider without breakers should be available")
	}

	// 一个端点熔断、另一个正常 → 整个 provider 仍可用
	_, _ = m.Call(Key("openai", "a"), fail)
	_, _ = m.Call(Key("openai", "b"), func() (interface{}, error) { return nil, nil })
	if m.IsProviderOpen("openai") {
		t.Fatal("provider with one healthy endpoint should be available")
	}

	// 所有端点熔断 → provider 不可用
	_, _ = m.Call(Key("openai", "b"), fail)
	if !m.IsProviderOpen("openai") {
		t.Fatal("provider with all endpoints open should be unavailable")
	}

	// 其他 provider 不受影响
	if m.IsProviderOpen("anthropic") {
		t.Fatal("unrelated provider should be available")
	}
}
