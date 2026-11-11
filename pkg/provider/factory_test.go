package provider

import (
	"strings"
	"testing"
)

// newTestFactory 用独立 Registry 避免测试间相互污染（阶段 14：Registry 实例化）
func newTestFactory() *Registry {
	return NewRegistry()
}

func TestRegisterAndGet(t *testing.T) {
	f := newTestFactory()
	f.Register("fake", func() Provider { return nil })

	if _, err := f.Get("fake"); err != nil {
		t.Fatalf("Get 已注册类型失败: %v", err)
	}
}

func TestGetUnknownType(t *testing.T) {
	f := newTestFactory()

	_, err := f.Get("not-registered")
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "unknown provider type: not-registered") {
		t.Errorf("错误信息不符合预期: %v", err)
	}
}

func TestAllRegisteredSorted(t *testing.T) {
	f := newTestFactory()
	f.Register("openai", func() Provider { return nil })
	f.Register("anthropic", func() Provider { return nil })
	f.Register("volcengine", func() Provider { return nil })

	got := strings.Join(f.AllRegistered(), ",")
	if want := "anthropic,openai,volcengine"; got != want {
		t.Errorf("AllRegistered = %q, want %q", got, want)
	}
}

func TestAllRegisteredEmpty(t *testing.T) {
	if got := newTestFactory().AllRegistered(); len(got) != 0 {
		t.Errorf("空工厂应返回空列表: %v", got)
	}
}
