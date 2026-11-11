package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestCache[T any](t *testing.T, opts Options) *Cache[T] {
	t.Helper()
	c := New[T](opts)
	t.Cleanup(c.Close)
	return c
}

func TestSetGetAndMiss(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test", DefaultTTL: time.Minute})

	if _, ok := c.Get("nope"); ok {
		t.Fatal("未写入的 key 应 miss")
	}

	c.Set("k1", "v1")
	got, ok := c.Get("k1")
	if !ok || got != "v1" {
		t.Fatalf("Get(k1) = %q,%v; want v1,true", got, ok)
	}

	// 覆盖写
	c.Set("k1", "v2")
	got, ok = c.Get("k1")
	if !ok || got != "v2" {
		t.Fatalf("覆盖后 Get(k1) = %q,%v; want v2,true", got, ok)
	}

	s := c.Stats()
	if s.Hits != 2 || s.Misses != 1 {
		t.Errorf("Stats = %+v; want Hits=2 Misses=1", s)
	}
	if s.Name != "test" || s.Size != 1 {
		t.Errorf("Stats = %+v; want Name=test Size=1", s)
	}
}

func TestTTLExpiry(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test"})

	c.SetTTL("short", "v", 50*time.Millisecond)
	if _, ok := c.Get("short"); !ok {
		t.Fatal("TTL 内应命中")
	}

	time.Sleep(80 * time.Millisecond)
	if _, ok := c.Get("short"); ok {
		t.Fatal("TTL 过后应 miss")
	}
	if got := c.Stats().Size; got != 0 {
		t.Fatalf("过期条目应被惰性删除，Size = %d; want 0", got)
	}
}

func TestDefaultTTLFromOptions(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test", DefaultTTL: 50 * time.Millisecond})

	c.Set("k", "v") // 用 DefaultTTL
	time.Sleep(80 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("DefaultTTL 过后应 miss")
	}
}

func TestNoTTLEntryNeverExpires(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test"})

	c.SetTTL("forever", "v", 0)
	time.Sleep(30 * time.Millisecond)
	if _, ok := c.Get("forever"); !ok {
		t.Fatal("无 TTL 条目不应过期")
	}
}

func TestSweeperRemovesExpired(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test", SweepInterval: 20 * time.Millisecond})

	c.SetTTL("a", "1", 30*time.Millisecond)
	c.SetTTL("b", "2", 30*time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Stats().Size == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("清扫 goroutine 未在超时前清掉过期条目，Size = %d", c.Stats().Size)
}

func TestInvalidate(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test", DefaultTTL: time.Minute})

	c.Set("k", "v")
	c.Invalidate("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("Invalidate 后应 miss")
	}
	if got := c.Stats().Size; got != 0 {
		t.Fatalf("Invalidate 后 Size = %d; want 0", got)
	}

	// 重复删除不 panic、计数不为负
	c.Invalidate("k")
	if got := c.Stats().Size; got != 0 {
		t.Fatalf("重复 Invalidate 后 Size = %d; want 0", got)
	}
}

func TestInvalidatePrefix(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test", DefaultTTL: time.Minute})

	c.Set("auth:1", "a")
	c.Set("auth:2", "b")
	c.Set("model:1", "c")

	if removed := c.InvalidatePrefix("auth:"); removed != 2 {
		t.Fatalf("InvalidatePrefix(auth:) removed = %d; want 2", removed)
	}
	if _, ok := c.Get("auth:1"); ok {
		t.Fatal("auth:1 应被删除")
	}
	if _, ok := c.Get("model:1"); !ok {
		t.Fatal("model:1 不应受影响")
	}

	// 空前缀 = 清空整个命名空间
	if removed := c.InvalidatePrefix(""); removed != 1 {
		t.Fatalf("InvalidatePrefix(\"\") removed = %d; want 1", removed)
	}
	if got := c.Stats().Size; got != 0 {
		t.Fatalf("清空后 Size = %d; want 0", got)
	}
}

func TestMaxEntriesEviction(t *testing.T) {
	c := newTestCache[string](t, Options{Name: "test", MaxEntries: 3, DefaultTTL: time.Minute})

	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("k%d", i), "v")
		if got := c.Stats().Size; got > 3 {
			t.Fatalf("写入 k%d 后 Size = %d，超过容量上限 3", i, got)
		}
	}

	// 覆盖已有 key 不触发驱逐、不计新增
	c.Set("k9", "v2")
	if got := c.Stats().Size; got > 3 {
		t.Fatalf("覆盖写后 Size = %d，超过容量上限 3", got)
	}
}

func TestCloseIdempotent(t *testing.T) {
	c := New[string](Options{Name: "test"})
	c.Close()
	c.Close() // 不应 panic
}

func TestConcurrentAccess(t *testing.T) {
	c := newTestCache[int](t, Options{Name: "test", MaxEntries: 64, DefaultTTL: time.Minute})

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("k%d", (g*200+i)%100)
				switch i % 4 {
				case 0:
					c.Set(key, i)
				case 1:
					c.Get(key)
				case 2:
					c.Invalidate(key)
				case 3:
					c.InvalidatePrefix("k1")
				}
			}
		}(g)
	}
	wg.Wait()

	if got := c.Stats().Size; got < 0 || got > 64 {
		t.Fatalf("并发结束后 Size = %d，应在 [0,64] 区间", got)
	}
}
