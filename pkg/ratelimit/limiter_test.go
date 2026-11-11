package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// newClientForTest 创建指向 miniredis 的 go-redis 客户端
func newClientForTest(t *testing.T, addr string) *goredis.Client {
	t.Helper()
	cli := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { cli.Close() })
	return cli
}

// newTestLimiter 起一个 miniredis + 假时钟的 Limiter
func newTestLimiter(t *testing.T) (*Limiter, *miniredis.Miniredis, *time.Time) {
	t.Helper()
	s := miniredis.RunT(t)
	cli := newClientForTest(t, s.Addr())
	l := New(cli)
	now := time.UnixMilli(1700000000000)
	l.SetClock(func() time.Time { return now })
	return l, s, &now
}

func TestAllowRPSBucketDepleteAndRefill(t *testing.T) {
	l, _, now := newTestLimiter(t)
	ctx := context.Background()

	// rate=10：桶容量 10，前 10 次全放行
	for i := 0; i < 10; i++ {
		ok, _, err := l.AllowRPS(ctx, 1, 10)
		if err != nil || !ok {
			t.Fatalf("第 %d 次应放行: ok=%v err=%v", i+1, ok, err)
		}
	}

	// 第 11 次：桶空，拒绝且 retryAfter ≈ 100ms（补 1 个令牌的时间）
	ok, retry, err := l.AllowRPS(ctx, 1, 10)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if ok {
		t.Fatal("桶空应拒绝")
	}
	if retry < 90*time.Millisecond || retry > 110*time.Millisecond {
		t.Errorf("retryAfter = %v, want ≈100ms", retry)
	}

	// 时钟前进 500ms → 补 5 个令牌，可再放行 5 次
	*now = now.Add(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		ok, _, err = l.AllowRPS(ctx, 1, 10)
		if err != nil || !ok {
			t.Fatalf("补充后第 %d 次应放行: ok=%v err=%v", i+1, ok, err)
		}
	}
	if ok, _, _ = l.AllowRPS(ctx, 1, 10); ok {
		t.Fatal("补充的 5 个令牌用完后应再次拒绝")
	}
}

func TestAllowRPSUnlimited(t *testing.T) {
	l, _, _ := newTestLimiter(t)
	ok, _, err := l.AllowRPS(context.Background(), 1, 0)
	if err != nil || !ok {
		t.Fatalf("rate<=0 应放行: ok=%v err=%v", ok, err)
	}
}

func TestAllowRPSTenantIsolation(t *testing.T) {
	l, _, _ := newTestLimiter(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, _, _ = l.AllowRPS(ctx, 1, 10)
	}
	if ok, _, _ := l.AllowRPS(ctx, 1, 10); ok {
		t.Fatal("租户 1 应已耗尽")
	}
	// 租户 2 的桶独立，不受影响
	if ok, _, _ := l.AllowRPS(ctx, 2, 10); !ok {
		t.Fatal("租户 2 应有独立满桶")
	}
}

func TestAllowTPMSlidingWindow(t *testing.T) {
	l, _, now := newTestLimiter(t)
	ctx := context.Background()

	// limit=100：记 60 + 40 放行，再记 1 拒绝
	if ok, _, err := l.AllowTPM(ctx, 1, 100, 60, "a"); err != nil || !ok {
		t.Fatalf("60 tokens 应放行: ok=%v err=%v", ok, err)
	}
	if ok, _, err := l.AllowTPM(ctx, 1, 100, 40, "b"); err != nil || !ok {
		t.Fatalf("累计 100 应放行: ok=%v err=%v", ok, err)
	}
	ok, retry, err := l.AllowTPM(ctx, 1, 100, 1, "c")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if ok {
		t.Fatal("超窗口限额应拒绝")
	}
	// retryAfter = 最早条目（60s 窗口边界）滑出的等待时间
	if retry <= 0 || retry > 60*time.Second {
		t.Errorf("retryAfter = %v, want (0, 60s]", retry)
	}

	// 窗口滑过 61s 后旧条目失效，重新可记
	*now = now.Add(61 * time.Second)
	if ok, _, err = l.AllowTPM(ctx, 1, 100, 100, "d"); err != nil || !ok {
		t.Fatalf("窗口滑过后应放行: ok=%v err=%v", ok, err)
	}
}

func TestAllowTPMUniqMemberOverwrite(t *testing.T) {
	l, _, _ := newTestLimiter(t)
	ctx := context.Background()

	// 同一 uniq 重复记账：ZADD 覆盖同一 member，不重复累加
	for i := 0; i < 3; i++ {
		if ok, _, err := l.AllowTPM(ctx, 1, 100, 60, "same"); err != nil || !ok {
			t.Fatalf("第 %d 次应放行: ok=%v err=%v", i+1, ok, err)
		}
	}
	// 实际窗口内只有 60，还能再记 40
	if ok, _, err := l.AllowTPM(ctx, 1, 100, 40, "other"); err != nil || !ok {
		t.Fatalf("同 uniq 覆盖后应仍有 40 余量: ok=%v err=%v", ok, err)
	}
}

func TestAllowTPMZeroEstimation(t *testing.T) {
	l, _, _ := newTestLimiter(t)
	ok, _, err := l.AllowTPM(context.Background(), 1, 100, 0, "x")
	if err != nil || !ok {
		t.Fatalf("tokens<=0 应放行且不记账: ok=%v err=%v", ok, err)
	}
}

func TestConcurrencyAcquireRelease(t *testing.T) {
	l, s, _ := newTestLimiter(t)
	ctx := context.Background()

	// limit=2：占 2 放 1 再占
	for i := 0; i < 2; i++ {
		ok, err := l.AcquireConcurrency(ctx, 1, 2)
		if err != nil || !ok {
			t.Fatalf("第 %d 次占用应成功: ok=%v err=%v", i+1, ok, err)
		}
	}
	if ok, err := l.AcquireConcurrency(ctx, 1, 2); err != nil || ok {
		t.Fatalf("超限应立即拒绝: ok=%v err=%v", ok, err)
	}
	// 拒绝不占用额度：计数仍应为 2
	if got, err := s.Get("rl:conc:1"); err != nil || got != "2" {
		t.Errorf("拒绝后计数 = %q err=%v, want 2（已回退）", got, err)
	}
	// key 带 TTL（兜底防泄漏）
	if ttl := s.TTL("rl:conc:1"); ttl <= 0 || ttl > DefaultConcurrencyTTL {
		t.Errorf("TTL = %v, want (0, %v]", ttl, DefaultConcurrencyTTL)
	}

	// 释放一个后可再占
	if err := l.ReleaseConcurrency(ctx, 1); err != nil {
		t.Fatalf("释放失败: %v", err)
	}
	if ok, err := l.AcquireConcurrency(ctx, 1, 2); err != nil || !ok {
		t.Fatalf("释放后应可再占: ok=%v err=%v", ok, err)
	}

	// 全部释放后 key 删除
	_ = l.ReleaseConcurrency(ctx, 1)
	_ = l.ReleaseConcurrency(ctx, 1)
	if s.Exists("rl:conc:1") {
		t.Error("计数归零后 key 应删除")
	}
	// 对不存在的 key 释放不应产生负计数
	if err := l.ReleaseConcurrency(ctx, 9); err != nil {
		t.Fatalf("空 key 释放不应报错: %v", err)
	}
	if s.Exists("rl:conc:9") {
		t.Error("空 key 释放不应创建负计数 key")
	}
}

func TestEstimateTokens(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"空 body", "", 0},
		{"非 JSON", "not-json", 0},
		{"无 messages", `{"model":"x"}`, 0},
		{
			"字符串 content",
			`{"messages":[{"role":"user","content":"0123456789abcdef"}]}`,
			4 + 16/4, // overhead 4 + 16字符/4
		},
		{
			"多模态 content",
			`{"messages":[{"role":"user","content":[{"type":"text","text":"01234567"},{"type":"image_url","image_url":{"url":"http://x"}}]}]}`,
			4 + 8/4,
		},
		{
			"多条 messages 累加",
			`{"messages":[{"role":"system","content":"0123"},{"role":"user","content":"0123"}]}`,
			2 * (4 + 4/4), // 每条 overhead 4 + 4字符/4
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EstimateTokens([]byte(tc.body)); got != tc.want {
				t.Errorf("EstimateTokens = %d, want %d", got, tc.want)
			}
		})
	}
}
